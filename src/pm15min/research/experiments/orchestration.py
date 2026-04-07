from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from pm15min.research._contracts_training import offset_weight_overrides_payload

from .specs import ExperimentRuntimePolicy


@dataclass(frozen=True)
class PlannedExperimentCaseAction:
    case_key: str
    action: str
    training_row: dict[str, object] | None = None
    backtest_row: dict[str, object] | None = None
    failed_row: dict[str, object] | None = None


@dataclass(frozen=True)
class ExperimentExecutionGroup:
    group_key: str
    group_label: str
    market_specs: tuple[object, ...]


@dataclass
class ExperimentRunState:
    training_rows_by_case_key: dict[str, dict[str, object]] = field(default_factory=dict)
    backtest_rows_by_case_key: dict[str, dict[str, object]] = field(default_factory=dict)
    failed_rows_by_case_key: dict[str, dict[str, object]] = field(default_factory=dict)

    @classmethod
    def load(cls, run_dir: Path) -> "ExperimentRunState":
        training_rows, backtest_rows = _load_existing_case_rows(run_dir)
        failed_rows = _load_existing_failed_rows(run_dir)
        return cls(
            training_rows_by_case_key=training_rows,
            backtest_rows_by_case_key=backtest_rows,
            failed_rows_by_case_key=failed_rows,
        )

    def plan_case(
        self,
        *,
        market_spec,
        case_key: str,
        runtime_policy: ExperimentRuntimePolicy,
    ) -> PlannedExperimentCaseAction:
        existing_backtest = self.backtest_rows_by_case_key.get(case_key)
        if existing_backtest is not None and runtime_policy.completed_cases == "resume":
            existing_training = self.training_rows_by_case_key.get(case_key)
            training_row = (
                dict(existing_training)
                if existing_training is not None
                else build_resumed_training_row(market_spec, case_key=case_key, backtest_row=existing_backtest)
            )
            training_row["resumed_from_existing"] = True
            backtest_row = dict(existing_backtest)
            backtest_row["resumed_from_existing"] = True
            return PlannedExperimentCaseAction(
                case_key=case_key,
                action="resume_existing",
                training_row=training_row,
                backtest_row=backtest_row,
            )

        existing_failed = self.failed_rows_by_case_key.get(case_key)
        if existing_failed is not None and existing_backtest is None and runtime_policy.failed_cases == "skip":
            return PlannedExperimentCaseAction(
                case_key=case_key,
                action="retain_failed",
                training_row=_copy_row(self.training_rows_by_case_key.get(case_key)),
                failed_row=dict(existing_failed),
            )

        return PlannedExperimentCaseAction(case_key=case_key, action="execute")

    def apply_planned_case(self, plan: PlannedExperimentCaseAction) -> None:
        if plan.action == "resume_existing":
            if plan.training_row is not None:
                self.training_rows_by_case_key[plan.case_key] = dict(plan.training_row)
            if plan.backtest_row is not None:
                self.backtest_rows_by_case_key[plan.case_key] = dict(plan.backtest_row)
            self.failed_rows_by_case_key.pop(plan.case_key, None)
            return
        if plan.action == "retain_failed":
            if plan.training_row is not None:
                self.training_rows_by_case_key[plan.case_key] = dict(plan.training_row)
            if plan.failed_row is not None:
                self.failed_rows_by_case_key[plan.case_key] = dict(plan.failed_row)
            self.backtest_rows_by_case_key.pop(plan.case_key, None)

    def set_training_row(self, row: dict[str, object]) -> None:
        case_key = str(row.get("case_key") or "").strip()
        if not case_key:
            return
        self.training_rows_by_case_key[case_key] = dict(row)

    def set_backtest_row(self, row: dict[str, object]) -> None:
        case_key = str(row.get("case_key") or "").strip()
        if not case_key:
            return
        self.backtest_rows_by_case_key[case_key] = dict(row)

    def set_failed_row(self, row: dict[str, object]) -> None:
        case_key = str(row.get("case_key") or "").strip()
        if not case_key:
            return
        self.failed_rows_by_case_key[case_key] = dict(row)

    def drop_failed_case(self, *, case_key: str) -> None:
        token = str(case_key or "").strip()
        if token:
            self.failed_rows_by_case_key.pop(token, None)

    def training_rows(self) -> list[dict[str, object]]:
        return list(self.training_rows_by_case_key.values())

    def backtest_rows(self) -> list[dict[str, object]]:
        return list(self.backtest_rows_by_case_key.values())

    def failed_rows(self) -> list[dict[str, object]]:
        return list(self.failed_rows_by_case_key.values())


def build_execution_groups(market_specs) -> tuple[ExperimentExecutionGroup, ...]:
    groups: dict[str, list[object]] = {}
    labels: dict[str, str] = {}
    ordered = sorted(market_specs, key=_execution_group_sort_key)
    for market_spec in ordered:
        group_key = _execution_group_key(market_spec)
        groups.setdefault(group_key, []).append(market_spec)
        labels.setdefault(group_key, _execution_group_label(market_spec))
    return tuple(
        ExperimentExecutionGroup(
            group_key=group_key,
            group_label=labels[group_key],
            market_specs=tuple(items),
        )
        for group_key, items in groups.items()
    )


def build_case_row_prefix(market_spec, *, case_key: str) -> dict[str, object]:
    return {
        "case_key": case_key,
        "market": market_spec.market,
        "group_name": _group_name(market_spec),
        "run_name": _run_name(market_spec),
        "matrix_parent_run_name": _matrix_parent_run_name(market_spec),
        "matrix_stake_label": _matrix_stake_label(market_spec),
        "profile": market_spec.profile,
        "target": market_spec.target,
        "variant_label": market_spec.variant_label,
        "variant_notes": market_spec.variant_notes,
        "tags_json": _json_text(list(_tags(market_spec))),
        "stake_usd": _stake_usd(market_spec),
        "max_notional_usd": _max_notional_usd(market_spec),
        "weight_variant_label": str(getattr(market_spec, "weight_variant_label", "default") or "default"),
        "balance_classes": getattr(market_spec, "balance_classes", None),
        "weight_by_vol": getattr(market_spec, "weight_by_vol", None),
        "inverse_vol": getattr(market_spec, "inverse_vol", None),
        "contrarian_weight": getattr(market_spec, "contrarian_weight", None),
        "contrarian_quantile": getattr(market_spec, "contrarian_quantile", None),
        "contrarian_return_col": getattr(market_spec, "contrarian_return_col", None),
        "offset_weight_overrides_json": _json_text(
            offset_weight_overrides_payload(getattr(market_spec, "offset_weight_overrides", None))
        ),
    }


def build_resumed_training_row(market_spec, *, case_key: str, backtest_row: dict[str, object]) -> dict[str, object]:
    return {
        **build_case_row_prefix(market_spec, case_key=case_key),
        "feature_set": market_spec.feature_set,
        "label_set": market_spec.label_set,
        "model_family": market_spec.model_family,
        "window": market_spec.window.label,
        "offsets": list(market_spec.offsets),
        "training_run_dir": None,
        "bundle_dir": backtest_row.get("bundle_dir"),
        "training_reused": False,
        "bundle_reused": False,
        "resumed_from_existing": True,
        "secondary_target": market_spec.hybrid_secondary_target,
        "secondary_training_run_dir": None,
        "secondary_bundle_dir": backtest_row.get("secondary_bundle_dir"),
        "secondary_training_reused": False,
        "secondary_bundle_reused": False,
        "parity_spec_json": _json_text(market_spec.parity.to_dict()),
    }


def build_failed_case_row(
    market_spec,
    *,
    case_key: str,
    failure_stage: str,
    error: Exception,
    train_summary: dict[str, object] | None,
    bundle_summary: dict[str, object] | None,
    secondary_train_summary: dict[str, object] | None,
    secondary_bundle_summary: dict[str, object] | None,
) -> dict[str, object]:
    return {
        **build_case_row_prefix(market_spec, case_key=case_key),
        "training_run_dir": None if train_summary is None else train_summary.get("run_dir"),
        "bundle_dir": None if bundle_summary is None else bundle_summary.get("bundle_dir"),
        "secondary_training_run_dir": None if secondary_train_summary is None else secondary_train_summary.get("run_dir"),
        "secondary_bundle_dir": None if secondary_bundle_summary is None else secondary_bundle_summary.get("bundle_dir"),
        "resumed_from_existing": False,
        "status": "failed",
        "failure_stage": str(failure_stage or "unknown"),
        "error_type": error.__class__.__name__,
        "error_message": str(error),
    }


def _load_existing_case_rows(run_dir: Path) -> tuple[dict[str, dict[str, object]], dict[str, dict[str, object]]]:
    training_rows = _rows_by_case_key(_read_parquet_or_empty(run_dir / "training_runs.parquet"))
    backtest_rows = {
        case_key: row
        for case_key, row in _rows_by_case_key(_read_parquet_or_empty(run_dir / "backtest_runs.parquet")).items()
        if _summary_exists(row.get("summary_path"))
    }
    return training_rows, backtest_rows


def _load_existing_failed_rows(run_dir: Path) -> dict[str, dict[str, object]]:
    return _rows_by_case_key(_read_parquet_or_empty(run_dir / "failed_cases.parquet"))


def _read_parquet_or_empty(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def _rows_by_case_key(frame: pd.DataFrame) -> dict[str, dict[str, object]]:
    if frame.empty or "case_key" not in frame.columns:
        return {}
    rows: dict[str, dict[str, object]] = {}
    for row in frame.to_dict(orient="records"):
        case_key = str(row.get("case_key") or "").strip()
        if case_key:
            rows[case_key] = row
    return rows


def _summary_exists(path_value: object) -> bool:
    token = str(path_value or "").strip()
    return bool(token) and Path(token).exists()


def _copy_row(row: dict[str, object] | None) -> dict[str, object] | None:
    return None if row is None else dict(row)


def _group_name(market_spec) -> str:
    return str(getattr(market_spec, "group_name", "") or "")


def _run_name(market_spec) -> str:
    return str(getattr(market_spec, "run_name", "") or "")


def _matrix_parent_run_name(market_spec) -> str:
    return str(getattr(market_spec, "matrix_parent_run_name", "") or "")


def _matrix_stake_label(market_spec) -> str:
    return str(getattr(market_spec, "matrix_stake_label", "") or "")


def _stake_usd(market_spec) -> float | None:
    raw = getattr(market_spec, "stake_usd", None)
    return None if raw in {None, ""} else float(raw)


def _max_notional_usd(market_spec) -> float | None:
    raw = getattr(market_spec, "max_notional_usd", None)
    return None if raw in {None, ""} else float(raw)


def _tags(market_spec) -> tuple[str, ...]:
    raw = getattr(market_spec, "tags", ()) or ()
    return tuple(str(tag) for tag in raw if str(tag))


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _execution_group_key(market_spec) -> str:
    payload = {
        "market": str(getattr(market_spec, "market", "") or ""),
        "group_name": _group_name(market_spec),
        "run_name": _matrix_parent_run_name(market_spec) or _run_name(market_spec),
        "profile": str(getattr(market_spec, "profile", "") or ""),
        "model_family": str(getattr(market_spec, "model_family", "") or ""),
        "feature_set": str(getattr(market_spec, "feature_set", "") or ""),
        "label_set": str(getattr(market_spec, "label_set", "") or ""),
        "target": str(getattr(market_spec, "target", "") or ""),
        "offsets": [int(value) for value in (getattr(market_spec, "offsets", ()) or ())],
        "window": str(getattr(getattr(market_spec, "window", None), "label", "") or ""),
        "backtest_spec": str(getattr(market_spec, "backtest_spec", "") or ""),
        "variant_label": str(getattr(market_spec, "variant_label", "") or ""),
        "variant_notes": str(getattr(market_spec, "variant_notes", "") or ""),
        "hybrid_secondary_target": getattr(market_spec, "hybrid_secondary_target", None),
        "hybrid_secondary_offsets": None
        if getattr(market_spec, "hybrid_secondary_offsets", None) is None
        else [int(value) for value in getattr(market_spec, "hybrid_secondary_offsets", ())],
        "hybrid_fallback_reasons": [str(value) for value in getattr(market_spec, "hybrid_fallback_reasons", ()) or ()],
        "parity": getattr(market_spec, "parity", None).to_dict() if getattr(market_spec, "parity", None) is not None else {},
        "max_notional_usd": _max_notional_usd(market_spec),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:20]


def _execution_group_label(market_spec) -> str:
    tokens = [str(getattr(market_spec, "market", "") or "")]
    group_name = _group_name(market_spec)
    run_name = _matrix_parent_run_name(market_spec) or _run_name(market_spec)
    variant_label = str(getattr(market_spec, "variant_label", "") or "")
    if group_name:
        tokens.append(group_name)
    if run_name:
        tokens.append(run_name)
    if variant_label and variant_label != "default":
        tokens.append(variant_label)
    return "/".join(token for token in tokens if token)


def _execution_group_sort_key(market_spec) -> tuple[object, ...]:
    return (
        str(getattr(market_spec, "market", "") or ""),
        _group_name(market_spec),
        _matrix_parent_run_name(market_spec) or _run_name(market_spec),
        str(getattr(market_spec, "variant_label", "") or ""),
        str(getattr(market_spec, "target", "") or ""),
        -1.0 if _stake_usd(market_spec) is None else float(_stake_usd(market_spec)),
        -1.0 if _max_notional_usd(market_spec) is None else float(_max_notional_usd(market_spec)),
        _run_name(market_spec),
    )
