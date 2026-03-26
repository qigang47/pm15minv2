from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import tempfile
import traceback
from typing import Any

import pandas as pd

from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.backtests.engine import run_research_backtest
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import BacktestRunSpec
from pm15min.research._contracts_runs import BacktestParitySpec
from pm15min.research.layout import slug_token


@dataclass(frozen=True)
class GroupedBacktestBundleSpec:
    market: str
    profile: str
    bundle_label: str
    target: str = "direction"
    spec_name: str = "baseline_truth"
    feature_set: str = "deep_otm_v1"
    label_set: str = "truth"
    model_family: str = "deep_otm"

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "GroupedBacktestBundleSpec":
        return cls(
            market=str(payload.get("market") or "").strip().lower(),
            profile=slug_token(str(payload.get("profile") or "deep_otm")),
            bundle_label=slug_token(str(payload.get("bundle_label") or "")),
            target=slug_token(str(payload.get("target") or "direction")),
            spec_name=slug_token(str(payload.get("spec_name") or payload.get("backtest_spec") or "baseline_truth")),
            feature_set=slug_token(str(payload.get("feature_set") or "deep_otm_v1")),
            label_set=slug_token(str(payload.get("label_set") or "truth")),
            model_family=slug_token(str(payload.get("model_family") or "deep_otm")),
        )


@dataclass(frozen=True)
class GroupedBacktestGridSpec:
    run_label: str
    cycle: str
    decision_start: str | None
    decision_end: str | None
    stake_usd_values: tuple[float, ...]
    max_trades_per_market_values: tuple[int | None, ...]
    bundles: tuple[GroupedBacktestBundleSpec, ...]
    parity: BacktestParitySpec

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "GroupedBacktestGridSpec":
        bundles_raw = payload.get("bundles") or ()
        if not isinstance(bundles_raw, list) or not bundles_raw:
            raise ValueError("Grouped backtest grid requires non-empty bundles list")
        return cls(
            run_label=slug_token(str(payload.get("run_label") or "planned")),
            cycle=slug_token(str(payload.get("cycle") or "15m")),
            decision_start=_coerce_optional_string(payload.get("decision_start")),
            decision_end=_coerce_optional_string(payload.get("decision_end")),
            stake_usd_values=_parse_float_seq(payload.get("stake_usd_values") or payload.get("stakes") or (1.0,)),
            max_trades_per_market_values=_parse_optional_int_seq(
                payload.get("max_trades_per_market_values") or payload.get("max_trades_values") or (None,)
            ),
            bundles=tuple(GroupedBacktestBundleSpec.from_mapping(item) for item in bundles_raw),
            parity=BacktestParitySpec.from_mapping(payload.get("parity")),
        )


@dataclass(frozen=True)
class GroupedBacktestCase:
    market: str
    profile: str
    target: str
    spec_name: str
    bundle_label: str
    feature_set: str
    label_set: str
    model_family: str
    cycle: str
    decision_start: str | None
    decision_end: str | None
    max_trades_per_market: int | None
    stake_usd: float
    group_label: str
    run_label: str


@dataclass(frozen=True)
class GroupedBacktestGroup:
    market: str
    profile: str
    target: str
    spec_name: str
    bundle_label: str
    feature_set: str
    label_set: str
    model_family: str
    cycle: str
    decision_start: str | None
    decision_end: str | None
    max_trades_per_market: int | None
    stake_usd_values: tuple[float, ...]
    group_label: str
    case_run_labels: tuple[str, ...]
    parity: BacktestParitySpec

    def to_payload(self) -> dict[str, object]:
        payload = asdict(self)
        payload["parity"] = self.parity.to_dict()
        return payload


def load_grouped_backtest_grid_spec(path: Path) -> GroupedBacktestGridSpec:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return GroupedBacktestGridSpec.from_mapping(payload)


def expand_grouped_backtest_groups(spec: GroupedBacktestGridSpec) -> tuple[GroupedBacktestGroup, ...]:
    groups: list[GroupedBacktestGroup] = []
    for bundle in spec.bundles:
        for max_trades in spec.max_trades_per_market_values:
            max_tag = _max_trades_tag(max_trades)
            group_label = f"{bundle.market}/{bundle.bundle_label}/{max_tag}"
            case_labels = tuple(
                _case_run_label(
                    parent_run_label=spec.run_label,
                    market=bundle.market,
                    bundle_label=bundle.bundle_label,
                    max_tag=max_tag,
                    stake_usd=stake_usd,
                )
                for stake_usd in spec.stake_usd_values
            )
            groups.append(
                GroupedBacktestGroup(
                    market=bundle.market,
                    profile=bundle.profile,
                    target=bundle.target,
                    spec_name=bundle.spec_name,
                    bundle_label=bundle.bundle_label,
                    feature_set=bundle.feature_set,
                    label_set=bundle.label_set,
                    model_family=bundle.model_family,
                    cycle=spec.cycle,
                    decision_start=spec.decision_start,
                    decision_end=spec.decision_end,
                    max_trades_per_market=max_trades,
                    stake_usd_values=tuple(spec.stake_usd_values),
                    group_label=group_label,
                    case_run_labels=case_labels,
                    parity=spec.parity,
                )
            )
    return tuple(groups)


def run_grouped_backtest_grid(
    *,
    spec: GroupedBacktestGridSpec,
    root: Path,
    group_workers: int,
    output_dir: Path | None = None,
) -> dict[str, object]:
    groups = expand_grouped_backtest_groups(spec)
    worker_count = min(max(1, int(group_workers)), max(1, len(groups)))
    run_dir = output_dir or (Path(root) / "v2" / "research" / "grid_backtests" / f"run={spec.run_label}")
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / "launcher.jsonl"
    results_path = run_dir / "results.parquet"
    summary_path = run_dir / "summary.json"

    all_rows: list[dict[str, object]] = []
    _append_log(
        log_path,
        {
            "event": "grid_started",
            "run_label": spec.run_label,
            "groups": len(groups),
            "group_workers": worker_count,
            "stakes": list(spec.stake_usd_values),
            "max_trades_per_market_values": [None if value is None else int(value) for value in spec.max_trades_per_market_values],
        },
    )

    completed = 0
    if worker_count <= 1:
        for group in groups:
            completed += 1
            payload = _run_group_worker(group.to_payload(), str(root))
            rows = list(payload.get("rows") or [])
            all_rows.extend(rows)
            _write_results(results_path, all_rows)
            _append_log(
                log_path,
                {
                    "event": "group_completed",
                    "completed_groups": completed,
                    "total_groups": len(groups),
                    "group_label": group.group_label,
                    "successes": int(sum(1 for row in rows if row.get("status") == "completed")),
                    "failures": int(sum(1 for row in rows if row.get("status") != "completed")),
                },
            )
    else:
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(_run_group_worker, group.to_payload(), str(root)): group
                for group in groups
            }
            for future in as_completed(future_map):
                group = future_map[future]
                completed += 1
                payload = future.result()
                rows = list(payload.get("rows") or [])
                all_rows.extend(rows)
                _write_results(results_path, all_rows)
                _append_log(
                    log_path,
                    {
                        "event": "group_completed",
                        "completed_groups": completed,
                        "total_groups": len(groups),
                        "group_label": group.group_label,
                        "successes": int(sum(1 for row in rows if row.get("status") == "completed")),
                        "failures": int(sum(1 for row in rows if row.get("status") != "completed")),
                    },
                )

    summary = {
        "run_label": spec.run_label,
        "groups": len(groups),
        "group_workers": worker_count,
        "completed_cases": int(sum(1 for row in all_rows if row.get("status") == "completed")),
        "failed_cases": int(sum(1 for row in all_rows if row.get("status") != "completed")),
        "results_path": str(results_path),
        "log_path": str(log_path),
        "run_dir": str(run_dir),
    }
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return summary


def _run_group_worker(group_payload: dict[str, object], root_text: str) -> dict[str, object]:
    os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "pm15min_mpl"))
    if "parity" in group_payload and not isinstance(group_payload.get("parity"), BacktestParitySpec):
        group_payload = dict(group_payload)
        group_payload["parity"] = BacktestParitySpec.from_mapping(group_payload.get("parity"))
    group = GroupedBacktestGroup(**group_payload)
    root = Path(root_text)
    cfg = ResearchConfig.build(
        market=group.market,
        cycle=group.cycle,
        profile=group.profile,
        source_surface="backtest",
        feature_set=group.feature_set,
        label_set=group.label_set,
        target=group.target,
        model_family=group.model_family,
        root=root / "v2",
    )
    rows: list[dict[str, object]] = []
    for stake_usd, run_label in zip(group.stake_usd_values, group.case_run_labels, strict=True):
        parity = group.parity.to_dict()
        if group.max_trades_per_market is not None:
            parity["regime_defense_max_trades_per_market"] = int(group.max_trades_per_market)
        try:
            summary = run_research_backtest(
                cfg,
                BacktestRunSpec(
                    profile=group.profile,
                    spec_name=group.spec_name,
                    run_label=run_label,
                    target=group.target,
                    decision_start=group.decision_start,
                    decision_end=group.decision_end,
                    bundle_label=group.bundle_label,
                    stake_usd=float(stake_usd),
                    parity=parity,
                ),
            )
            payload = json.loads(Path(str(summary["summary_path"])).read_text(encoding="utf-8"))
            rows.append(
                {
                    "status": "completed",
                    "group_label": group.group_label,
                    "market": group.market,
                    "profile": group.profile,
                    "bundle_label": group.bundle_label,
                    "max_trades_per_market": group.max_trades_per_market,
                    "stake_usd": float(stake_usd),
                    "run_label": run_label,
                    "summary_path": str(summary["summary_path"]),
                    "run_dir": str(summary["run_dir"]),
                    **payload,
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "status": "failed",
                    "group_label": group.group_label,
                    "market": group.market,
                    "profile": group.profile,
                    "bundle_label": group.bundle_label,
                    "max_trades_per_market": group.max_trades_per_market,
                    "stake_usd": float(stake_usd),
                    "run_label": run_label,
                    "error_type": exc.__class__.__name__,
                    "error_message": str(exc),
                    "traceback": traceback.format_exc(),
                }
            )
    return {"group_label": group.group_label, "rows": rows}


def _case_run_label(
    *,
    parent_run_label: str,
    market: str,
    bundle_label: str,
    max_tag: str,
    stake_usd: float,
) -> str:
    readable = f"{parent_run_label}-{market}-{bundle_label}-{max_tag}-usd{_float_label_token(stake_usd)}"
    digest = hashlib.sha1(readable.encode("utf-8")).hexdigest()[:8]
    return slug_token(f"{readable}-{digest}")


def _max_trades_tag(value: int | None) -> str:
    return "maxu" if value is None else f"max{int(value)}"


def _float_label_token(value: float) -> str:
    token = f"{float(value):f}".rstrip("0").rstrip(".")
    return (token or "0").replace("-", "neg_").replace(".", "p")


def _parse_float_seq(raw: object) -> tuple[float, ...]:
    values: list[object]
    if raw is None:
        return ()
    if isinstance(raw, str):
        values = [token.strip() for token in raw.split(",") if token.strip()]
    else:
        values = list(raw)  # type: ignore[arg-type]
    out: list[float] = []
    for value in values:
        numeric = float(value)
        if numeric not in out:
            out.append(numeric)
    return tuple(out)


def _parse_optional_int_seq(raw: object) -> tuple[int | None, ...]:
    values: list[object]
    if raw is None:
        return ()
    if isinstance(raw, str):
        values = [token.strip() for token in raw.split(",") if token.strip()]
    else:
        values = list(raw)  # type: ignore[arg-type]
    out: list[int | None] = []
    for value in values:
        normalized = _coerce_optional_int(value)
        if normalized not in out:
            out.append(normalized)
    return tuple(out)


def _coerce_optional_int(raw: object) -> int | None:
    if raw is None:
        return None
    token = str(raw).strip().lower()
    if token in {"", "none", "null", "u", "unlimited", "inf", "infinite"}:
        return None
    return int(token)


def _coerce_optional_string(raw: object) -> str | None:
    if raw is None:
        return None
    token = str(raw).strip()
    return token or None


def _append_log(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        handle.write("\n")


def _write_results(path: Path, rows: list[dict[str, object]]) -> None:
    frame = pd.DataFrame(rows)
    write_parquet_atomic(frame, path)


__all__ = [
    "GroupedBacktestBundleSpec",
    "GroupedBacktestCase",
    "GroupedBacktestGridSpec",
    "GroupedBacktestGroup",
    "expand_grouped_backtest_groups",
    "load_grouped_backtest_grid_spec",
    "run_grouped_backtest_grid",
]
