from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pm15min.research.features.registry import feature_group, feature_set_columns


def build_attempt_record(
    project_root: Path,
    run_payload: dict[str, Any],
    *,
    track: str | None,
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    suite_name = str(run_payload.get("suite_name") or "").strip()
    top_case = run_payload.get("top_case") if isinstance(run_payload.get("top_case"), dict) else {}
    suite_payload = _read_suite_spec(root, suite_name)
    feature_sets = _extract_feature_sets(suite_payload)
    top_feature_set = str(top_case.get("feature_set") or "").strip()
    if top_feature_set and top_feature_set not in feature_sets:
        feature_sets.insert(0, top_feature_set)
    registry = _read_custom_feature_sets(root)
    widths = sorted(
        {
            int(registry[name]["width"])
            for name in feature_sets
            if name in registry and _is_int_like(registry[name].get("width"))
        }
    )
    family_signature = _factor_family_signature(root, feature_sets)
    model_families = sorted(set(_extract_model_families(suite_payload)))
    density = top_case.get("density_bottleneck") if isinstance(top_case.get("density_bottleneck"), dict) else {}
    trades = _int_or_none(top_case.get("trades") or top_case.get("trade_rows"))
    captures = _int_or_none(top_case.get("profitable_pool_capture_rows"))
    outcome = _classify_outcome(trades=trades, captures=captures)
    return {
        "market": _infer_market(run_payload=run_payload, top_case=top_case, suite_payload=suite_payload),
        "track": str(track or "").strip().lower(),
        "suite_name": suite_name,
        "run_label": str(run_payload.get("run_label") or "").strip(),
        "train_end": str(run_payload.get("train_end") or "").strip(),
        "decision_start": str(run_payload.get("decision_start") or "").strip(),
        "decision_end": str(run_payload.get("decision_end") or "").strip(),
        "feature_sets": feature_sets,
        "widths": widths,
        "factor_families": [item.split(":", 1)[0] for item in family_signature],
        "factor_family_signature": family_signature,
        "model_families": model_families,
        "trades": trades,
        "captures": captures,
        "primary_bottleneck": str(density.get("primary_bottleneck") or "").strip(),
        "recommended_route": str(density.get("recommended_route") or "").strip(),
        "outcome": outcome,
    }


def format_attempt_record_line(attempt: dict[str, Any]) -> str:
    return (
        f"{attempt.get('market')}: suite={attempt.get('suite_name')} / "
        f"run={attempt.get('run_label')} / outcome={attempt.get('outcome')} / "
        f"trades={attempt.get('trades')} / captures={attempt.get('captures')} / "
        f"widths={','.join(str(v) for v in attempt.get('widths') or []) or 'unknown'} / "
        f"models={','.join(str(v) for v in attempt.get('model_families') or []) or 'unknown'} / "
        f"families={','.join(str(v) for v in attempt.get('factor_family_signature') or []) or 'unknown'} / "
        f"features={','.join(str(v) for v in (attempt.get('feature_sets') or [])[:3]) or 'unknown'} / "
        f"bottleneck={attempt.get('primary_bottleneck') or 'unknown'}"
    )


def _read_suite_spec(root: Path, suite_name: str) -> dict[str, Any]:
    path = root / "research" / "experiments" / "suite_specs" / f"{suite_name}.json"
    if not suite_name or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _read_custom_feature_sets(root: Path) -> dict[str, dict[str, Any]]:
    path = root / "research" / "experiments" / "custom_feature_sets.json"
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {str(key): value for key, value in payload.items() if isinstance(value, dict)} if isinstance(payload, dict) else {}


def _extract_feature_sets(payload: dict[str, Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            token = str(value.get("feature_set") or "").strip()
            if token and token not in seen:
                seen.add(token)
                out.append(token)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    return out


def _infer_market(
    *,
    run_payload: dict[str, Any],
    top_case: dict[str, Any],
    suite_payload: dict[str, Any],
) -> str:
    for value in (
        run_payload.get("market"),
        top_case.get("market"),
    ):
        token = str(value or "").strip().lower()
        if token:
            return token
    markets = suite_payload.get("markets")
    if isinstance(markets, dict) and len(markets) == 1:
        return next(iter(markets.keys())).strip().lower()
    if isinstance(markets, list):
        normalized = [
            str((item.get("market") if isinstance(item, dict) else item) or "").strip().lower()
            for item in markets
        ]
        normalized = [item for item in normalized if item]
        if len(set(normalized)) == 1:
            return normalized[0]
    return ""


def _extract_model_families(payload: dict[str, Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            token = str(value.get("model_family") or "").strip()
            if token and token not in seen:
                seen.add(token)
                out.append(token)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    return out or ["deep_otm"]


def _factor_family_signature(root: Path, feature_sets: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    for feature_set in feature_sets:
        try:
            columns = feature_set_columns(feature_set, root=root)
        except Exception:
            continue
        for column in columns:
            family = feature_group(str(column)) or "unknown"
            counts[family] = counts.get(family, 0) + 1
    return [
        f"{family}:{count}"
        for family, count in sorted(counts.items(), key=lambda item: (item[0], item[1]))
        if count > 0
    ]


def _classify_outcome(*, trades: int | None, captures: int | None) -> str:
    if trades is None:
        return "unknown"
    if trades < 56:
        return "sparse"
    if captures is not None and captures <= 0:
        return "no_capture"
    return "candidate"


def _is_int_like(value: Any) -> bool:
    try:
        int(value)
        return True
    except (TypeError, ValueError):
        return False


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
