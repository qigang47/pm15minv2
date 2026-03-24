from __future__ import annotations

import json
from pathlib import Path


def float_or_none(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def int_or_none(value: object) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def ratio_or_none(numerator: object, denominator: object) -> float | None:
    numerator_value = float_or_none(numerator)
    denominator_value = float_or_none(denominator)
    if numerator_value is None or denominator_value in (None, 0.0):
        return None
    return float(numerator_value) / float(denominator_value)


def summarize_live_risk_alerts(*, alerts: list[object]) -> dict[str, object]:
    counts = {
        "critical": 0,
        "warning": 0,
        "info": 0,
        "other": 0,
        "total": 0,
    }
    highest = "none"
    highest_rank = -1
    rank = {"critical": 3, "warning": 2, "info": 1, "other": 0}
    for row in alerts:
        if not isinstance(row, dict):
            continue
        severity = str(row.get("severity") or "other").strip().lower() or "other"
        bucket = severity if severity in {"critical", "warning", "info"} else "other"
        counts[bucket] += 1
        counts["total"] += 1
        severity_rank = rank.get(bucket, 0)
        if severity_rank > highest_rank:
            highest_rank = severity_rank
            highest = bucket
    return {
        "counts": counts,
        "highest_severity": highest,
        "has_critical": counts["critical"] > 0,
    }


def read_json_path(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None
