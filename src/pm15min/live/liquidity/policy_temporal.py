from __future__ import annotations

from typing import Any

import pandas as pd


def can_reuse_previous(*, previous_payload: dict[str, Any] | None, now: pd.Timestamp, refresh_seconds: float) -> bool:
    if not isinstance(previous_payload, dict):
        return False
    checked_at = pd.to_datetime(previous_payload.get("checked_at"), utc=True, errors="coerce")
    if checked_at is None or pd.isna(checked_at):
        return False
    age_seconds = (now - checked_at).total_seconds()
    return age_seconds >= 0.0 and age_seconds < max(0.0, float(refresh_seconds))


def apply_temporal_filter(
    *,
    raw_result: dict[str, Any],
    previous_payload: dict[str, Any] | None,
    min_failed_checks: int,
    min_recovered_checks: int,
    block_on_degrade: bool,
) -> dict[str, Any]:
    prev_temporal = previous_payload.get("temporal_state") if isinstance(previous_payload, dict) else {}
    raw_fail_streak = int((prev_temporal or {}).get("raw_fail_streak") or 0)
    raw_pass_streak = int((prev_temporal or {}).get("raw_pass_streak") or 0)
    blocked_state = bool((prev_temporal or {}).get("blocked_state", False))
    metrics = dict(raw_result.get("metrics") or {})

    if bool(raw_result.get("ok")):
        raw_pass_streak += 1
        raw_fail_streak = 0
        metrics["raw_fail_streak"] = float(raw_fail_streak)
        metrics["raw_pass_streak"] = float(raw_pass_streak)
        if blocked_state and raw_pass_streak < max(1, int(min_recovered_checks)):
            return {
                "ok": False,
                "blocked": bool(block_on_degrade),
                "reason_codes": ["recovering_pending"] + list(raw_result.get("reason_codes") or []),
                "metrics": metrics,
                "error": raw_result.get("error"),
                "temporal_state": {
                    "raw_fail_streak": raw_fail_streak,
                    "raw_pass_streak": raw_pass_streak,
                    "blocked_state": True,
                    "previous_snapshot_ts": None if previous_payload is None else previous_payload.get("snapshot_ts"),
                },
            }
        return {
            "ok": True,
            "blocked": False,
            "reason_codes": list(raw_result.get("reason_codes") or []),
            "metrics": metrics,
            "error": raw_result.get("error"),
            "temporal_state": {
                "raw_fail_streak": raw_fail_streak,
                "raw_pass_streak": raw_pass_streak,
                "blocked_state": False,
                "previous_snapshot_ts": None if previous_payload is None else previous_payload.get("snapshot_ts"),
            },
        }

    raw_fail_streak += 1
    raw_pass_streak = 0
    metrics["raw_fail_streak"] = float(raw_fail_streak)
    metrics["raw_pass_streak"] = float(raw_pass_streak)
    if raw_fail_streak < max(1, int(min_failed_checks)):
        return {
            "ok": True,
            "blocked": False,
            "reason_codes": ["filtered_pending"] + list(raw_result.get("reason_codes") or []),
            "metrics": metrics,
            "error": raw_result.get("error"),
            "temporal_state": {
                "raw_fail_streak": raw_fail_streak,
                "raw_pass_streak": raw_pass_streak,
                "blocked_state": blocked_state,
                "previous_snapshot_ts": None if previous_payload is None else previous_payload.get("snapshot_ts"),
            },
        }
    return {
        "ok": False,
        "blocked": bool(block_on_degrade),
        "reason_codes": list(raw_result.get("reason_codes") or []),
        "metrics": metrics,
        "error": raw_result.get("error"),
        "temporal_state": {
            "raw_fail_streak": raw_fail_streak,
            "raw_pass_streak": raw_pass_streak,
            "blocked_state": True,
            "previous_snapshot_ts": None if previous_payload is None else previous_payload.get("snapshot_ts"),
        },
    }
