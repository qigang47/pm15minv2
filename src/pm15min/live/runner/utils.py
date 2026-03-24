from __future__ import annotations

from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_side_effect_error_payload(*, stage: str, exc: Exception) -> dict[str, object]:
    return {
        "status": "error",
        "reason": f"{stage}_exception",
        "error_type": type(exc).__name__,
        "error": str(exc),
    }


def build_account_state_error_payload(*, stage: str, exc: Exception) -> dict[str, object]:
    reason = f"{stage}_exception"
    detail = f"{type(exc).__name__}: {exc}"
    return {
        "status": "error",
        "reason": reason,
        "snapshot_ts": None,
        "open_orders": {
            "status": "error",
            "reason": reason,
            "detail": detail,
        },
        "positions": {
            "status": "error",
            "reason": reason,
            "detail": detail,
        },
    }
