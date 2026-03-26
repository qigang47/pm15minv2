from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..layout import LiveStateLayout
from ..persistence import write_live_payload_pair


def persist_regime_state_snapshot(*, rewrite_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    layout = LiveStateLayout.discover(root=rewrite_root)
    latest_path = layout.latest_regime_path(
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        profile=str(payload["profile"]),
    )
    snapshot_path = layout.regime_snapshot_path(
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        profile=str(payload["profile"]),
        snapshot_ts=str(payload["snapshot_ts"]),
    )
    write_live_payload_pair(
        payload=payload,
        latest_path=latest_path,
        snapshot_path=snapshot_path,
        write_snapshot_history=False,
    )
    payload["latest_regime_path"] = str(latest_path)
    payload["regime_snapshot_path"] = str(snapshot_path)
    return payload


def load_latest_regime_state_snapshot(
    *,
    rewrite_root: Path,
    market: str,
    cycle: str,
    profile: str,
) -> dict[str, Any] | None:
    path = LiveStateLayout.discover(root=rewrite_root).latest_regime_path(
        market=market,
        cycle=cycle,
        profile=profile,
    )
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def summarize_regime_state(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    return {
        "snapshot_ts": payload.get("snapshot_ts"),
        "status": payload.get("status"),
        "reason": payload.get("reason"),
        "enabled": bool(payload.get("enabled", False)),
        "state": payload.get("state"),
        "target_state": payload.get("target_state"),
        "pressure": payload.get("pressure"),
        "reason_codes": list(payload.get("reason_codes") or []),
        "min_liquidity_ratio": payload.get("min_liquidity_ratio"),
        "soft_fail_count": payload.get("soft_fail_count"),
        "hard_fail_count": payload.get("hard_fail_count"),
        "ret_15m": payload.get("ret_15m"),
        "ret_30m": payload.get("ret_30m"),
        "pending_target": payload.get("pending_target"),
        "pending_count": payload.get("pending_count"),
        "guard_hints": dict(payload.get("guard_hints") or {}),
    }
