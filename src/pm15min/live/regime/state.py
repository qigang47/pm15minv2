from __future__ import annotations

from typing import Any

import pandas as pd

from ..liquidity import load_latest_liquidity_state_snapshot, summarize_liquidity_state
from ..profiles import resolve_live_profile_spec
from .controller import (
    PRESSURE_NEUTRAL,
    REGIME_NORMAL,
    RegimeController,
    latest_regime_returns,
    resolve_checked_at,
    seed_regime_controller,
)
from .persistence import (
    load_latest_regime_state_snapshot,
    persist_regime_state_snapshot,
)


def build_regime_state_snapshot(
    cfg,
    *,
    features: pd.DataFrame | None = None,
    liquidity_payload: dict[str, Any] | None = None,
    persist: bool = True,
    now: pd.Timestamp | None = None,
    utc_snapshot_label_fn,
) -> dict[str, Any]:
    cycle = f"{int(cfg.cycle_minutes)}m"
    spec = resolve_live_profile_spec(cfg.profile)
    snapshot_ts = utc_snapshot_label_fn()
    previous = load_latest_regime_state_snapshot(
        rewrite_root=cfg.layout.rewrite.root,
        market=cfg.asset.slug,
        cycle=cycle,
        profile=cfg.profile,
    )
    checked_at = resolve_checked_at(features=features, now=now)
    liquidity_state = summarize_liquidity_state(liquidity_payload)
    if liquidity_state is None:
        liquidity_state = summarize_liquidity_state(
            load_latest_liquidity_state_snapshot(
                rewrite_root=cfg.layout.rewrite.root,
                market=cfg.asset.slug,
                cycle=cycle,
                profile=cfg.profile,
            )
        )

    payload = {
        "domain": "live",
        "dataset": "live_regime_state",
        "snapshot_ts": snapshot_ts,
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": cycle,
        "enabled": bool(spec.regime_controller_enabled),
        "status": "ok",
        "reason": None,
        "checked_at": checked_at.isoformat(),
        "state": REGIME_NORMAL,
        "target_state": REGIME_NORMAL,
        "pressure": PRESSURE_NEUTRAL,
        "reason_codes": [],
        "min_liquidity_ratio": 1.0,
        "soft_fail_count": 0,
        "hard_fail_count": 0,
        "ret_15m": None,
        "ret_30m": None,
        "pending_target": REGIME_NORMAL,
        "pending_count": 0,
        "liquidity_state_snapshot_ts": None if liquidity_state is None else liquidity_state.get("snapshot_ts"),
        "liquidity_state_status": None if liquidity_state is None else liquidity_state.get("status"),
        "liquidity_state_reason": None if liquidity_state is None else liquidity_state.get("reason"),
        "liquidity_state_blocked": False if liquidity_state is None else bool(liquidity_state.get("blocked", False)),
        "liquidity_reason_codes": [] if liquidity_state is None else list(liquidity_state.get("reason_codes") or []),
        "guard_hints": {
            "min_dir_prob_boost": 0.0,
            "disabled_offsets": [],
            "defense_force_with_pressure": bool(spec.regime_defense_force_with_pressure),
            "defense_max_trades_per_market": int(spec.regime_defense_max_trades_per_market),
        },
        "source_of_truth": {
            "liquidity_state_available": bool(liquidity_state is not None),
            "feature_returns_available": False,
        },
    }
    if not spec.regime_controller_enabled:
        payload["reason"] = "regime_controller_disabled"
        payload["reason_codes"] = ["disabled"]
        return persist_regime_state_snapshot(rewrite_root=cfg.layout.rewrite.root, payload=payload) if persist else payload

    ret_15m, ret_30m = latest_regime_returns(features, cycle=cycle)
    controller = RegimeController(
        caution_min_liquidity_ratio=float(spec.regime_caution_min_liquidity_ratio),
        defense_min_liquidity_ratio=float(spec.regime_defense_min_liquidity_ratio),
        caution_soft_fail_count=int(spec.regime_caution_soft_fail_count),
        defense_soft_fail_count=int(spec.regime_defense_soft_fail_count),
        switch_confirmations=int(spec.regime_switch_confirmations),
        recover_confirmations=int(spec.regime_recover_confirmations),
        up_pressure_ret_15m=float(spec.regime_up_pressure_ret_15m),
        up_pressure_ret_30m=float(spec.regime_up_pressure_ret_30m),
        down_pressure_ret_15m=float(spec.regime_down_pressure_ret_15m),
        down_pressure_ret_30m=float(spec.regime_down_pressure_ret_30m),
    )
    seed_regime_controller(controller=controller, previous_payload=previous)
    liquidity_metrics = None if liquidity_state is None else dict(liquidity_state.get("metrics") or {})
    snapshot = controller.evaluate(
        now=checked_at,
        liquidity_metrics=liquidity_metrics,
        liquidity_blocked=False if liquidity_state is None else bool(liquidity_state.get("blocked", False)),
        ret_15m=ret_15m,
        ret_30m=ret_30m,
    )
    payload.update(
        {
            "reason": "regime_state_built",
            "state": snapshot["state"],
            "target_state": snapshot["target_state"],
            "pressure": snapshot["pressure"],
            "reason_codes": list(snapshot["reason_codes"]),
            "min_liquidity_ratio": float(snapshot["min_liquidity_ratio"]),
            "soft_fail_count": int(snapshot["soft_fail_count"]),
            "hard_fail_count": int(snapshot["hard_fail_count"]),
            "ret_15m": snapshot["ret_15m"],
            "ret_30m": snapshot["ret_30m"],
            "pending_target": snapshot["pending_target"],
            "pending_count": int(snapshot["pending_count"]),
            "guard_hints": {
                "min_dir_prob_boost": float(spec.regime_min_dir_prob_boost_for(snapshot["state"])),
                "disabled_offsets": list(spec.regime_disabled_offsets_for(snapshot["state"])),
                "defense_force_with_pressure": bool(spec.regime_defense_force_with_pressure),
                "defense_max_trades_per_market": int(spec.regime_defense_max_trades_per_market),
            },
            "source_of_truth": {
                "liquidity_state_available": bool(liquidity_state is not None),
                "feature_returns_available": ret_15m is not None or ret_30m is not None,
            },
        }
    )
    if persist:
        return persist_regime_state_snapshot(rewrite_root=cfg.layout.rewrite.root, payload=payload)
    return payload
