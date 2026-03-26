from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from ..layout import LiveStateLayout
from .fetch import PERP_FALLBACK_BASES, SPOT_FALLBACK_BASES
from ..persistence import write_live_payload_pair
from ..profiles import resolve_live_profile_spec


def build_liquidity_state_snapshot(
    cfg,
    *,
    persist: bool = True,
    force_refresh: bool = False,
    now: pd.Timestamp | None = None,
    utc_snapshot_label_fn,
    normalize_now_fn,
    load_latest_liquidity_state_snapshot_fn,
    can_reuse_previous_fn,
    liquidity_thresholds_fn,
    evaluate_liquidity_raw_fn,
    apply_temporal_filter_fn,
    persist_liquidity_state_snapshot_fn,
) -> dict[str, Any]:
    cycle = f"{int(cfg.cycle_minutes)}m"
    spec = resolve_live_profile_spec(cfg.profile)
    eval_now = normalize_now_fn(now)
    previous = load_latest_liquidity_state_snapshot_fn(
        rewrite_root=cfg.layout.rewrite.root,
        market=cfg.asset.slug,
        cycle=cycle,
        profile=cfg.profile,
    )
    if not force_refresh and can_reuse_previous_fn(
        previous_payload=previous,
        now=eval_now,
        refresh_seconds=spec.liquidity_guard_refresh_seconds,
    ):
        return previous

    snapshot_ts = utc_snapshot_label_fn()
    thresholds = liquidity_thresholds_fn(spec=spec, market=cfg.asset.slug)
    payload = {
        "domain": "live",
        "dataset": "live_liquidity_state",
        "snapshot_ts": snapshot_ts,
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": cycle,
        "guard_enabled": bool(spec.liquidity_guard_enabled),
        "block_on_degrade": bool(spec.liquidity_guard_block),
        "fail_open": bool(spec.liquidity_guard_fail_open),
        "refresh_seconds": float(spec.liquidity_guard_refresh_seconds),
        "checked_at": eval_now.isoformat(),
        "status": "ok",
        "reason": None,
        "ok": True,
        "blocked": False,
        "reason_codes": [],
        "error": None,
        "metrics": {},
        "thresholds": thresholds,
        "raw_result": None,
        "temporal_state": {
            "raw_fail_streak": 0,
            "raw_pass_streak": 0,
            "blocked_state": False,
            "previous_snapshot_ts": None if previous is None else previous.get("snapshot_ts"),
        },
    }
    if not spec.liquidity_guard_enabled:
        payload["reason"] = "liquidity_guard_disabled"
        payload["reason_codes"] = ["disabled"]
        return persist_liquidity_state_snapshot_fn(rewrite_root=cfg.layout.rewrite.root, payload=payload) if persist else payload

    try:
        raw_result = evaluate_liquidity_raw_fn(
            symbol=cfg.asset.binance_symbol,
            now=eval_now,
            thresholds=thresholds,
            lookback_minutes=int(spec.liquidity_guard_lookback_minutes),
            baseline_minutes=int(spec.liquidity_guard_baseline_minutes),
            soft_fail_min_count=int(spec.liquidity_guard_soft_fail_min_count),
            hard_spread_multiplier=float(spec.liquidity_guard_hard_spread_multiplier),
            hard_basis_multiplier=float(spec.liquidity_guard_hard_basis_multiplier),
            spot_base_url=(os.getenv("PM15MIN_BINANCE_SPOT_BASE_URL") or SPOT_FALLBACK_BASES[0]).strip().rstrip("/"),
            perp_base_url=(os.getenv("PM15MIN_BINANCE_PERP_BASE_URL") or PERP_FALLBACK_BASES[0]).strip().rstrip("/"),
        )
    except Exception as exc:
        if spec.liquidity_guard_fail_open:
            raw_result = {
                "ok": True,
                "blocked": False,
                "reason_codes": ["fetch_error_fail_open"],
                "metrics": {},
                "error": f"{type(exc).__name__}: {exc}",
            }
        else:
            raw_result = {
                "ok": False,
                "blocked": False,
                "reason_codes": ["fetch_error"],
                "metrics": {},
                "error": f"{type(exc).__name__}: {exc}",
            }

    filtered_result = apply_temporal_filter_fn(
        raw_result=raw_result,
        previous_payload=previous,
        min_failed_checks=int(spec.liquidity_guard_min_failed_checks),
        min_recovered_checks=int(spec.liquidity_guard_min_recovered_checks),
        block_on_degrade=bool(spec.liquidity_guard_block),
    )
    payload["status"] = "ok"
    payload["reason"] = None if not filtered_result["reason_codes"] else filtered_result["reason_codes"][0]
    payload["ok"] = bool(filtered_result["ok"])
    payload["blocked"] = bool(filtered_result["blocked"])
    payload["reason_codes"] = list(filtered_result["reason_codes"])
    payload["error"] = filtered_result.get("error")
    payload["metrics"] = dict(filtered_result.get("metrics") or {})
    payload["raw_result"] = raw_result
    payload["temporal_state"] = dict(filtered_result.get("temporal_state") or {})
    if persist:
        return persist_liquidity_state_snapshot_fn(rewrite_root=cfg.layout.rewrite.root, payload=payload)
    return payload


def persist_liquidity_state_snapshot(*, rewrite_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    layout = LiveStateLayout.discover(root=rewrite_root)
    latest_path = layout.latest_liquidity_path(
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        profile=str(payload["profile"]),
    )
    snapshot_path = layout.liquidity_snapshot_path(
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
    payload["latest_liquidity_path"] = str(latest_path)
    payload["liquidity_snapshot_path"] = str(snapshot_path)
    return payload


def load_latest_liquidity_state_snapshot(
    *,
    rewrite_root: Path,
    market: str,
    cycle: str,
    profile: str,
) -> dict[str, Any] | None:
    path = LiveStateLayout.discover(root=rewrite_root).latest_liquidity_path(
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


def summarize_liquidity_state(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    return {
        "snapshot_ts": payload.get("snapshot_ts"),
        "status": payload.get("status"),
        "reason": payload.get("reason"),
        "guard_enabled": bool(payload.get("guard_enabled")),
        "ok": bool(payload.get("ok", False)),
        "blocked": bool(payload.get("blocked", False)),
        "fail_open": bool(payload.get("fail_open", True)),
        "reason_codes": list(payload.get("reason_codes") or []),
        "metrics": dict(payload.get("metrics") or {}),
    }
