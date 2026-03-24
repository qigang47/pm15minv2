from __future__ import annotations

from typing import Any

import pandas as pd

from ..account import build_account_state_snapshot, load_latest_open_orders_snapshot
from .builders import (
    build_action_key,
    build_cancel_action_signature,
    build_cancel_candidates,
    load_live_market_table,
    normalize_now,
)
from .gate import apply_gate_context, evaluate_action_gate, record_attempt_result
from .persistence import persist_cancel_payload
from ..profiles import resolve_live_profile_spec
from ..trading.gateway import LiveTradingGateway
from ..trading.service import (
    build_live_trading_gateway_from_env_if_ready,
    describe_live_trading_gateway,
)


def apply_cancel_policy(
    cfg,
    *,
    persist: bool = True,
    refresh_account_state: bool = True,
    dry_run: bool = False,
    now: pd.Timestamp | None = None,
    gateway: LiveTradingGateway | None = None,
    utc_snapshot_label_fn,
) -> dict[str, Any]:
    cycle = f"{int(cfg.cycle_minutes)}m"
    snapshot_ts = utc_snapshot_label_fn()
    spec = resolve_live_profile_spec(cfg.profile)
    cancel_window = getattr(spec, "cancel_markets_when_minutes_left", None)
    gateway_meta = describe_live_trading_gateway(gateway)
    account_state = None
    open_orders_snapshot = None
    if refresh_account_state:
        account_state = build_account_state_snapshot(cfg, persist=persist, gateway=gateway)
        open_orders_snapshot = account_state.get("open_orders") if isinstance(account_state, dict) else None
    else:
        open_orders_snapshot = load_latest_open_orders_snapshot(rewrite_root=cfg.layout.rewrite.root, market=cfg.asset.slug)

    payload = {
        "domain": "live",
        "dataset": "live_cancel_policy_action",
        "snapshot_ts": snapshot_ts,
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": cycle,
        "dry_run": bool(dry_run),
        "refresh_account_state": bool(refresh_account_state),
        "trading_gateway": gateway_meta,
        "cancel_window_minutes": int(cancel_window) if cancel_window is not None else None,
        "open_orders_snapshot_status": None if open_orders_snapshot is None else open_orders_snapshot.get("status"),
        "open_orders_snapshot_reason": None if open_orders_snapshot is None else open_orders_snapshot.get("reason"),
        "account_state_snapshot_ts": None if account_state is None else account_state.get("snapshot_ts"),
        "status": "skipped",
        "reason": None,
        "action_key": None,
        "action_signature": None,
        "attempt": 0,
        "attempted": False,
        "last_attempt_snapshot_ts": None,
        "last_attempt_status": None,
        "last_attempt_reason": None,
        "gate": None,
        "candidate_orders": [],
        "results": [],
        "summary": {
            "candidate_orders": 0,
            "submitted_orders": 0,
            "cancelled_orders": 0,
            "error_orders": 0,
        },
    }

    if cancel_window is None:
        payload["reason"] = "cancel_window_not_configured"
        return persist_cancel_payload(cfg=cfg, payload=payload, persist=persist)
    if not isinstance(open_orders_snapshot, dict):
        payload["reason"] = "open_orders_snapshot_missing"
        return persist_cancel_payload(cfg=cfg, payload=payload, persist=persist)
    if str(open_orders_snapshot.get("status") or "") != "ok":
        payload["reason"] = "open_orders_snapshot_unavailable"
        return persist_cancel_payload(cfg=cfg, payload=payload, persist=persist)

    market_table = load_live_market_table(cfg)
    if market_table.empty:
        payload["reason"] = "market_catalog_missing"
        return persist_cancel_payload(cfg=cfg, payload=payload, persist=persist)

    eval_now = normalize_now(now)
    candidates = build_cancel_candidates(
        open_orders_snapshot=open_orders_snapshot,
        market_table=market_table,
        cancel_window_minutes=int(cancel_window),
        now=eval_now,
    )
    payload["candidate_orders"] = candidates
    payload["summary"]["candidate_orders"] = len(candidates)
    payload["action_signature"] = build_cancel_action_signature(candidates=candidates)
    payload["action_key"] = build_action_key(payload["action_signature"])
    if not candidates:
        payload["status"] = "ok"
        payload["reason"] = "no_orders_in_cancel_window"
        return persist_cancel_payload(cfg=cfg, payload=payload, persist=persist)

    gate = evaluate_action_gate(
        cfg=cfg,
        action_type="cancel",
        cycle=cycle,
        target=None,
        spec=spec,
        action_key=str(payload["action_key"]),
        snapshot_ts=snapshot_ts,
        dry_run=dry_run,
    )
    apply_gate_context(payload=payload, gate=gate)
    if gate["decision"] == "skip":
        payload["reason"] = gate["reason"]
        return persist_cancel_payload(cfg=cfg, payload=payload, persist=persist)

    if dry_run:
        payload["status"] = "ok"
        payload["reason"] = "dry_run"
        payload["results"] = [
            {
                "order_id": row.get("order_id"),
                "market_id": row.get("market_id"),
                "token_id": row.get("token_id"),
                "status": "dry_run",
            }
            for row in candidates
        ]
        return persist_cancel_payload(cfg=cfg, payload=payload, persist=persist)

    if gateway is None:
        resolved_gateway, missing_reason = build_live_trading_gateway_from_env_if_ready(require_auth=True)
        if resolved_gateway is None:
            payload["reason"] = missing_reason or "missing_polymarket_private_key"
            return persist_cancel_payload(cfg=cfg, payload=payload, persist=persist)
    else:
        resolved_gateway = gateway

    results: list[dict[str, Any]] = []
    cancelled = 0
    errors = 0
    for row in candidates:
        order_id = str(row.get("order_id") or "").strip()
        if not order_id:
            results.append(
                {
                    "order_id": None,
                    "market_id": row.get("market_id"),
                    "token_id": row.get("token_id"),
                    "status": "error",
                    "reason": "order_id_missing",
                }
            )
            errors += 1
            continue
        try:
            result = resolved_gateway.cancel_order(order_id)
        except Exception as exc:
            result = None
            error_reason = f"{type(exc).__name__}: {exc}"
        else:
            error_reason = str(result.message or result.status or "cancel_order_failed")
        if result is not None and bool(result.success):
            cancelled += 1
            results.append(
                {
                    "order_id": order_id,
                    "market_id": row.get("market_id"),
                    "token_id": row.get("token_id"),
                    "status": "cancelled",
                }
            )
        else:
            errors += 1
            results.append(
                {
                    "order_id": order_id,
                    "market_id": row.get("market_id"),
                    "token_id": row.get("token_id"),
                    "status": "error",
                    "reason": error_reason,
                }
            )

    payload["results"] = results
    payload["summary"]["submitted_orders"] = len([row for row in results if row.get("status") != "dry_run"])
    payload["summary"]["cancelled_orders"] = cancelled
    payload["summary"]["error_orders"] = errors
    payload["status"] = "ok" if errors == 0 else ("ok_with_errors" if cancelled > 0 else "error")
    payload["reason"] = "cancel_policy_applied"
    record_attempt_result(payload=payload)
    return persist_cancel_payload(cfg=cfg, payload=payload, persist=persist)
