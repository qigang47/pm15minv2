from __future__ import annotations

from typing import Any

from ..account import build_account_state_snapshot, load_latest_positions_snapshot
from .builders import (
    build_action_key,
    build_redeem_action_signature,
)
from .gate import apply_gate_context, evaluate_action_gate, record_attempt_result
from .persistence import persist_redeem_payload
from ..profiles import resolve_live_profile_spec
from ..trading.contracts import RedeemRequest
from ..trading.gateway import LiveTradingGateway
from ..trading.service import (
    build_live_trading_gateway_from_env_if_ready,
    describe_live_trading_gateway,
)


def apply_redeem_policy(
    cfg,
    *,
    persist: bool = True,
    refresh_account_state: bool = True,
    dry_run: bool = False,
    max_conditions: int | None = None,
    gateway: LiveTradingGateway | None = None,
    utc_snapshot_label_fn,
) -> dict[str, Any]:
    cycle = f"{int(cfg.cycle_minutes)}m"
    snapshot_ts = utc_snapshot_label_fn()
    gateway_meta = describe_live_trading_gateway(gateway)
    account_state = None
    positions_snapshot = None
    if refresh_account_state:
        account_state = build_account_state_snapshot(cfg, persist=persist, gateway=gateway)
        positions_snapshot = account_state.get("positions") if isinstance(account_state, dict) else None
    else:
        positions_snapshot = load_latest_positions_snapshot(rewrite_root=cfg.layout.rewrite.root, market=cfg.asset.slug)

    payload = {
        "domain": "live",
        "dataset": "live_redeem_policy_action",
        "snapshot_ts": snapshot_ts,
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": cycle,
        "dry_run": bool(dry_run),
        "refresh_account_state": bool(refresh_account_state),
        "trading_gateway": gateway_meta,
        "max_conditions": None if max_conditions is None else int(max_conditions),
        "positions_snapshot_status": None if positions_snapshot is None else positions_snapshot.get("status"),
        "positions_snapshot_reason": None if positions_snapshot is None else positions_snapshot.get("reason"),
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
        "candidates": [],
        "results": [],
        "summary": {
            "candidate_conditions": 0,
            "submitted_conditions": 0,
            "redeemed_conditions": 0,
            "error_conditions": 0,
        },
    }
    if not isinstance(positions_snapshot, dict):
        payload["reason"] = "positions_snapshot_missing"
        return persist_redeem_payload(cfg=cfg, payload=payload, persist=persist)
    if str(positions_snapshot.get("status") or "") != "ok":
        payload["reason"] = "positions_snapshot_unavailable"
        return persist_redeem_payload(cfg=cfg, payload=payload, persist=persist)

    redeem_plan = positions_snapshot.get("redeem_plan") or {}
    candidates = []
    for condition_id, row in redeem_plan.items():
        if not isinstance(row, dict):
            continue
        candidates.append(
            {
                "condition_id": condition_id,
                "index_sets": list(row.get("index_sets") or []),
                "positions_count": row.get("positions_count"),
                "current_value_sum": row.get("current_value_sum"),
                "cash_pnl_sum": row.get("cash_pnl_sum"),
            }
        )
    candidates.sort(key=lambda row: str(row.get("condition_id") or ""))
    if max_conditions is not None:
        candidates = candidates[: max(0, int(max_conditions))]
    payload["candidates"] = candidates
    payload["summary"]["candidate_conditions"] = len(candidates)
    payload["action_signature"] = build_redeem_action_signature(candidates=candidates)
    payload["action_key"] = build_action_key(payload["action_signature"])
    if not candidates:
        payload["status"] = "ok"
        payload["reason"] = "no_redeemable_conditions"
        return persist_redeem_payload(cfg=cfg, payload=payload, persist=persist)

    spec = resolve_live_profile_spec(cfg.profile)
    gate = evaluate_action_gate(
        cfg=cfg,
        action_type="redeem",
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
        return persist_redeem_payload(cfg=cfg, payload=payload, persist=persist)
    if gateway is None:
        resolved_gateway, missing_reason = build_live_trading_gateway_from_env_if_ready(
            require_auth=True,
            require_redeem=True,
        )
        if resolved_gateway is None:
            payload["reason"] = missing_reason or "missing_redeem_relay_config"
            return persist_redeem_payload(cfg=cfg, payload=payload, persist=persist)
    else:
        resolved_gateway = gateway

    results: list[dict[str, Any]] = []
    redeemed = 0
    errors = 0
    for row in candidates:
        condition_id = str(row.get("condition_id") or "").strip()
        index_sets = [int(v) for v in (row.get("index_sets") or [])]
        if dry_run:
            results.append(
                {
                    "condition_id": condition_id,
                    "index_sets": index_sets,
                    "status": "dry_run",
                }
            )
            continue
        try:
            out = resolved_gateway.redeem_positions(
                RedeemRequest(condition_id=condition_id, index_sets=index_sets)
            ).to_dict()
            redeemed += 1
            results.append(
                {
                    "condition_id": condition_id,
                    "index_sets": index_sets,
                    "status": "redeemed",
                    "tx_hash": out.get("tx_hash"),
                    "state": out.get("state"),
                }
            )
        except Exception as exc:
            errors += 1
            results.append(
                {
                    "condition_id": condition_id,
                    "index_sets": index_sets,
                    "status": "error",
                    "reason": f"{type(exc).__name__}: {exc}",
                }
            )

    payload["results"] = results
    payload["summary"]["submitted_conditions"] = len([row for row in results if row.get("status") != "dry_run"])
    payload["summary"]["redeemed_conditions"] = redeemed
    payload["summary"]["error_conditions"] = errors
    payload["status"] = "ok" if errors == 0 else ("ok_with_errors" if redeemed > 0 else "error")
    payload["reason"] = "redeem_policy_applied" if not dry_run else "dry_run"
    if not dry_run:
        record_attempt_result(payload=payload)
    return persist_redeem_payload(cfg=cfg, payload=payload, persist=persist)
