from __future__ import annotations

from typing import Any

from pm15min.data.layout import utc_snapshot_label
from .persistence import (
    load_latest_open_orders_snapshot,
    load_latest_positions_snapshot,
    persist_open_orders_snapshot,
    persist_positions_snapshot,
)
from .summary import (
    build_redeem_plan,
    summarize_account_state_payload,
    summarize_open_orders_rows,
    summarize_positions_rows,
)
from ..trading.gateway import LiveTradingGateway
from ..trading.service import (
    build_live_trading_gateway_from_env_if_ready,
    describe_live_trading_gateway,
    load_live_trading_env_configs,
)


def build_account_state_snapshot(
    cfg,
    *,
    persist: bool = True,
    gateway: LiveTradingGateway | None = None,
) -> dict[str, Any]:
    gateway_meta = describe_live_trading_gateway(gateway)
    open_orders = build_open_orders_snapshot(cfg, persist=persist, gateway=gateway)
    positions = build_positions_snapshot(cfg, persist=persist, gateway=gateway)
    summary = summarize_account_state_payload(
        {
            "snapshot_ts": utc_snapshot_label(),
            "open_orders": open_orders,
            "positions": positions,
        }
    )
    return {
        "domain": "live",
        "dataset": "live_account_state_sync",
        "market": cfg.asset.slug,
        "snapshot_ts": summary.get("snapshot_ts"),
        "trading_gateway": gateway_meta,
        "open_orders": open_orders,
        "positions": positions,
        "summary": summary,
    }


def build_open_orders_snapshot(
    cfg,
    *,
    persist: bool = True,
    gateway: LiveTradingGateway | None = None,
) -> dict[str, Any]:
    snapshot_ts = utc_snapshot_label()
    auth_config, _, _ = load_live_trading_env_configs()
    gateway_available = gateway is not None or auth_config.is_configured
    gateway_meta = describe_live_trading_gateway(gateway)

    payload = {
        "domain": "live",
        "dataset": "live_open_orders_snapshot",
        "snapshot_ts": snapshot_ts,
        "market": cfg.asset.slug,
        "source": "polymarket_clob_open_orders",
        "trading_gateway": gateway_meta,
        "status": "skipped",
        "reason": "missing_polymarket_private_key",
        "funder_address": auth_config.funder_address,
        "orders": [],
        "summary": {
            "total_orders": 0,
            "by_market_id": {},
            "by_token_id": {},
        },
        "prerequisites": {
            "polymarket_private_key_present": bool(gateway_available),
        },
    }
    if not gateway_available:
        return _persist_open_orders_payload(cfg=cfg, payload=payload, persist=persist)

    try:
        if gateway is None:
            resolved_gateway, missing_reason = build_live_trading_gateway_from_env_if_ready(require_auth=True)
            if resolved_gateway is None:
                payload["reason"] = missing_reason or "missing_polymarket_private_key"
                return _persist_open_orders_payload(cfg=cfg, payload=payload, persist=persist)
        else:
            resolved_gateway = gateway
        orders = [row.to_dict() for row in resolved_gateway.list_open_orders()]
        payload["status"] = "ok"
        payload["reason"] = None
        payload["orders"] = orders
        payload["summary"] = summarize_open_orders_rows(orders)
    except Exception as exc:
        payload["status"] = "error"
        payload["reason"] = f"{type(exc).__name__}: {exc}"

    return _persist_open_orders_payload(cfg=cfg, payload=payload, persist=persist)


def build_positions_snapshot(
    cfg,
    *,
    persist: bool = True,
    gateway: LiveTradingGateway | None = None,
) -> dict[str, Any]:
    snapshot_ts = utc_snapshot_label()
    auth_config, data_api_config, _ = load_live_trading_env_configs()
    positions_available = gateway is not None or data_api_config.is_configured
    cash_balance_available = gateway is not None or auth_config.is_configured
    gateway_meta = describe_live_trading_gateway(gateway)

    payload = {
        "domain": "live",
        "dataset": "live_positions_snapshot",
        "snapshot_ts": snapshot_ts,
        "market": cfg.asset.slug,
        "source": "polymarket_data_api_positions",
        "trading_gateway": gateway_meta,
        "status": "skipped",
        "reason": "missing_polymarket_user_address",
        "user_address": data_api_config.user_address,
        "data_api_base": data_api_config.base_url,
        "positions": [],
        "redeem_plan": {},
        "cash_balance_usd": None,
        "cash_balance_status": "skipped",
        "cash_balance_reason": "missing_polymarket_private_key",
        "summary": {
            "total_positions": 0,
            "redeemable_positions": 0,
            "redeemable_conditions": 0,
        },
        "prerequisites": {
            "polymarket_user_address_present": bool(positions_available),
            "polymarket_private_key_present": bool(cash_balance_available),
        },
    }
    if not positions_available and not cash_balance_available:
        return _persist_positions_payload(cfg=cfg, payload=payload, persist=persist)

    resolved_gateway = gateway
    try:
        if resolved_gateway is None:
            resolved_gateway, missing_reason = build_live_trading_gateway_from_env_if_ready(
                require_auth=cash_balance_available,
                require_data_api=positions_available,
            )
            if resolved_gateway is None:
                payload["reason"] = missing_reason or payload["reason"]
                return _persist_positions_payload(cfg=cfg, payload=payload, persist=persist)
        if positions_available:
            positions = [row.to_dict() for row in resolved_gateway.list_positions()]
            redeem_plan = build_redeem_plan(positions)
            payload["status"] = "ok"
            payload["reason"] = None
            payload["positions"] = positions
            payload["redeem_plan"] = redeem_plan
            payload["summary"] = summarize_positions_rows(positions, redeem_plan=redeem_plan)
    except Exception as exc:
        if positions_available:
            payload["status"] = "error"
            payload["reason"] = f"{type(exc).__name__}: {exc}"

    if cash_balance_available and resolved_gateway is not None:
        try:
            balance_getter = getattr(resolved_gateway, "get_cash_balance", None)
            balance = None if balance_getter is None else balance_getter()
            payload["cash_balance_usd"] = None if balance is None else float(balance)
            payload["cash_balance_status"] = "ok" if balance is not None else "unavailable"
            payload["cash_balance_reason"] = None if balance is not None else "cash_balance_unavailable"
        except Exception as exc:
            payload["cash_balance_status"] = "error"
            payload["cash_balance_reason"] = f"{type(exc).__name__}: {exc}"

    return _persist_positions_payload(cfg=cfg, payload=payload, persist=persist)


def _persist_open_orders_payload(*, cfg, payload: dict[str, Any], persist: bool) -> dict[str, Any]:
    if persist:
        paths = persist_open_orders_snapshot(rewrite_root=cfg.layout.rewrite.root, payload=payload)
        payload["latest_open_orders_path"] = str(paths["latest"])
        payload["open_orders_snapshot_path"] = str(paths["snapshot"])
    return payload


def _persist_positions_payload(*, cfg, payload: dict[str, Any], persist: bool) -> dict[str, Any]:
    if persist:
        paths = persist_positions_snapshot(rewrite_root=cfg.layout.rewrite.root, payload=payload)
        payload["latest_positions_path"] = str(paths["latest"])
        payload["positions_snapshot_path"] = str(paths["snapshot"])
    return payload
