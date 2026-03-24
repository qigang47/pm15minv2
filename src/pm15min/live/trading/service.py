from __future__ import annotations

import os
from typing import Any

from .auth import (
    load_data_api_config_from_env,
    load_redeem_relay_config_from_env,
    load_trading_auth_config_from_env,
)
from .contracts import DataApiConfig, PlaceOrderRequest, RedeemRelayConfig, TradingAuthConfig
from .direct_adapter import DirectLiveTradingGateway
from .gateway import LiveTradingGateway
from .legacy_adapter import LegacyLiveTradingGateway

DEFAULT_LIVE_TRADING_ADAPTER = "direct"


def load_live_trading_env_configs() -> tuple[TradingAuthConfig, DataApiConfig, RedeemRelayConfig]:
    return (
        load_trading_auth_config_from_env(),
        load_data_api_config_from_env(),
        load_redeem_relay_config_from_env(),
    )


def build_live_trading_gateway(
    *,
    auth_config: TradingAuthConfig | None = None,
    data_api_config: DataApiConfig | None = None,
    redeem_config: RedeemRelayConfig | None = None,
    adapter: str = DEFAULT_LIVE_TRADING_ADAPTER,
) -> LiveTradingGateway:
    selected = normalize_live_trading_adapter(adapter)
    if selected == "legacy":
        return LegacyLiveTradingGateway(
            auth_config=auth_config,
            data_api_config=data_api_config,
        )
    if selected == "direct":
        return DirectLiveTradingGateway(
            auth_config=auth_config,
            data_api_config=data_api_config,
            redeem_config=redeem_config,
        )
    raise ValueError(f"Unsupported live trading adapter: {adapter!r}")


def build_live_trading_gateway_from_env(*, adapter_override: str | None = None) -> LiveTradingGateway:
    auth_config, data_api_config, redeem_config = load_live_trading_env_configs()
    return build_live_trading_gateway(
        auth_config=auth_config,
        data_api_config=data_api_config,
        redeem_config=redeem_config,
        adapter=current_live_trading_adapter(adapter_override=adapter_override),
    )


def normalize_live_trading_adapter(adapter: str | None) -> str:
    token = str(adapter or DEFAULT_LIVE_TRADING_ADAPTER).strip().lower()
    if token in {"legacy", "direct"}:
        return token
    raise ValueError(f"Unsupported live trading adapter: {adapter!r}")


def current_live_trading_adapter_from_env() -> str:
    return normalize_live_trading_adapter(os.getenv("PM15MIN_LIVE_TRADING_ADAPTER", DEFAULT_LIVE_TRADING_ADAPTER))


def current_live_trading_adapter(*, adapter_override: str | None = None) -> str:
    if adapter_override is not None:
        return normalize_live_trading_adapter(adapter_override)
    return current_live_trading_adapter_from_env()


def build_live_trading_gateway_from_env_if_ready(
    *,
    adapter_override: str | None = None,
    require_auth: bool = False,
    require_data_api: bool = False,
    require_redeem: bool = False,
) -> tuple[LiveTradingGateway | None, str | None]:
    auth_config, data_api_config, redeem_config = load_live_trading_env_configs()
    missing_reason = _first_missing_live_trading_reason(
        auth_config=auth_config,
        data_api_config=data_api_config,
        redeem_config=redeem_config,
        require_auth=require_auth,
        require_data_api=require_data_api,
        require_redeem=require_redeem,
    )
    if missing_reason is not None:
        return None, missing_reason
    return (
        build_live_trading_gateway(
            auth_config=auth_config,
            data_api_config=data_api_config,
            redeem_config=redeem_config,
            adapter=current_live_trading_adapter(adapter_override=adapter_override),
        ),
        None,
    )


def build_place_order_request_from_payload(payload: dict[str, Any]) -> PlaceOrderRequest:
    return PlaceOrderRequest(
        market_id=str(payload["market_id"]),
        token_id=str(payload["token_id"]),
        side=str(payload["side"]),
        order_type=str(payload["order_type"]),
        price=float(payload["price"]),
        size=float(payload["size"]),
        order_kind=str(payload.get("order_kind") or "market"),
        action=str(payload.get("action") or "BUY"),
        decision_ts=None if payload.get("decision_ts") is None else str(payload.get("decision_ts")),
        metadata=dict(payload.get("metadata") or {}),
    )


def describe_live_trading_gateway(
    gateway: LiveTradingGateway | None = None,
    *,
    adapter: str | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    if gateway is not None:
        resolved_adapter = str(getattr(gateway, "adapter_name", adapter or "custom")).strip().lower() or "custom"
        return {
            "adapter": resolved_adapter,
            "source": source or "injected_gateway",
            "gateway_class": gateway.__class__.__name__,
            "gateway_module": gateway.__class__.__module__,
            "injected": True,
        }
    resolved_adapter = current_live_trading_adapter(adapter_override=adapter)
    return {
        "adapter": resolved_adapter,
        "source": source or "env_selection",
        "gateway_class": None,
        "gateway_module": None,
        "injected": False,
    }


def _first_missing_live_trading_reason(
    *,
    auth_config: TradingAuthConfig,
    data_api_config: DataApiConfig,
    redeem_config: RedeemRelayConfig,
    require_auth: bool,
    require_data_api: bool,
    require_redeem: bool,
) -> str | None:
    if require_auth and not auth_config.is_configured:
        return "missing_polymarket_private_key"
    if require_data_api and not data_api_config.is_configured:
        return "missing_polymarket_user_address"
    if require_redeem and not redeem_config.is_configured:
        return "missing_redeem_relay_config"
    return None
