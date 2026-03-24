from __future__ import annotations

import pytest

from pm15min.live.trading.contracts import DataApiConfig, RedeemRequest, TradingAuthConfig
from pm15min.live.trading.direct_adapter import DirectLiveTradingGateway
from pm15min.live.trading.legacy_adapter import LegacyLiveTradingGateway
from pm15min.live.trading.service import (
    build_place_order_request_from_payload,
    build_live_trading_gateway,
    build_live_trading_gateway_from_env,
    build_live_trading_gateway_from_env_if_ready,
    current_live_trading_adapter_from_env,
    describe_live_trading_gateway,
    normalize_live_trading_adapter,
)


def test_build_live_trading_gateway_defaults_to_direct() -> None:
    gateway = build_live_trading_gateway(
        auth_config=TradingAuthConfig(private_key="pk"),
        data_api_config=DataApiConfig(user_address="0xuser"),
    )

    assert isinstance(gateway, DirectLiveTradingGateway)


def test_build_live_trading_gateway_supports_direct_adapter() -> None:
    gateway = build_live_trading_gateway(
        auth_config=TradingAuthConfig(private_key="pk"),
        data_api_config=DataApiConfig(user_address="0xuser"),
        adapter="direct",
    )

    assert isinstance(gateway, DirectLiveTradingGateway)


def test_build_live_trading_gateway_from_env_respects_adapter(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_LIVE_TRADING_ADAPTER", "direct")
    monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", "pk")
    monkeypatch.setenv("POLYMARKET_USER_ADDRESS", "0xuser")

    gateway = build_live_trading_gateway_from_env()

    assert isinstance(gateway, DirectLiveTradingGateway)
    assert current_live_trading_adapter_from_env() == "direct"


def test_current_live_trading_adapter_from_env_defaults_to_direct(monkeypatch) -> None:
    monkeypatch.delenv("PM15MIN_LIVE_TRADING_ADAPTER", raising=False)

    assert current_live_trading_adapter_from_env() == "direct"


def test_build_live_trading_gateway_from_env_allows_override(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_LIVE_TRADING_ADAPTER", "legacy")
    monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", "pk")
    monkeypatch.setenv("POLYMARKET_USER_ADDRESS", "0xuser")

    gateway = build_live_trading_gateway_from_env(adapter_override="direct")

    assert isinstance(gateway, DirectLiveTradingGateway)


def test_build_live_trading_gateway_from_env_if_ready_reports_missing_prerequisites(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", "")
    monkeypatch.setenv("POLYMARKET_USER_ADDRESS", "")
    monkeypatch.setenv("RPC_URL", "")
    monkeypatch.setenv("POLYGON_RPC", "")
    monkeypatch.setenv("POLYGON_RPC_URL", "")
    monkeypatch.setenv("WEB3_PROVIDER_URI", "")
    monkeypatch.setenv("RPC_URL_BACKUPS", "")
    monkeypatch.setenv("POLYGON_RPC_BACKUPS", "")
    monkeypatch.setenv("RPC_FALLBACKS", "")
    monkeypatch.setenv("POLYGON_RPC_FALLBACKS", "")
    monkeypatch.setenv("BUILDER_API_KEY", "")
    monkeypatch.setenv("BUILDER_SECRET", "")
    monkeypatch.setenv("BUILDER_PASS_PHRASE", "")

    gateway, reason = build_live_trading_gateway_from_env_if_ready(require_auth=True)
    assert gateway is None
    assert reason == "missing_polymarket_private_key"

    gateway, reason = build_live_trading_gateway_from_env_if_ready(require_data_api=True)
    assert gateway is None
    assert reason == "missing_polymarket_user_address"

    gateway, reason = build_live_trading_gateway_from_env_if_ready(require_auth=True, require_redeem=True)
    assert gateway is None
    assert reason == "missing_polymarket_private_key"


def test_normalize_live_trading_adapter_rejects_unknown() -> None:
    with pytest.raises(ValueError):
        normalize_live_trading_adapter("weird")


def test_direct_adapter_is_explicitly_not_implemented() -> None:
    gateway = DirectLiveTradingGateway()

    with pytest.raises(ValueError):
        gateway.redeem_positions(RedeemRequest(condition_id="cond-1", index_sets=[1]))


def test_describe_live_trading_gateway_reports_adapter() -> None:
    gateway = LegacyLiveTradingGateway()

    payload = describe_live_trading_gateway(gateway)

    assert payload["adapter"] == "legacy"
    assert payload["injected"] is True
    assert payload["gateway_class"] == "LegacyLiveTradingGateway"


def test_describe_live_trading_gateway_defaults_env_selection_to_direct(monkeypatch) -> None:
    monkeypatch.delenv("PM15MIN_LIVE_TRADING_ADAPTER", raising=False)

    payload = describe_live_trading_gateway()

    assert payload["adapter"] == "direct"
    assert payload["source"] == "env_selection"
    assert payload["injected"] is False


def test_build_place_order_request_from_payload_normalizes_defaults() -> None:
    request = build_place_order_request_from_payload(
        {
            "market_id": "market-1",
            "token_id": "token-up",
            "side": "UP",
            "order_type": "FAK",
            "price": "0.25",
            "size": "4.0",
            "decision_ts": "2026-03-20T00:08:00+00:00",
            "metadata": {"offset": 7},
        }
    )

    assert request.market_id == "market-1"
    assert request.token_id == "token-up"
    assert request.side == "UP"
    assert request.order_type == "FAK"
    assert request.price == 0.25
    assert request.size == 4.0
    assert request.order_kind == "market"
    assert request.action == "BUY"
    assert request.decision_ts == "2026-03-20T00:08:00+00:00"
    assert request.metadata == {"offset": 7}
