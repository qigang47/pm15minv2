from __future__ import annotations

import sys
import types
from types import SimpleNamespace

import pandas as pd

from pm15min.live.trading.contracts import DataApiConfig, PlaceOrderRequest, RedeemRequest, TradingAuthConfig
from pm15min.live.trading.legacy_adapter import LegacyLiveTradingGateway


def _install_fake_legacy_modules(monkeypatch, *, trader_cls, order_request_cls, redeem_fn) -> None:
    live_trading_mod = types.ModuleType("live_trading")
    configs_mod = types.ModuleType("live_trading.configs")
    shared_mod = types.ModuleType("live_trading.configs.shared")
    infra_mod = types.ModuleType("live_trading.infra")
    client_mod = types.ModuleType("live_trading.infra.polymarket_client")
    runners_mod = types.ModuleType("live_trading.runners")
    redeem_mod = types.ModuleType("live_trading.runners.auto_redeem")

    class PolymarketAuth:
        def __init__(self, *, host="https://clob.polymarket.com", private_key, signature_type, funder_address=None) -> None:
            self.host = host
            self.private_key = private_key
            self.signature_type = signature_type
            self.funder_address = funder_address

    shared_mod.PolymarketAuth = PolymarketAuth
    client_mod.PolymarketTrader = trader_cls
    client_mod.OrderRequest = order_request_cls
    redeem_mod.redeem_positions = redeem_fn

    monkeypatch.setitem(sys.modules, "live_trading", live_trading_mod)
    monkeypatch.setitem(sys.modules, "live_trading.configs", configs_mod)
    monkeypatch.setitem(sys.modules, "live_trading.configs.shared", shared_mod)
    monkeypatch.setitem(sys.modules, "live_trading.infra", infra_mod)
    monkeypatch.setitem(sys.modules, "live_trading.infra.polymarket_client", client_mod)
    monkeypatch.setitem(sys.modules, "live_trading.runners", runners_mod)
    monkeypatch.setitem(sys.modules, "live_trading.runners.auto_redeem", redeem_mod)


def test_legacy_gateway_list_open_orders_and_place_order(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeOrderRequest:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeTrader:
        def __init__(self, auth) -> None:
            captured["auth"] = auth

        def get_open_orders(self):
            return [
                {
                    "id": "order-1",
                    "market": "market-1",
                    "asset_id": "token-up",
                    "side": "buy",
                    "status": "open",
                    "price": "0.21",
                    "size": "3.5",
                    "createdAt": "2026-03-20T00:00:00Z",
                }
            ]

        def place_order(self, req):
            captured["place_request"] = req
            return SimpleNamespace(success=True, status="live", order_id="order-123", message=None)

        def cancel_order(self, order_id):
            captured["cancel_order_id"] = order_id
            return True

        def get_cash_balance(self):
            return 123.45

    def _redeem(*args, **kwargs):
        raise AssertionError("redeem should not be called in this test")

    _install_fake_legacy_modules(
        monkeypatch,
        trader_cls=FakeTrader,
        order_request_cls=FakeOrderRequest,
        redeem_fn=_redeem,
    )

    gateway = LegacyLiveTradingGateway(
        auth_config=TradingAuthConfig(private_key="pk", signature_type=3, funder_address="0xfunder")
    )

    orders = gateway.list_open_orders()
    assert len(orders) == 1
    assert orders[0].order_id == "order-1"
    assert orders[0].market_id == "market-1"
    assert orders[0].side == "BUY"
    assert orders[0].price == 0.21
    assert getattr(captured["auth"], "host") == "https://clob.polymarket.com"
    assert getattr(captured["auth"], "private_key") == "pk"
    assert getattr(captured["auth"], "signature_type") == 3
    assert getattr(captured["auth"], "funder_address") == "0xfunder"

    result = gateway.place_order(
        PlaceOrderRequest(
            market_id="market-1",
            token_id="token-up",
            side="UP",
            order_type="FAK",
            order_kind="market",
            action="BUY",
            price=0.20,
            size=5.0,
            decision_ts="2026-03-20T00:08:00+00:00",
            metadata={"offset": 7},
        )
    )
    assert result.success is True
    assert result.order_id == "order-123"
    place_request = captured["place_request"]
    assert place_request.market_id == "market-1"
    assert place_request.token_id == "token-up"
    assert place_request.side == "UP"
    assert place_request.order_type == "FAK"
    assert place_request.order_kind == "market"
    assert place_request.action == "BUY"
    assert place_request.price == 0.20
    assert place_request.size == 5.0
    assert place_request.metadata == {"offset": 7}
    assert isinstance(place_request.decision_ts, pd.Timestamp)
    assert gateway.get_cash_balance() == 123.45


def test_legacy_gateway_cancel_order_and_redeem_positions(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeOrderRequest:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeTrader:
        def __init__(self, auth) -> None:
            captured["auth"] = auth

        def get_open_orders(self):
            return []

        def place_order(self, req):
            return SimpleNamespace(success=True, status="live", order_id="order-123", message=None)

        def cancel_order(self, order_id):
            captured["cancel_order_id"] = order_id
            return order_id == "order-9"

    def _redeem(condition_id, *, index_sets):
        captured["redeem_condition_id"] = condition_id
        captured["redeem_index_sets"] = list(index_sets)
        return {"tx_hash": "0xtx", "state": "confirmed"}

    _install_fake_legacy_modules(
        monkeypatch,
        trader_cls=FakeTrader,
        order_request_cls=FakeOrderRequest,
        redeem_fn=_redeem,
    )

    gateway = LegacyLiveTradingGateway(auth_config=TradingAuthConfig(private_key="pk"))

    cancel_result = gateway.cancel_order("order-9")
    assert cancel_result.success is True
    assert cancel_result.status == "cancelled"
    assert captured["cancel_order_id"] == "order-9"

    redeem_result = gateway.redeem_positions(RedeemRequest(condition_id="cond-1", index_sets=[1, 2]))
    assert redeem_result.success is True
    assert redeem_result.tx_hash == "0xtx"
    assert redeem_result.state == "confirmed"
    assert captured["redeem_condition_id"] == "cond-1"
    assert captured["redeem_index_sets"] == [1, 2]


def test_legacy_gateway_list_positions_normalizes_payload(monkeypatch) -> None:
    rows = [
        {
            "marketId": "market-1",
            "conditionId": "cond-1",
            "asset": "token-up",
            "size": "3.5",
            "redeemable": True,
            "outcomeIndex": 0,
            "currentValue": "1.2",
            "cashPnl": "0.0",
        }
    ]

    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, object], int]] = []

        def get(self, url, *, params, timeout):
            self.calls.append((url, params, timeout))
            return FakeResponse(rows)

    fake_session = FakeSession()
    monkeypatch.setattr("pm15min.live.trading.legacy_adapter.requests.Session", lambda: fake_session)

    gateway = LegacyLiveTradingGateway(
        data_api_config=DataApiConfig(
            user_address="0xuser",
            base_url="https://data-api.polymarket.com",
        )
    )

    positions = gateway.list_positions()
    assert len(positions) == 1
    assert positions[0].market_id == "market-1"
    assert positions[0].condition_id == "cond-1"
    assert positions[0].token_id == "token-up"
    assert positions[0].index_set == 1
    assert positions[0].current_value == 1.2
    assert positions[0].cash_pnl == 0.0
    assert fake_session.calls[0][0] == "https://data-api.polymarket.com/positions"
    assert fake_session.calls[0][1]["user"] == "0xuser"
    assert fake_session.calls[0][1]["limit"] == 500


def test_legacy_gateway_requires_private_key_for_open_orders() -> None:
    gateway = LegacyLiveTradingGateway()

    try:
        gateway.list_open_orders()
    except ValueError as exc:
        assert str(exc) == "missing_polymarket_private_key"
    else:
        raise AssertionError("expected missing_polymarket_private_key")


def test_legacy_gateway_requires_user_address_for_positions() -> None:
    gateway = LegacyLiveTradingGateway()

    try:
        gateway.list_positions()
    except ValueError as exc:
        assert str(exc) == "missing_polymarket_user_address"
    else:
        raise AssertionError("expected missing_polymarket_user_address")


def test_legacy_gateway_rejects_non_list_positions_payload(monkeypatch) -> None:
    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return {"positions": []}

    class FakeSession:
        def get(self, url, *, params, timeout):
            return FakeResponse()

    monkeypatch.setattr("pm15min.live.trading.legacy_adapter.requests.Session", lambda: FakeSession())

    gateway = LegacyLiveTradingGateway(
        data_api_config=DataApiConfig(user_address="0xuser", base_url="https://data-api.polymarket.com")
    )

    try:
        gateway.list_positions()
    except TypeError as exc:
        assert str(exc) == "positions API returned non-list payload"
    else:
        raise AssertionError("expected non-list payload to raise TypeError")


def test_legacy_gateway_cancel_failure_and_empty_redeem_payload(monkeypatch) -> None:
    class FakeOrderRequest:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeTrader:
        def __init__(self, auth) -> None:
            return None

        def get_open_orders(self):
            return []

        def place_order(self, req):
            return SimpleNamespace(success=True, status="live", order_id="order-123", message=None)

        def cancel_order(self, order_id):
            return False

    def _redeem(condition_id, *, index_sets):
        return {}

    _install_fake_legacy_modules(
        monkeypatch,
        trader_cls=FakeTrader,
        order_request_cls=FakeOrderRequest,
        redeem_fn=_redeem,
    )

    gateway = LegacyLiveTradingGateway(auth_config=TradingAuthConfig(private_key="pk"))

    cancel_result = gateway.cancel_order("order-x")
    assert cancel_result.success is False
    assert cancel_result.status == "cancel_order_failed"
    assert cancel_result.message == "cancel_order_failed"

    redeem_result = gateway.redeem_positions(RedeemRequest(condition_id="cond-1", index_sets=[1]))
    assert redeem_result.success is False
    assert redeem_result.status == "error"


def test_legacy_gateway_maps_unsuccessful_place_order_response(monkeypatch) -> None:
    class FakeOrderRequest:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeTrader:
        def __init__(self, auth) -> None:
            return None

        def get_open_orders(self):
            return []

        def place_order(self, req):
            return SimpleNamespace(success=False, status="rejected", order_id=None, message="no_liquidity")

        def cancel_order(self, order_id):
            return True

    def _redeem(*args, **kwargs):
        return {"tx_hash": "0xtx", "state": "confirmed"}

    _install_fake_legacy_modules(
        monkeypatch,
        trader_cls=FakeTrader,
        order_request_cls=FakeOrderRequest,
        redeem_fn=_redeem,
    )

    gateway = LegacyLiveTradingGateway(auth_config=TradingAuthConfig(private_key="pk"))

    result = gateway.place_order(
        PlaceOrderRequest(
            market_id="market-1",
            token_id="token-up",
            side="UP",
            order_type="FAK",
            order_kind="market",
            action="BUY",
            price=0.20,
            size=5.0,
        )
    )

    assert result.success is False
    assert result.status == "rejected"
    assert result.message == "no_liquidity"
