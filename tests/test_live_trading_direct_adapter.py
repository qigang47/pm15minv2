from __future__ import annotations

import sys
import types
from types import SimpleNamespace

from pm15min.live.trading.contracts import DataApiConfig, PlaceOrderRequest, RedeemRelayConfig, RedeemRequest, TradingAuthConfig
from pm15min.live.trading.direct_adapter import DirectLiveTradingGateway


def _install_fake_py_clob_client(
    monkeypatch,
    *,
    clob_client_cls,
    order_args_cls,
    market_order_args_cls,
    order_type_cls,
    balance_allowance_params_cls=None,
    asset_type_cls=None,
) -> None:
    root_mod = types.ModuleType("py_clob_client")
    client_mod = types.ModuleType("py_clob_client.client")
    clob_types_mod = types.ModuleType("py_clob_client.clob_types")

    client_mod.ClobClient = clob_client_cls
    clob_types_mod.OrderArgs = order_args_cls
    clob_types_mod.MarketOrderArgs = market_order_args_cls
    clob_types_mod.OrderType = order_type_cls
    clob_types_mod.BalanceAllowanceParams = balance_allowance_params_cls or type("BalanceAllowanceParams", (), {})
    clob_types_mod.AssetType = asset_type_cls or type("AssetType", (), {"COLLATERAL": "COLLATERAL"})

    monkeypatch.setitem(sys.modules, "py_clob_client", root_mod)
    monkeypatch.setitem(sys.modules, "py_clob_client.client", client_mod)
    monkeypatch.setitem(sys.modules, "py_clob_client.clob_types", clob_types_mod)


def test_direct_adapter_lists_open_orders_places_and_cancels(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeOrderArgs:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeMarketOrderArgs:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeOrderType:
        GTC = "GTC"
        FAK = "FAK"
        FOK = "FOK"
        GTD = "GTD"

    class FakeClobClient:
        def __init__(self, host, *, key, chain_id, signature_type, funder) -> None:
            captured["host"] = host
            captured["key"] = key
            captured["chain_id"] = chain_id
            captured["signature_type"] = signature_type
            captured["funder"] = funder

        def create_or_derive_api_creds(self):
            return SimpleNamespace(api_key="k", api_secret="s", api_passphrase="p")

        def set_api_creds(self, creds):
            captured["creds"] = creds

        def get_orders(self):
            return [
                {
                    "id": "order-1",
                    "market": "market-1",
                    "asset_id": "token-up",
                    "side": "buy",
                    "status": "open",
                    "price": "0.20",
                    "size": "5",
                }
            ]

        def create_market_order(self, args):
            captured["market_order_args"] = args
            return {"signed": "market"}

        def create_order(self, args):
            captured["order_args"] = args
            return {"signed": "limit"}

        def post_order(self, order, order_type):
            captured["posted_order"] = order
            captured["posted_order_type"] = order_type
            return {"success": True, "status": "live", "orderID": "order-123"}

        def cancel(self, order_id):
            captured["cancel_order_id"] = order_id
            return {"success": True}

    _install_fake_py_clob_client(
        monkeypatch,
        clob_client_cls=FakeClobClient,
        order_args_cls=FakeOrderArgs,
        market_order_args_cls=FakeMarketOrderArgs,
        order_type_cls=FakeOrderType,
    )

    gateway = DirectLiveTradingGateway(
        auth_config=TradingAuthConfig(
            host="https://clob.polymarket.com",
            chain_id=137,
            private_key="pk",
            signature_type=2,
            funder_address="0xfunder",
        )
    )

    orders = gateway.list_open_orders()
    assert len(orders) == 1
    assert orders[0].order_id == "order-1"
    assert captured["host"] == "https://clob.polymarket.com"
    assert captured["chain_id"] == 137
    assert captured["signature_type"] == 2
    assert captured["funder"] == "0xfunder"

    result = gateway.place_order(
        PlaceOrderRequest(
            market_id="market-1",
            token_id="token-up",
            side="UP",
            order_type="FAK",
            price=0.20,
            size=5.0,
            order_kind="market",
            action="BUY",
        )
    )
    assert result.success is True
    assert result.order_id == "order-123"
    market_args = captured["market_order_args"]
    assert market_args.token_id == "token-up"
    assert market_args.side == "BUY"
    assert market_args.amount == 1.0
    assert market_args.price == 0.20
    assert captured["posted_order_type"] == "FAK"

    cancel_result = gateway.cancel_order("order-123")
    assert cancel_result.success is True
    assert captured["cancel_order_id"] == "order-123"


def test_direct_adapter_reads_cash_balance(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeOrderArgs:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeMarketOrderArgs:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeBalanceAllowanceParams:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeAssetType:
        COLLATERAL = "COLLATERAL"

    class FakeOrderType:
        GTC = "GTC"
        FAK = "FAK"

    class FakeClobClient:
        def __init__(self, host, *, key, chain_id, signature_type, funder) -> None:
            captured["host"] = host

        def create_or_derive_api_creds(self):
            return SimpleNamespace(api_key="k", api_secret="s", api_passphrase="p")

        def set_api_creds(self, creds):
            captured["creds"] = creds

        def get_balance_allowance(self, params):
            captured["balance_params"] = params
            return {"balanceAllowance": {"available": "187.75"}}

    _install_fake_py_clob_client(
        monkeypatch,
        clob_client_cls=FakeClobClient,
        order_args_cls=FakeOrderArgs,
        market_order_args_cls=FakeMarketOrderArgs,
        order_type_cls=FakeOrderType,
        balance_allowance_params_cls=FakeBalanceAllowanceParams,
        asset_type_cls=FakeAssetType,
    )

    gateway = DirectLiveTradingGateway(auth_config=TradingAuthConfig(private_key="pk"))

    balance = gateway.get_cash_balance()

    assert balance == 187.75
    assert captured["balance_params"].asset_type == "COLLATERAL"


def test_direct_adapter_reads_cash_balance_from_micro_usdc(monkeypatch) -> None:
    class FakeOrderArgs:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeMarketOrderArgs:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeBalanceAllowanceParams:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeAssetType:
        COLLATERAL = "COLLATERAL"

    class FakeOrderType:
        GTC = "GTC"
        FAK = "FAK"

    class FakeClobClient:
        def __init__(self, host, *, key, chain_id, signature_type, funder) -> None:
            return None

        def create_or_derive_api_creds(self):
            return SimpleNamespace(api_key="k", api_secret="s", api_passphrase="p")

        def set_api_creds(self, creds):
            return None

        def get_balance_allowance(self, params):
            return {"balance": "246926661"}

    _install_fake_py_clob_client(
        monkeypatch,
        clob_client_cls=FakeClobClient,
        order_args_cls=FakeOrderArgs,
        market_order_args_cls=FakeMarketOrderArgs,
        order_type_cls=FakeOrderType,
        balance_allowance_params_cls=FakeBalanceAllowanceParams,
        asset_type_cls=FakeAssetType,
    )

    gateway = DirectLiveTradingGateway(auth_config=TradingAuthConfig(private_key="pk"))

    balance = gateway.get_cash_balance()

    assert balance == 246.926661


def test_direct_adapter_lists_positions_via_data_api(monkeypatch) -> None:
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
            self.calls = []

        def get(self, url, *, params, timeout):
            self.calls.append((url, params, timeout))
            return FakeResponse(rows)

    fake_session = FakeSession()
    monkeypatch.setattr("pm15min.live.trading.direct_adapter.requests.Session", lambda: fake_session)

    gateway = DirectLiveTradingGateway(
        data_api_config=DataApiConfig(user_address="0xuser", base_url="https://data-api.polymarket.com")
    )

    positions = gateway.list_positions()
    assert len(positions) == 1
    assert positions[0].condition_id == "cond-1"
    assert positions[0].index_set == 1
    assert fake_session.calls[0][0] == "https://data-api.polymarket.com/positions"


def test_direct_adapter_redeem_positions_uses_relayer_helper(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_redeem_positions_via_relayer(*, auth_config, relay_config, request):
        captured["auth_config"] = auth_config
        captured["relay_config"] = relay_config
        captured["request"] = request
        from pm15min.live.trading.contracts import RedeemResult

        return RedeemResult(success=True, status="confirmed", tx_hash="0xtx", state="confirmed")

    monkeypatch.setattr(
        "pm15min.live.trading.direct_adapter.redeem_positions_via_relayer",
        _fake_redeem_positions_via_relayer,
    )

    gateway = DirectLiveTradingGateway(
        auth_config=TradingAuthConfig(private_key="pk"),
        redeem_config=RedeemRelayConfig(
            rpc_urls=("https://rpc-1",),
            relayer_url="https://relayer",
            builder_api_key="key",
            builder_secret="secret",
            builder_passphrase="pass",
        ),
    )

    result = gateway.redeem_positions(RedeemRequest(condition_id="0x" + "11" * 32, index_sets=[1, 2]))

    assert result.success is True
    assert result.tx_hash == "0xtx"
    assert captured["auth_config"].private_key == "pk"
    assert captured["relay_config"].relayer_url == "https://relayer"
    assert captured["request"].index_sets == [1, 2]


def test_direct_adapter_requires_private_key_for_open_orders() -> None:
    gateway = DirectLiveTradingGateway()

    try:
        gateway.list_open_orders()
    except ValueError as exc:
        assert str(exc) == "missing_polymarket_private_key"
    else:
        raise AssertionError("expected missing_polymarket_private_key")


def test_direct_adapter_requires_user_address_for_positions() -> None:
    gateway = DirectLiveTradingGateway()

    try:
        gateway.list_positions()
    except ValueError as exc:
        assert str(exc) == "missing_polymarket_user_address"
    else:
        raise AssertionError("expected missing_polymarket_user_address")


def test_direct_adapter_rejects_non_list_positions_payload(monkeypatch) -> None:
    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return {"positions": []}

    class FakeSession:
        def get(self, url, *, params, timeout):
            return FakeResponse()

    monkeypatch.setattr("pm15min.live.trading.direct_adapter.requests.Session", lambda: FakeSession())

    gateway = DirectLiveTradingGateway(
        data_api_config=DataApiConfig(user_address="0xuser", base_url="https://data-api.polymarket.com")
    )

    try:
        gateway.list_positions()
    except TypeError as exc:
        assert str(exc) == "positions API returned non-list payload"
    else:
        raise AssertionError("expected non-list payload to raise TypeError")


def test_direct_adapter_rejects_unsupported_action(monkeypatch) -> None:
    class FakeOrderArgs:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeMarketOrderArgs:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeOrderType:
        GTC = "GTC"
        FAK = "FAK"
        FOK = "FOK"
        GTD = "GTD"

    class FakeClobClient:
        def __init__(self, host, *, key, chain_id, signature_type, funder) -> None:
            return None

        def create_or_derive_api_creds(self):
            return SimpleNamespace(api_key="k", api_secret="s", api_passphrase="p")

        def set_api_creds(self, creds):
            return None

    _install_fake_py_clob_client(
        monkeypatch,
        clob_client_cls=FakeClobClient,
        order_args_cls=FakeOrderArgs,
        market_order_args_cls=FakeMarketOrderArgs,
        order_type_cls=FakeOrderType,
    )

    gateway = DirectLiveTradingGateway(
        auth_config=TradingAuthConfig(
            host="https://clob.polymarket.com",
            chain_id=137,
            private_key="pk",
            signature_type=2,
            funder_address="0xfunder",
        )
    )

    try:
        gateway.place_order(
            PlaceOrderRequest(
                market_id="market-1",
                token_id="token-up",
                side="UP",
                order_type="GTC",
                price=0.20,
                size=5.0,
                order_kind="limit",
                action="HOLD",
            )
        )
    except ValueError as exc:
        assert str(exc) == "unsupported_action:HOLD"
    else:
        raise AssertionError("expected unsupported action to raise ValueError")


def test_direct_adapter_limit_sell_and_cancel_failure_paths(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeOrderArgs:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeMarketOrderArgs:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeOrderType:
        GTC = "GTC"
        FAK = "FAK"
        FOK = "FOK"
        GTD = "GTD"

    class FakeClobClient:
        def __init__(self, host, *, key, chain_id, signature_type, funder) -> None:
            return None

        def create_or_derive_api_creds(self):
            return SimpleNamespace(api_key="k", api_secret="s", api_passphrase="p")

        def set_api_creds(self, creds):
            return None

        def create_market_order(self, args):
            captured["market_order_args"] = args
            return {"signed": "market"}

        def create_order(self, args):
            captured["order_args"] = args
            return {"signed": "limit"}

        def post_order(self, order, order_type):
            captured["posted_order_type"] = order_type
            return {"success": False, "status": "rejected", "message": "no_liquidity"}

        def cancel(self, order_id):
            return {"success": False, "message": "order_not_found"}

    _install_fake_py_clob_client(
        monkeypatch,
        clob_client_cls=FakeClobClient,
        order_args_cls=FakeOrderArgs,
        market_order_args_cls=FakeMarketOrderArgs,
        order_type_cls=FakeOrderType,
    )

    gateway = DirectLiveTradingGateway(
        auth_config=TradingAuthConfig(
            host="https://clob.polymarket.com",
            chain_id=137,
            private_key="pk",
            signature_type=2,
            funder_address="0xfunder",
        )
    )

    place_result = gateway.place_order(
        PlaceOrderRequest(
            market_id="market-1",
            token_id="token-down",
            side="DOWN",
            order_type="GTC",
            price=0.41,
            size=3.0,
            order_kind="limit",
            action="SELL",
        )
    )
    assert place_result.success is False
    assert place_result.status == "rejected"
    assert place_result.message == "no_liquidity"
    assert "order_args" in captured
    assert "market_order_args" not in captured
    assert captured["order_args"].side == "SELL"
    assert captured["order_args"].price == 0.41
    assert captured["posted_order_type"] == "GTC"

    cancel_result = gateway.cancel_order("missing-order")
    assert cancel_result.success is False
    assert cancel_result.status == "cancel_order_failed"
    assert cancel_result.message == "order_not_found"


def test_direct_adapter_treats_invalid_cancel_response_as_failure(monkeypatch) -> None:
    class FakeOrderArgs:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeMarketOrderArgs:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    class FakeOrderType:
        GTC = "GTC"
        FAK = "FAK"
        FOK = "FOK"
        GTD = "GTD"

    class FakeClobClient:
        def __init__(self, host, *, key, chain_id, signature_type, funder) -> None:
            return None

        def create_or_derive_api_creds(self):
            return SimpleNamespace(api_key="k", api_secret="s", api_passphrase="p")

        def set_api_creds(self, creds):
            return None

        def cancel(self, order_id):
            return None

    _install_fake_py_clob_client(
        monkeypatch,
        clob_client_cls=FakeClobClient,
        order_args_cls=FakeOrderArgs,
        market_order_args_cls=FakeMarketOrderArgs,
        order_type_cls=FakeOrderType,
    )

    gateway = DirectLiveTradingGateway(
        auth_config=TradingAuthConfig(
            host="https://clob.polymarket.com",
            chain_id=137,
            private_key="pk",
            signature_type=2,
            funder_address="0xfunder",
        )
    )

    cancel_result = gateway.cancel_order("missing-order")
    assert cancel_result.success is False
    assert cancel_result.status == "cancel_order_failed"
    assert cancel_result.message == "cancel_order_response_invalid"
