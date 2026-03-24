from __future__ import annotations

import pandas as pd
import requests

from .contracts import (
    CancelOrderResult,
    DataApiConfig,
    OpenOrderRecord,
    PlaceOrderRequest,
    PlaceOrderResult,
    PositionRecord,
    RedeemRequest,
    RedeemResult,
    TradingAuthConfig,
)
from .normalize import first_string, normalize_open_order_row
from .positions_api import list_positions_from_data_api


class LegacyLiveTradingGateway:
    adapter_name = "legacy"

    def __init__(
        self,
        *,
        auth_config: TradingAuthConfig | None = None,
        data_api_config: DataApiConfig | None = None,
    ) -> None:
        self.auth_config = auth_config or TradingAuthConfig()
        self.data_api_config = data_api_config or DataApiConfig()

    def list_open_orders(self) -> list[OpenOrderRecord]:
        trader = self._build_trader()
        raw_orders = trader.get_open_orders()
        if not isinstance(raw_orders, list):
            return []
        return [normalize_open_order_row(raw) for raw in raw_orders if isinstance(raw, dict)]

    def list_positions(self) -> list[PositionRecord]:
        return list_positions_from_data_api(
            self.data_api_config,
            session_factory=requests.Session,
        )

    def get_cash_balance(self) -> float | None:
        trader = self._build_trader()
        balance = trader.get_cash_balance()
        try:
            if balance is None:
                return None
            return float(balance)
        except Exception:
            return None

    def place_order(self, request: PlaceOrderRequest) -> PlaceOrderResult:
        from live_trading.infra.polymarket_client import OrderRequest

        trader = self._build_trader()
        legacy_request = OrderRequest(
            market_id=str(request.market_id),
            token_id=str(request.token_id),
            side=str(request.side),
            order_type=str(request.order_type),
            price=float(request.price),
            size=float(request.size),
            order_kind=str(request.order_kind),
            decision_ts=None
            if request.decision_ts is None
            else pd.to_datetime(request.decision_ts, utc=True, errors="coerce"),
            action=str(request.action),
            metadata=dict(request.metadata or {}),
        )
        resp = trader.place_order(legacy_request)
        return PlaceOrderResult(
            success=bool(resp.success),
            status=str(resp.status or ""),
            order_id=resp.order_id,
            message=resp.message,
            raw={
                "success": bool(resp.success),
                "status": str(resp.status or ""),
                "order_id": resp.order_id,
                "message": resp.message,
            },
        )

    def cancel_order(self, order_id: str) -> CancelOrderResult:
        trader = self._build_trader()
        success = bool(trader.cancel_order(order_id))
        return CancelOrderResult(
            success=success,
            status="cancelled" if success else "cancel_order_failed",
            order_id=order_id,
            message=None if success else "cancel_order_failed",
            raw={"order_id": order_id, "success": success},
        )

    def redeem_positions(self, request: RedeemRequest) -> RedeemResult:
        from live_trading.runners.auto_redeem import redeem_positions

        out = redeem_positions(request.condition_id, index_sets=list(request.index_sets))
        payload = out if isinstance(out, dict) else {}
        tx_hash = first_string(payload, "tx_hash", "txHash", "transaction_hash")
        state = first_string(payload, "state", "status")
        success = bool(tx_hash or state not in {None, "", "error"})
        return RedeemResult(
            success=success,
            status=state or ("ok" if success else "error"),
            tx_hash=tx_hash,
            state=state,
            message=None if success else first_string(payload, "message", "error"),
            raw=payload,
        )

    def _build_trader(self):
        if not self.auth_config.is_configured:
            raise ValueError("missing_polymarket_private_key")
        from live_trading.configs.shared import PolymarketAuth
        from live_trading.infra.polymarket_client import PolymarketTrader

        return PolymarketTrader(
            PolymarketAuth(
                host=str(self.auth_config.host),
                private_key=str(self.auth_config.private_key),
                signature_type=int(self.auth_config.signature_type),
                funder_address=self.auth_config.funder_address,
            )
        )
