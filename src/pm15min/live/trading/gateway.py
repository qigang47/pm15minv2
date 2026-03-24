from __future__ import annotations

from typing import Protocol

from .contracts import (
    CancelOrderResult,
    OpenOrderRecord,
    PlaceOrderRequest,
    PlaceOrderResult,
    PositionRecord,
    RedeemRequest,
    RedeemResult,
)


class LiveTradingGateway(Protocol):
    def list_open_orders(self) -> list[OpenOrderRecord]: ...

    def list_positions(self) -> list[PositionRecord]: ...

    def get_cash_balance(self) -> float | None: ...

    def place_order(self, request: PlaceOrderRequest) -> PlaceOrderResult: ...

    def cancel_order(self, order_id: str) -> CancelOrderResult: ...

    def redeem_positions(self, request: RedeemRequest) -> RedeemResult: ...
