from __future__ import annotations

from typing import Any

import requests

from .contracts import (
    CancelOrderResult,
    DataApiConfig,
    PlaceOrderRequest,
    PlaceOrderResult,
    RedeemRelayConfig,
    RedeemRequest,
    TradingAuthConfig,
)
from .normalize import first_string, normalize_open_order_row
from .positions_api import list_positions_from_data_api
from .redeem_relayer import redeem_positions_via_relayer


class DirectLiveTradingGateway:
    adapter_name = "direct"

    def __init__(
        self,
        *,
        auth_config: TradingAuthConfig | None = None,
        data_api_config: DataApiConfig | None = None,
        redeem_config: RedeemRelayConfig | None = None,
    ) -> None:
        self.auth_config = auth_config or TradingAuthConfig()
        self.data_api_config = data_api_config or DataApiConfig()
        self.redeem_config = redeem_config or RedeemRelayConfig()
        self._client = None

    def list_open_orders(self):
        client = self._build_client()
        rows = client.get_orders()
        if not isinstance(rows, list):
            return []
        return [normalize_open_order_row(row) for row in rows if isinstance(row, dict)]

    def list_positions(self):
        return list_positions_from_data_api(
            self.data_api_config,
            session_factory=requests.Session,
        )

    def get_cash_balance(self) -> float | None:
        client = self._build_client()
        params = _balance_allowance_params_cls()(asset_type=_asset_type_cls().COLLATERAL)
        resp = client.get_balance_allowance(params)
        amount = _extract_collateral_balance_amount(resp)
        if amount is None:
            return None
        return float(amount)

    def place_order(self, request: PlaceOrderRequest):
        client = self._build_client()
        side = _normalize_action_side(request.action)
        order_kind = str(request.order_kind or "market").strip().lower()
        order_type = _resolve_order_type(request.order_type)

        if side == "BUY" and order_kind != "limit":
            args = _market_order_args_cls()(
                token_id=str(request.token_id),
                amount=float(request.price) * float(request.size),
                side=side,
                price=float(request.price),
                order_type=order_type,
            )
            signed = client.create_market_order(args)
        else:
            args = _order_args_cls()(
                token_id=str(request.token_id),
                price=float(request.price),
                size=float(request.size),
                side=side,
            )
            signed = client.create_order(args)

        resp = client.post_order(signed, order_type)
        payload = resp if isinstance(resp, dict) else {}
        return PlaceOrderResult(
            success=bool(payload.get("success", False)),
            status=str(payload.get("status") or ""),
            order_id=first_string(payload, "orderID", "orderId", "id"),
            message=first_string(payload, "errorMsg", "message", "error"),
            raw=payload,
        )

    def cancel_order(self, order_id: str):
        client = self._build_client()
        resp = client.cancel(order_id)
        payload = resp if isinstance(resp, dict) else {}
        success = _cancel_success(payload)
        status = "cancelled" if success else (first_string(payload, "status") or "cancel_order_failed")
        message = None if success else (
            first_string(payload, "errorMsg", "message", "error")
            or ("cancel_order_response_invalid" if not payload else status)
        )
        return CancelOrderResult(
            success=success,
            status=status,
            order_id=order_id,
            message=message,
            raw=payload if payload else {"order_id": order_id, "success": success},
        )

    def redeem_positions(self, request: RedeemRequest):
        return redeem_positions_via_relayer(
            auth_config=self.auth_config,
            relay_config=self.redeem_config,
            request=request,
        )

    def _build_client(self):
        if self._client is not None:
            return self._client
        if not self.auth_config.is_configured:
            raise ValueError("missing_polymarket_private_key")
        client_cls = _clob_client_cls()
        client = client_cls(
            str(self.auth_config.host),
            key=str(self.auth_config.private_key),
            chain_id=int(self.auth_config.chain_id),
            signature_type=int(self.auth_config.signature_type),
            funder=self.auth_config.funder_address,
        )
        creds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        self._client = client
        return client


def _clob_client_cls():
    from py_clob_client.client import ClobClient

    return ClobClient


def _order_args_cls():
    from py_clob_client.clob_types import OrderArgs

    return OrderArgs


def _market_order_args_cls():
    from py_clob_client.clob_types import MarketOrderArgs

    return MarketOrderArgs


def _balance_allowance_params_cls():
    from py_clob_client.clob_types import BalanceAllowanceParams

    return BalanceAllowanceParams


def _asset_type_cls():
    from py_clob_client.clob_types import AssetType

    return AssetType


def _resolve_order_type(token: str):
    from py_clob_client.clob_types import OrderType

    normalized = str(token or "GTC").strip().upper()
    return getattr(OrderType, normalized, OrderType.GTC)


def _normalize_action_side(action: str | None) -> str:
    token = str(action or "BUY").strip().upper()
    if token in {"BUY", "SELL"}:
        return token
    raise ValueError(f"unsupported_action:{action}")


def _cancel_success(payload: dict[str, Any]) -> bool:
    if not payload:
        return False
    success = payload.get("success")
    if success is not None:
        return bool(success)
    status = str(payload.get("status") or "").strip().lower()
    return status in {"ok", "success", "cancelled", "canceled"}


def _extract_balance_amount(payload: Any) -> float | None:
    try:
        if payload is None:
            return None
        if isinstance(payload, (int, float, str)):
            out = float(payload)
            if out != out:
                return None
            return out
        if isinstance(payload, dict):
            for key in (
                "balance",
                "available",
                "free",
                "amount",
                "total",
                "balanceAllowance",
                "balance_allowance",
            ):
                if key not in payload:
                    continue
                out = _extract_balance_amount(payload.get(key))
                if out is not None:
                    return out
            nested = payload.get("data")
            if nested is not None:
                return _extract_balance_amount(nested)
        if isinstance(payload, list):
            for item in payload:
                out = _extract_balance_amount(item)
                if out is not None:
                    return out
    except Exception:
        return None
    return None


def _extract_balance_scalar(payload: Any) -> Any | None:
    try:
        if payload is None:
            return None
        if isinstance(payload, (int, float, str)):
            return payload
        if isinstance(payload, dict):
            for key in (
                "balance",
                "available",
                "free",
                "amount",
                "total",
                "balanceAllowance",
                "balance_allowance",
            ):
                if key not in payload:
                    continue
                out = _extract_balance_scalar(payload.get(key))
                if out is not None:
                    return out
            nested = payload.get("data")
            if nested is not None:
                return _extract_balance_scalar(nested)
        if isinstance(payload, list):
            for item in payload:
                out = _extract_balance_scalar(item)
                if out is not None:
                    return out
    except Exception:
        return None
    return None


def _extract_collateral_balance_amount(payload: Any) -> float | None:
    raw = _extract_balance_scalar(payload)
    if raw is None:
        return None
    normalized = _normalize_collateral_balance_scalar(raw)
    if normalized is not None:
        return normalized
    return _extract_balance_amount(payload)


def _normalize_collateral_balance_scalar(value: Any) -> float | None:
    try:
        if isinstance(value, str):
            token = value.strip()
            if not token:
                return None
            if any(ch in token for ch in ".eE"):
                out = float(token)
                return None if out != out else out
            return int(token) / 1_000_000.0
        if isinstance(value, int):
            return float(value) / 1_000_000.0
        if isinstance(value, float):
            if value != value:
                return None
            if value.is_integer():
                return float(int(value)) / 1_000_000.0
            return float(value)
    except Exception:
        return None
    return None
