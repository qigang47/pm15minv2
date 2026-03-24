from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class TradingAuthConfig:
    host: str = "https://clob.polymarket.com"
    chain_id: int = 137
    private_key: str = ""
    signature_type: int = 2
    funder_address: str | None = None

    @property
    def is_configured(self) -> bool:
        return bool(str(self.private_key or "").strip())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DataApiConfig:
    user_address: str | None = None
    base_url: str = "https://data-api.polymarket.com"

    @property
    def is_configured(self) -> bool:
        return bool(str(self.user_address or "").strip())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RedeemRelayConfig:
    rpc_urls: tuple[str, ...] = ()
    relayer_url: str = "https://relayer-v2.polymarket.com/"
    builder_api_key: str = ""
    builder_secret: str = ""
    builder_passphrase: str = ""

    @property
    def is_configured(self) -> bool:
        return bool(
            self.rpc_urls
            and str(self.builder_api_key or "").strip()
            and str(self.builder_secret or "").strip()
            and str(self.builder_passphrase or "").strip()
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OpenOrderRecord:
    order_id: str | None
    market_id: str | None
    token_id: str | None
    side: str | None
    status: str | None
    price: float | None
    size: float | None
    created_at: str | None
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PositionRecord:
    market_id: str | None
    condition_id: str | None
    token_id: str | None
    size: float
    redeemable: bool
    outcome_index: int | None
    index_set: int | None
    current_value: float
    cash_pnl: float
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PlaceOrderRequest:
    market_id: str
    token_id: str
    side: str
    order_type: str
    price: float
    size: float
    order_kind: str = "market"
    action: str = "BUY"
    decision_ts: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PlaceOrderResult:
    success: bool
    status: str
    order_id: str | None = None
    message: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CancelOrderResult:
    success: bool
    status: str
    order_id: str | None = None
    message: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RedeemRequest:
    condition_id: str
    index_sets: list[int]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RedeemResult:
    success: bool
    status: str
    tx_hash: str | None = None
    state: str | None = None
    message: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
