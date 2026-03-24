from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class MarketCatalogRecord:
    market_id: str
    condition_id: str
    asset: str
    cycle: str
    cycle_start_ts: int
    cycle_end_ts: int
    token_up: str
    token_down: str
    slug: str
    question: str
    resolution_source: str
    event_id: str
    event_slug: str
    event_title: str
    series_slug: str
    closed_ts: int | None
    source_snapshot_ts: str

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OrderbookSnapshotRecord:
    captured_ts_ms: int
    source_ts_ms: int | None
    market_id: str
    token_id: str
    side: str
    asset: str
    cycle: str
    asks: list[dict[str, float]]
    bids: list[dict[str, float]]
    source: str = "clob"

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OrderbookIndexRow:
    captured_ts_ms: int
    market_id: str
    token_id: str
    side: str
    best_ask: float | None
    best_bid: float | None
    ask_size_1: float | None
    bid_size_1: float | None
    spread: float | None

    def to_row(self) -> dict[str, Any]:
        return asdict(self)
