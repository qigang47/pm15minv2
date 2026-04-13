from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from pmshared.time import cycle_seconds


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
        decision_ts_ms = int(self.captured_ts_ms // 60_000) * 60_000
        cycle_minutes = max(1, int(cycle_seconds(self.cycle) // 60))
        offset = int((decision_ts_ms // 60_000) % cycle_minutes)
        logged_at = datetime.fromtimestamp(self.captured_ts_ms / 1000.0, tz=timezone.utc).isoformat()
        decision_ts = datetime.fromtimestamp(decision_ts_ms / 1000.0, tz=timezone.utc).isoformat()
        orderbook_ts_ms = int(self.source_ts_ms) if self.source_ts_ms is not None else int(self.captured_ts_ms)
        orderbook_ts = datetime.fromtimestamp(orderbook_ts_ms / 1000.0, tz=timezone.utc).isoformat()
        return {
            "captured_ts_ms": self.captured_ts_ms,
            "source_ts_ms": self.source_ts_ms,
            "market_id": self.market_id,
            "token_id": self.token_id,
            "side": self.side,
            "logged_at": logged_at,
            "orderbook_ts": orderbook_ts,
            "decision_ts": decision_ts,
            "offset": offset,
            "asset": self.asset,
            "cycle": self.cycle,
            "asks": self.asks,
            "bids": self.bids,
            "source": self.source,
        }


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
        return {
            "captured_ts_ms": self.captured_ts_ms,
            "market_id": self.market_id,
            "token_id": self.token_id,
            "side": self.side,
            "best_ask": self.best_ask,
            "best_bid": self.best_bid,
            "ask_size_1": self.ask_size_1,
            "bid_size_1": self.bid_size_1,
            "spread": self.spread,
        }
