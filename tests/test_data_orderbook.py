from __future__ import annotations

from pm15min.data.contracts import OrderbookSnapshotRecord
from pm15min.data.pipelines.orderbook_recording import _index_row_from_snapshot


def test_index_row_from_snapshot() -> None:
    snapshot = OrderbookSnapshotRecord(
        captured_ts_ms=1234567890000,
        source_ts_ms=1234567890001,
        market_id="m1",
        token_id="t1",
        side="up",
        asset="sol",
        cycle="15m",
        asks=[{"price": 0.12, "size": 10.0}, {"price": 0.13, "size": 5.0}],
        bids=[{"price": 0.11, "size": 8.0}],
    )

    row = _index_row_from_snapshot(snapshot).to_row()
    assert row["best_ask"] == 0.12
    assert row["best_bid"] == 0.11
    assert row["ask_size_1"] == 10.0
    assert row["bid_size_1"] == 8.0
    assert row["spread"] == 0.01


def test_snapshot_to_row_includes_legacy_time_fields_and_offset() -> None:
    snapshot = OrderbookSnapshotRecord(
        captured_ts_ms=1766398680123,  # 2025-12-22T10:18:00.123Z
        source_ts_ms=1766398680456,
        market_id="m1",
        token_id="t1",
        side="up",
        asset="sol",
        cycle="15m",
        asks=[{"price": 0.12, "size": 10.0}],
        bids=[{"price": 0.11, "size": 8.0}],
    )

    row = snapshot.to_row()
    assert row["logged_at"] == "2025-12-22T10:18:00.123000+00:00"
    assert row["orderbook_ts"] == "2025-12-22T10:18:00.456000+00:00"
    assert row["decision_ts"] == "2025-12-22T10:18:00+00:00"
    assert row["offset"] == 3
