from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm15min.core.config import LiveConfig
from pm15min.data.config import DataConfig
from pm15min.data.io.json_files import write_json_atomic
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.live.quotes import build_quote_snapshot


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def test_quote_snapshot_uses_latest_in_window_orderbook_row(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    cycle_start = pd.Timestamp("2026-03-12T15:00:00Z")
    cycle_end = cycle_start + pd.Timedelta(minutes=15)
    captured_ts = pd.Timestamp("2026-03-12T15:07:30Z")
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "asset": "sol",
                    "cycle": "15m",
                    "cycle_start_ts": int(cycle_start.timestamp()),
                    "cycle_end_ts": int(cycle_end.timestamp()),
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "slug": f"sol-up-or-down-15m-{int(cycle_start.timestamp())}",
                    "question": "Sol up or down",
                    "resolution_source": "https://data.chain.link/streams/sol-usd",
                    "event_id": "event-1",
                    "event_slug": "slug-1",
                    "event_title": "title-1",
                    "series_slug": "sol-up-or-down-15m",
                    "closed_ts": None,
                    "source_snapshot_ts": "2026-03-19T00-00-00Z",
                }
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "captured_ts_ms": int(captured_ts.timestamp() * 1000),
                    "market_id": "market-1",
                    "token_id": "token-up",
                    "side": "up",
                    "best_ask": 0.51,
                    "best_bid": 0.49,
                    "ask_size_1": 10.0,
                    "bid_size_1": 11.0,
                    "spread": 0.02,
                },
                {
                    "captured_ts_ms": int(captured_ts.timestamp() * 1000),
                    "market_id": "market-1",
                    "token_id": "token-down",
                    "side": "down",
                    "best_ask": 0.61,
                    "best_bid": 0.39,
                    "ask_size_1": 12.0,
                    "bid_size_1": 13.0,
                    "spread": 0.22,
                },
            ]
        ),
        data_cfg.layout.orderbook_index_path("2026-03-12"),
    )
    quote = build_quote_snapshot(
        cfg=cfg,
        signal_payload={
            "target": "direction",
            "snapshot_ts": "manual",
            "offset_signals": [
                {
                    "offset": 7,
                    "decision_ts": "2026-03-12T15:07:00+00:00",
                    "cycle_start_ts": "2026-03-12T15:00:00+00:00",
                }
            ],
        },
        persist=False,
        now=pd.Timestamp("2026-03-12T15:07:45Z"),
    )
    row = quote["quote_rows"][0]
    assert row["status"] == "ok"
    assert row["condition_id"] == "cond-1"
    assert row["cycle_end_ts"] == cycle_end.isoformat()
    assert row["quote_up_ask"] == 0.51
    assert row["quote_down_bid"] == 0.39
    assert row["quote_age_ms_up"] == 15000


def test_quote_snapshot_rejects_expired_signal_window(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    feature_cycle_start = pd.Timestamp("2026-03-20T12:30:00Z")
    feature_cycle_end = feature_cycle_start + pd.Timedelta(minutes=15)
    trade_cycle_start = feature_cycle_end
    trade_cycle_end = trade_cycle_start + pd.Timedelta(minutes=15)
    captured_ts = pd.Timestamp("2026-03-20T12:48:00Z")
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-prev",
                    "condition_id": "cond-prev",
                    "asset": "sol",
                    "cycle": "15m",
                    "cycle_start_ts": int(feature_cycle_start.timestamp()),
                    "cycle_end_ts": int(feature_cycle_end.timestamp()),
                    "token_up": "token-prev-up",
                    "token_down": "token-prev-down",
                    "question": "previous market",
                    "source_snapshot_ts": "2026-03-20T12-40-00Z",
                },
                {
                    "market_id": "market-next",
                    "condition_id": "cond-next",
                    "asset": "sol",
                    "cycle": "15m",
                    "cycle_start_ts": int(trade_cycle_start.timestamp()),
                    "cycle_end_ts": int(trade_cycle_end.timestamp()),
                    "token_up": "token-next-up",
                    "token_down": "token-next-down",
                    "question": "next market",
                    "source_snapshot_ts": "2026-03-20T12-46-00Z",
                },
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "captured_ts_ms": int(captured_ts.timestamp() * 1000),
                    "market_id": "market-next",
                    "token_id": "token-next-up",
                    "side": "up",
                    "best_ask": 0.22,
                    "best_bid": 0.21,
                    "ask_size_1": 8.0,
                    "bid_size_1": 9.0,
                    "spread": 0.01,
                },
                {
                    "captured_ts_ms": int(captured_ts.timestamp() * 1000),
                    "market_id": "market-next",
                    "token_id": "token-next-down",
                    "side": "down",
                    "best_ask": 0.78,
                    "best_bid": 0.77,
                    "ask_size_1": 10.0,
                    "bid_size_1": 11.0,
                    "spread": 0.01,
                },
            ]
        ),
        data_cfg.layout.orderbook_index_path("2026-03-20"),
    )
    quote = build_quote_snapshot(
        cfg=cfg,
        signal_payload={
            "target": "direction",
            "snapshot_ts": "manual",
            "offset_signals": [
                {
                    "offset": 7,
                    "decision_ts": "2026-03-20T12:38:00+00:00",
                    "cycle_start_ts": feature_cycle_start.isoformat(),
                    "cycle_end_ts": feature_cycle_end.isoformat(),
                }
            ],
        },
        persist=False,
        now=pd.Timestamp("2026-03-20T12:48:00Z"),
    )
    row = quote["quote_rows"][0]
    assert row["status"] == "missing_quote_inputs"
    assert row["market_id"] is None
    assert row["condition_id"] is None
    assert row["quote_up_ask"] is None
    assert row["quote_down_ask"] is None
    assert row["reasons"] == ["signal_window_expired"]


def test_quote_snapshot_uses_recent_orderbook_cache_when_daily_index_missing(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    cycle_start = pd.Timestamp("2026-03-21T16:45:00Z")
    cycle_end = cycle_start + pd.Timedelta(minutes=15)
    captured_ts = pd.Timestamp("2026-03-21T16:54:30Z")
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "asset": "sol",
                    "cycle": "15m",
                    "cycle_start_ts": int(cycle_start.timestamp()),
                    "cycle_end_ts": int(cycle_end.timestamp()),
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "question": "Sol up or down",
                    "source_snapshot_ts": "2026-03-21T16-45-00Z",
                }
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "captured_ts_ms": int(captured_ts.timestamp() * 1000),
                    "market_id": "market-1",
                    "token_id": "token-up",
                    "side": "up",
                    "best_ask": 0.24,
                    "best_bid": 0.23,
                    "ask_size_1": 9.0,
                    "bid_size_1": 8.0,
                    "spread": 0.01,
                },
                {
                    "captured_ts_ms": int(captured_ts.timestamp() * 1000),
                    "market_id": "market-1",
                    "token_id": "token-down",
                    "side": "down",
                    "best_ask": 0.76,
                    "best_bid": 0.75,
                    "ask_size_1": 10.0,
                    "bid_size_1": 11.0,
                    "spread": 0.01,
                },
            ]
        ),
        data_cfg.layout.orderbook_recent_path,
    )
    quote = build_quote_snapshot(
        cfg=cfg,
        signal_payload={
            "target": "direction",
            "snapshot_ts": "manual",
            "offset_signals": [
                {
                    "offset": 8,
                    "decision_ts": "2026-03-21T16:54:00+00:00",
                    "cycle_start_ts": cycle_start.isoformat(),
                    "cycle_end_ts": cycle_end.isoformat(),
                }
            ],
        },
        persist=False,
        now=pd.Timestamp("2026-03-21T16:54:35Z"),
    )
    row = quote["quote_rows"][0]
    assert row["status"] == "ok"
    assert row["quote_up_ask"] == 0.24
    assert row["quote_down_ask"] == 0.76


class _FakeOrderbookProvider:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_orderbook_summary(self, token_id: str, *, levels: int = 0, timeout: float = 1.2, force_refresh: bool = False):
        self.calls.append(str(token_id))
        if token_id == "token-up":
            return {
                "timestamp": "2026-03-21T16:54:10Z",
                "asks": [{"price": "0.31", "size": "7"}],
                "bids": [{"price": "0.30", "size": "6"}],
            }
        if token_id == "token-down":
            return {
                "timestamp": "2026-03-21T16:54:10Z",
                "asks": [{"price": "0.69", "size": "8"}],
                "bids": [{"price": "0.68", "size": "5"}],
            }
        return None

    def sync_subscriptions(self, token_ids, *, replace=True, prefetch=False, levels=0, timeout=1.2):
        return {"ok": True}


def test_quote_snapshot_prefers_provider_over_stale_index(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    cycle_start = pd.Timestamp("2026-03-21T16:45:00Z")
    cycle_end = cycle_start + pd.Timedelta(minutes=15)
    stale_ts = pd.Timestamp("2026-03-21T16:58:30Z")
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "asset": "sol",
                    "cycle": "15m",
                    "cycle_start_ts": int(cycle_start.timestamp()),
                    "cycle_end_ts": int(cycle_end.timestamp()),
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "question": "Sol up or down",
                    "source_snapshot_ts": "2026-03-21T16-45-00Z",
                }
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "captured_ts_ms": int(stale_ts.timestamp() * 1000),
                    "market_id": "market-1",
                    "token_id": "token-up",
                    "side": "up",
                    "best_ask": 0.11,
                    "best_bid": 0.10,
                    "ask_size_1": 4.0,
                    "bid_size_1": 3.0,
                    "spread": 0.01,
                },
                {
                    "captured_ts_ms": int(stale_ts.timestamp() * 1000),
                    "market_id": "market-1",
                    "token_id": "token-down",
                    "side": "down",
                    "best_ask": 0.89,
                    "best_bid": 0.88,
                    "ask_size_1": 5.0,
                    "bid_size_1": 4.0,
                    "spread": 0.01,
                },
            ]
        ),
        data_cfg.layout.orderbook_recent_path,
    )
    provider = _FakeOrderbookProvider()
    quote = build_quote_snapshot(
        cfg=cfg,
        signal_payload={
            "target": "direction",
            "snapshot_ts": "manual",
            "offset_signals": [
                {
                    "offset": 8,
                    "decision_ts": "2026-03-21T16:54:00+00:00",
                    "cycle_start_ts": cycle_start.isoformat(),
                    "cycle_end_ts": cycle_end.isoformat(),
                }
            ],
        },
        persist=False,
        now=pd.Timestamp("2026-03-21T16:54:15Z"),
        orderbook_provider=provider,
    )
    row = quote["quote_rows"][0]
    assert row["status"] == "ok"
    assert row["quote_up_ask"] == 0.31
    assert row["quote_down_ask"] == 0.69
    assert provider.calls == ["token-up", "token-down"]


def test_quote_snapshot_prefers_latest_full_snapshot_over_provider_and_recent_index(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    cycle_start = pd.Timestamp("2026-03-21T16:45:00Z")
    cycle_end = cycle_start + pd.Timedelta(minutes=15)
    captured_ts = pd.Timestamp("2026-03-21T16:54:10Z")
    latest_full_ts_ms = int(pd.Timestamp("2026-03-21T16:54:12Z").timestamp() * 1000)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "asset": "sol",
                    "cycle": "15m",
                    "cycle_start_ts": int(cycle_start.timestamp()),
                    "cycle_end_ts": int(cycle_end.timestamp()),
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "question": "Sol up or down",
                    "source_snapshot_ts": "2026-03-21T16-45-00Z",
                }
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "captured_ts_ms": int(captured_ts.timestamp() * 1000),
                    "market_id": "market-1",
                    "token_id": "token-up",
                    "side": "up",
                    "best_ask": 0.24,
                    "best_bid": 0.23,
                    "ask_size_1": 9.0,
                    "bid_size_1": 8.0,
                    "spread": 0.01,
                },
                {
                    "captured_ts_ms": int(captured_ts.timestamp() * 1000),
                    "market_id": "market-1",
                    "token_id": "token-down",
                    "side": "down",
                    "best_ask": 0.76,
                    "best_bid": 0.75,
                    "ask_size_1": 10.0,
                    "bid_size_1": 11.0,
                    "spread": 0.01,
                },
            ]
        ),
        data_cfg.layout.orderbook_recent_path,
    )
    write_json_atomic(
        {
            "dataset": "orderbook_latest_full_snapshot",
            "market": "sol",
            "cycle": "15m",
            "captured_ts_ms": latest_full_ts_ms,
            "records": [
                {
                    "captured_ts_ms": latest_full_ts_ms,
                    "source_ts_ms": latest_full_ts_ms,
                    "market_id": "market-1",
                    "token_id": "token-up",
                    "side": "up",
                    "asks": [{"price": 0.27, "size": 7.0}],
                    "bids": [{"price": 0.26, "size": 6.0}],
                },
                {
                    "captured_ts_ms": latest_full_ts_ms,
                    "source_ts_ms": latest_full_ts_ms,
                    "market_id": "market-1",
                    "token_id": "token-down",
                    "side": "down",
                    "asks": [{"price": 0.73, "size": 8.0}],
                    "bids": [{"price": 0.72, "size": 5.0}],
                },
            ],
        },
        data_cfg.layout.orderbook_latest_full_snapshot_path,
    )
    provider = _FakeOrderbookProvider()
    quote = build_quote_snapshot(
        cfg=cfg,
        signal_payload={
            "target": "direction",
            "snapshot_ts": "manual",
            "offset_signals": [
                {
                    "offset": 8,
                    "decision_ts": "2026-03-21T16:54:00+00:00",
                    "cycle_start_ts": cycle_start.isoformat(),
                    "cycle_end_ts": cycle_end.isoformat(),
                }
            ],
        },
        persist=False,
        now=pd.Timestamp("2026-03-21T16:54:15Z"),
        orderbook_provider=provider,
    )
    row = quote["quote_rows"][0]
    assert row["status"] == "ok"
    assert row["quote_up_ask"] == 0.27
    assert row["quote_down_ask"] == 0.73
    assert provider.calls == []
    assert row["quote_source_path"] == str(data_cfg.layout.orderbook_latest_full_snapshot_path)


def test_quote_snapshot_loads_index_once_per_iteration(tmp_path: Path, monkeypatch) -> None:
    import pm15min.live.quotes.orderbook as orderbook_module

    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    cycle_start = pd.Timestamp("2026-03-21T16:45:00Z")
    cycle_end = cycle_start + pd.Timedelta(minutes=15)
    captured_ts = pd.Timestamp("2026-03-21T16:54:10Z")
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "asset": "sol",
                    "cycle": "15m",
                    "cycle_start_ts": int(cycle_start.timestamp()),
                    "cycle_end_ts": int(cycle_end.timestamp()),
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "question": "Sol up or down",
                    "source_snapshot_ts": "2026-03-21T16-45-00Z",
                }
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "captured_ts_ms": int(captured_ts.timestamp() * 1000),
                    "market_id": "market-1",
                    "token_id": "token-up",
                    "side": "up",
                    "best_ask": 0.24,
                    "best_bid": 0.23,
                    "ask_size_1": 9.0,
                    "bid_size_1": 8.0,
                    "spread": 0.01,
                },
                {
                    "captured_ts_ms": int(captured_ts.timestamp() * 1000),
                    "market_id": "market-1",
                    "token_id": "token-down",
                    "side": "down",
                    "best_ask": 0.76,
                    "best_bid": 0.75,
                    "ask_size_1": 10.0,
                    "bid_size_1": 11.0,
                    "spread": 0.01,
                },
            ]
        ),
        data_cfg.layout.orderbook_index_path("2026-03-21"),
    )
    original_loader = orderbook_module.load_orderbook_index_frame
    calls = {"count": 0}

    def _counting_loader(*, index_path, recent_path=None):
        calls["count"] += 1
        return original_loader(index_path=index_path, recent_path=recent_path)

    monkeypatch.setattr(orderbook_module, "load_orderbook_index_frame", _counting_loader)

    quote = build_quote_snapshot(
        cfg=cfg,
        signal_payload={
            "target": "direction",
            "snapshot_ts": "manual",
            "offset_signals": [
                {
                    "offset": 7,
                    "decision_ts": "2026-03-21T16:54:00+00:00",
                    "cycle_start_ts": cycle_start.isoformat(),
                    "cycle_end_ts": cycle_end.isoformat(),
                },
                {
                    "offset": 8,
                    "decision_ts": "2026-03-21T16:54:00+00:00",
                    "cycle_start_ts": cycle_start.isoformat(),
                    "cycle_end_ts": cycle_end.isoformat(),
                },
                {
                    "offset": 9,
                    "decision_ts": "2026-03-21T16:54:00+00:00",
                    "cycle_start_ts": cycle_start.isoformat(),
                    "cycle_end_ts": cycle_end.isoformat(),
                },
            ],
        },
        persist=False,
        now=pd.Timestamp("2026-03-21T16:54:15Z"),
    )

    assert [row["status"] for row in quote["quote_rows"]] == ["ok", "ok", "ok"]
    assert calls["count"] == 1
