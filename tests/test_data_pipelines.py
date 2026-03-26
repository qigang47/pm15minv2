from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path

import pandas as pd
import pytest

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.pipelines.binance_klines import sync_binance_klines_1m
from pm15min.data.pipelines.market_catalog import sync_market_catalog
from pm15min.data.pipelines.orderbook_recording import _select_markets, record_orderbooks_once
from pm15min.data.pipelines.orderbook_recent import update_recent_orderbook_index


class _FakeGammaClient:
    def fetch_closed_events(self, *, start_ts: int, end_ts: int, limit: int, max_pages: int, sleep_sec: float):
        return [
            {
                "id": "event-1",
                "slug": "btc-up-or-down-15m-1700000000",
                "title": "Bitcoin Up or Down 15m",
                "seriesSlug": "btc-up-or-down-15m",
                "resolutionSource": "https://data.chain.link/streams/btc-usd",
                "closed": True,
                "markets": [
                    {
                        "id": "market-1",
                        "conditionId": "cond-1",
                        "slug": "btc-up-or-down-15m-1700000000",
                        "question": "Bitcoin Up or Down",
                        "endDate": "2023-11-14T22:28:20Z",
                        "closedTime": "2023-11-14T22:29:00Z",
                        "outcomes": ["Up", "Down"],
                        "clobTokenIds": ["token-up", "token-down"],
                    }
                ],
            }
        ]

    def fetch_active_markets(self, *, start_ts: int, end_ts: int, limit: int, max_pages: int, sleep_sec: float):
        return [
            {
                "id": "market-live-1",
                "conditionId": "cond-live-1",
                "slug": "sol-up-or-down-15m-1772374800",
                "question": "Solana Up or Down - March 9, 9:00AM-9:15AM ET",
                "resolutionSource": "https://data.chain.link/streams/sol-usd",
                "endDate": "2026-03-09T13:15:00Z",
                "outcomes": ["Up", "Down"],
                "clobTokenIds": ["token-up", "token-down"],
                "active": True,
                "closed": False,
                "events": [
                    {
                        "id": "event-live-1",
                        "slug": "sol-up-or-down-15m-1772374800",
                        "title": "Solana Up or Down 15m",
                        "seriesSlug": "sol-up-or-down-15m",
                        "resolutionSource": "https://data.chain.link/streams/sol-usd",
                    }
                ],
            }
        ]


class _FakeClobClient:
    def fetch_book(self, token_id: str, *, levels: int = 0, timeout_sec: float = 1.2):
        return {
            "timestamp": "2026-03-19T09:00:00Z",
            "asks": [{"price": "0.12", "size": "10"}],
            "bids": [{"price": "0.11", "size": "8"}],
        }


class _FakeBinanceClient:
    def fetch_klines(self, request):
        start = pd.to_datetime(int(request.start_time_ms), unit="ms", utc=True)
        rows = []
        for idx in range(3):
            open_time = start + pd.Timedelta(minutes=idx)
            close_time = open_time + pd.Timedelta(minutes=1) - pd.Timedelta(milliseconds=1)
            rows.append(
                [
                    int(open_time.timestamp() * 1000),
                    100 + idx,
                    101 + idx,
                    99 + idx,
                    100.5 + idx,
                    10 + idx,
                    int(close_time.timestamp() * 1000),
                    1000 + idx,
                    20 + idx,
                    5 + idx,
                    500 + idx,
                    0,
                ]
            )
        return pd.DataFrame(rows, columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore",
        ])


def test_sync_market_catalog_writes_snapshot_and_canonical(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="btc", cycle="15m", root=tmp_path / "v2")
    summary = sync_market_catalog(
        cfg,
        start_ts=1_700_000_000,
        end_ts=1_700_000_900,
        client=_FakeGammaClient(),
        now=datetime(2026, 3, 19, 9, 0, tzinfo=timezone.utc),
    )

    snapshot_path = Path(summary["snapshot_path"])
    canonical_path = Path(summary["canonical_path"])
    assert snapshot_path.exists()
    assert canonical_path.exists()

    snapshot = pd.read_parquet(snapshot_path)
    canonical = pd.read_parquet(canonical_path)
    assert len(snapshot) == 1
    assert len(canonical) == 1
    assert canonical.iloc[0]["market_id"] == "market-1"
    assert canonical.iloc[0]["token_up"] == "token-up"


def test_record_orderbooks_once_writes_depth_and_index(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", root=tmp_path / "v2", market_depth=1)
    market_table = pd.DataFrame(
        [
            {
                "market_id": "market-1",
                "condition_id": "cond-1",
                "asset": "sol",
                "cycle": "15m",
                "cycle_start_ts": 1_710_000_000,
                "cycle_end_ts": 1_910_000_000,
                "token_up": "token-up",
                "token_down": "token-down",
                "slug": "sol-up-or-down-15m-1710000000",
                "question": "Sol up or down",
                "resolution_source": "https://data.chain.link/streams/sol-usd",
                "event_id": "event-1",
                "event_slug": "event-slug",
                "event_title": "title",
                "series_slug": "sol-up-or-down-15m",
                "closed_ts": None,
                "source_snapshot_ts": "2026-03-19T09-00-00Z",
            }
        ]
    )
    write_parquet_atomic(market_table, cfg.layout.market_catalog_table_path)

    summary = record_orderbooks_once(
        cfg,
        client=_FakeClobClient(),
        captured_ts_ms=1_742_374_800_000,
    )

    depth_path = Path(summary["depth_path"])
    index_path = Path(summary["index_path"])
    recent_path = Path(summary["recent_path"])
    latest_full_snapshot_path = Path(summary["latest_full_snapshot_path"])
    assert depth_path.exists()
    assert index_path.exists()
    assert recent_path.exists()
    assert latest_full_snapshot_path.exists()

    index_df = pd.read_parquet(index_path)
    recent_df = pd.read_parquet(recent_path)
    assert len(index_df) == 2
    assert len(recent_df) == 2
    assert set(index_df["token_id"]) == {"token-up", "token-down"}
    assert float(index_df.iloc[0]["best_ask"]) == 0.12
    latest_full_snapshot = __import__("json").loads(latest_full_snapshot_path.read_text(encoding="utf-8"))
    assert latest_full_snapshot["captured_ts_ms"] == 1_742_374_800_000
    assert len(latest_full_snapshot["records"]) == 2


def test_record_orderbooks_once_prunes_recent_window(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", root=tmp_path / "v2", market_depth=1)
    market_table = pd.DataFrame(
        [
            {
                "market_id": "market-1",
                "condition_id": "cond-1",
                "asset": "sol",
                "cycle": "15m",
                "cycle_start_ts": 1_710_000_000,
                "cycle_end_ts": 1_910_000_000,
                "token_up": "token-up",
                "token_down": "token-down",
                "question": "Sol up or down",
                "source_snapshot_ts": "2026-03-19T09-00-00Z",
            }
        ]
    )
    write_parquet_atomic(market_table, cfg.layout.market_catalog_table_path)

    record_orderbooks_once(
        cfg,
        client=_FakeClobClient(),
        captured_ts_ms=1_742_374_800_000,
        recent_window_minutes=15,
    )
    record_orderbooks_once(
        cfg,
        client=_FakeClobClient(),
        captured_ts_ms=1_742_374_800_000 + 16 * 60_000,
        recent_window_minutes=15,
    )

    recent_df = pd.read_parquet(cfg.layout.orderbook_recent_path)
    assert set(recent_df["captured_ts_ms"]) == {1_742_374_800_000 + 16 * 60_000}


def test_record_orderbooks_once_refreshes_stale_live_market_catalog(tmp_path: Path, monkeypatch) -> None:
    cfg = DataConfig.build(market="btc", cycle="5m", surface="live", root=tmp_path / "v2", market_depth=1)
    captured_ts_ms = 1_742_374_800_000
    stale_market_table = pd.DataFrame(
        [
            {
                "market_id": "stale-market-1",
                "condition_id": "cond-stale-1",
                "asset": "btc",
                "cycle": "5m",
                "cycle_start_ts": 1_742_360_000,
                "cycle_end_ts": 1_742_360_300,
                "token_up": "stale-up",
                "token_down": "stale-down",
                "slug": "btc-up-or-down-5m-1742360000",
                "question": "BTC stale up or down",
                "resolution_source": "https://data.chain.link/streams/btc-usd",
                "event_id": "event-stale-1",
                "event_slug": "event-stale",
                "event_title": "stale",
                "series_slug": "btc-up-or-down-5m",
                "closed_ts": None,
                "source_snapshot_ts": "2026-03-19T08-00-00Z",
            }
        ]
    )
    write_parquet_atomic(stale_market_table, cfg.layout.market_catalog_table_path)
    stale_mtime = captured_ts_ms / 1000.0 - 3600.0
    os.utime(cfg.layout.market_catalog_table_path, (stale_mtime, stale_mtime))

    refreshed_market_table = pd.DataFrame(
        [
            {
                "market_id": "fresh-market-1",
                "condition_id": "cond-fresh-1",
                "asset": "btc",
                "cycle": "5m",
                "cycle_start_ts": 1_742_374_500,
                "cycle_end_ts": 1_742_374_900,
                "token_up": "token-up",
                "token_down": "token-down",
                "slug": "btc-up-or-down-5m-1742374500",
                "question": "BTC fresh up or down",
                "resolution_source": "https://data.chain.link/streams/btc-usd",
                "event_id": "event-fresh-1",
                "event_slug": "event-fresh",
                "event_title": "fresh",
                "series_slug": "btc-up-or-down-5m",
                "closed_ts": None,
                "source_snapshot_ts": "2026-03-19T09-00-00Z",
            }
        ]
    )
    refresh_calls: list[dict[str, object]] = []

    def _fake_sync_market_catalog(local_cfg, *, start_ts: int, end_ts: int, client=None, now=None):
        refresh_calls.append(
            {
                "market": local_cfg.asset.slug,
                "cycle": local_cfg.cycle,
                "start_ts": int(start_ts),
                "end_ts": int(end_ts),
            }
        )
        write_parquet_atomic(refreshed_market_table, local_cfg.layout.market_catalog_table_path)
        return {"status": "ok"}

    monkeypatch.setattr("pm15min.data.pipelines.orderbook_recording.sync_market_catalog", _fake_sync_market_catalog)

    summary = record_orderbooks_once(
        cfg,
        client=_FakeClobClient(),
        captured_ts_ms=captured_ts_ms,
    )

    assert len(refresh_calls) == 1
    assert summary["selected_markets"] == 1
    assert summary["selected_market_ids"] == ["fresh-market-1"]
    assert summary["snapshot_rows"] == 2


def test_record_orderbooks_once_live_raises_when_refreshed_catalog_still_has_no_live_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cfg = DataConfig.build(market="btc", cycle="5m", surface="live", root=tmp_path / "v2", market_depth=1)
    captured_ts_ms = 1_742_374_800_000
    stale_market_table = pd.DataFrame(
        [
            {
                "market_id": "stale-market-1",
                "condition_id": "cond-stale-1",
                "asset": "btc",
                "cycle": "5m",
                "cycle_start_ts": 1_742_360_000,
                "cycle_end_ts": 1_742_360_300,
                "token_up": "stale-up",
                "token_down": "stale-down",
                "slug": "btc-up-or-down-5m-1742360000",
                "question": "BTC stale up or down",
                "resolution_source": "https://data.chain.link/streams/btc-usd",
                "event_id": "event-stale-1",
                "event_slug": "event-stale",
                "event_title": "stale",
                "series_slug": "btc-up-or-down-5m",
                "closed_ts": None,
                "source_snapshot_ts": "2026-03-19T08-00-00Z",
            }
        ]
    )
    write_parquet_atomic(stale_market_table, cfg.layout.market_catalog_table_path)
    stale_mtime = captured_ts_ms / 1000.0 - 3600.0
    os.utime(cfg.layout.market_catalog_table_path, (stale_mtime, stale_mtime))
    refresh_calls: list[str] = []

    def _fake_sync_market_catalog(local_cfg, *, start_ts: int, end_ts: int, client=None, now=None):
        del start_ts, end_ts, client, now
        refresh_calls.append(local_cfg.asset.slug)
        write_parquet_atomic(stale_market_table, local_cfg.layout.market_catalog_table_path)
        return {"status": "ok"}

    monkeypatch.setattr("pm15min.data.pipelines.orderbook_recording.sync_market_catalog", _fake_sync_market_catalog)

    with pytest.raises(RuntimeError, match="no active or future markets"):
        record_orderbooks_once(
            cfg,
            client=_FakeClobClient(),
            captured_ts_ms=captured_ts_ms,
        )

    assert refresh_calls == ["btc"]


def test_update_recent_orderbook_index_recovers_corrupt_existing_file(tmp_path: Path) -> None:
    path = tmp_path / "recent.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"not-a-parquet-file")

    incoming = pd.DataFrame(
        [
            {
                "captured_ts_ms": 1_742_374_800_000,
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "best_ask": 0.12,
                "best_bid": 0.11,
                "ask_size_1": 10.0,
                "bid_size_1": 8.0,
                "spread": 0.01,
            }
        ]
    )

    out = update_recent_orderbook_index(
        path=path,
        incoming=incoming,
        now_ts_ms=1_742_374_800_000,
        window_minutes=15,
    )

    assert len(out) == 1
    assert len(pd.read_parquet(path)) == 1
    assert len(list(path.parent.glob("recent.parquet.corrupt.*"))) == 1


def test_record_orderbooks_once_recovers_corrupt_orderbook_index(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", root=tmp_path / "v2", market_depth=1)
    market_table = pd.DataFrame(
        [
            {
                "market_id": "market-1",
                "condition_id": "cond-1",
                "asset": "sol",
                "cycle": "15m",
                "cycle_start_ts": 1_710_000_000,
                "cycle_end_ts": 1_910_000_000,
                "token_up": "token-up",
                "token_down": "token-down",
                "slug": "sol-up-or-down-15m-1710000000",
                "question": "Sol up or down",
                "resolution_source": "https://data.chain.link/streams/sol-usd",
                "event_id": "event-1",
                "event_slug": "event-slug",
                "event_title": "title",
                "series_slug": "sol-up-or-down-15m",
                "closed_ts": None,
                "source_snapshot_ts": "2026-03-19T09-00-00Z",
            }
        ]
    )
    write_parquet_atomic(market_table, cfg.layout.market_catalog_table_path)
    index_path = cfg.layout.orderbook_index_path("2025-03-19")
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_bytes(b"not-a-parquet-file")

    summary = record_orderbooks_once(
        cfg,
        client=_FakeClobClient(),
        captured_ts_ms=1_742_374_800_000,
    )

    rebuilt_index_path = Path(summary["index_path"])
    assert len(pd.read_parquet(rebuilt_index_path)) == 2
    assert len(list(rebuilt_index_path.parent.glob("data.parquet.corrupt.*"))) == 1


def test_select_markets_honors_start_offset() -> None:
    markets = pd.DataFrame(
        [
            {"market_id": "active-0", "cycle_start_ts": 100, "cycle_end_ts": 200},
            {"market_id": "future-1", "cycle_start_ts": 200, "cycle_end_ts": 300},
            {"market_id": "future-2", "cycle_start_ts": 300, "cycle_end_ts": 400},
            {"market_id": "future-3", "cycle_start_ts": 400, "cycle_end_ts": 500},
        ]
    )
    selected = _select_markets(
        markets,
        now_ts=150,
        market_depth=2,
        market_start_offset=2,
    )

    assert selected["market_id"].tolist() == ["future-2", "future-3"]


def test_sync_market_catalog_live_uses_active_markets(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    summary = sync_market_catalog(
        cfg,
        start_ts=1_772_374_800,
        end_ts=1_772_461_200,
        client=_FakeGammaClient(),
        now=datetime(2026, 3, 19, 9, 0, tzinfo=timezone.utc),
    )

    canonical = pd.read_parquet(Path(summary["canonical_path"]))
    assert summary["source_mode"] == "gamma_active_markets"
    assert summary["rows_fetched"] == 1
    assert len(canonical) == 1
    assert canonical.iloc[0]["market_id"] == "market-live-1"
    assert canonical.iloc[0]["token_up"] == "token-up"


def test_sync_binance_klines_1m_appends_new_rows(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "open_time": pd.Timestamp("2026-03-19T00:00:00Z"),
                    "open": 99.0,
                    "high": 100.0,
                    "low": 98.0,
                    "close": 99.5,
                    "volume": 9.0,
                    "close_time": pd.Timestamp("2026-03-19T00:00:59.999Z"),
                    "quote_asset_volume": 900.0,
                    "number_of_trades": 10,
                    "taker_buy_base_volume": 4.0,
                    "taker_buy_quote_volume": 400.0,
                    "ignore": 0.0,
                }
            ]
        ),
        cfg.layout.binance_klines_path(),
    )
    summary = sync_binance_klines_1m(
        cfg,
        client=_FakeBinanceClient(),
        now=datetime(2026, 3, 19, 0, 4, tzinfo=timezone.utc),
    )

    out = pd.read_parquet(cfg.layout.binance_klines_path())
    assert summary["rows_fetched"] == 3
    assert len(out) == 4
    assert out["open_time"].max() == pd.Timestamp("2026-03-19T00:03:00Z")
