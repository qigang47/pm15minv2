from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.ndjson_zst import append_ndjson_zst
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.pipelines.oracle_prices import build_oracle_prices_15m
from pm15min.data.pipelines.orderbook_recording import build_orderbook_index_from_depth
from pm15min.data.pipelines.source_ingest import (
    import_legacy_market_catalog,
    import_legacy_orderbook_depth,
    import_legacy_settlement_truth,
    import_legacy_streams,
)
from pm15min.data.pipelines.truth import build_truth_15m


def test_import_legacy_streams_partitions_by_month(tmp_path: Path) -> None:
    source = tmp_path / "streams.csv"
    pd.DataFrame(
        [
            {
                "asset": "btc",
                "tx_hash": "0x1",
                "extra_ts": 1700000000,
                "observation_ts": 1700000000,
                "benchmark_price_raw": 100_0000000000000000000,
                "report_feed_id": "feed",
                "requester": "req",
                "path": "keeper_transmit",
                "perform_idx": 0,
                "value_idx": 0,
            }
        ]
    ).to_csv(source, index=False)

    cfg = DataConfig.build(market="btc", cycle="15m", root=tmp_path / "v2")
    summary = import_legacy_streams(cfg, source_path=source)
    assert summary["rows_imported"] == 1

    paths = list(cfg.layout.streams_source_root.glob("year=*/month=*/data.parquet"))
    assert len(paths) == 1
    df = pd.read_parquet(paths[0])
    assert df.iloc[0]["asset"] == "btc"
    assert float(df.iloc[0]["price"]) == 1000.0


def test_build_oracle_prices_and_truth_from_imported_sources(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="btc", cycle="15m", surface="backtest", root=tmp_path / "v2")

    market_table = pd.DataFrame(
        [
            {
                "market_id": "market-1",
                "condition_id": "cond-1",
                "asset": "btc",
                "cycle": "15m",
                "cycle_start_ts": 1700000000,
                "cycle_end_ts": 1700000900,
                "token_up": "token-up",
                "token_down": "token-down",
                "slug": "btc-up-or-down-15m-1700000000",
                "question": "Bitcoin Up or Down",
                "resolution_source": "https://data.chain.link/streams/btc-usd",
                "event_id": "event-1",
                "event_slug": "slug-1",
                "event_title": "title-1",
                "series_slug": "btc-up-or-down-15m",
                "closed_ts": None,
                "source_snapshot_ts": "2026-03-19T09-00-00Z",
            }
        ]
    )
    write_parquet_atomic(market_table, cfg.layout.market_catalog_table_path)

    streams_source = tmp_path / "streams.csv"
    pd.DataFrame(
        [
            {
                "asset": "btc",
                "tx_hash": "0x1",
                "extra_ts": 1700000000,
                "observation_ts": 1700000000,
                "benchmark_price_raw": 100_0000000000000000000,
                "report_feed_id": "feed",
                "requester": "req",
                "path": "keeper_transmit",
                "perform_idx": 0,
                "value_idx": 0,
            },
            {
                "asset": "btc",
                "tx_hash": "0x2",
                "extra_ts": 1700000900,
                "observation_ts": 1700000900,
                "benchmark_price_raw": 110_0000000000000000000,
                "report_feed_id": "feed",
                "requester": "req",
                "path": "keeper_transmit",
                "perform_idx": 0,
                "value_idx": 0,
            },
        ]
    ).to_csv(streams_source, index=False)
    import_legacy_streams(cfg, source_path=streams_source)

    oracle_summary = build_oracle_prices_15m(cfg)
    oracle = pd.read_parquet(cfg.layout.oracle_prices_table_path)
    assert oracle_summary["rows_written"] == 1
    assert float(oracle.iloc[0]["price_to_beat"]) == 1000.0
    assert float(oracle.iloc[0]["final_price"]) == 1100.0
    assert bool(oracle.iloc[0]["has_both"]) is True

    settlement_source = tmp_path / "settlement_truth.csv"
    pd.DataFrame(
        [
            {
                "market_id": "market-1",
                "condition_id": "cond-1",
                "asset": "btc",
                "end_ts": 1700000900,
                "winner_side": "UP",
                "label_updown": "UP",
                "onchain_resolved": True,
                "stream_match_exact": True,
                "full_truth": True,
                "stream_price": 1100.0,
                "stream_extra_ts": 1700000900,
                "slug": "btc-up-or-down-15m-1700000000",
                "question": "Bitcoin Up or Down",
                "resolution_source": "https://data.chain.link/streams/btc-usd",
            }
        ]
    ).to_csv(settlement_source, index=False)
    import_legacy_settlement_truth(cfg, source_path=settlement_source)

    truth_summary = build_truth_15m(cfg)
    truth = pd.read_parquet(cfg.layout.truth_table_path)
    assert truth_summary["rows_written"] == 1
    assert truth.iloc[0]["truth_source"] == "settlement_truth"
    assert truth.iloc[0]["winner_side"] == "UP"
    assert bool(truth.iloc[0]["full_truth"]) is True


def test_build_truth_preserves_chainlink_source_granularity_from_oracle_prices(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=tmp_path / "v2")
    market_table = pd.DataFrame(
        [
            {
                "market_id": "market-1",
                "condition_id": "cond-1",
                "asset": "sol",
                "cycle": "15m",
                "cycle_start_ts": 1700000000,
                "cycle_end_ts": 1700000900,
                "token_up": "token-up",
                "token_down": "token-down",
                "slug": "sol-up-or-down-15m-1700000000",
                "question": "Sol Up or Down",
                "resolution_source": "https://data.chain.link/streams/sol-usd",
                "event_id": "event-1",
                "event_slug": "slug-1",
                "event_title": "title-1",
                "series_slug": "sol-up-or-down-15m",
                "closed_ts": None,
                "source_snapshot_ts": "2026-03-19T09-00-00Z",
            },
            {
                "market_id": "market-2",
                "condition_id": "cond-2",
                "asset": "sol",
                "cycle": "15m",
                "cycle_start_ts": 1700000900,
                "cycle_end_ts": 1700001800,
                "token_up": "token-up-2",
                "token_down": "token-down-2",
                "slug": "sol-up-or-down-15m-1700000900",
                "question": "Sol Up or Down",
                "resolution_source": "https://data.chain.link/streams/sol-usd",
                "event_id": "event-2",
                "event_slug": "slug-2",
                "event_title": "title-2",
                "series_slug": "sol-up-or-down-15m",
                "closed_ts": None,
                "source_snapshot_ts": "2026-03-19T09-00-00Z",
            },
            {
                "market_id": "market-3",
                "condition_id": "cond-3",
                "asset": "sol",
                "cycle": "15m",
                "cycle_start_ts": 1700001800,
                "cycle_end_ts": 1700002700,
                "token_up": "token-up-3",
                "token_down": "token-down-3",
                "slug": "sol-up-or-down-15m-1700001800",
                "question": "Sol Up or Down",
                "resolution_source": "https://data.chain.link/streams/sol-usd",
                "event_id": "event-3",
                "event_slug": "slug-3",
                "event_title": "title-3",
                "series_slug": "sol-up-or-down-15m",
                "closed_ts": None,
                "source_snapshot_ts": "2026-03-19T09-00-00Z",
            },
        ]
    )
    oracle_table = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1700000000,
                "cycle_end_ts": 1700000900,
                "price_to_beat": 100.0,
                "final_price": 101.0,
                "source_price_to_beat": "chainlink_streams_rpc",
                "source_final_price": "chainlink_streams_rpc",
                "has_price_to_beat": True,
                "has_final_price": True,
                "has_both": True,
            },
            {
                "asset": "sol",
                "cycle_start_ts": 1700000900,
                "cycle_end_ts": 1700001800,
                "price_to_beat": 200.0,
                "final_price": 199.0,
                "source_price_to_beat": "chainlink_datafeeds_rpc",
                "source_final_price": "chainlink_streams_rpc",
                "has_price_to_beat": True,
                "has_final_price": True,
                "has_both": True,
            },
            {
                "asset": "sol",
                "cycle_start_ts": 1700001800,
                "cycle_end_ts": 1700002700,
                "price_to_beat": 300.0,
                "final_price": 301.0,
                "source_price_to_beat": "polymarket_api_crypto_price",
                "source_final_price": "chainlink_streams_rpc",
                "has_price_to_beat": True,
                "has_final_price": True,
                "has_both": True,
            },
        ]
    )
    write_parquet_atomic(market_table, cfg.layout.market_catalog_table_path)
    write_parquet_atomic(oracle_table, cfg.layout.oracle_prices_table_path)

    truth_summary = build_truth_15m(cfg)
    truth = pd.read_parquet(cfg.layout.truth_table_path).sort_values("cycle_start_ts").reset_index(drop=True)

    assert truth_summary["rows_written"] == 3
    assert truth["truth_source"].tolist() == ["streams", "chainlink_mixed", "oracle_prices"]
    assert truth["winner_side"].tolist() == ["UP", "DOWN", "UP"]


def test_import_legacy_market_catalog_builds_canonical_table(tmp_path: Path) -> None:
    source = tmp_path / "solana_updown_15m_markets_last3y_20260312_150335.csv"
    pd.DataFrame(
        [
            {
                "id": "market-1",
                "conditionId": "cond-1",
                "slug": "sol-up-or-down-15m-1772374800",
                "question": "Solana Up or Down - March 9, 9:00AM-9:15AM ET",
                "resolutionSource": "https://data.chain.link/streams/sol-usd",
                "endDate": "2026-03-09T13:15:00Z",
                "outcomes": '["Up", "Down"]',
                "clobTokenIds": '["token-up", "token-down"]',
                "active": True,
                "closed": False,
                "events": '[{"id": "event-1", "slug": "sol-up-or-down-15m-1772374800", "title": "Solana Up or Down 15m", "seriesSlug": "sol-up-or-down-15m", "resolutionSource": "https://data.chain.link/streams/sol-usd"}]',
            }
        ]
    ).to_csv(source, index=False)

    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    summary = import_legacy_market_catalog(cfg, source_path=source)
    canonical = pd.read_parquet(cfg.layout.market_catalog_table_path)

    assert summary["snapshot_rows"] == 1
    assert len(canonical) == 1
    assert canonical.iloc[0]["market_id"] == "market-1"
    assert canonical.iloc[0]["token_down"] == "token-down"


def test_import_legacy_orderbook_depth_and_build_index(tmp_path: Path) -> None:
    source = tmp_path / "orderbook_depth_20260312.ndjson.zst"
    append_ndjson_zst(
        source,
        [
            {
                "logged_at": "2026-03-12T00:05:00.307737+00:00",
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "orderbook_ts": "2026-03-12T00:05:00.316415+00:00",
                "asks": [[0.51, 10.0], [0.52, 8.0]],
                "bids": [[0.49, 11.0], [0.48, 5.0]],
            },
            {
                "logged_at": "2026-03-12T00:05:00.307737+00:00",
                "market_id": "market-1",
                "token_id": "token-down",
                "side": "down",
                "orderbook_ts": "2026-03-12T00:05:00.316857+00:00",
                "asks": [[0.61, 12.0]],
                "bids": [[0.39, 13.0]],
            },
        ],
    )

    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    copy_summary = import_legacy_orderbook_depth(cfg, source_paths=[source])
    index_summary = build_orderbook_index_from_depth(cfg, date_str="2026-03-12")
    index_df = pd.read_parquet(cfg.layout.orderbook_index_path("2026-03-12"))

    assert copy_summary["copied_count"] == 1
    assert index_summary["rows_parsed"] == 2
    assert len(index_df) == 2
    assert float(index_df[index_df["side"] == "up"].iloc[0]["best_ask"]) == 0.51
    assert float(index_df[index_df["side"] == "down"].iloc[0]["best_bid"]) == 0.39


def test_build_oracle_prices_prefers_direct_source(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    market_table = pd.DataFrame(
        [
            {
                "market_id": "market-1",
                "condition_id": "cond-1",
                "asset": "sol",
                "cycle": "15m",
                "cycle_start_ts": 1700000000,
                "cycle_end_ts": 1700000900,
                "token_up": "token-up",
                "token_down": "token-down",
                "slug": "sol-up-or-down-15m-1700000000",
                "question": "Sol Up or Down",
                "resolution_source": "https://data.chain.link/streams/sol-usd",
                "event_id": "event-1",
                "event_slug": "slug-1",
                "event_title": "title-1",
                "series_slug": "sol-up-or-down-15m",
                "closed_ts": None,
                "source_snapshot_ts": "2026-03-19T09-00-00Z",
            }
        ]
    )
    write_parquet_atomic(market_table, cfg.layout.market_catalog_table_path)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "tx_hash": "0xabc",
                    "block_number": 101,
                    "observation_ts": 1700000900,
                    "extra_ts": 1700000900,
                    "benchmark_price_raw": 1.1e21,
                    "price": 1100.0,
                    "report_feed_id": "feed",
                    "requester": "req",
                    "path": "keeper_transmit",
                    "perform_idx": 0,
                    "value_idx": 0,
                    "source_file": "rpc",
                    "ingested_at": "2026-03-19T09:00:00Z",
                },
                {
                    "asset": "sol",
                    "tx_hash": "0xdef",
                    "block_number": 102,
                    "observation_ts": 1700000000,
                    "extra_ts": 1700000000,
                    "benchmark_price_raw": 1.0e21,
                    "price": 1000.0,
                    "report_feed_id": "feed",
                    "requester": "req",
                    "path": "keeper_transmit",
                    "perform_idx": 0,
                    "value_idx": 0,
                    "source_file": "rpc",
                    "ingested_at": "2026-03-19T09:00:00Z",
                },
            ]
        ),
        cfg.layout.streams_partition_path(2023, 11),
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "cycle": "15m",
                    "cycle_start_ts": 1700000000,
                    "cycle_end_ts": 1700000900,
                    "price_to_beat": 999.0,
                    "final_price": None,
                    "has_price_to_beat": True,
                    "has_final_price": False,
                    "has_both": False,
                    "completed": False,
                    "incomplete": True,
                    "cached": False,
                    "api_timestamp_ms": 1700000000000,
                    "http_status": 200,
                    "source": "polymarket_api_crypto_price",
                    "source_priority": 3,
                    "fetched_at": "2026-03-19T09:00:00Z",
                }
            ]
        ),
        cfg.layout.direct_oracle_source_path,
    )
    oracle_summary = build_oracle_prices_15m(cfg)
    oracle = pd.read_parquet(cfg.layout.oracle_prices_table_path)
    assert oracle_summary["rows_written"] == 1
    assert float(oracle.iloc[0]["price_to_beat"]) == 999.0
    assert float(oracle.iloc[0]["final_price"]) == 1100.0
    assert oracle.iloc[0]["source_price_to_beat"] == "polymarket_api_crypto_price"
    assert oracle.iloc[0]["source_final_price"] == "rpc"
