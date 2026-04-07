from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.pipelines.direct_sync import (
    sync_datafeeds_from_rpc,
    sync_settlement_truth_from_gamma,
    sync_settlement_truth_from_rpc,
    sync_streams_from_rpc,
)
from pm15min.data.pipelines.source_ingest import import_legacy_settlement_truth


class _FakeRpc:
    def __init__(self) -> None:
        self._calls = []

    def eth_block_number(self) -> int:
        return 1000

    def find_first_block_at_or_after_ts(self, target_ts: int, lo_block: int, hi_block: int) -> int:
        return max(1, min(1000, int(target_ts % 1000) + 1))


class _FakeGammaClient:
    def fetch_markets_by_ids(self, market_ids: list[str], *, sleep_sec: float = 0.0):
        return [
            {
                "id": "market-1",
                "outcomes": '["Up", "Down"]',
                "outcomePrices": '["0", "1"]',
                "umaResolutionStatus": "resolved",
            }
        ]


class _FakeChainlinkSource:
    def __init__(self, rpc=None) -> None:
        self.rpc = rpc

    def scan_report_verified_logs(self, *, asset: str, from_block: int, to_block: int, chunk_blocks: int = 1000, sleep_sec: float = 0.02, report_verified_address: str = ""):
        return [
            {
                "tx_hash": "0xabc",
                "block_number": 101,
                "log_index": 1,
                "feed_id_log": "feed",
                "requester": "req",
            }
        ]

    def decode_streams_from_logs(self, *, asset: str, logs: list[dict], include_block_timestamp: bool = False):
        return [
            {
                "asset": asset,
                "tx_hash": "0xabc",
                "block_number": 101,
                "block_timestamp": 1700000000,
                "requester": "req",
                "feed_id_log": "feed",
                "report_feed_id": "feed",
                "perform_idx": 0,
                "value_idx": 0,
                "extra_code": 1,
                "extra_ts": 1700000000,
                "valid_from_ts": 1700000000,
                "observation_ts": 1700000000,
                "expires_at_ts": 1700000900,
                "benchmark_price_raw": 100_0000000000000000000,
                "bid_raw": 99_0000000000000000000,
                "ask_raw": 101_0000000000000000000,
                "path": "keeper_transmit",
            }
        ]

    def scan_condition_resolutions(self, *, from_block: int, to_block: int, chunk_blocks: int = 3000, sleep_sec: float = 0.01, ctf_address: str = ""):
        return {
            "cond-1": {
                "condition_id": "cond-1",
                "resolve_tx_hash": "0xresolve",
                "resolve_block_number": 202,
                "resolve_log_index": 2,
                "oracle_address": "0xoracle",
                "question_id_topic": "0xquestion",
                "outcome_slot_count": 2,
                "payout_numerators_json": "[1,0]",
                "winner_index": 0,
                "winner_side": "UP",
            }
        }

    def scan_datafeeds_answer_updated_logs(self, *, asset: str, from_block: int, to_block: int, chunk_blocks: int = 5000, sleep_sec: float = 0.02):
        return [
            {
                "asset": asset,
                "feed_name": "BTC / USD",
                "proxy_address": "0xproxy",
                "aggregator_address": "0xagg",
                "decimals": 8,
                "block_number": 101,
                "tx_hash": "0xfeed",
                "log_index": 2,
                "round_id": 7,
                "updated_at": 1700000000,
                "updated_at_iso": "2023-11-14T22:13:20Z",
                "answer_raw": 123456789,
                "answer": 1234.56789,
            }
        ]


def test_sync_streams_from_rpc_writes_partition(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("pm15min.data.pipelines.direct_sync.ChainlinkRpcSource", _FakeChainlinkSource)
    cfg = DataConfig.build(market="btc", cycle="15m", root=tmp_path / "v2")
    summary = sync_streams_from_rpc(
        cfg,
        start_ts=1700000000,
        end_ts=1700000900,
        rpc=_FakeRpc(),
    )
    assert summary["rows_imported"] == 1
    paths = list(cfg.layout.streams_source_root.glob("year=*/month=*/data.parquet"))
    assert len(paths) == 1
    df = pd.read_parquet(paths[0])
    assert df.iloc[0]["tx_hash"] == "0xabc"


def test_sync_datafeeds_from_rpc_writes_partition(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("pm15min.data.pipelines.direct_sync.ChainlinkRpcSource", _FakeChainlinkSource)
    cfg = DataConfig.build(market="btc", cycle="15m", root=tmp_path / "v2")

    summary = sync_datafeeds_from_rpc(
        cfg,
        start_ts=1700000000,
        end_ts=1700000900,
        rpc=_FakeRpc(),
    )

    assert summary["rows_imported"] == 1
    paths = list(cfg.layout.datafeeds_source_root.glob("year=*/month=*/data.parquet"))
    assert len(paths) == 1
    df = pd.read_parquet(paths[0])
    assert df.iloc[0]["tx_hash"] == "0xfeed"
    assert float(df.iloc[0]["answer"]) == 1234.56789


def test_sync_settlement_truth_from_rpc_writes_source_table(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("pm15min.data.pipelines.direct_sync.ChainlinkRpcSource", _FakeChainlinkSource)
    cfg = DataConfig.build(market="btc", cycle="15m", root=tmp_path / "v2")

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
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "btc",
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
                }
            ]
        ),
        cfg.layout.streams_partition_path(2023, 11),
    )

    summary = sync_settlement_truth_from_rpc(cfg, rpc=_FakeRpc())
    assert summary["rows_imported"] == 1
    df = pd.read_parquet(cfg.layout.settlement_truth_source_path)
    assert df.iloc[0]["winner_side"] == "UP"
    assert bool(df.iloc[0]["full_truth"]) is True


def test_sync_settlement_truth_from_rpc_supports_5m(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("pm15min.data.pipelines.direct_sync.ChainlinkRpcSource", _FakeChainlinkSource)
    cfg = DataConfig.build(market="eth", cycle="5m", root=tmp_path / "v2")
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "asset": "eth",
                    "cycle": "5m",
                    "cycle_start_ts": 1766031900,
                    "cycle_end_ts": 1766032200,
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "slug": "eth-updown-5m-1766031900",
                    "question": "Ethereum Up or Down",
                    "resolution_source": "https://data.chain.link/streams/eth-usd",
                }
            ]
        ),
        cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "eth",
                    "tx_hash": "0xabc",
                    "observation_ts": 1766032200,
                    "extra_ts": 1766032200,
                    "benchmark_price_raw": 1.1e21,
                    "price": 1100.0,
                    "perform_idx": 0,
                    "value_idx": 0,
                    "source_file": "rpc",
                    "ingested_at": "2026-03-28T10:00:00Z",
                }
            ]
        ),
        cfg.layout.streams_partition_path(2026, 3),
    )

    summary = sync_settlement_truth_from_rpc(cfg, rpc=_FakeRpc())

    assert summary["rows_imported"] == 1


def test_import_legacy_settlement_truth_requires_explicit_source_for_5m(tmp_path: Path, monkeypatch) -> None:
    cfg = DataConfig.build(market="eth", cycle="5m", root=tmp_path / "v2")
    discovered = tmp_path / "polymarket_15m_settlement_truth.csv"
    pd.DataFrame(
        [
            {
                "asset": "eth",
                "end_ts": 1766032200,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "slug": "eth-updown-15m-1766031300",
                "question": "Ethereum Up or Down",
                "resolution_source": "legacy-15m",
                "winner_side": "UP",
                "label_updown": "UP",
                "onchain_resolved": True,
                "stream_match_exact": True,
                "full_truth": True,
                "stream_price": 2042.5,
                "stream_extra_ts": 1766032200,
            }
        ]
    ).to_csv(discovered, index=False)
    monkeypatch.setattr(
        "pm15min.data.pipelines.source_ingest.discover_legacy_settlement_truth_csv",
        lambda: discovered,
    )

    with pytest.raises(ValueError, match="explicit source_path"):
        import_legacy_settlement_truth(cfg)


def test_import_legacy_settlement_truth_supports_5m_with_explicit_source(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="eth", cycle="5m", root=tmp_path / "v2")
    source_path = tmp_path / "polymarket_5m_settlement_truth.csv"
    pd.DataFrame(
        [
            {
                "asset": "eth",
                "end_ts": 1766032200,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "slug": "eth-updown-5m-1766031900",
                "question": "Ethereum Up or Down",
                "resolution_source": "legacy-5m",
                "winner_side": "DOWN",
                "label_updown": "DOWN",
                "onchain_resolved": True,
                "stream_match_exact": True,
                "full_truth": True,
                "stream_price": 2042.5,
                "stream_extra_ts": 1766032200,
            }
        ]
    ).to_csv(source_path, index=False)

    summary = import_legacy_settlement_truth(cfg, source_path=source_path)

    assert summary["rows_imported"] == 1
    df = pd.read_parquet(cfg.layout.settlement_truth_source_path)
    assert df.iloc[0]["cycle"] == "5m"
    assert int(df.iloc[0]["cycle_start_ts"]) == 1766031900
    assert df.iloc[0]["source_file"] == str(source_path)


def test_import_legacy_settlement_truth_rejects_explicit_15m_source_for_5m(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="eth", cycle="5m", root=tmp_path / "v2")
    source_path = tmp_path / "polymarket_15m_settlement_truth.csv"
    pd.DataFrame(
        [
            {
                "asset": "eth",
                "end_ts": 1766032200,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "slug": "eth-updown-15m-1766031300",
                "question": "Ethereum Up or Down",
                "resolution_source": "legacy-15m",
                "winner_side": "DOWN",
                "label_updown": "DOWN",
                "onchain_resolved": True,
                "stream_match_exact": True,
                "full_truth": True,
                "stream_price": 2042.5,
                "stream_extra_ts": 1766032200,
            }
        ]
    ).to_csv(source_path, index=False)

    with pytest.raises(ValueError, match="compatible with cycle=5m"):
        import_legacy_settlement_truth(cfg, source_path=source_path)


def test_sync_settlement_truth_from_gamma_writes_source_table(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="eth", cycle="5m", root=tmp_path / "v2")

    market_table = pd.DataFrame(
        [
            {
                "market_id": "market-1",
                "condition_id": "cond-1",
                "asset": "eth",
                "cycle": "5m",
                "cycle_start_ts": 1766031900,
                "cycle_end_ts": 1766032200,
                "token_up": "token-up",
                "token_down": "token-down",
                "slug": "eth-updown-5m-1766031900",
                "question": "Ethereum Up or Down",
                "resolution_source": "https://data.chain.link/streams/eth-usd",
                "event_id": "event-1",
                "event_slug": "slug-1",
                "event_title": "title-1",
                "series_slug": "eth-up-or-down-5m",
                "closed_ts": 1766032520,
                "source_snapshot_ts": "2026-03-28T10:00:00Z",
            }
        ]
    )
    write_parquet_atomic(market_table, cfg.layout.market_catalog_table_path)

    summary = sync_settlement_truth_from_gamma(cfg, client=_FakeGammaClient())

    assert summary["rows_imported"] == 1
    assert summary["rows_resolved"] == 1
    df = pd.read_parquet(cfg.layout.settlement_truth_source_path)
    assert df.iloc[0]["winner_side"] == "DOWN"
    assert bool(df.iloc[0]["full_truth"]) is True


def test_sync_settlement_truth_from_gamma_does_not_promote_unresolved_market_prices_to_truth(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="eth", cycle="5m", root=tmp_path / "v2")

    market_table = pd.DataFrame(
        [
            {
                "market_id": "market-1",
                "condition_id": "cond-1",
                "asset": "eth",
                "cycle": "5m",
                "cycle_start_ts": 1766031900,
                "cycle_end_ts": 1766032200,
                "token_up": "token-up",
                "token_down": "token-down",
                "slug": "eth-updown-5m-1766031900",
                "question": "Ethereum Up or Down",
                "resolution_source": "https://data.chain.link/streams/eth-usd",
                "event_id": "event-1",
                "event_slug": "slug-1",
                "event_title": "title-1",
                "series_slug": "eth-up-or-down-5m",
                "closed_ts": 1766032520,
                "source_snapshot_ts": "2026-03-28T10:00:00Z",
            }
        ]
    )
    write_parquet_atomic(market_table, cfg.layout.market_catalog_table_path)

    class _UnresolvedGammaClient:
        def fetch_markets_by_ids(self, market_ids: list[str], *, sleep_sec: float = 0.0):
            return [
                {
                    "id": "market-1",
                    "outcomes": '["Up", "Down"]',
                    "outcomePrices": '["0.6", "0.4"]',
                }
            ]

    summary = sync_settlement_truth_from_gamma(cfg, client=_UnresolvedGammaClient())

    assert summary["rows_imported"] == 1
    assert summary["rows_resolved"] == 0
    df = pd.read_parquet(cfg.layout.settlement_truth_source_path)
    assert df.iloc[0]["winner_side"] == ""
    assert bool(df.iloc[0]["onchain_resolved"]) is False
    assert bool(df.iloc[0]["full_truth"]) is False
