from __future__ import annotations

import importlib
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pm5min.data.config import DataConfig
from pmshared.io.parquet import write_parquet_atomic


def _load_pm5min_pipeline(module_name: str):
    module_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "pm5min"
        / "data"
        / "pipelines"
        / f"{module_name}.py"
    )
    assert module_path.exists(), f"Expected local pm5min pipeline module at {module_path}"
    return importlib.import_module(f"pm5min.data.pipelines.{module_name}")


def _fixed_now() -> datetime:
    return datetime(2026, 4, 12, 10, 0, tzinfo=timezone.utc)


class _FakeGamma:
    def fetch_active_markets(self, **kwargs):
        return [
            {
                "id": "market-sol-1",
                "conditionId": "condition-sol-1",
                "slug": "sol-updown-5m-1775987700",
                "question": "Will SOL close up in the next 5 minutes?",
                "resolutionSource": "https://data.chain.link/streams/sol-usd",
                "endDate": "1775988000",
                "outcomes": '["Up", "Down"]',
                "clobTokenIds": '["token-up", "token-down"]',
            }
        ]


class _FakeBinance:
    def __init__(self) -> None:
        self._calls = 0

    def fetch_klines(self, request):
        self._calls += 1
        if self._calls > 1:
            return pd.DataFrame(
                columns=[
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
                ]
            )
        return pd.DataFrame(
            [
                [
                    1_775_987_400_000,
                    "100.0",
                    "101.0",
                    "99.0",
                    "100.5",
                    "250.0",
                    1_775_987_459_999,
                    "25125.0",
                    18,
                    "125.0",
                    "12562.5",
                    "0",
                ]
            ],
            columns=[
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
            ],
        )


class _FakeRpc:
    def eth_block_number(self) -> int:
        return 1_000

    def find_first_block_at_or_after_ts(self, target_ts: int, lo_block: int, hi_block: int) -> int:
        return max(1, min(int(hi_block), int(target_ts % 1_000) + 1))


class _FakeChainlinkSource:
    def __init__(self, rpc=None) -> None:
        self.rpc = rpc

    def scan_report_verified_logs(
        self,
        *,
        asset: str,
        from_block: int,
        to_block: int,
        chunk_blocks: int = 1000,
        sleep_sec: float = 0.02,
    ):
        return [
            {
                "tx_hash": "0xstream",
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
                "tx_hash": "0xstream",
                "block_number": 101,
                "observation_ts": 1_775_987_700,
                "extra_ts": 1_775_987_700,
                "benchmark_price_raw": 101_000_000_000_000_000_000,
                "report_feed_id": "feed",
                "requester": "req",
                "path": "keeper_transmit",
                "perform_idx": 0,
                "value_idx": 0,
            }
        ]


class _FakeOracleClient:
    def __init__(self, timeout_sec: float = 20.0) -> None:
        self.timeout_sec = timeout_sec

    def fetch_crypto_price(
        self,
        *,
        symbol: str,
        cycle_start_ts: int,
        cycle_seconds: int,
        sleep_sec: float = 0.0,
        max_retries: int = 1,
    ) -> dict[str, object]:
        return {
            "openPrice": 100.25,
            "closePrice": 101.75,
            "completed": True,
            "incomplete": False,
            "cached": False,
            "timestamp": cycle_start_ts * 1_000,
            "source": "polymarket_api_crypto_price",
        }


def _write_legacy_streams_csv(root: Path) -> Path:
    path = root / "legacy_streams.csv"
    pd.DataFrame(
        [
            {
                "asset": "sol",
                "extra_ts": 1_775_987_400,
                "benchmark_price_raw": 101_500_000_000_000_000_000,
                "tx_hash": "0xabc",
                "observation_ts": 1_775_987_390,
                "block_number": 123,
                "perform_idx": 0,
                "value_idx": 0,
                "report_feed_id": "feed-sol",
                "requester": "chainlink",
                "path": "/streams/sol",
            }
        ]
    ).to_csv(path, index=False)
    return path


def test_pm5min_market_catalog_pipeline_uses_5m_layout(tmp_path) -> None:
    module = _load_pm5min_pipeline("market_catalog")
    cfg = DataConfig.build(market="sol", cycle="5m", surface="live", root=tmp_path)

    payload = module.sync_market_catalog(
        cfg,
        start_ts=1_775_987_700,
        end_ts=1_775_988_000,
        client=_FakeGamma(),
        now=_fixed_now(),
    )

    assert payload["cycle"] == "5m"
    assert payload["source_mode"] == "gamma_active_markets"
    assert str(payload["canonical_path"]).endswith("data/live/tables/markets/cycle=5m/asset=sol/data.parquet")


def test_pm5min_binance_pipeline_writes_under_5m_root(tmp_path) -> None:
    module = _load_pm5min_pipeline("binance_klines")
    cfg = DataConfig.build(market="sol", cycle="5m", surface="live", root=tmp_path)

    payload = module.sync_binance_klines_1m(
        cfg,
        client=_FakeBinance(),
        now=_fixed_now(),
        lookback_minutes=60,
        batch_limit=10,
    )

    assert payload["rows_written"] == 1
    assert str(payload["target_path"]).endswith("data/live/sources/binance/klines_1m/symbol=SOLUSDT/data.parquet")


def test_pm5min_legacy_streams_import_uses_5m_surface_root(tmp_path) -> None:
    module = _load_pm5min_pipeline("source_ingest")
    cfg = DataConfig.build(market="sol", cycle="5m", surface="backtest", root=tmp_path)

    payload = module.import_legacy_streams(
        cfg,
        source_path=_write_legacy_streams_csv(tmp_path),
    )

    assert payload["rows_imported"] == 1
    assert str(payload["target_root"]).endswith("data/backtest/sources/chainlink/streams/asset=sol")


def test_pm5min_direct_sync_pipeline_uses_5m_layout(tmp_path, monkeypatch) -> None:
    module = _load_pm5min_pipeline("direct_sync")
    monkeypatch.setattr(module, "ChainlinkRpcSource", _FakeChainlinkSource)
    cfg = DataConfig.build(market="sol", cycle="5m", surface="backtest", root=tmp_path)

    payload = module.sync_streams_from_rpc(
        cfg,
        start_ts=1_775_987_700,
        end_ts=1_775_988_000,
        rpc=_FakeRpc(),
    )

    assert payload["rows_imported"] == 1
    assert str(payload["target_root"]).endswith("data/backtest/sources/chainlink/streams/asset=sol")


def test_pm5min_direct_oracle_pipeline_rebuilds_5m_outputs(tmp_path, monkeypatch) -> None:
    module = _load_pm5min_pipeline("direct_oracle_prices")
    cfg = DataConfig.build(market="sol", cycle="5m", surface="backtest", root=tmp_path)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-sol-1",
                    "condition_id": "condition-sol-1",
                    "asset": "sol",
                    "cycle": "5m",
                    "cycle_start_ts": 1_775_987_700,
                    "cycle_end_ts": 1_775_988_000,
                }
            ]
        ),
        cfg.layout.market_catalog_table_path,
    )

    monkeypatch.setattr(module, "PolymarketOracleApiClient", _FakeOracleClient)
    monkeypatch.setattr(
        module,
        "build_oracle_prices_table",
        lambda data_cfg: {
            "dataset": "oracle_prices_5m",
            "market": data_cfg.asset.slug,
            "cycle": data_cfg.cycle,
        },
    )

    def _fake_build_truth_table(data_cfg):
        write_parquet_atomic(
            pd.DataFrame(
                [
                    {
                        "cycle_start_ts": 1_775_987_700,
                        "resolved": True,
                    }
                ]
            ),
            data_cfg.layout.truth_table_path,
        )
        return {
            "dataset": "truth_5m",
            "market": data_cfg.asset.slug,
            "cycle": data_cfg.cycle,
        }

    monkeypatch.setattr(module, "build_truth_table", _fake_build_truth_table)

    payload = module.backfill_direct_oracle_prices(
        cfg,
        workers=1,
        flush_every=1,
        sleep_sec=0.0,
    )

    assert payload["cycle"] == "5m"
    assert payload["pending"] == 1
    assert payload["fetched"] == 1
