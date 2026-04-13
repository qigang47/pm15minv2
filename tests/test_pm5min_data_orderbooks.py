from __future__ import annotations

import importlib
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


class _FakeOrderbookProvider:
    def get_orderbook_summary(
        self,
        token_id: str,
        *,
        levels: int = 0,
        timeout: float = 1.2,
        force_refresh: bool = False,
    ) -> dict[str, object] | None:
        del token_id, levels, timeout, force_refresh
        return {
            "timestamp": "2026-04-12T10:00:00Z",
            "asks": [{"price": "0.12", "size": "10"}],
            "bids": [{"price": "0.11", "size": "8"}],
        }

    def sync_subscriptions(
        self,
        token_ids: list[str],
        *,
        replace: bool = True,
        prefetch: bool = False,
        levels: int = 0,
        timeout: float = 1.2,
    ) -> dict[str, object] | None:
        del token_ids, replace, prefetch, levels, timeout
        return {"ok": True}

    def get_update_marker(self):
        return None

    def wait_for_update(self, *, since_marker=None, timeout_sec: float = 0.0):
        del since_marker, timeout_sec
        return None


def _write_market_catalog(cfg: DataConfig) -> None:
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-sol-1",
                    "condition_id": "condition-sol-1",
                    "asset": "sol",
                    "cycle": cfg.cycle,
                    "cycle_start_ts": 1_775_987_700,
                    "cycle_end_ts": 1_875_988_000,
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "slug": "sol-updown-5m-1775987700",
                    "question": "Will SOL close up?",
                    "resolution_source": "https://data.chain.link/streams/sol-usd",
                    "event_id": "event-sol-1",
                    "event_slug": "event-sol-1",
                    "event_title": "SOL Up or Down",
                    "series_slug": "sol-updown-5m",
                    "closed_ts": None,
                    "source_snapshot_ts": "2026-04-12T10-00-00Z",
                }
            ]
        ),
        cfg.layout.market_catalog_table_path,
    )


def test_pm5min_orderbook_recording_persists_under_5m_root(tmp_path) -> None:
    module = _load_pm5min_pipeline("orderbook_recording")
    cfg = DataConfig.build(market="sol", cycle="5m", surface="live", root=tmp_path)
    batch = module.CapturedOrderbookBatch(
        captured_ts_ms=1_775_987_800_000,
        date_str="2026-04-12",
        selected_markets=1,
        market_start_offset=0,
        selected_market_ids=["market-sol-1"],
        recent_window_minutes=15,
        snapshot_rows=[
            {
                "captured_ts_ms": 1_775_987_800_000,
                "market_id": "market-sol-1",
                "token_id": "token-up",
                "side": "up",
                "asks": [{"price": 0.12, "size": 10.0}],
                "bids": [{"price": 0.11, "size": 8.0}],
            }
        ],
        index_rows=[
            {
                "captured_ts_ms": 1_775_987_800_000,
                "market_id": "market-sol-1",
                "token_id": "token-up",
                "side": "up",
                "best_ask": 0.12,
                "best_bid": 0.11,
                "ask_size_1": 10.0,
                "bid_size_1": 8.0,
                "spread": 0.01,
            }
        ],
    )

    summary = module.persist_captured_orderbooks_once(cfg, batch=batch)

    assert "cycle=5m/asset=sol" in summary["depth_path"]


def test_pm5min_orderbook_runtime_reports_5m_cycle(tmp_path) -> None:
    module = _load_pm5min_pipeline("orderbook_runtime")
    cfg = DataConfig.build(market="sol", cycle="5m", surface="live", root=tmp_path)
    _write_market_catalog(cfg)

    payload = module.run_orderbook_recorder(
        cfg,
        iterations=1,
        loop=False,
        provider=_FakeOrderbookProvider(),
    )

    assert payload["cycle"] == "5m"


def test_pm5min_orderbook_fleet_process_command_uses_pm5min_runtime(monkeypatch) -> None:
    module = _load_pm5min_pipeline("orderbook_fleet")
    monkeypatch.setattr(
        module,
        "run_orderbook_recorder",
        lambda cfg, **kwargs: {
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "status": "ok",
        },
    )

    payload = module.run_orderbook_recorder_fleet(
        markets="sol",
        cycle="5m",
        surface="live",
        iterations=1,
    )

    assert payload["cycle"] == "5m"


def test_pm5min_backtest_refresh_defaults_to_5m(tmp_path, monkeypatch) -> None:
    module = _load_pm5min_pipeline("backtest_refresh")
    monkeypatch.setattr(module, "sync_market_catalog", lambda cfg, **kwargs: {"dataset": "market_catalog", "cycle": cfg.cycle})
    monkeypatch.setattr(module, "sync_binance_klines_1m", lambda cfg, **kwargs: {"dataset": "binance", "cycle": cfg.cycle})
    monkeypatch.setattr(module, "sync_streams_from_rpc", lambda cfg, **kwargs: {"dataset": "streams", "cycle": cfg.cycle})
    monkeypatch.setattr(module, "sync_polymarket_oracle_prices_direct", lambda cfg, **kwargs: {"dataset": "oracle_direct", "cycle": cfg.cycle})
    monkeypatch.setattr(module, "sync_settlement_truth_from_rpc", lambda cfg, **kwargs: {"dataset": "settlement_rpc", "cycle": cfg.cycle})
    monkeypatch.setattr(module, "build_oracle_prices_15m", lambda cfg: {"dataset": "oracle_prices", "cycle": cfg.cycle})
    monkeypatch.setattr(module, "build_truth_15m", lambda cfg: {"dataset": "truth", "cycle": cfg.cycle})

    payload = module.run_backtest_data_refresh(markets=["sol"], root=tmp_path)

    assert payload["cycle"] == "5m"
