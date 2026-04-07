from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd

import pm15min.research.backtests.orderbook_surface as orderbook_surface_module
from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.backtests.guard_parity import apply_live_guard_parity
from pm15min.research.backtests.orderbook_surface import attach_canonical_quote_surface
from pm15min.research.config import ResearchConfig
from pm15min.research.experiments.runner import run_experiment_suite
from pm15min.research.experiments.specs import load_suite_definition


def _sample_klines(symbol: str, *, start: str, periods: int, price_base: float) -> pd.DataFrame:
    ts = pd.date_range(start, periods=periods, freq="min", tz="UTC")
    close = pd.Series(range(periods), dtype=float) * 0.1 + price_base
    return pd.DataFrame(
        {
            "open_time": ts,
            "open": close - 0.1,
            "high": close + 0.2,
            "low": close - 0.2,
            "close": close,
            "volume": 1000.0,
            "quote_asset_volume": close * 1000.0,
            "taker_buy_quote_volume": close * 400.0,
            "symbol": symbol,
        }
    )


def _sample_oracle_prices(asset: str, *, cycle_start_ts: int, n_cycles: int, price_base: float) -> pd.DataFrame:
    rows = []
    for idx in range(n_cycles):
        start_ts = cycle_start_ts + idx * 900
        rows.append(
            {
                "asset": asset,
                "cycle_start_ts": start_ts,
                "cycle_end_ts": start_ts + 900,
                "price_to_beat": price_base + idx,
                "final_price": price_base + idx + (1.0 if idx % 2 == 0 else -1.0),
                "source_price_to_beat": "direct_api",
                "source_final_price": "streams_rpc",
                "has_price_to_beat": True,
                "has_final_price": True,
                "has_both": True,
            }
        )
    return pd.DataFrame(rows)


def test_attach_canonical_quote_surface_reads_orderbook_index_once_per_date(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "m-1",
                    "condition_id": "c-1",
                    "token_up": "tok-up",
                    "token_down": "tok-down",
                    "question": "SOL up?",
                    "cycle_start_ts": 1_772_323_200,
                    "cycle_end_ts": 1_772_324_100,
                }
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "captured_ts_ms": 1_772_323_250_000,
                    "market_id": "m-1",
                    "token_id": "tok-up",
                    "side": "up",
                    "best_ask": 0.41,
                    "best_bid": 0.39,
                    "ask_size_1": 15.0,
                    "bid_size_1": 10.0,
                },
                {
                    "captured_ts_ms": 1_772_323_250_000,
                    "market_id": "m-1",
                    "token_id": "tok-down",
                    "side": "down",
                    "best_ask": 0.59,
                    "best_bid": 0.57,
                    "ask_size_1": 13.0,
                    "bid_size_1": 11.0,
                },
            ]
        ),
        data_cfg.layout.orderbook_index_path("2026-03-01"),
    )

    load_count = {"value": 0}
    original_loader = orderbook_surface_module.load_orderbook_index_frame

    def _counting_loader(*, index_path, recent_path=None):
        load_count["value"] += 1
        return original_loader(index_path=index_path, recent_path=recent_path)

    monkeypatch.setattr(orderbook_surface_module, "load_orderbook_index_frame", _counting_loader)

    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "condition_id": "c-1",
            },
            {
                "decision_ts": "2026-03-01T00:02:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 8,
                "market_id": "m-1",
                "condition_id": "c-1",
            }
        ]
    )

    out, summary = attach_canonical_quote_surface(replay=replay, data_cfg=data_cfg)

    assert load_count["value"] == 1
    assert summary.quote_ready_rows == 2
    assert out.loc[0, "quote_status"] == "ok"
    assert out.loc[0, "token_up"] == "tok-up"
    assert float(out.loc[0, "quote_up_ask"]) == 0.41
    assert float(out.loc[0, "quote_down_ask"]) == 0.59


def test_attach_canonical_quote_surface_backfills_blank_market_id_from_cycle_catalog(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "m-blank",
                    "condition_id": "c-blank",
                    "token_up": "tok-up-blank",
                    "token_down": "tok-down-blank",
                    "question": "SOL up?",
                    "cycle_start_ts": 1_772_323_200,
                    "cycle_end_ts": 1_772_324_100,
                }
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "captured_ts_ms": 1_772_323_250_000,
                    "market_id": "m-blank",
                    "token_id": "tok-up-blank",
                    "side": "up",
                    "best_ask": 0.41,
                    "best_bid": 0.39,
                    "ask_size_1": 15.0,
                    "bid_size_1": 10.0,
                },
                {
                    "captured_ts_ms": 1_772_323_250_000,
                    "market_id": "m-blank",
                    "token_id": "tok-down-blank",
                    "side": "down",
                    "best_ask": 0.59,
                    "best_bid": 0.57,
                    "ask_size_1": 13.0,
                    "bid_size_1": 11.0,
                },
            ]
        ),
        data_cfg.layout.orderbook_index_path("2026-03-01"),
    )

    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "",
                "condition_id": "",
            }
        ]
    )

    out, summary = attach_canonical_quote_surface(replay=replay, data_cfg=data_cfg)

    assert summary.quote_ready_rows == 1
    assert out.loc[0, "market_id"] == "m-blank"
    assert out.loc[0, "condition_id"] == "c-blank"
    assert out.loc[0, "token_up"] == "tok-up-blank"
    assert out.loc[0, "token_down"] == "tok-down-blank"
    assert out.loc[0, "quote_status"] == "ok"
    assert float(out.loc[0, "quote_up_ask"]) == 0.41
    assert float(out.loc[0, "quote_down_ask"]) == 0.59


def test_attach_canonical_quote_surface_keeps_market_specific_metadata_with_shared_cycle(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "m-1",
                    "condition_id": "c-1",
                    "token_up": "tok-up-1",
                    "token_down": "tok-down-1",
                    "question": "SOL up contract 1?",
                    "cycle_start_ts": 1_772_323_200,
                    "cycle_end_ts": 1_772_324_100,
                },
                {
                    "market_id": "m-2",
                    "condition_id": "c-2",
                    "token_up": "tok-up-2",
                    "token_down": "tok-down-2",
                    "question": "SOL up contract 2?",
                    "cycle_start_ts": 1_772_323_200,
                    "cycle_end_ts": 1_772_324_100,
                },
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "captured_ts_ms": 1_772_323_250_000,
                    "market_id": "m-1",
                    "token_id": "tok-up-1",
                    "side": "up",
                    "best_ask": 0.41,
                    "best_bid": 0.39,
                    "ask_size_1": 15.0,
                    "bid_size_1": 10.0,
                },
                {
                    "captured_ts_ms": 1_772_323_250_000,
                    "market_id": "m-1",
                    "token_id": "tok-down-1",
                    "side": "down",
                    "best_ask": 0.59,
                    "best_bid": 0.57,
                    "ask_size_1": 13.0,
                    "bid_size_1": 11.0,
                },
                {
                    "captured_ts_ms": 1_772_323_250_000,
                    "market_id": "m-2",
                    "token_id": "tok-up-2",
                    "side": "up",
                    "best_ask": 0.25,
                    "best_bid": 0.23,
                    "ask_size_1": 7.0,
                    "bid_size_1": 6.0,
                },
                {
                    "captured_ts_ms": 1_772_323_250_000,
                    "market_id": "m-2",
                    "token_id": "tok-down-2",
                    "side": "down",
                    "best_ask": 0.75,
                    "best_bid": 0.73,
                    "ask_size_1": 8.0,
                    "bid_size_1": 7.0,
                },
            ]
        ),
        data_cfg.layout.orderbook_index_path("2026-03-01"),
    )

    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "condition_id": "c-1",
            },
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-2",
                "condition_id": "c-2",
            },
        ]
    )

    out, summary = attach_canonical_quote_surface(replay=replay, data_cfg=data_cfg)

    assert summary.quote_ready_rows == 2
    out = out.sort_values("market_id").reset_index(drop=True)
    assert out.loc[0, "token_up"] == "tok-up-1"
    assert out.loc[0, "quote_up_ask"] == 0.41
    assert out.loc[1, "token_up"] == "tok-up-2"
    assert out.loc[1, "quote_up_ask"] == 0.25


def test_attach_canonical_quote_surface_rebuilds_stale_orderbook_index(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "m-1",
                    "condition_id": "c-1",
                    "token_up": "tok-up",
                    "token_down": "tok-down",
                    "question": "SOL up?",
                    "cycle_start_ts": 1_772_323_200,
                    "cycle_end_ts": 1_772_324_100,
                }
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    depth_path = data_cfg.layout.orderbook_depth_path("2026-03-01")
    depth_path.parent.mkdir(parents=True, exist_ok=True)
    depth_path.write_text("placeholder", encoding="utf-8")
    stale_index = data_cfg.layout.orderbook_index_path("2026-03-01")
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "captured_ts_ms": 1,
                    "market_id": "stale",
                    "token_id": "stale",
                    "side": "up",
                    "best_ask": 0.99,
                    "best_bid": 0.01,
                    "ask_size_1": 1.0,
                    "bid_size_1": 1.0,
                }
            ]
        ),
        stale_index,
    )
    os.utime(stale_index, (1, 1))
    os.utime(depth_path, None)

    rebuild_calls = {"count": 0}

    def _rebuild(cfg, *, date_str):
        rebuild_calls["count"] += 1
        write_parquet_atomic(
            pd.DataFrame(
                [
                    {
                        "captured_ts_ms": 1_772_323_250_000,
                        "market_id": "m-1",
                        "token_id": "tok-up",
                        "side": "up",
                        "best_ask": 0.41,
                        "best_bid": 0.39,
                        "ask_size_1": 15.0,
                        "bid_size_1": 10.0,
                    },
                    {
                        "captured_ts_ms": 1_772_323_250_000,
                        "market_id": "m-1",
                        "token_id": "tok-down",
                        "side": "down",
                        "best_ask": 0.59,
                        "best_bid": 0.57,
                        "ask_size_1": 13.0,
                        "bid_size_1": 11.0,
                    },
                ]
            ),
            cfg.layout.orderbook_index_path(date_str),
        )

    monkeypatch.setattr("pm15min.research.backtests.data_surface_fallback._rebuild_orderbook_index", _rebuild)

    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "condition_id": "c-1",
            }
        ]
    )

    out, summary = attach_canonical_quote_surface(replay=replay, data_cfg=data_cfg)

    assert rebuild_calls["count"] == 1
    assert summary.quote_ready_rows == 1
    assert out.loc[0, "quote_status"] == "ok"
    assert float(out.loc[0, "quote_up_ask"]) == 0.41
    assert float(out.loc[0, "quote_down_ask"]) == 0.59


def test_attach_canonical_quote_surface_rebuilds_sparse_orderbook_index(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "m-1",
                    "condition_id": "c-1",
                    "token_up": "tok-up",
                    "token_down": "tok-down",
                    "question": "SOL up?",
                    "cycle_start_ts": 1_772_323_200,
                    "cycle_end_ts": 1_772_324_100,
                }
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    depth_path = data_cfg.layout.orderbook_depth_path("2026-03-01")
    depth_path.parent.mkdir(parents=True, exist_ok=True)
    depth_path.write_bytes(b"x" * 1_200_000)
    sparse_index = data_cfg.layout.orderbook_index_path("2026-03-01")
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "captured_ts_ms": 1,
                    "market_id": "stale",
                    "token_id": "stale",
                    "side": "up",
                    "best_ask": 0.99,
                    "best_bid": 0.01,
                    "ask_size_1": 1.0,
                    "bid_size_1": 1.0,
                }
            ]
        ),
        sparse_index,
    )

    rebuild_calls = {"count": 0}

    def _rebuild(cfg, *, date_str):
        rebuild_calls["count"] += 1
        write_parquet_atomic(
            pd.DataFrame(
                [
                    {
                        "captured_ts_ms": 1_772_323_250_000,
                        "market_id": "m-1",
                        "token_id": "tok-up",
                        "side": "up",
                        "best_ask": 0.41,
                        "best_bid": 0.39,
                        "ask_size_1": 15.0,
                        "bid_size_1": 10.0,
                    },
                    {
                        "captured_ts_ms": 1_772_323_250_000,
                        "market_id": "m-1",
                        "token_id": "tok-down",
                        "side": "down",
                        "best_ask": 0.59,
                        "best_bid": 0.57,
                        "ask_size_1": 13.0,
                        "bid_size_1": 11.0,
                    },
                ]
            ),
            cfg.layout.orderbook_index_path(date_str),
        )

    monkeypatch.setattr("pm15min.research.backtests.data_surface_fallback._rebuild_orderbook_index", _rebuild)

    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "condition_id": "c-1",
            }
        ]
    )

    out, summary = attach_canonical_quote_surface(replay=replay, data_cfg=data_cfg)

    assert rebuild_calls["count"] == 1
    assert summary.quote_ready_rows == 1
    assert out.loc[0, "quote_status"] == "ok"
    assert float(out.loc[0, "quote_up_ask"]) == 0.41
    assert float(out.loc[0, "quote_down_ask"]) == 0.59


def test_apply_live_guard_parity_blocks_trade_when_quote_surface_is_missing() -> None:
    decisions = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "p_up": 0.72,
                "p_down": 0.28,
                "score_valid": True,
                "score_reason": "",
                "policy_action": "trade",
                "policy_reason": "trade",
                "trade_decision": True,
                "quote_status": "missing_quote_inputs",
                "quote_reason": "up_quote_missing",
            }
        ]
    )

    out, summary = apply_live_guard_parity(
        market="sol",
        profile="deep_otm",
        decisions=decisions,
    )

    assert summary.blocked_rows == 1
    assert out.loc[0, "policy_action"] == "reject"
    assert out.loc[0, "guard_primary_reason"] == "quote_missing_inputs"


def test_load_suite_definition_parses_hybrid_variant_fields(tmp_path: Path) -> None:
    path = tmp_path / "suite.json"
    path.write_text(
        json.dumps(
            {
                "suite_name": "hybrid_suite",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "offsets": [7, 8],
                "window": {"start": "2026-03-01", "end": "2026-03-01"},
                "markets": [
                    {
                        "market": "sol",
                        "variant_label": "hybrid_reversal",
                        "variant_notes": "direction_then_reversal",
                        "hybrid_secondary_target": "reversal",
                        "hybrid_secondary_offsets": [7],
                        "hybrid_fallback_reasons": ["direction_prob"],
                    }
                ],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    suite = load_suite_definition(path)
    market = suite.markets[0]
    assert market.variant_label == "hybrid_reversal"
    assert market.variant_notes == "direction_then_reversal"
    assert market.hybrid_secondary_target == "reversal"
    assert market.hybrid_secondary_offsets == (7,)
    assert market.hybrid_fallback_reasons == ("direction_prob",)


def test_run_experiment_suite_keeps_hybrid_variant_metadata(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(
        _sample_klines("SOLUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=120.0),
        data_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_klines("BTCUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=50_000.0),
        btc_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_oracle_prices("sol", cycle_start_ts=1_772_323_200, n_cycles=32, price_base=120.0),
        data_cfg.layout.oracle_prices_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "cycle_start_ts": 1_772_323_200 + idx * 900,
                    "cycle_end_ts": 1_772_324_100 + idx * 900,
                    "market_id": f"market-{idx}",
                    "condition_id": f"cond-{idx}",
                    "winner_side": "UP" if idx % 2 == 0 else "DOWN",
                    "label_updown": "UP" if idx % 2 == 0 else "DOWN",
                    "resolved": True,
                    "truth_source": "settlement_truth",
                    "full_truth": True,
                }
                for idx in range(32)
            ]
        ),
        data_cfg.layout.truth_table_path,
    )

    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        model_family="deep_otm",
        root=root,
    )
    suite_path = cfg.layout.storage.suite_spec_path("sol_hybrid_suite")
    suite_path.parent.mkdir(parents=True, exist_ok=True)
    suite_path.write_text(
        json.dumps(
            {
                "suite_name": "sol_hybrid_suite",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "offsets": [7, 8],
                "window": {"start": "2026-03-01", "end": "2026-03-01"},
                "backtest_spec": "baseline_truth",
                "markets": [
                    {
                        "market": "sol",
                        "variant_label": "hybrid_shadow",
                        "variant_notes": "smoke",
                        "hybrid_secondary_target": "direction",
                        "hybrid_fallback_reasons": ["policy_low_confidence"],
                    }
                ],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    summary = run_experiment_suite(cfg=cfg, suite_name="sol_hybrid_suite", run_label="hybrid-exp")
    run_dir = Path(summary["run_dir"])
    training_runs = pd.read_parquet(run_dir / "training_runs.parquet")
    backtest_runs = pd.read_parquet(run_dir / "backtest_runs.parquet")

    assert training_runs.loc[0, "variant_label"] == "hybrid_shadow"
    assert training_runs.loc[0, "secondary_target"] == "direction"
    assert isinstance(training_runs.loc[0, "secondary_bundle_dir"], str)
    assert backtest_runs.loc[0, "variant_label"] == "hybrid_shadow"
    assert isinstance(backtest_runs.loc[0, "secondary_bundle_dir"], str)
