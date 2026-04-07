from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.pipelines.oracle_prices import build_oracle_prices_15m
from pm15min.data.pipelines.truth import build_truth_15m
from pm15min.research.config import ResearchConfig
from pm15min.research.bundles.builder import build_model_bundle
from pm15min.research.backtests.engine import run_research_backtest
from pm15min.research.datasets.feature_frames import build_feature_frame_dataset
from pm15min.research.datasets.training_sets import build_training_set_dataset
from pm15min.research.experiments.runner import run_experiment_suite
from pm15min.research.evaluation.calibration import run_calibration_evaluation
from pm15min.research.evaluation.drift import run_drift_evaluation
from pm15min.research.evaluation.poly_eval import run_poly_eval_report
from pm15min.research.manifests import read_manifest
from pm15min.research.contracts import BacktestRunSpec, DateWindow, ModelBundleSpec, TrainingSetSpec
from pm15min.research.labels.datasets import build_label_frame_dataset
from pm15min.research.training.runner import train_research_run
from pm15min.research.contracts import EvaluationRunSpec, TrainingRunSpec


def _sample_klines(symbol: str, *, start: str, periods: int, price_base: float) -> pd.DataFrame:
    ts = pd.date_range(start, periods=periods, freq="min", tz="UTC")
    idx = np.arange(periods, dtype=float)
    close = price_base + 0.2 * idx + np.sin(idx / 8.0)
    return pd.DataFrame(
        {
            "open_time": ts,
            "open": close - 0.1,
            "high": close + 0.3,
            "low": close - 0.3,
            "close": close,
            "volume": 1000.0 + 5.0 * idx,
            "quote_asset_volume": (1000.0 + 5.0 * idx) * close,
            "taker_buy_quote_volume": (450.0 + 2.0 * idx) * close,
            "symbol": symbol,
        }
    )


def _sample_oracle_prices(asset: str, *, cycle_start_ts: int, n_cycles: int, price_base: float) -> pd.DataFrame:
    rows = []
    for offset in range(n_cycles):
        start_ts = cycle_start_ts + offset * 900
        rows.append(
            {
                "asset": asset,
                "cycle_start_ts": start_ts,
                "cycle_end_ts": start_ts + 900,
                "price_to_beat": price_base + offset * 3.0,
                "final_price": price_base + offset * 3.0 + 1.5,
                "source_price_to_beat": "direct_api",
                "source_final_price": "streams_rpc",
                "has_price_to_beat": True,
                "has_final_price": True,
                "has_both": True,
            }
        )
    return pd.DataFrame(rows)


def _sample_oracle_prices_for_cycle(
    asset: str,
    *,
    cycle_start_ts: int,
    cycle_seconds: int,
    n_cycles: int,
    price_base: float,
) -> pd.DataFrame:
    rows = []
    for offset in range(n_cycles):
        start_ts = cycle_start_ts + offset * cycle_seconds
        rows.append(
            {
                "asset": asset,
                "cycle_start_ts": start_ts,
                "cycle_end_ts": start_ts + cycle_seconds,
                "price_to_beat": price_base + offset * 2.0,
                "final_price": price_base + offset * 2.0 + 1.0,
                "source_price_to_beat": "direct_api",
                "source_final_price": "streams_rpc",
                "has_price_to_beat": True,
                "has_final_price": True,
                "has_both": True,
            }
        )
    return pd.DataFrame(rows)


def test_build_feature_frame_dataset(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(
        _sample_klines("SOLUSDT", start="2026-03-01T00:00:00Z", periods=180, price_base=120.0),
        data_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_klines("BTCUSDT", start="2026-03-01T00:00:00Z", periods=180, price_base=50000.0),
        btc_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_oracle_prices("sol", cycle_start_ts=1_772_323_200, n_cycles=16, price_base=120.0),
        data_cfg.layout.oracle_prices_table_path,
    )

    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        root=root,
    )
    summary = build_feature_frame_dataset(cfg, skip_freshness=True)

    out = pd.read_parquet(cfg.layout.feature_frame_path(cfg.feature_set, source_surface=cfg.source_surface))
    assert summary["rows_written"] == len(out)
    assert "decision_ts" in out.columns
    assert "ret_from_strike" in out.columns
    assert "basis_bp" in out.columns
    assert "btc_ret_5m" in out.columns
    assert out["has_oracle_strike"].max() == 1
    assert out["decision_ts"].is_monotonic_increasing
    offset_7_row = out.loc[pd.to_datetime(out["decision_ts"], utc=True, errors="coerce").eq(pd.Timestamp("2026-03-01T00:07:00Z"))]
    assert len(offset_7_row) == 1
    assert int(offset_7_row.iloc[0]["offset"]) == 7
    cycle_boundary_row = out.loc[
        pd.to_datetime(out["decision_ts"], utc=True, errors="coerce").eq(pd.Timestamp("2026-03-01T00:15:00Z"))
    ]
    assert len(cycle_boundary_row) == 1
    assert int(cycle_boundary_row.iloc[0]["offset"]) == 0
    assert cycle_boundary_row.iloc[0]["cycle_start_ts"] == pd.Timestamp("2026-03-01T00:15:00Z")
    assert float(cycle_boundary_row.iloc[0]["ret_from_cycle_open"]) == 0.0
    assert float(cycle_boundary_row.iloc[0]["first_half_ret"]) == 0.0


def test_build_feature_frame_dataset_uses_custom_feature_sets_from_cfg_root(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="backtest", root=root)
    (root / "research" / "experiments").mkdir(parents=True, exist_ok=True)
    (root / "research" / "experiments" / "custom_feature_sets.json").write_text(
        json.dumps(
            {
                "focus_sol_4_v1": {
                    "columns": [
                        "q_bs_up_strike",
                        "first_half_ret",
                        "bb_pos_20",
                        "ret_from_cycle_open",
                    ]
                }
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    write_parquet_atomic(
        _sample_klines("SOLUSDT", start="2026-03-01T00:00:00Z", periods=180, price_base=120.0),
        data_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_klines("BTCUSDT", start="2026-03-01T00:00:00Z", periods=180, price_base=50000.0),
        btc_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_oracle_prices("sol", cycle_start_ts=1_772_323_200, n_cycles=16, price_base=120.0),
        data_cfg.layout.oracle_prices_table_path,
    )

    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="focus_sol_4_v1",
        root=root,
    )

    summary = build_feature_frame_dataset(cfg, skip_freshness=True)
    out = pd.read_parquet(cfg.layout.feature_frame_path(cfg.feature_set, source_surface=cfg.source_surface))

    assert summary["feature_set"] == "focus_sol_4_v1"
    assert {"q_bs_up_strike", "first_half_ret", "bb_pos_20", "ret_from_cycle_open"}.issubset(out.columns)


def test_build_feature_frame_uses_decision_time_definitions(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="backtest", root=root)

    sol_klines = _sample_klines("SOLUSDT", start="2026-03-01T23:40:00Z", periods=40, price_base=120.0)
    btc_klines = _sample_klines("BTCUSDT", start="2026-03-01T23:40:00Z", periods=40, price_base=50000.0)

    write_parquet_atomic(sol_klines, data_cfg.layout.binance_klines_path())
    write_parquet_atomic(btc_klines, btc_cfg.layout.binance_klines_path())
    write_parquet_atomic(
        _sample_oracle_prices("sol", cycle_start_ts=1_772_409_300, n_cycles=4, price_base=120.0),
        data_cfg.layout.oracle_prices_table_path,
    )

    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="alpha_search_direction_live",
        root=root,
    )
    build_feature_frame_dataset(cfg)
    out = pd.read_parquet(cfg.layout.feature_frame_path(cfg.feature_set, source_surface=cfg.source_surface))

    boundary_row = out.loc[
        pd.to_datetime(out["decision_ts"], utc=True, errors="coerce").eq(pd.Timestamp("2026-03-02T00:00:00Z"))
    ]
    assert len(boundary_row) == 1
    row = boundary_row.iloc[0]
    assert row["cycle_start_ts"] == pd.Timestamp("2026-03-02T00:00:00Z")
    assert int(row["offset"]) == 0
    assert float(row["ret_from_cycle_open"]) == 0.0
    assert float(row["hour_sin"]) == 0.0
    assert float(row["hour_cos"]) == 1.0

    expected_dow = pd.Timestamp("2026-03-02T00:00:00Z").dayofweek
    expected_dow_angle = 2.0 * np.pi * expected_dow / 7.0
    assert float(row["dow_sin"]) == pytest.approx(float(np.sin(expected_dow_angle)))
    assert float(row["dow_cos"]) == pytest.approx(float(np.cos(expected_dow_angle)))

    btc = btc_klines.copy()
    btc["decision_ts"] = pd.to_datetime(btc["open_time"], utc=True, errors="coerce") + pd.Timedelta(minutes=1)
    btc = btc.set_index("decision_ts")
    btc_close = pd.to_numeric(btc["close"], errors="coerce")
    expected_btc_ret_5m = float(btc_close.pct_change(5).loc[pd.Timestamp("2026-03-02T00:00:00Z")])
    assert float(row["btc_ret_5m"]) == pytest.approx(expected_btc_ret_5m)


def test_build_label_frame_dataset_truth(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="xrp", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(
        pd.DataFrame(
                [
                    {
                        "asset": "xrp",
                        "cycle_start_ts": 1_772_323_200,
                        "cycle_end_ts": 1_772_324_100,
                        "market_id": "market-1",
                        "condition_id": "cond-1",
                        "winner_side": "UP",
                    "label_updown": "UP",
                    "resolved": True,
                    "truth_source": "settlement_truth",
                    "full_truth": True,
                }
            ]
        ),
        data_cfg.layout.truth_table_path,
    )
    write_parquet_atomic(
        _sample_oracle_prices("xrp", cycle_start_ts=1_772_323_200, n_cycles=1, price_base=2.0),
        data_cfg.layout.oracle_prices_table_path,
    )

    cfg = ResearchConfig.build(
        market="xrp",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        label_set="truth",
        root=root,
    )
    summary = build_label_frame_dataset(cfg, skip_freshness=True)
    out = pd.read_parquet(cfg.layout.label_frame_path(cfg.label_set))
    manifest = read_manifest(cfg.layout.label_frame_manifest_path(cfg.label_set))

    assert summary["rows_written"] == 1
    assert out.iloc[0]["label_set"] == "truth"
    assert float(out.iloc[0]["direction_up"]) == 1.0
    assert float(out.iloc[0]["price_to_beat"]) == 2.0
    assert out.iloc[0]["settlement_source"] == "settlement_truth"
    assert manifest.metadata["status"] == "ok"
    assert manifest.metadata["truth_table_rows"] == 1
    assert manifest.metadata["oracle_has_both_rows"] == 1


def test_build_label_frame_dataset_truth_prefers_resolved_oracle_candidate(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="xrp", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "xrp",
                    "cycle_start_ts": 1_772_323_200,
                    "cycle_end_ts": 1_772_324_100,
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "winner_side": "",
                    "label_updown": "",
                    "resolved": False,
                    "truth_source": "settlement_truth",
                    "full_truth": False,
                },
                {
                    "asset": "xrp",
                    "cycle_start_ts": 1_772_323_200,
                    "cycle_end_ts": 1_772_324_100,
                    "market_id": "",
                    "condition_id": "",
                    "winner_side": "DOWN",
                    "label_updown": "DOWN",
                    "resolved": True,
                    "truth_source": "oracle_prices",
                    "full_truth": True,
                },
            ]
        ),
        data_cfg.layout.truth_table_path,
    )
    write_parquet_atomic(
        _sample_oracle_prices("xrp", cycle_start_ts=1_772_323_200, n_cycles=1, price_base=2.0),
        data_cfg.layout.oracle_prices_table_path,
    )

    cfg = ResearchConfig.build(
        market="xrp",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        label_set="truth",
        root=root,
    )
    summary = build_label_frame_dataset(cfg, skip_freshness=True)
    out = pd.read_parquet(cfg.layout.label_frame_path(cfg.label_set))

    assert summary["rows_written"] == 1
    assert len(out) == 1
    assert out.iloc[0]["winner_side"] == "UP"
    assert bool(out.iloc[0]["resolved"]) is True
    assert bool(out.iloc[0]["full_truth"]) is True
    assert out.iloc[0]["settlement_source"] == "streams_rpc"


def test_build_oracle_truth_and_label_frame_support_cycle_5m(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="xrp", cycle="5m", surface="backtest", root=root)

    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "asset": "xrp",
                    "cycle": "5m",
                    "cycle_start_ts": 1_772_323_200,
                    "cycle_end_ts": 1_772_323_500,
                    "token_up": "tok-up-1",
                    "token_down": "tok-down-1",
                    "slug": "xrp-updown-5m-1772323200",
                    "question": "XRP up?",
                    "resolution_source": "data.chain.link/streams/xrp-usd",
                    "event_id": "event-1",
                    "event_slug": "event-1",
                    "event_title": "XRP up?",
                    "series_slug": "xrp-up-or-down-5m",
                    "closed_ts": 1_772_323_500,
                    "source_snapshot_ts": "2026-03-01T00-05-00Z",
                },
                {
                    "market_id": "market-2",
                    "condition_id": "cond-2",
                    "asset": "xrp",
                    "cycle": "5m",
                    "cycle_start_ts": 1_772_323_500,
                    "cycle_end_ts": 1_772_323_800,
                    "token_up": "tok-up-2",
                    "token_down": "tok-down-2",
                    "slug": "xrp-updown-5m-1772323500",
                    "question": "XRP up again?",
                    "resolution_source": "data.chain.link/streams/xrp-usd",
                    "event_id": "event-2",
                    "event_slug": "event-2",
                    "event_title": "XRP up again?",
                    "series_slug": "xrp-up-or-down-5m",
                    "closed_ts": 1_772_323_800,
                    "source_snapshot_ts": "2026-03-01T00-10-00Z",
                },
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        _sample_oracle_prices_for_cycle(
            "xrp",
            cycle_start_ts=1_772_323_200,
            cycle_seconds=300,
            n_cycles=2,
            price_base=2.0,
        ),
        data_cfg.layout.direct_oracle_source_path,
    )

    oracle_summary = build_oracle_prices_15m(data_cfg)
    truth_summary = build_truth_15m(data_cfg)

    cfg = ResearchConfig.build(
        market="xrp",
        cycle="5m",
        profile="deep_otm",
        source_surface="backtest",
        label_set="truth",
        root=root,
    )
    label_summary = build_label_frame_dataset(cfg, skip_freshness=True)
    out = pd.read_parquet(cfg.layout.label_frame_path(cfg.label_set))
    manifest = read_manifest(cfg.layout.label_frame_manifest_path(cfg.label_set))

    assert oracle_summary["dataset"] == "oracle_prices_5m"
    assert truth_summary["dataset"] == "truth_5m"
    assert label_summary["rows_written"] == 2
    assert len(out) == 2
    assert out["market_id"].tolist() == ["market-1", "market-2"]
    assert out["winner_side"].tolist() == ["UP", "UP"]
    assert float(out.iloc[0]["price_to_beat"]) == 2.0
    assert float(out.iloc[0]["final_price"]) == 3.0
    input_kinds = [str(item.get("kind")) for item in manifest.inputs]
    assert "truth_5m" in input_kinds
    assert "oracle_prices_5m" in input_kinds


def test_build_training_set_dataset_direction_and_reversal(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(
        _sample_klines("SOLUSDT", start="2026-03-01T00:00:00Z", periods=240, price_base=120.0),
        data_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_klines("BTCUSDT", start="2026-03-01T00:00:00Z", periods=240, price_base=50000.0),
        btc_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_oracle_prices("sol", cycle_start_ts=1_772_323_200, n_cycles=16, price_base=120.0),
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
                    "winner_side": "UP",
                    "label_updown": "UP",
                    "resolved": True,
                    "truth_source": "settlement_truth",
                    "full_truth": True,
                }
                for idx in range(16)
            ]
        ),
        data_cfg.layout.truth_table_path,
    )

    feature_cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        root=root,
    )
    build_feature_frame_dataset(feature_cfg)
    build_label_frame_dataset(feature_cfg)

    direction_spec = TrainingSetSpec(
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
        offset=7,
    )
    direction_summary = build_training_set_dataset(feature_cfg, direction_spec)
    direction_out = pd.read_parquet(
        feature_cfg.layout.training_set_path(
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window="2026-03-01_2026-03-01",
            offset=7,
        )
    )
    assert direction_summary["rows_written"] == len(direction_out)
    assert set(direction_out["y"].dropna().unique()) <= {0, 1}
    assert set(direction_out["offset"].dropna().astype(int).unique()) == {7}
    assert direction_out["target"].eq("direction").all()

    reversal_spec = TrainingSetSpec(
        feature_set="deep_otm_v1",
        label_set="truth",
        target="reversal",
        window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
        offset=7,
    )
    reversal_summary = build_training_set_dataset(feature_cfg, reversal_spec)
    reversal_out = pd.read_parquet(
        feature_cfg.layout.training_set_path(
            feature_set="deep_otm_v1",
            label_set="truth",
            target="reversal",
            window="2026-03-01_2026-03-01",
            offset=7,
        )
    )
    assert reversal_summary["rows_written"] == len(reversal_out)
    assert reversal_out["target"].eq("reversal").all()
    assert reversal_out["current_ret_col"].eq("ret_from_strike").all()
    assert set(reversal_out["y"].dropna().unique()) <= {0, 1}


def test_train_research_run_writes_offset_artifacts(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(
        _sample_klines("SOLUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=120.0),
        data_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_klines("BTCUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=50000.0),
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
    build_feature_frame_dataset(cfg)
    build_label_frame_dataset(cfg)
    for offset in (7, 8):
        build_training_set_dataset(
            cfg,
            TrainingSetSpec(
                feature_set="deep_otm_v1",
                label_set="truth",
                target="direction",
                window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
                offset=offset,
            ),
        )

    summary = train_research_run(
        cfg,
        TrainingRunSpec(
            model_family="deep_otm",
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            run_label="smoke",
            offsets=(7, 8),
        ),
    )

    run_dir = Path(summary["run_dir"])
    assert run_dir.exists()
    assert (run_dir / "summary.json").exists()
    assert (run_dir / "manifest.json").exists()
    for offset in (7, 8):
        offset_dir = run_dir / "offsets" / f"offset={offset}"
        assert (offset_dir / "metrics.json").exists()
        assert (offset_dir / "feature_schema.json").exists()
        assert (offset_dir / "oof_predictions.parquet").exists()
        assert (offset_dir / "models" / "lgbm_sigmoid.joblib").exists()
        assert (offset_dir / "models" / "logreg_sigmoid.joblib").exists()
        assert (offset_dir / "calibration" / "blend_weights.json").exists()


def test_build_model_bundle_from_training_run(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(
        _sample_klines("SOLUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=120.0),
        data_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_klines("BTCUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=50000.0),
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
    build_feature_frame_dataset(cfg)
    build_label_frame_dataset(cfg)
    for offset in (7, 8):
        build_training_set_dataset(
            cfg,
            TrainingSetSpec(
                feature_set="deep_otm_v1",
                label_set="truth",
                target="direction",
                window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
                offset=offset,
            ),
        )
    train_research_run(
        cfg,
        TrainingRunSpec(
            model_family="deep_otm",
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            run_label="bundle-source",
            offsets=(7, 8),
        ),
    )

    summary = build_model_bundle(
        cfg,
        ModelBundleSpec(
            profile="deep_otm",
            target="direction",
            bundle_label="bundle-smoke",
            offsets=(7, 8),
            source_training_run="bundle-source",
        ),
    )
    bundle_dir = Path(summary["bundle_dir"])
    assert bundle_dir.exists()
    assert (bundle_dir / "manifest.json").exists()
    for offset in (7, 8):
        offset_dir = bundle_dir / "offsets" / f"offset={offset}"
        assert (offset_dir / "feature_schema.json").exists()
        assert (offset_dir / "feature_cols.joblib").exists()
        assert (offset_dir / "bundle_config.json").exists()
        assert (offset_dir / "models" / "lgbm_sigmoid.joblib").exists()
        assert (offset_dir / "models" / "logreg_sigmoid.joblib").exists()
        assert (offset_dir / "calibration" / "blend_weights.json").exists()


def test_run_research_backtest_from_bundle(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(
        _sample_klines("SOLUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=120.0),
        data_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_klines("BTCUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=50000.0),
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
    build_feature_frame_dataset(cfg)
    build_label_frame_dataset(cfg)
    for offset in (7, 8):
        build_training_set_dataset(
            cfg,
            TrainingSetSpec(
                feature_set="deep_otm_v1",
                label_set="truth",
                target="direction",
                window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
                offset=offset,
            ),
        )
    train_research_run(
        cfg,
        TrainingRunSpec(
            model_family="deep_otm",
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            run_label="bt-source",
            offsets=(7, 8),
        ),
    )
    build_model_bundle(
        cfg,
        ModelBundleSpec(
            profile="deep_otm",
            target="direction",
            bundle_label="bt-bundle",
            offsets=(7, 8),
            source_training_run="bt-source",
        ),
    )

    summary = run_research_backtest(
        cfg,
        BacktestRunSpec(
            profile="deep_otm",
            spec_name="baseline_truth",
            run_label="bt-run",
            target="direction",
            bundle_label="bt-bundle",
        ),
    )

    run_dir = Path(summary["run_dir"])
    assert run_dir.exists()
    assert (run_dir / "summary.json").exists()
    assert (run_dir / "manifest.json").exists()
    assert (run_dir / "trades.parquet").exists()
    assert (run_dir / "rejects.parquet").exists()
    assert (run_dir / "markets.parquet").exists()
    assert (run_dir / "equity_curve.parquet").exists()
    trades = pd.read_parquet(run_dir / "trades.parquet")
    assert not trades.empty


def test_run_experiment_suite(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(
        _sample_klines("SOLUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=120.0),
        data_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_klines("BTCUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=50000.0),
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
    suite_path = cfg.layout.storage.suite_spec_path("sol_suite_smoke")
    suite_path.parent.mkdir(parents=True, exist_ok=True)
    suite_path.write_text(
        json.dumps(
            {
                "suite_name": "sol_suite_smoke",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "offsets": [7, 8],
                "window": {"start": "2026-03-01", "end": "2026-03-01"},
                "backtest_spec": "baseline_truth",
                "markets": ["sol"],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    summary = run_experiment_suite(cfg=cfg, suite_name="sol_suite_smoke", run_label="exp-smoke")
    run_dir = Path(summary["run_dir"])
    assert run_dir.exists()
    assert (run_dir / "manifest.json").exists()
    assert (run_dir / "training_runs.parquet").exists()
    assert (run_dir / "backtest_runs.parquet").exists()
    assert (run_dir / "leaderboard.parquet").exists()
    assert (run_dir / "leaderboard.csv").exists()
    leaderboard = pd.read_parquet(run_dir / "leaderboard.parquet")
    assert not leaderboard.empty


def test_run_evaluations_from_backtest(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(
        _sample_klines("SOLUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=120.0),
        data_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_klines("BTCUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=50000.0),
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
    build_feature_frame_dataset(cfg)
    build_label_frame_dataset(cfg)
    for offset in (7, 8):
        build_training_set_dataset(
            cfg,
            TrainingSetSpec(
                feature_set="deep_otm_v1",
                label_set="truth",
                target="direction",
                window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
                offset=offset,
            ),
        )
    train_research_run(
        cfg,
        TrainingRunSpec(
            model_family="deep_otm",
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            run_label="eval-source",
            offsets=(7, 8),
        ),
    )
    build_model_bundle(
        cfg,
        ModelBundleSpec(
            profile="deep_otm",
            target="direction",
            bundle_label="eval-bundle",
            offsets=(7, 8),
            source_training_run="eval-source",
        ),
    )
    run_research_backtest(
        cfg,
        BacktestRunSpec(
            profile="deep_otm",
            spec_name="baseline_truth",
            run_label="eval-backtest",
            target="direction",
            bundle_label="eval-bundle",
        ),
    )

    cal = run_calibration_evaluation(
        cfg,
        EvaluationRunSpec(
            category="calibration",
            scope_label="baseline_truth",
            run_label="eval-cal",
            backtest_spec="baseline_truth",
            backtest_run_label="eval-backtest",
        ),
    )
    drift = run_drift_evaluation(
        cfg,
        EvaluationRunSpec(
            category="drift",
            scope_label="baseline_truth",
            run_label="eval-drift",
            backtest_spec="baseline_truth",
            backtest_run_label="eval-backtest",
        ),
    )
    poly_eval = run_poly_eval_report(
        cfg,
        EvaluationRunSpec(
            category="poly-eval",
            scope_label="baseline_truth",
            run_label="eval-poly",
            backtest_spec="baseline_truth",
            backtest_run_label="eval-backtest",
        ),
    )

    cal_dir = Path(cal["run_dir"])
    assert (cal_dir / "summary.json").exists()
    assert (cal_dir / "reliability.parquet").exists()
    assert (cal_dir / "report.md").exists()
    reliability = pd.read_parquet(cal_dir / "reliability.parquet")
    assert list(reliability.columns) == ["bin_left", "bin_right", "n", "avg_pred", "empirical_rate", "brier"]

    drift_dir = Path(drift["run_dir"])
    assert (drift_dir / "summary.json").exists()
    assert (drift_dir / "slices.parquet").exists()
    assert (drift_dir / "report.md").exists()
    slices = pd.read_parquet(drift_dir / "slices.parquet")
    assert list(slices.columns) == ["date", "trades", "win_rate", "avg_pred", "pnl_sum", "cumulative_pnl"]

    poly_dir = Path(poly_eval["run_dir"])
    assert (poly_dir / "summary.json").exists()
    assert (poly_dir / "report.md").exists()
    assert (poly_dir / "outputs" / "trade_metrics.parquet").exists()
    trade_metrics = pd.read_parquet(poly_dir / "outputs" / "trade_metrics.parquet")
    assert list(trade_metrics.columns) == ["offset", "trades", "win_rate", "avg_pred", "pnl_sum"]
