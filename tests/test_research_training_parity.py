from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import DateWindow, TrainingRunSpec, TrainingSetSpec
from pm15min.research.datasets.feature_frames import build_feature_frame_dataset
from pm15min.research.datasets.training_sets import build_training_set_dataset
from pm15min.research.labels.datasets import build_label_frame_dataset
from pm15min.research.training.reports import render_offset_training_report, render_training_run_report
from pm15min.research.training.runner import train_research_run
from pm15min.research.training.splits import build_purged_time_series_splits
from pm15min.research.training.trainers import TrainerConfig, fit_lgbm, generate_oof_predictions
from pm15min.research.training.weights import compute_sample_weights


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
                "final_price": price_base + offset * 3.0 + (1.5 if offset % 2 == 0 else -1.5),
                "source_price_to_beat": "direct_api",
                "source_final_price": "streams_rpc",
                "has_price_to_beat": True,
                "has_final_price": True,
                "has_both": True,
            }
        )
    return pd.DataFrame(rows)


def _prepare_cfg(tmp_path: Path) -> ResearchConfig:
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

    return ResearchConfig.build(
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


def test_build_purged_time_series_splits_respects_purge_and_embargo() -> None:
    decision_ts = pd.date_range("2026-03-01T00:00:00Z", periods=12, freq="15min", tz="UTC")
    splits = build_purged_time_series_splits(
        pd.Series(decision_ts),
        n_splits=3,
        purge_minutes=15,
        embargo_minutes=15,
    )

    assert splits
    train_idx, test_idx = splits[0]
    assert int(train_idx[-1]) < int(test_idx[0])


def test_compute_sample_weights_balances_classes_and_weights_vol() -> None:
    frame = pd.DataFrame(
        {
            "rv_30": [1.0, 2.0, 4.0, 8.0],
            "ret_from_strike": [0.1, 0.2, 1.0, 1.5],
        }
    )
    y = pd.Series([0, 0, 1, 1], dtype=int)
    weights = compute_sample_weights(
        frame,
        y,
        balance_classes=True,
        weight_by_vol=True,
        inverse_vol=False,
        contrarian_weight=2.0,
        contrarian_quantile=0.75,
    )

    assert len(weights) == 4
    assert float(weights.max()) > float(weights.min())


def test_compute_sample_weights_upweights_tradeable_winner_rows() -> None:
    frame = pd.DataFrame(
        {
            "winner_in_band": [True, False, True, False],
        }
    )
    y = pd.Series([0, 0, 1, 1], dtype=int)

    weights = compute_sample_weights(
        frame,
        y,
        balance_classes=False,
        weight_by_vol=False,
        inverse_vol=False,
        contrarian_weight=1.0,
        winner_in_band_weight=2.5,
    )

    assert weights.tolist() == [2.5, 1.0, 2.5, 1.0]


def test_train_research_run_writes_reports_and_rich_offset_summary(tmp_path: Path) -> None:
    cfg = _prepare_cfg(tmp_path)
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
            run_label="training-parity",
            offsets=(7, 8),
        ),
    )

    run_dir = Path(summary["run_dir"])
    assert (run_dir / "summary.json").exists()
    assert (run_dir / "report.md").exists()

    payload = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert payload["offset_summaries"]

    for offset in (7, 8):
        offset_dir = run_dir / "offsets" / f"offset={offset}"
        assert (offset_dir / "summary.json").exists()
        assert (offset_dir / "report.md").exists()
        assert (offset_dir / "feature_pruning.json").exists()
        assert (offset_dir / "logreg_coefficients.json").exists()
        assert (offset_dir / "lgb_feature_importance.json").exists()
        assert (offset_dir / "factor_direction_summary.json").exists()
        assert (offset_dir / "factor_correlations.parquet").exists()
        assert (offset_dir / "probe.json").exists()
        assert (offset_dir / "calibration" / "reliability_bins.json").exists()
        summary_payload = json.loads((offset_dir / "summary.json").read_text(encoding="utf-8"))
        assert summary_payload["weight_summary"]["mean_weight"] is not None
        assert summary_payload["split_summary"]["folds_built"] >= summary_payload["split_summary"]["folds_used"]
        assert summary_payload["explainability"]["top_logreg_coefficients"]
        assert summary_payload["explainability"]["top_lgb_importance"]
        factor_direction_payload = json.loads((offset_dir / "factor_direction_summary.json").read_text(encoding="utf-8"))
        assert factor_direction_payload["rows"]
        correlation_payload = pd.read_parquet(offset_dir / "factor_correlations.parquet")
        assert not correlation_payload.empty


def test_train_research_run_rerun_removes_stale_offset_outputs(monkeypatch, tmp_path: Path) -> None:
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        model_family="deep_otm",
        root=tmp_path / "v2",
    )
    run_dir = cfg.layout.training_run_dir(model_family="deep_otm", target="direction", run_label_text="rerun-clean")
    stale_dir = run_dir / "offsets" / "offset=8"
    stale_dir.mkdir(parents=True, exist_ok=True)
    (stale_dir / "stale.txt").write_text("old", encoding="utf-8")

    monkeypatch.setattr("pm15min.research.training.runner.prepare_research_artifacts", lambda *args, **kwargs: None)

    def _fake_execute_training_offset(*, cfg, spec, offset, run_dir, trainer_cfg, reporter=None):
        offset_dir = run_dir / "offsets" / f"offset={offset}"
        offset_dir.mkdir(parents=True, exist_ok=True)
        (offset_dir / "summary.json").write_text("{}", encoding="utf-8")
        return run_dir / f"offset={offset}.parquet", {
            "rows": 16,
            "positive_rate": 0.5,
            "metrics": {
                "lgbm": {"brier": 0.2, "auc": 0.7},
                "logreg": {"brier": 0.21, "auc": 0.69},
                "blend": {"brier": 0.19, "auc": 0.71},
            },
            "dropped_features": [],
            "weight_summary": {},
            "split_summary": {"folds_built": 1, "folds_used": 1},
            "explainability": {},
        }

    monkeypatch.setattr("pm15min.research.training.runner._execute_training_offset", _fake_execute_training_offset)

    summary = train_research_run(
        cfg,
        TrainingRunSpec(
            model_family="deep_otm",
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            run_label="rerun-clean",
            offsets=(7,),
        ),
    )

    resolved_run_dir = Path(summary["run_dir"])
    assert (resolved_run_dir / "offsets" / "offset=7").exists()
    assert not (resolved_run_dir / "offsets" / "offset=8").exists()


def test_generate_oof_predictions_reports_fold_heartbeats() -> None:
    X = pd.DataFrame(
        {
            "feature_a": np.linspace(0.0, 1.1, 12),
            "feature_b": np.linspace(1.1, 0.0, 12),
        }
    )
    y = pd.Series([0, 1] * 6, dtype=int)
    decision_ts = pd.Series(pd.date_range("2026-03-01T00:00:00Z", periods=12, freq="15min", tz="UTC"))
    raw_frame = pd.DataFrame(
        {
            "rv_30": np.linspace(1.0, 2.1, 12),
            "ret_from_strike": np.linspace(-1.2, 1.0, 12),
        }
    )
    cfg = TrainerConfig(n_splits=3)
    events: list[dict[str, object]] = []

    oof = generate_oof_predictions(
        X,
        y,
        decision_ts=decision_ts,
        raw_frame=raw_frame,
        cfg=cfg,
        reporter=lambda **payload: events.append(payload),
    )

    expected_folds = build_purged_time_series_splits(
        decision_ts,
        n_splits=min(int(cfg.n_splits), max(2, len(X) // 4)),
        purge_minutes=cfg.purge_minutes,
        embargo_minutes=cfg.embargo_minutes,
    )
    assert not oof.empty
    assert len(events) == len(expected_folds)
    assert {event["current_stage"] for event in events} == {"training_oof"}
    assert all(str(event["summary"]).startswith("Generating OOF predictions (fold ") for event in events)


def test_fit_lgbm_scales_n_jobs_with_parallel_workers(monkeypatch) -> None:
    monkeypatch.setattr("pm15min.research.training.trainers.os.cpu_count", lambda: 8)
    X = pd.DataFrame({"feature_a": np.linspace(0.0, 1.0, 12), "feature_b": np.linspace(1.0, 0.0, 12)})
    y = pd.Series([0, 1] * 6, dtype=int)

    model = fit_lgbm(X, y, cfg=TrainerConfig(parallel_workers=2))

    assert model.get_params()["n_jobs"] == 4


def test_fit_lgbm_honors_experiment_cpu_thread_cap(monkeypatch) -> None:
    monkeypatch.setattr("pm15min.research.training.trainers.os.cpu_count", lambda: 24)
    monkeypatch.setenv("PM15MIN_EXPERIMENT_CPU_THREADS", "6")
    X = pd.DataFrame({"feature_a": np.linspace(0.0, 1.0, 12), "feature_b": np.linspace(1.0, 0.0, 12)})
    y = pd.Series([0, 1] * 6, dtype=int)

    model = fit_lgbm(X, y, cfg=TrainerConfig(parallel_workers=1))

    assert model.get_params()["n_jobs"] == 6


def test_fit_lgbm_splits_experiment_cpu_thread_cap_across_parallel_workers(monkeypatch) -> None:
    monkeypatch.setattr("pm15min.research.training.trainers.os.cpu_count", lambda: 24)
    monkeypatch.setenv("PM15MIN_EXPERIMENT_CPU_THREADS", "6")
    X = pd.DataFrame({"feature_a": np.linspace(0.0, 1.0, 12), "feature_b": np.linspace(1.0, 0.0, 12)})
    y = pd.Series([0, 1] * 6, dtype=int)

    model = fit_lgbm(X, y, cfg=TrainerConfig(parallel_workers=3))

    assert model.get_params()["n_jobs"] == 2


def test_train_research_run_reports_offset_progress_and_oof_heartbeats(tmp_path: Path) -> None:
    cfg = _prepare_cfg(tmp_path)
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

    events: list[dict[str, object]] = []
    train_research_run(
        cfg,
        TrainingRunSpec(
            model_family="deep_otm",
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            run_label="training-progress",
            offsets=(7, 8),
        ),
        reporter=lambda **payload: events.append(payload),
    )

    prepare_events = [event for event in events if event["current_stage"] == "training_prepare"]
    assert prepare_events
    assert all(str(event["summary"]).startswith("Preparing offset ") for event in prepare_events)
    assert {"summary": "Completed offset 7 (1/2)", "current": 1, "total": 2, "current_stage": "training_offsets", "progress_pct": 50, "heartbeat": None} in events
    assert {"summary": "Completed offset 8 (2/2)", "current": 2, "total": 2, "current_stage": "training_offsets", "progress_pct": 100, "heartbeat": None} in events
    oof_events = [event for event in events if event["current_stage"] == "training_oof"]
    assert oof_events
    assert {event["current"] for event in oof_events} == {0, 1}
    assert all(event["total"] == 2 for event in oof_events)
    assert all(str(event["summary"]).startswith("Offset ") for event in oof_events)
    artifact_events = [event for event in events if event["current_stage"] == "training_artifacts"]
    assert artifact_events
    assert all("writing artifacts" in str(event["summary"]).lower() for event in artifact_events)
    assert {"summary": "Writing training outputs", "current": 2, "total": 2, "current_stage": "training_finalize", "progress_pct": 100, "heartbeat": None} in events


def test_train_research_run_honors_weight_overrides(tmp_path: Path) -> None:
    cfg = _prepare_cfg(tmp_path)
    build_feature_frame_dataset(cfg)
    build_label_frame_dataset(cfg)
    build_training_set_dataset(
        cfg,
        TrainingSetSpec(
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            offset=7,
        ),
    )

    default_summary = train_research_run(
        cfg,
        TrainingRunSpec(
            model_family="deep_otm",
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            run_label="weights-default",
            offsets=(7,),
            weight_variant_label="current_default",
        ),
    )
    novol_summary = train_research_run(
        cfg,
        TrainingRunSpec(
            model_family="deep_otm",
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            run_label="weights-novol",
            offsets=(7,),
            weight_variant_label="no_vol_weight",
            weight_by_vol=False,
        ),
    )

    default_offset = json.loads(
        (Path(default_summary["run_dir"]) / "offsets" / "offset=7" / "summary.json").read_text(encoding="utf-8")
    )
    novol_offset = json.loads(
        (Path(novol_summary["run_dir"]) / "offsets" / "offset=7" / "summary.json").read_text(encoding="utf-8")
    )

    assert default_offset["weight_summary"]["max_weight"] != novol_offset["weight_summary"]["max_weight"]
    assert default_offset["weight_summary"]["mean_weight"] != novol_offset["weight_summary"]["mean_weight"]


def test_train_research_run_honors_offset_weight_overrides(tmp_path: Path) -> None:
    cfg = _prepare_cfg(tmp_path)
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
            run_label="weights-offset-specific",
            offsets=(7, 8),
            weight_variant_label="direction_offset_reversal_mild",
            offset_weight_overrides={
                7: {"contrarian_weight": 1.25, "contrarian_quantile": 0.8},
                8: {"contrarian_weight": 2.0, "contrarian_quantile": 0.75},
            },
        ),
    )

    run_dir = Path(summary["run_dir"])
    offset7 = json.loads((run_dir / "offsets" / "offset=7" / "summary.json").read_text(encoding="utf-8"))
    offset8 = json.loads((run_dir / "offsets" / "offset=8" / "summary.json").read_text(encoding="utf-8"))
    training_summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))

    assert offset7["weight_summary"]["contrarian_weight"] == 1.25
    assert offset7["weight_summary"]["contrarian_quantile"] == 0.8
    assert offset8["weight_summary"]["contrarian_weight"] == 2.0
    assert offset8["weight_summary"]["contrarian_quantile"] == 0.75
    assert offset7["weight_summary"]["mean_weight"] != offset8["weight_summary"]["mean_weight"]
    assert training_summary["offset_weight_overrides"] == {
        "7": {"contrarian_weight": 1.25, "contrarian_quantile": 0.8},
        "8": {"contrarian_weight": 2.0, "contrarian_quantile": 0.75},
    }


def test_training_reports_fall_back_without_tabulate(monkeypatch) -> None:
    def _raise_import_error(self, *args, **kwargs):
        raise ImportError("Missing optional dependency 'tabulate'")

    monkeypatch.setattr(pd.DataFrame, "to_markdown", _raise_import_error)

    offset_report = render_offset_training_report(
        offset=7,
        rows=12,
        positive_rate=0.5,
        feature_count=3,
        dropped_features=["a"],
        metrics={"blend": {"brier": 0.1, "logloss": 0.2, "auc": 0.8}},
        explainability={
            "top_logreg_coefficients": [{"feature": "x", "coefficient": 1.2}],
            "top_lgb_importance": [{"feature": "x", "gain_share": 0.7}],
            "top_positive_factors": [{"feature": "x", "direction_score": 0.4}],
            "top_negative_factors": [{"feature": "y", "direction_score": -0.2}],
        },
    )
    run_report = render_training_run_report(
        {
            "market": "btc",
            "cycle": "15m",
            "model_family": "deep_otm",
            "feature_set": "bs_q_replace_direction",
            "label_set": "truth",
            "target": "direction",
            "window": "2025-10-27_2026-03-27",
            "weight_variant_label": "offset_reversal_mild",
            "offset_summaries": [
                {
                    "offset": 7,
                    "rows": 12,
                    "positive_rate": 0.5,
                    "dropped_features": ["a"],
                    "brier_lgb": 0.2,
                    "brier_lr": 0.3,
                    "brier_blend": 0.1,
                    "auc_lgb": 0.7,
                    "auc_lr": 0.6,
                    "auc_blend": 0.8,
                }
            ],
        }
    )

    assert "# Training Offset 7" in offset_report
    assert "| feature |" in offset_report
    assert "# Training Run Summary" in run_report
    assert "| offset | rows |" in run_report
