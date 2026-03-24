from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm15min.console.read_models.bundles import (
    list_console_model_bundles,
    load_console_model_bundle,
)
from pm15min.console.read_models.training_runs import (
    list_console_training_runs,
    load_console_training_run,
)
from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.bundles.builder import build_model_bundle
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import DateWindow, ModelBundleSpec, TrainingRunSpec, TrainingSetSpec
from pm15min.research.datasets.feature_frames import build_feature_frame_dataset
from pm15min.research.datasets.training_sets import build_training_set_dataset
from pm15min.research.labels.datasets import build_label_frame_dataset
from pm15min.research.service import activate_model_bundle
from pm15min.research.training.runner import train_research_run


def _sample_klines(symbol: str, *, start: str, periods: int, price_base: float) -> pd.DataFrame:
    ts = pd.date_range(start, periods=periods, freq="min", tz="UTC")
    idx = pd.Series(range(periods), dtype=float)
    close = price_base + 0.2 * idx
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
    build_feature_frame_dataset(cfg)
    build_label_frame_dataset(cfg)
    return cfg


def _build_training_and_bundle(tmp_path: Path) -> tuple[ResearchConfig, dict[str, object], dict[str, object]]:
    cfg = _prepare_cfg(tmp_path)
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

    training_summary = train_research_run(
        cfg,
        TrainingRunSpec(
            model_family="deep_otm",
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            run_label="console-assets",
            offsets=(7, 8),
        ),
    )
    bundle_summary = build_model_bundle(
        cfg,
        ModelBundleSpec(
            profile="deep_otm",
            target="direction",
            bundle_label="console-bundle",
            offsets=(7, 8),
            source_training_run="console-assets",
        ),
    )
    activate_model_bundle(
        cfg,
        profile="deep_otm",
        target="direction",
        bundle_label="console-bundle",
        notes="console test activation",
    )
    return cfg, training_summary, bundle_summary


def test_list_and_load_console_training_runs(tmp_path: Path) -> None:
    cfg, training_summary, _ = _build_training_and_bundle(tmp_path)

    rows = list_console_training_runs(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        model_family="deep_otm",
        target="direction",
        root=cfg.layout.storage.rewrite_root,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["run_label"] == "console-assets"
    assert row["summary_exists"] is True
    assert row["report_exists"] is True
    assert row["offsets"] == [7, 8]
    assert row["feature_set"] == "deep_otm_v1"
    assert row["label_set"] == "truth"
    assert row["rows_total"] and row["rows_total"] > 0
    assert row["positive_rate_avg"] is not None
    assert row["feature_count_range"] is not None
    assert row["bundle_readiness"]["is_ready"] is True
    assert row["bundle_readiness"]["ready_offset_count"] == 2
    assert row["metric_summary"]["offsets_with_metrics"] == 2
    assert row["metric_summary"]["offsets_with_brier"] == 2
    assert row["metric_summary"]["best_brier_offset"]["offset"] in {7, 8}
    assert row["overview_cards"][0] == {"card_id": "offset_count", "label": "Offsets", "value": 2}
    assert row["overview_cards"][-1] == {"card_id": "bundle_ready_offsets", "label": "Bundle Ready Offsets", "value": 2}
    assert row["action_context"]["model_family"] == "deep_otm"
    assert row["action_context"]["target"] == "direction"
    assert row["action_context"]["run_label"] == "console-assets"
    assert row["action_context"]["offsets"] == [7, 8]

    detail = load_console_training_run(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        model_family="deep_otm",
        target="direction",
        run_label="console-assets",
        root=cfg.layout.storage.rewrite_root,
    )

    assert detail["run_dir"] == training_summary["run_dir"]
    assert detail["summary"]["run_label"] == "console-assets"
    assert "Training Run Summary" in str(detail["report_text"])
    assert len(detail["offset_details"]) == 2
    assert detail["offset_details"][0]["feature_pruning_exists"] is True
    assert detail["offset_details"][0]["probe_exists"] is True
    assert detail["offset_details"][0]["reliability_bins_exists"] is True
    assert detail["offset_details"][0]["summary"]["split_summary"]["folds_built"] >= 0
    assert detail["run_overview"]["offset_count"] == 2
    assert detail["run_overview"]["rows_total"] == row["rows_total"]
    assert detail["run_overview"]["positive_rate_avg"] == row["positive_rate_avg"]
    assert detail["run_overview"]["bundle_ready_offset_count"] == 2
    assert detail["metric_summary"]["best_brier_offset"]["offset"] in {7, 8}
    assert detail["metric_summary"]["mean_brier"] is not None
    assert detail["metric_summary"]["offsets_with_logloss"] == 2
    assert detail["bundle_readiness"]["is_ready"] is True
    assert detail["bundle_readiness"]["offset_rows"][0]["is_ready"] is True
    assert "blend_weights" in detail["bundle_readiness"]["required_artifacts"]
    assert detail["offset_preview"]["row_count"] == 2
    assert detail["offset_preview"]["rows"][0]["offset"] == 7
    assert detail["offset_preview"]["rows"][0]["auc"] is not None
    assert detail["offset_preview"]["rows"][0]["brier"] is not None
    assert detail["offset_preview"]["rows"][0]["blend_w_lgb"] is not None
    assert detail["offset_preview"]["rows"][0]["blend_w_lr"] is not None
    assert detail["offset_preview"]["rows"][0]["bundle_ready"] is True
    assert detail["offset_preview"]["rows"][0]["missing_bundle_artifacts"] == []
    assert detail["offset_preview"]["rows"][0]["top_logreg_feature"]
    assert detail["offset_preview"]["rows"][0]["top_lgb_feature"]
    assert detail["explainability_overview"]["offsets_with_explainability"] == 2
    assert detail["explainability_overview"]["offsets_with_logreg_coefficients"] == 2
    assert detail["explainability_overview"]["offsets_with_lgb_importance"] == 2
    assert detail["explainability_overview"]["offsets_with_factor_direction_summary"] == 2
    assert detail["explainability_overview"]["offsets_with_blend_weights"] == 2
    assert detail["explainability_overview"]["bundle_ready_offsets"] == 2
    assert detail["explainability_overview"]["unique_top_logreg_feature_count"] >= 1
    assert detail["explainability_overview"]["top_logreg_coefficients"]
    assert detail["action_context"]["window"] == "2026-03-01_2026-03-01"
    assert detail["action_context"]["run_dir"] == training_summary["run_dir"]


def test_list_and_load_console_model_bundles(tmp_path: Path) -> None:
    cfg, training_summary, bundle_summary = _build_training_and_bundle(tmp_path)

    rows = list_console_model_bundles(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        profile="deep_otm",
        target="direction",
        root=cfg.layout.storage.rewrite_root,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["bundle_label"] == "console-bundle"
    assert row["summary_exists"] is True
    assert row["report_exists"] is True
    assert row["is_active"] is True
    assert row["offsets"] == [7, 8]
    assert row["feature_set"] == "deep_otm_v1"
    assert row["action_context"]["profile"] == "deep_otm"
    assert row["action_context"]["target"] == "direction"
    assert row["action_context"]["bundle_label"] == "console-bundle"
    assert row["action_context"]["model_family"] == "deep_otm"

    detail = load_console_model_bundle(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        profile="deep_otm",
        target="direction",
        bundle_label="console-bundle",
        root=cfg.layout.storage.rewrite_root,
    )

    assert detail["bundle_dir"] == bundle_summary["bundle_dir"]
    assert detail["summary"]["bundle_label"] == "console-bundle"
    assert "Model Bundle Summary" in str(detail["report_text"])
    assert detail["is_active"] is True
    assert detail["active_selection"]["selection"]["bundle_label"] == "console-bundle"
    assert len(detail["offset_details"]) == 2
    assert detail["offset_details"][0]["bundle_config_exists"] is True
    assert "summary.json" in detail["offset_details"][0]["diagnostic_files"]
    assert detail["offset_details"][0]["diagnostics"]["summary"] is not None
    assert detail["action_context"]["source_training_run_dir"] == training_summary["run_dir"]
    assert detail["action_context"]["bundle_dir"] == bundle_summary["bundle_dir"]
