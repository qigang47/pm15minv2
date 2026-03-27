from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
import pm15min.research.backtests.engine as backtest_engine
from pm15min.research.backtests.engine import run_research_backtest
from pm15min.research.backtests.fills import BacktestFillConfig, build_proxy_fills
from pm15min.research.backtests.hybrid import apply_hybrid_score_fallback
from pm15min.research.backtests.policy import build_policy_decisions, split_policy_decisions
from pm15min.research.backtests.replay_loader import build_replay_frame
from pm15min.research.backtests.runtime_cache import clear_process_backtest_runtime_cache
from pm15min.research.backtests.settlement import settle_trade_fills
from pm15min.research.bundles.builder import build_model_bundle
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import BacktestRunSpec, DateWindow, ModelBundleSpec, TrainingRunSpec, TrainingSetSpec
from pm15min.research.datasets.feature_frames import build_feature_frame_dataset
from pm15min.research.datasets.training_sets import build_training_set_dataset
from pm15min.research.labels.datasets import build_label_frame_dataset
from pm15min.research.training.runner import train_research_run


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


def test_build_replay_frame_tracks_coverage() -> None:
    features = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
            },
            {
                "decision_ts": "2026-03-01T00:16:00Z",
                "cycle_start_ts": "2026-03-01T00:15:00Z",
                "cycle_end_ts": "2026-03-01T00:30:00Z",
                "offset": 8,
            },
            {
                "decision_ts": "2026-03-01T00:31:00Z",
                "cycle_start_ts": "2026-03-01T00:30:00Z",
                "cycle_end_ts": "2026-03-01T00:45:00Z",
                "offset": 7,
            },
        ]
    )
    labels = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "market_id": "m-1",
                "condition_id": "c-1",
                "label_set": "truth",
                "settlement_source": "settlement_truth",
                "label_source": "settlement_truth",
                "resolved": True,
                "price_to_beat": 120.0,
                "final_price": 121.0,
                "winner_side": "UP",
                "direction_up": 1.0,
                "full_truth": True,
            },
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_324_100,
                "cycle_end_ts": 1_772_325_000,
                "market_id": "m-2",
                "condition_id": "c-2",
                "label_set": "truth",
                "settlement_source": "settlement_truth",
                "label_source": "settlement_truth",
                "resolved": True,
                "price_to_beat": 121.0,
                "final_price": 119.0,
                "winner_side": "DOWN",
                "direction_up": 0.0,
                "full_truth": True,
            },
        ]
    )
    scores = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "p_up": 0.72,
                "p_down": 0.28,
                "p_lgb": 0.7,
                "p_lr": 0.74,
                "p_signal": 0.72,
                "score_valid": True,
                "score_reason": "",
            }
        ]
    )

    replay, summary = build_replay_frame(
        features=features,
        labels=labels,
        score_frames=[scores],
        available_offsets=[7],
    )

    assert len(replay) == 3
    assert summary.score_covered_rows == 1
    assert summary.score_missing_rows == 2
    assert summary.bundle_offset_missing_rows == 1
    assert summary.unresolved_label_rows == 1
    assert replay["bundle_offset_available"].tolist() == [True, False, True]


def test_build_bundle_replay_scopes_replay_to_bundle_offsets(tmp_path: Path, monkeypatch) -> None:
    bundle_dir = tmp_path / "bundle"
    (bundle_dir / "offsets" / "offset=7").mkdir(parents=True)
    features = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "ret_1m": 0.1,
            },
            {
                "decision_ts": "2026-03-01T00:16:00Z",
                "cycle_start_ts": "2026-03-01T00:15:00Z",
                "cycle_end_ts": "2026-03-01T00:30:00Z",
                "offset": 8,
                "ret_1m": -0.1,
            },
        ]
    )
    labels = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "label_set": "truth",
                "resolved": True,
                "winner_side": "UP",
            }
        ]
    )

    def _fake_score_bundle_offset(_bundle_dir: Path, feature_frame: pd.DataFrame, *, offset: int) -> pd.DataFrame:
        assert set(feature_frame["offset"].tolist()) == {7, 8}
        assert offset == 7
        return pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-01T00:01:00Z",
                    "cycle_start_ts": "2026-03-01T00:00:00Z",
                    "cycle_end_ts": "2026-03-01T00:15:00Z",
                    "offset": 7,
                    "p_up": 0.74,
                    "p_down": 0.26,
                    "score_valid": True,
                }
            ]
        )

    monkeypatch.setattr(backtest_engine, "score_bundle_offset", _fake_score_bundle_offset)

    replay, summary, available_offsets = backtest_engine._build_bundle_replay(
        bundle_dir=bundle_dir,
        features=features,
        labels=labels,
    )

    assert available_offsets == [7]
    assert replay["offset"].tolist() == [7]
    assert summary.feature_rows == 1
    assert summary.bundle_offset_missing_rows == 0


def test_policy_fill_and_hybrid_helpers() -> None:
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "condition_id": "c-1",
                "bundle_offset_available": True,
                "score_present": True,
                "score_valid": True,
                "score_reason": "",
                "resolved": True,
                "winner_side": "UP",
                "p_up": 0.70,
                "p_down": 0.30,
            },
            {
                "decision_ts": "2026-03-01T00:16:00Z",
                "cycle_start_ts": "2026-03-01T00:15:00Z",
                "cycle_end_ts": "2026-03-01T00:30:00Z",
                "offset": 7,
                "market_id": "m-2",
                "condition_id": "c-2",
                "bundle_offset_available": True,
                "score_present": True,
                "score_valid": True,
                "score_reason": "",
                "resolved": True,
                "winner_side": "DOWN",
                "p_up": 0.51,
                "p_down": 0.49,
            },
        ]
    )

    primary = build_policy_decisions(replay, model_source="primary")
    primary.loc[1, "trade_decision"] = False
    primary.loc[1, "reject_reason"] = "confidence_below_floor"
    secondary = build_policy_decisions(replay, model_source="secondary")
    secondary.loc[1, "trade_decision"] = True
    secondary.loc[1, "reject_reason"] = ""

    hybrid = apply_hybrid_score_fallback(primary, secondary, fallback_reasons=("confidence_below_floor",))
    assert hybrid.loc[1, "model_source"] == "secondary"

    accepted, rejected = split_policy_decisions(primary.iloc[[0]])
    assert rejected.empty
    fills, fill_rejects = build_proxy_fills(accepted, config=BacktestFillConfig(base_stake=1.0, max_stake=2.0))
    trades = settle_trade_fills(fills)

    assert fill_rejects.empty
    assert len(trades) == 1
    assert float(trades.iloc[0]["entry_price"]) > 0.0
    assert float(trades.iloc[0]["stake"]) >= 1.0
    assert bool(trades.iloc[0]["win"]) is True
    assert float(trades.iloc[0]["pnl"]) > 0.0


def test_run_research_backtest_writes_parity_artifacts(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="backtest", root=root)
    reporter_events: list[dict[str, object]] = []

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
            run_label="bt-parity-source",
            offsets=(7, 8),
        ),
    )
    build_model_bundle(
        cfg,
        ModelBundleSpec(
            profile="deep_otm",
            target="direction",
            bundle_label="bt-parity-bundle",
            offsets=(7, 8),
            source_training_run="bt-parity-source",
        ),
    )

    summary = run_research_backtest(
        cfg,
        BacktestRunSpec(
            profile="deep_otm",
            spec_name="baseline_truth",
            run_label="bt-parity-run",
            target="direction",
            bundle_label="bt-parity-bundle",
            stake_usd=5.0,
            max_notional_usd=8.0,
            fallback_reasons=("direction_prob", "policy_low_confidence"),
            parity={"liquidity_proxy_mode": "spot_kline_mirror"},
        ),
        reporter=lambda **payload: reporter_events.append(payload),
    )
    run_dir = Path(summary["run_dir"])
    payload = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    decisions = pd.read_parquet(run_dir / "decisions.parquet")

    assert (run_dir / "report.md").exists()
    assert (run_dir / "decisions.parquet").exists()
    assert (run_dir / "stake_sweep.parquet").exists()
    assert (run_dir / "offset_summary.parquet").exists()
    assert (run_dir / "factor_pnl.parquet").exists()
    assert payload["score_covered_rows"] >= payload["trades"]
    assert "ready_rows" in payload
    assert "reject_reason_counts" in payload
    assert payload["liquidity_proxy_mode"] == "spot_kline_mirror"
    assert payload["stake_usd"] == 5.0
    assert payload["max_notional_usd"] == 8.0
    assert payload["fallback_reasons"] == ["direction_prob", "policy_low_confidence"]
    assert payload["liquidity_available_rows"] >= 1
    assert isinstance(payload["regime_state_counts"], dict)
    assert "orderbook_preflight_status_counts" in payload
    assert "regime_state" in decisions.columns
    assert "liquidity_status" in decisions.columns
    stake_sweep = pd.read_parquet(run_dir / "stake_sweep.parquet")
    offset_summary = pd.read_parquet(run_dir / "offset_summary.parquet")
    factor_pnl = pd.read_parquet(run_dir / "factor_pnl.parquet")
    assert stake_sweep.loc[0, "stake_usd"] == 5.0
    assert set(offset_summary["offset"].tolist()) == {7, 8}
    assert not factor_pnl.empty
    stage_sequence = list(dict.fromkeys(event["current_stage"] for event in reporter_events))
    assert stage_sequence == [
        "load_inputs",
        "bundle_replay",
        "orderbook_preflight",
        "depth_replay",
        "quote_surface",
        "live_state_surface",
        "policy_decisions",
        "fills_materialization",
        "settlement_summary",
        "write_outputs",
        "finished",
    ]
    assert reporter_events[0]["summary"] == "Loading backtest inputs"
    assert reporter_events[0]["current"] == 1
    assert reporter_events[0]["total"] == 10
    assert reporter_events[0]["progress_pct"] == 0
    assert reporter_events[-1]["summary"] == "Backtest completed"
    assert reporter_events[-1]["progress_pct"] == 100
    assert all(isinstance(event["heartbeat"], str) and event["heartbeat"] for event in reporter_events)


def test_run_research_backtest_reuses_shared_runtime_for_stake_matrix_cases(monkeypatch, tmp_path: Path) -> None:
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
            run_label="bt-cache-source",
            offsets=(7, 8),
        ),
    )
    build_model_bundle(
        cfg,
        ModelBundleSpec(
            profile="deep_otm",
            target="direction",
            bundle_label="bt-cache-bundle",
            offsets=(7, 8),
            source_training_run="bt-cache-source",
        ),
    )

    stage_counts = {"bundle_replay": 0, "depth_replay": 0, "runtime_surface": 0}
    original_build_bundle_replay = backtest_engine._build_bundle_replay
    original_build_depth_replay = backtest_engine.build_raw_depth_replay_frame
    original_attach_runtime_surface = backtest_engine._attach_replay_runtime_surface

    def _count_bundle_replay(*args, **kwargs):
        stage_counts["bundle_replay"] += 1
        return original_build_bundle_replay(*args, **kwargs)

    def _count_depth_replay(*args, **kwargs):
        stage_counts["depth_replay"] += 1
        return original_build_depth_replay(*args, **kwargs)

    def _count_runtime_surface(*args, **kwargs):
        stage_counts["runtime_surface"] += 1
        return original_attach_runtime_surface(*args, **kwargs)

    monkeypatch.setattr(backtest_engine, "_build_bundle_replay", _count_bundle_replay)
    monkeypatch.setattr(backtest_engine, "build_raw_depth_replay_frame", _count_depth_replay)
    monkeypatch.setattr(backtest_engine, "_attach_replay_runtime_surface", _count_runtime_surface)

    clear_process_backtest_runtime_cache()
    try:
        first = run_research_backtest(
            cfg,
            BacktestRunSpec(
                profile="deep_otm",
                spec_name="baseline_truth",
                run_label="bt-cache-run-1",
                target="direction",
                bundle_label="bt-cache-bundle",
                stake_usd=1.0,
                max_notional_usd=1.0,
            ),
        )
        second = run_research_backtest(
            cfg,
            BacktestRunSpec(
                profile="deep_otm",
                spec_name="baseline_truth",
                run_label="bt-cache-run-2",
                target="direction",
                bundle_label="bt-cache-bundle",
                stake_usd=5.0,
                max_notional_usd=8.0,
            ),
        )
    finally:
        clear_process_backtest_runtime_cache()

    assert stage_counts == {"bundle_replay": 1, "depth_replay": 1, "runtime_surface": 1}
    assert Path(first["run_dir"]) != Path(second["run_dir"])
    assert first["shared_runtime_cache_status"] == "built"
    assert second["shared_runtime_cache_status"] == "reused"
    first_summary = json.loads((Path(first["run_dir"]) / "summary.json").read_text(encoding="utf-8"))
    second_summary = json.loads((Path(second["run_dir"]) / "summary.json").read_text(encoding="utf-8"))
    assert first_summary["stake_usd"] == 1.0
    assert first_summary["max_notional_usd"] == 1.0
    assert first_summary["shared_runtime_cache_status"] == "built"
    assert second_summary["stake_usd"] == 5.0
    assert second_summary["max_notional_usd"] == 8.0
    assert second_summary["shared_runtime_cache_status"] == "reused"
    second_log = (Path(second["run_dir"]) / "logs" / "backtest.jsonl").read_text(encoding="utf-8")
    assert '"event": "backtest_runtime_resolved"' in second_log
    assert '"shared_runtime_cache_status": "reused"' in second_log
