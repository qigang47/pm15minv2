from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pm15min.research._contracts_runs import BacktestParitySpec
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import BacktestRunSpec
from pm15min.research.experiments.runner import run_experiment_suite
from pm15min.research.experiments.specs import load_suite_definition


def test_backtest_run_spec_coerces_parity_mapping() -> None:
    spec = BacktestRunSpec(
        profile="deep_otm",
        spec_name="baseline_truth",
        run_label="parity-smoke",
        stake_usd="5",
        max_notional_usd="8",
        parity={
            "regime_controller_enabled": "true",
            "raw_depth_fak_refresh_enabled": "true",
            "regime_caution_min_liquidity_ratio": "0.6",
            "regime_defense_disable_offsets": ["7", 8, 7],
            "regime_defense_max_trades_per_market": "2",
            "regime_liquidity_proxy_mode": "spot_kline_mirror",
            "liquidity_guard_lookback_minutes": "15",
            "liquidity_guard_soft_fail_min_count": "3",
            "liquidity_min_spot_quote_volume_ratio": "0.2",
        },
    )

    assert isinstance(spec.parity, BacktestParitySpec)
    assert spec.parity.regime_enabled is True
    assert spec.parity.raw_depth_fak_refresh_enabled is True
    assert spec.parity.regime_caution_min_liquidity_ratio == 0.6
    assert spec.parity.regime_defense_disable_offsets == (7, 8)
    assert spec.parity.regime_defense_max_trades_per_market == 2
    assert spec.parity.liquidity_proxy_mode == "spot_kline_mirror"
    assert spec.parity.liquidity_lookback_minutes == 15
    assert spec.stake_usd == 5.0
    assert spec.max_notional_usd == 8.0
    assert spec.to_dict()["parity"] == {
        "regime_enabled": True,
        "raw_depth_fak_refresh_enabled": True,
        "regime_caution_min_liquidity_ratio": 0.6,
        "regime_defense_disable_offsets": (7, 8),
        "regime_defense_max_trades_per_market": 2,
        "liquidity_proxy_mode": "spot_kline_mirror",
        "liquidity_lookback_minutes": 15,
        "liquidity_soft_fail_min_count": 3,
        "liquidity_min_spot_quote_volume_ratio": 0.2,
    }


def test_load_suite_definition_parses_nested_parity_defaults_and_overrides(tmp_path: Path) -> None:
    path = tmp_path / "suite.json"
    path.write_text(
        json.dumps(
            {
                "suite_name": "parity_suite",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "offsets": [7, 8],
                "window": {"start": "2026-03-01", "end": "2026-03-01"},
                "parity": {
                    "regime_enabled": True,
                    "liquidity_proxy_mode": "spot_kline_mirror",
                    "liquidity_soft_fail_min_count": 2,
                    "regime_caution_disable_offsets": [7],
                },
                "markets": [
                    {
                        "market": "sol",
                    },
                    {
                        "market": "xrp",
                        "regime_controller_enabled": False,
                        "regime_defense_max_trades_per_market": 1,
                        "liquidity_min_spot_quote_volume_ratio": 0.25,
                    },
                ],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    suite = load_suite_definition(path)
    sol, xrp = suite.markets

    assert sol.parity.regime_enabled is True
    assert sol.parity.liquidity_proxy_mode == "spot_kline_mirror"
    assert sol.parity.liquidity_soft_fail_min_count == 2
    assert sol.parity.regime_caution_disable_offsets == (7,)
    assert xrp.parity.regime_enabled is False
    assert xrp.parity.liquidity_proxy_mode == "spot_kline_mirror"
    assert xrp.parity.regime_defense_max_trades_per_market == 1
    assert xrp.parity.liquidity_min_spot_quote_volume_ratio == 0.25


def test_run_experiment_suite_propagates_parity_spec(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
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
    suite_path = cfg.layout.storage.suite_spec_path("parity_suite")
    suite_path.parent.mkdir(parents=True, exist_ok=True)
    suite_path.write_text(
        json.dumps(
            {
                "suite_name": "parity_suite",
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
                        "parity": {
                            "regime_enabled": True,
                            "raw_depth_fak_refresh_enabled": True,
                            "regime_defense_max_trades_per_market": 1,
                            "liquidity_proxy_mode": "spot_kline_mirror",
                            "regime_defense_disable_offsets": [8],
                            "liquidity_min_spot_quote_volume_ratio": 0.3,
                        },
                    }
                ],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("pm15min.research.experiments.runner.build_feature_frame_dataset", lambda cfg: {"ok": True})
    monkeypatch.setattr("pm15min.research.experiments.runner.build_label_frame_dataset", lambda cfg: {"ok": True})
    monkeypatch.setattr(
        "pm15min.research.experiments.runner.train_research_run",
        lambda cfg, spec: {"run_dir": str(root / "research" / "training_runs" / spec.run_label)},
    )
    monkeypatch.setattr(
        "pm15min.research.experiments.runner.build_model_bundle",
        lambda cfg, spec: {"bundle_dir": str(root / "research" / "model_bundles" / spec.bundle_label), "bundle_label": spec.bundle_label},
    )
    captured: dict[str, object] = {}

    def _fake_backtest(cfg, spec):
        captured["spec"] = spec
        run_dir = root / "research" / "backtests" / spec.run_label
        run_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(
            json.dumps({"trades": 1, "rejects": 2, "wins": 1, "losses": 0, "pnl_sum": 1.0, "stake_sum": 1.0, "roi_pct": 100.0}),
            encoding="utf-8",
        )
        return {"run_dir": str(run_dir), "summary_path": str(summary_path)}

    monkeypatch.setattr("pm15min.research.experiments.runner.run_research_backtest", _fake_backtest)
    monkeypatch.setattr(
        "pm15min.research.experiments.runner.build_leaderboard",
        lambda frame: frame[["market", "roi_pct"]].copy(),
    )

    summary = run_experiment_suite(cfg=cfg, suite_name="parity_suite", run_label="parity-exp")
    run_dir = Path(summary["run_dir"])
    training_runs = pd.read_parquet(run_dir / "training_runs.parquet")
    backtest_runs = pd.read_parquet(run_dir / "backtest_runs.parquet")
    suite_log = (run_dir / "logs" / "suite.jsonl").read_text(encoding="utf-8")
    backtest_spec = captured["spec"]

    assert isinstance(backtest_spec, BacktestRunSpec)
    assert backtest_spec.parity.regime_enabled is True
    assert backtest_spec.parity.raw_depth_fak_refresh_enabled is True
    assert backtest_spec.parity.regime_defense_max_trades_per_market == 1
    assert backtest_spec.parity.regime_defense_disable_offsets == (8,)
    assert backtest_spec.parity.liquidity_proxy_mode == "spot_kline_mirror"
    assert backtest_spec.parity.liquidity_min_spot_quote_volume_ratio == 0.3
    assert '"regime_enabled": true' in training_runs.loc[0, "parity_spec_json"]
    assert '"liquidity_proxy_mode": "spot_kline_mirror"' in backtest_runs.loc[0, "parity_spec_json"]
    assert '"regime_defense_disable_offsets": [8]' in suite_log
