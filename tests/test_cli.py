from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from pm15min.cli import main
from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import DateWindow, ModelBundleSpec, TrainingRunSpec, TrainingSetSpec
from pm15min.research.datasets.feature_frames import build_feature_frame_dataset
from pm15min.research.datasets.training_sets import build_training_set_dataset
from pm15min.research.labels.datasets import build_label_frame_dataset
from pm15min.research.bundles.builder import build_model_bundle
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
                "final_price": price_base + offset * 3.0 + 1.5,
                "source_price_to_beat": "direct_api",
                "source_final_price": "streams_rpc",
                "has_price_to_beat": True,
                "has_final_price": True,
                "has_both": True,
            }
        )
    return pd.DataFrame(rows)


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def _prepare_sol_research_inputs(root: Path) -> ResearchConfig:
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
                    "winner_side": "UP" if idx % 2 == 0 else "DOWN",
                    "label_updown": "UP" if idx % 2 == 0 else "DOWN",
                    "resolved": True,
                    "truth_source": "settlement_truth",
                    "full_truth": True,
                }
                for idx in range(16)
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
        root=root,
    )


def _write_sample_copula_returns(path: Path) -> Path:
    idx = pd.Series(range(160), dtype=float)
    frame = pd.DataFrame(
        {
            "xrp_ret": (idx - 80.0) / 400.0,
            "sol_ret": (idx * 1.5 - 100.0) / 500.0 + ((idx % 5) - 2.0) / 1000.0,
            "btc_ret": (160.0 - idx) / 600.0 + ((idx % 7) - 3.0) / 1500.0,
        }
    )
    frame.to_csv(path, index=False)
    return path


def test_top_level_layout_command(capsys) -> None:
    rc = main(["layout", "--market", "sol", "--cycle", "15m", "--json"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["rewrite_root"].endswith("pm15min/v2")
    assert payload["surface"] == "backtest"


def test_data_show_summary(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.data.cli.show_data_summary",
        lambda cfg, persist=False: {
            "domain": "data",
            "dataset": "data_surface_summary",
            "market": cfg.asset.slug,
            "surface": cfg.surface,
            "persist": persist,
            "summary": {"dataset_count": 10},
        },
    )
    rc = main(["data", "show-summary", "--market", "sol", "--surface", "live", "--write-state"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["surface"] == "live"
    assert payload["persist"] is True
    assert payload["summary"]["dataset_count"] == 10


def test_data_sync_datafeeds_rpc(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.data.cli.sync_datafeeds_from_rpc",
        lambda cfg, start_ts, end_ts, chunk_blocks=5000, sleep_sec=0.02: {
            "dataset": "chainlink_datafeeds_rpc",
            "market": cfg.asset.slug,
            "rows_imported": 1,
            "partitions_written": 1,
        },
    )
    rc = main(["data", "sync", "datafeeds-rpc", "--market", "sol", "--surface", "backtest", "--lookback-days", "1"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "chainlink_datafeeds_rpc"
    assert payload["market"] == "sol"
    assert payload["rows_imported"] == 1


def test_live_show_config(capsys) -> None:
    rc = main(["live", "show-config", "--market", "xrp", "--profile", "deep_otm", "--loop"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "xrp"
    assert payload["profile"] == "deep_otm"
    assert payload["loop"] is True
    assert payload["canonical_live_scope"]["ok"] is True
    assert payload["profile_spec_resolution"]["status"] == "exact_match"
    assert payload["cli_boundary"]["command_role"] == "compatibility_inspection"
    assert payload["cli_boundary"]["requested_scope_classification"] == "canonical_live_scope"
    assert payload["cli_boundary"]["canonical_operator_entry"] is False
    assert "active_direction_bundle_selection_path" in payload
    assert "rewrite_live_data_root" in payload["layout"]
    assert "legacy_reference_market_root" in payload["layout"]
    assert "market_root" not in payload["layout"]
    assert "artifacts_root" not in payload["layout"]


def test_live_show_layout_marks_requested_scope(capsys) -> None:
    rc = main(["live", "show-layout", "--market", "xrp"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "xrp"
    assert payload["profile"] == "deep_otm"
    assert payload["canonical_live_scope"]["ok"] is True
    assert payload["cli_boundary"]["command"] == "show-layout"
    assert payload["cli_boundary"]["requested_scope_classification"] == "canonical_live_scope"


def test_live_show_config_marks_noncanonical_profile_as_compatibility_view(capsys) -> None:
    rc = main(["live", "show-config", "--market", "btc", "--profile", "deep_otm_baseline"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "btc"
    assert payload["profile"] == "deep_otm_baseline"
    assert payload["canonical_live_scope"]["ok"] is False
    assert payload["canonical_live_scope"]["market_in_scope"] is False
    assert payload["canonical_live_scope"]["profile_in_scope"] is False
    assert payload["profile_spec_resolution"]["status"] == "compatibility_fallback"
    assert payload["profile_spec_resolution"]["resolved_profile_spec"] == "default"
    assert payload["cli_boundary"]["requested_scope_classification"] == "non_canonical_scope"
    assert payload["cli_boundary"]["canonical_live_contract"]["profile"] == "deep_otm"


def test_live_sync_account_state(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.sync_live_account_state",
        lambda cfg, persist=True, adapter=None: {
            "domain": "live",
            "dataset": "live_account_state_sync",
            "market": cfg.asset.slug,
            "adapter": adapter,
            "open_orders": {"status": "ok"},
            "positions": {"status": "ok"},
        },
    )
    rc = main(["live", "sync-account-state", "--market", "sol", "--adapter", "direct", "--no-persist"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["adapter"] == "direct"
    assert payload["open_orders"]["status"] == "ok"
    assert payload["positions"]["status"] == "ok"


def test_live_sync_liquidity_state(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.sync_live_liquidity_state",
        lambda cfg, persist=True, force_refresh=False: {
            "domain": "live",
            "dataset": "live_liquidity_state",
            "market": cfg.asset.slug,
            "status": "ok",
            "blocked": False,
            "force_refresh": force_refresh,
        },
    )
    rc = main(["live", "sync-liquidity-state", "--market", "sol", "--force-refresh", "--no-persist"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["status"] == "ok"
    assert payload["blocked"] is False
    assert payload["force_refresh"] is True


def test_live_check_trading_gateway(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.check_live_trading_gateway",
        lambda cfg, adapter=None, probe_open_orders=False, probe_positions=False: {
            "domain": "live",
            "dataset": "live_trading_gateway_check",
            "market": cfg.asset.slug,
            "ok": True,
            "adapter": adapter,
            "probe_open_orders": probe_open_orders,
            "probe_positions": probe_positions,
        },
    )
    rc = main(
        [
            "live",
            "check-trading-gateway",
            "--market",
            "sol",
            "--adapter",
            "direct",
            "--probe-open-orders",
            "--probe-positions",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["ok"] is True
    assert payload["adapter"] == "direct"
    assert payload["probe_open_orders"] is True
    assert payload["probe_positions"] is True


def test_live_show_latest_runner(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.show_live_latest_runner",
        lambda cfg, target="direction", risk_only=False: {
            "domain": "live",
            "dataset": "live_runner_status",
            "market": cfg.asset.slug,
            "target": target,
            "risk_only": risk_only,
            "status": "ok",
            "operator_summary": {"can_run_side_effects": True},
        },
    )
    rc = main(["live", "show-latest-runner", "--market", "sol", "--risk-only"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["target"] == "direction"
    assert payload["risk_only"] is True
    assert payload["operator_summary"]["can_run_side_effects"] is True


def test_live_show_ready(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.show_live_ready",
        lambda cfg, target="direction", adapter=None: {
            "domain": "live",
            "dataset": "live_ready_status",
            "market": cfg.asset.slug,
            "target": target,
            "adapter_override": adapter,
            "status": "ready",
            "ready_for_side_effects": True,
        },
    )
    rc = main(["live", "show-ready", "--market", "sol", "--adapter", "direct"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["target"] == "direction"
    assert payload["adapter_override"] == "direct"
    assert payload["status"] == "ready"


def test_live_apply_cancel_policy(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.execute_live_cancel_policy",
        lambda cfg, persist=True, refresh_account_state=True, dry_run=False, adapter=None: {
            "domain": "live",
            "dataset": "live_cancel_policy_action",
            "market": cfg.asset.slug,
            "status": "ok",
            "adapter": adapter,
            "dry_run": dry_run,
        },
    )
    rc = main(["live", "apply-cancel-policy", "--market", "sol", "--adapter", "direct", "--dry-run", "--no-persist"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["status"] == "ok"
    assert payload["adapter"] == "direct"
    assert payload["dry_run"] is True


def test_live_apply_redeem_policy(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.execute_live_redeem_policy",
        lambda cfg, persist=True, refresh_account_state=True, dry_run=False, max_conditions=None, adapter=None: {
            "domain": "live",
            "dataset": "live_redeem_policy_action",
            "market": cfg.asset.slug,
            "status": "ok",
            "adapter": adapter,
            "max_conditions": max_conditions,
        },
    )
    rc = main(["live", "apply-redeem-policy", "--market", "sol", "--adapter", "direct", "--max-conditions", "2", "--no-persist"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["status"] == "ok"
    assert payload["adapter"] == "direct"
    assert payload["max_conditions"] == 2


def test_live_redeem_loop(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.run_live_redeem_loop",
        lambda cfg, iterations=1, loop=False, sleep_sec=60.0, persist=True, refresh_account_state=True, dry_run=False, max_conditions=None, adapter=None: {
            "domain": "live",
            "dataset": "live_redeem_runner_summary",
            "market": cfg.asset.slug,
            "status": "ok",
            "adapter": adapter,
            "iterations": iterations,
            "loop": loop,
            "max_conditions": max_conditions,
        },
    )
    rc = main(["live", "redeem-loop", "--market", "sol", "--adapter", "direct", "--iterations", "2", "--max-conditions", "3"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "live_redeem_runner_summary"
    assert payload["adapter"] == "direct"
    assert payload["iterations"] == 2
    assert payload["max_conditions"] == 3


def test_live_redeem_loop_forever_mode(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.run_live_redeem_loop",
        lambda cfg, iterations=1, loop=False, sleep_sec=60.0, persist=True, refresh_account_state=True, dry_run=False, max_conditions=None, adapter=None: {
            "domain": "live",
            "dataset": "live_redeem_runner_summary",
            "market": cfg.asset.slug,
            "iterations": iterations,
            "loop": loop,
        },
    )
    rc = main(["live", "redeem-loop", "--market", "sol", "--loop", "--iterations", "0"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["loop"] is True
    assert payload["iterations"] == 0


def test_live_execute_latest(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.execute_live_latest",
        lambda cfg, target="direction", feature_set=None, persist=True, dry_run=False, refresh_account_state=True, adapter=None: {
            "domain": "live",
            "dataset": "live_order_action",
            "market": cfg.asset.slug,
            "status": "ok",
            "adapter": adapter,
            "dry_run": dry_run,
        },
    )
    rc = main(["live", "execute-latest", "--market", "sol", "--adapter", "direct", "--dry-run", "--no-persist"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["status"] == "ok"
    assert payload["adapter"] == "direct"
    assert payload["dry_run"] is True


def test_live_execute_latest_rejects_noncanonical_target(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.execute_live_latest",
        lambda *args, **kwargs: {
            "domain": "live",
            "dataset": "live_order_action",
        },
    )
    with pytest.raises(SystemExit):
        main(["live", "execute-latest", "--market", "sol", "--target", "reversal"])
    assert "invalid choice" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("command", "patched_symbol"),
    [
        ("score-latest", "score_live_latest"),
        ("quote-latest", "quote_live_latest"),
        ("check-latest", "check_live_latest"),
        ("decide-latest", "decide_live_latest"),
        ("execution-simulate", "simulate_live_execution"),
    ],
)
def test_live_readonly_commands_reject_noncanonical_target(monkeypatch, capsys, command: str, patched_symbol: str) -> None:
    monkeypatch.setattr(
        f"pm15min.live.cli.{patched_symbol}",
        lambda *args, **kwargs: {
            "domain": "live",
            "dataset": "noop",
        },
    )

    with pytest.raises(SystemExit):
        main(["live", command, "--market", "sol", "--target", "reversal"])
    assert "invalid choice" in capsys.readouterr().err


def test_live_runner_once_side_effect_flags(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.run_live_runner_once",
        lambda cfg, target="direction", feature_set=None, persist=True, run_foundation=True, foundation_include_direct_oracle=True, foundation_include_orderbooks=True, apply_side_effects=True, side_effect_dry_run=False, adapter=None: {
            "domain": "live",
            "dataset": "live_runner_summary",
            "market": cfg.asset.slug,
            "adapter": adapter,
            "apply_side_effects": apply_side_effects,
            "side_effect_dry_run": side_effect_dry_run,
        },
    )
    rc = main(
        [
            "live",
            "runner-once",
            "--market",
            "sol",
            "--adapter",
            "direct",
            "--no-side-effects",
            "--dry-run-side-effects",
            "--no-persist",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["adapter"] == "direct"
    assert payload["apply_side_effects"] is False
    assert payload["side_effect_dry_run"] is True


def test_live_runner_once_rejects_noncanonical_target(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.run_live_runner_once",
        lambda *args, **kwargs: {
            "domain": "live",
            "dataset": "live_runner_summary",
        },
    )
    with pytest.raises(SystemExit):
        main(["live", "runner-once", "--market", "sol", "--target", "reversal"])
    assert "invalid choice" in capsys.readouterr().err


def test_live_runner_loop_rejects_noncanonical_target(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "pm15min.live.cli.run_live_runner_loop",
        lambda *args, **kwargs: {
            "domain": "live",
            "dataset": "live_runner_loop_summary",
        },
    )
    with pytest.raises(SystemExit):
        main(["live", "runner-loop", "--market", "sol", "--target", "reversal"])
    assert "invalid choice" in capsys.readouterr().err


def test_live_score_latest(capsys, tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    live_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    live_btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="live", root=root)
    write_parquet_atomic(
        _sample_klines("SOLUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=120.0),
        live_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_klines("BTCUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=50000.0),
        live_btc_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_oracle_prices("sol", cycle_start_ts=1_772_323_200, n_cycles=32, price_base=120.0),
        live_cfg.layout.oracle_prices_table_path,
    )

    research_cfg = _prepare_sol_research_inputs(root)
    build_feature_frame_dataset(research_cfg)
    build_label_frame_dataset(research_cfg)
    for offset in (7, 8):
        build_training_set_dataset(
            research_cfg,
            TrainingSetSpec(
                feature_set="deep_otm_v1",
                label_set="truth",
                target="direction",
                window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
                offset=offset,
            ),
        )
    train_summary = train_research_run(
        research_cfg,
        TrainingRunSpec(
            model_family="deep_otm",
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            run_label="live_smoke",
            offsets=(7, 8),
        ),
    )
    bundle_summary = build_model_bundle(
        research_cfg,
        ModelBundleSpec(
            profile="deep_otm",
            target="direction",
            bundle_label="live_smoke_bundle",
            offsets=(7, 8),
            source_training_run=Path(train_summary["run_dir"]).name.split("=", 1)[1],
        ),
    )
    activate_model_bundle(
        research_cfg,
        profile="deep_otm",
        target="direction",
        bundle_label=bundle_summary["bundle_label"],
    )

    rc = main(["live", "score-latest", "--market", "sol", "--profile", "deep_otm", "--no-persist"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["target"] == "direction"
    assert payload["bundle_label"] == "live_smoke_bundle"
    assert len(payload["offset_signals"]) == 2
    assert payload["offset_signals"][0]["coverage"]["required_feature_count"] > 0


def test_live_check_and_decide_latest(capsys, tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    live_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    live_btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="live", root=root)
    write_parquet_atomic(
        _sample_klines("SOLUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=120.0),
        live_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_klines("BTCUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=50000.0),
        live_btc_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_oracle_prices("sol", cycle_start_ts=1_772_323_200, n_cycles=32, price_base=120.0),
        live_cfg.layout.oracle_prices_table_path,
    )

    research_cfg = _prepare_sol_research_inputs(root)
    build_feature_frame_dataset(research_cfg)
    research_v6_cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="v6_user_core",
        label_set="truth",
        root=root,
    )
    build_feature_frame_dataset(research_v6_cfg)
    build_label_frame_dataset(research_cfg)
    for offset in (7, 8):
        build_training_set_dataset(
            research_v6_cfg,
            TrainingSetSpec(
                feature_set="v6_user_core",
                label_set="truth",
                target="direction",
                window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
                offset=offset,
            ),
        )
    train_summary = train_research_run(
        research_v6_cfg,
        TrainingRunSpec(
            model_family="deep_otm",
            feature_set="v6_user_core",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            run_label="live_check_smoke",
            offsets=(7, 8),
        ),
    )
    bundle_summary = build_model_bundle(
        research_cfg,
        ModelBundleSpec(
            profile="deep_otm",
            target="direction",
            bundle_label="live_check_bundle",
            offsets=(7, 8),
            source_training_run=Path(train_summary["run_dir"]).name.split("=", 1)[1],
        ),
    )
    activate_model_bundle(
        research_cfg,
        profile="deep_otm",
        target="direction",
        bundle_label=bundle_summary["bundle_label"],
    )

    rc = main(["live", "check-latest", "--market", "sol", "--profile", "deep_otm"])
    assert rc == 0
    check_payload = json.loads(capsys.readouterr().out)
    assert check_payload["ok"] is False
    assert any(item["name"] == "offset_signals_valid" for item in check_payload["checks"])

    rc = main(["live", "decide-latest", "--market", "sol", "--profile", "deep_otm", "--no-persist"])
    assert rc == 0
    decision_payload = json.loads(capsys.readouterr().out)
    assert decision_payload["market"] == "sol"
    assert decision_payload["decision"]["status"] in {"accept", "reject"}


def test_live_quote_latest_reports_missing_inputs(capsys, tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    live_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    live_btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="live", root=root)
    write_parquet_atomic(
        _sample_klines("SOLUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=120.0),
        live_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_klines("BTCUSDT", start="2026-03-01T00:00:00Z", periods=480, price_base=50000.0),
        live_btc_cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        _sample_oracle_prices("sol", cycle_start_ts=1_772_323_200, n_cycles=32, price_base=120.0),
        live_cfg.layout.oracle_prices_table_path,
    )

    research_cfg = _prepare_sol_research_inputs(root)
    build_feature_frame_dataset(research_cfg)
    build_label_frame_dataset(research_cfg)
    for offset in (7, 8):
        build_training_set_dataset(
            research_cfg,
            TrainingSetSpec(
                feature_set="deep_otm_v1",
                label_set="truth",
                target="direction",
                window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
                offset=offset,
            ),
        )
    train_summary = train_research_run(
        research_cfg,
        TrainingRunSpec(
            model_family="deep_otm",
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            run_label="quote_smoke",
            offsets=(7, 8),
        ),
    )
    bundle_summary = build_model_bundle(
        research_cfg,
        ModelBundleSpec(
            profile="deep_otm",
            target="direction",
            bundle_label="quote_smoke_bundle",
            offsets=(7, 8),
            source_training_run=Path(train_summary["run_dir"]).name.split("=", 1)[1],
        ),
    )
    activate_model_bundle(
        research_cfg,
        profile="deep_otm",
        target="direction",
        bundle_label=bundle_summary["bundle_label"],
    )

    rc = main(["live", "quote-latest", "--market", "sol", "--profile", "deep_otm", "--no-persist"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["quote_rows"][0]["status"] == "missing_quote_inputs"


def test_research_list_runs_is_json(capsys) -> None:
    rc = main(["research", "list-runs", "--market", "btc"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert isinstance(payload, list)


def test_research_show_layout(capsys) -> None:
    rc = main(["research", "show-layout", "--market", "sol", "--cycle", "15m"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["cycle"] == "15m"
    assert payload["market_training_runs_root"].endswith("v2/research/training_runs/cycle=15m/asset=sol")


def test_research_training_set_build(capsys, tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _prepare_sol_research_inputs(root)

    rc = main(["research", "build", "feature-frame", "--market", "sol"])
    assert rc == 0
    capsys.readouterr()
    rc = main(["research", "build", "label-frame", "--market", "sol"])
    assert rc == 0
    capsys.readouterr()
    rc = main(
        [
            "research",
            "build",
            "training-set",
            "--market",
            "sol",
            "--window-start",
            "2026-03-01",
            "--window-end",
            "2026-03-01",
            "--offset",
            "7",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["rows_written"] > 0
    assert payload["target"] == "direction"
    assert "offset=7" in payload["target_path"]

def test_research_train_run(capsys, tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _prepare_sol_research_inputs(root)
    assert main(["research", "build", "feature-frame", "--market", "sol"]) == 0
    capsys.readouterr()
    assert main(["research", "build", "label-frame", "--market", "sol"]) == 0
    capsys.readouterr()
    for offset in ("7", "8"):
        assert (
            main(
                [
                    "research",
                    "build",
                    "training-set",
                    "--market",
                    "sol",
                    "--window-start",
                    "2026-03-01",
                    "--window-end",
                    "2026-03-01",
                    "--offset",
                    offset,
                ]
            )
            == 0
        )
        capsys.readouterr()

    rc = main(
        [
            "research",
            "train",
            "run",
            "--market",
            "sol",
            "--window-start",
            "2026-03-01",
            "--window-end",
            "2026-03-01",
            "--offsets",
            "7,8",
            "--run-label",
            "cli-smoke",
            "--parallel-workers",
            "2",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["run_label"] == "cli-smoke"
    assert payload["offsets"] == [7, 8]
    assert payload["parallel_workers"] == 2
    assert "training_runs" in payload["run_dir"]


def test_research_bundle_build(capsys, tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _prepare_sol_research_inputs(root)
    assert main(["research", "build", "feature-frame", "--market", "sol"]) == 0
    capsys.readouterr()
    assert main(["research", "build", "label-frame", "--market", "sol"]) == 0
    capsys.readouterr()
    for offset in ("7", "8"):
        assert (
            main(
                [
                    "research",
                    "build",
                    "training-set",
                    "--market",
                    "sol",
                    "--window-start",
                    "2026-03-01",
                    "--window-end",
                    "2026-03-01",
                    "--offset",
                    offset,
                ]
            )
            == 0
        )
        capsys.readouterr()
    assert (
        main(
            [
                "research",
                "train",
                "run",
                "--market",
                "sol",
                "--window-start",
                "2026-03-01",
                "--window-end",
                "2026-03-01",
                "--offsets",
                "7,8",
                "--run-label",
                "bundle-cli-source",
            ]
        )
        == 0
    )
    capsys.readouterr()

    rc = main(
        [
            "research",
            "bundle",
            "build",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--offsets",
            "7,8",
            "--bundle-label",
            "bundle-cli",
            "--source-training-run",
            "bundle-cli-source",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["bundle_label"] == "bundle-cli"
    assert payload["offsets"] == [7, 8]
    assert "model_bundles" in payload["bundle_dir"]


def test_research_backtest_run(capsys, tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _prepare_sol_research_inputs(root)
    assert main(["research", "build", "feature-frame", "--market", "sol"]) == 0
    capsys.readouterr()
    assert main(["research", "build", "label-frame", "--market", "sol"]) == 0
    capsys.readouterr()
    for offset in ("7", "8"):
        assert (
            main(
                [
                    "research",
                    "build",
                    "training-set",
                    "--market",
                    "sol",
                    "--window-start",
                    "2026-03-01",
                    "--window-end",
                    "2026-03-01",
                    "--offset",
                    offset,
                ]
            )
            == 0
        )
        capsys.readouterr()
    assert (
        main(
            [
                "research",
                "train",
                "run",
                "--market",
                "sol",
                "--window-start",
                "2026-03-01",
                "--window-end",
                "2026-03-01",
                "--offsets",
                "7,8",
                "--run-label",
                "bt-cli-source",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        main(
            [
                "research",
                "bundle",
                "build",
                "--market",
                "sol",
                "--profile",
                "deep_otm",
                "--offsets",
                "7,8",
                "--bundle-label",
                "bt-cli-bundle",
                "--source-training-run",
                "bt-cli-source",
            ]
        )
        == 0
    )
    capsys.readouterr()

    rc = main(
        [
            "research",
            "backtest",
            "run",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--spec",
            "baseline_truth",
            "--run-label",
            "bt-cli-run",
            "--bundle-label",
            "bt-cli-bundle",
            "--stake-usd",
            "5",
            "--max-notional-usd",
            "8",
            "--secondary-bundle-label",
            "bt-cli-bundle",
            "--fallback-reasons",
            "direction_prob,policy_low_confidence",
            "--parity-json",
            '{"regime_enabled": true}',
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["run_label"] == "bt-cli-run"
    assert payload["trades"] > 0
    assert payload["stake_usd"] == 5.0
    assert payload["max_notional_usd"] == 8.0
    assert payload["secondary_bundle_label"] == "bt-cli-bundle"
    assert payload["fallback_reasons"] == ["direction_prob", "policy_low_confidence"]
    assert payload["parity"] == {"regime_enabled": True}
    assert "backtests" in payload["run_dir"]


def test_research_experiment_run_suite(capsys, tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _prepare_sol_research_inputs(root)

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
    suite_path = cfg.layout.storage.suite_spec_path("cli_suite_smoke")
    suite_path.parent.mkdir(parents=True, exist_ok=True)
    suite_path.write_text(
        json.dumps(
            {
                "suite_name": "cli_suite_smoke",
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

    rc = main(
        [
            "research",
            "experiment",
            "run-suite",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--suite",
            "cli_suite_smoke",
            "--run-label",
            "cli-exp-run",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["suite_name"] == "cli_suite_smoke"
    assert payload["run_label"] == "cli-exp-run"
    assert "experiments/runs" in payload["run_dir"]


def test_research_evaluate_commands(capsys, tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _prepare_sol_research_inputs(root)

    assert main(["research", "build", "feature-frame", "--market", "sol"]) == 0
    capsys.readouterr()
    assert main(["research", "build", "label-frame", "--market", "sol"]) == 0
    capsys.readouterr()
    for offset in ("7", "8"):
        assert (
            main(
                [
                    "research",
                    "build",
                    "training-set",
                    "--market",
                    "sol",
                    "--window-start",
                    "2026-03-01",
                    "--window-end",
                    "2026-03-01",
                    "--offset",
                    offset,
                ]
            )
            == 0
        )
        capsys.readouterr()
    assert (
        main(
            [
                "research",
                "train",
                "run",
                "--market",
                "sol",
                "--window-start",
                "2026-03-01",
                "--window-end",
                "2026-03-01",
                "--offsets",
                "7,8",
                "--run-label",
                "eval-cli-source",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        main(
            [
                "research",
                "bundle",
                "build",
                "--market",
                "sol",
                "--profile",
                "deep_otm",
                "--offsets",
                "7,8",
                "--bundle-label",
                "eval-cli-bundle",
                "--source-training-run",
                "eval-cli-source",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        main(
            [
                "research",
                "backtest",
                "run",
                "--market",
                "sol",
                "--profile",
                "deep_otm",
                "--spec",
                "baseline_truth",
                "--run-label",
                "eval-cli-backtest",
                "--bundle-label",
                "eval-cli-bundle",
            ]
        )
        == 0
    )
    capsys.readouterr()

    rc = main(
        [
            "research",
            "evaluate",
            "calibration",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--scope",
            "baseline_truth",
            "--backtest-spec",
            "baseline_truth",
            "--backtest-run-label",
            "eval-cli-backtest",
            "--run-label",
            "eval-cli-cal",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["category"] == "calibration"
    assert "evaluations/calibration" in payload["run_dir"]

    rc = main(
        [
            "research",
            "evaluate",
            "drift",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--scope",
            "baseline_truth",
            "--backtest-spec",
            "baseline_truth",
            "--backtest-run-label",
            "eval-cli-backtest",
            "--run-label",
            "eval-cli-drift",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["category"] == "drift"
    assert "evaluations/drift" in payload["run_dir"]

    rc = main(
        [
            "research",
            "evaluate",
            "poly-eval",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--scope",
            "offset_metrics",
            "--backtest-spec",
            "baseline_truth",
            "--backtest-run-label",
            "eval-cli-backtest",
            "--run-label",
            "eval-cli-poly",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["category"] == "poly_eval"
    assert "evaluations/poly_eval" in payload["run_dir"]
    assert payload["scope_label"] == "offset_metrics"


def test_research_evaluate_poly_eval_demo_commands(capsys, tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    copula_input = _write_sample_copula_returns(tmp_path / "copula_returns.csv")

    rc = main(
        [
            "research",
            "evaluate",
            "deep-otm-demo",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--scope",
            "probability_demo",
            "--run-label",
            "cli-deep",
            "--method",
            "crude",
            "--n-paths",
            "256",
            "--n-steps",
            "12",
            "--seed",
            "7",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["category"] == "deep-otm-demo"
    assert "evaluations/deep-otm-demo" in payload["run_dir"]
    deep_summary = json.loads(Path(payload["summary_path"]).read_text(encoding="utf-8"))
    assert deep_summary["estimate_method"] == "crude_mc"
    assert Path(payload["report_path"]).exists()
    assert Path(payload["manifest_path"]).exists()

    rc = main(
        [
            "research",
            "evaluate",
            "smc-demo",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--scope",
            "state_demo",
            "--run-label",
            "cli-smc",
            "--seed",
            "7",
            "--n-particles",
            "64",
            "--synthetic-n",
            "36",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["category"] == "smc-demo"
    assert "evaluations/smc-demo" in payload["run_dir"]
    smc_summary = json.loads(Path(payload["summary_path"]).read_text(encoding="utf-8"))
    assert smc_summary["rows"] == 36
    assert smc_summary["synthetic"] is True
    assert Path(payload["report_path"]).exists()
    assert Path(payload["manifest_path"]).exists()

    rc = main(
        [
            "research",
            "evaluate",
            "copula-risk",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--scope",
            "tail_risk",
            "--run-label",
            "cli-copula",
            "--input",
            str(copula_input),
            "--cols",
            "xrp_ret,sol_ret,btc_ret",
            "--n-sim",
            "2000",
            "--seed",
            "7",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["category"] == "copula-risk"
    assert "evaluations/copula-risk" in payload["run_dir"]
    copula_summary = json.loads(Path(payload["summary_path"]).read_text(encoding="utf-8"))
    assert copula_summary["rows_used"] == 160
    assert copula_summary["family"] == "t"
    assert Path(payload["report_path"]).exists()
    assert Path(payload["manifest_path"]).exists()

    rc = main(
        [
            "research",
            "evaluate",
            "stack-demo",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--scope",
            "production_stack",
            "--run-label",
            "cli-stack",
            "--seed",
            "7",
            "--n-steps",
            "40",
            "--n-particles",
            "64",
            "--copula-n-sim",
            "5000",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["category"] == "stack-demo"
    assert "evaluations/stack-demo" in payload["run_dir"]
    stack_summary = json.loads(Path(payload["summary_path"]).read_text(encoding="utf-8"))
    assert "layer1_data_ingestion" in stack_summary["layers"]
    assert "layer4_risk_management" in stack_summary["layers"]
    assert Path(payload["report_path"]).exists()
    assert Path(payload["manifest_path"]).exists()


def test_research_evaluate_poly_eval_scope_router_commands(capsys, tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    copula_input = _write_sample_copula_returns(tmp_path / "copula_scope_returns.csv")

    rc = main(
        [
            "research",
            "evaluate",
            "poly-eval",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--scope",
            "deep_otm",
            "--run-label",
            "scope-deep",
            "--method",
            "crude",
            "--n-paths",
            "256",
            "--n-steps",
            "12",
            "--seed",
            "7",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["category"] == "poly_eval"
    assert payload["scope_label"] == "deep_otm"
    assert "evaluations/poly_eval" in payload["run_dir"]
    deep_summary = json.loads(Path(payload["summary_path"]).read_text(encoding="utf-8"))
    assert deep_summary["category"] == "poly_eval"
    assert deep_summary["scope_label"] == "deep_otm"
    assert Path(payload["manifest_path"]).exists()

    rc = main(
        [
            "research",
            "evaluate",
            "poly-eval",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--scope",
            "abm",
            "--run-label",
            "scope-abm",
            "--seed",
            "7",
            "--n-steps",
            "40",
            "--true-prob",
            "0.67",
            "--init-price",
            "0.52",
            "--n-informed",
            "9",
            "--n-noise",
            "38",
            "--n-mm",
            "6",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["category"] == "poly_eval"
    assert payload["scope_label"] == "abm"
    abm_summary = json.loads(Path(payload["summary_path"]).read_text(encoding="utf-8"))
    assert abm_summary["category"] == "poly_eval"
    assert abm_summary["scope_label"] == "abm"
    assert abm_summary["simulation_rows"] == 41
    assert Path(payload["report_path"]).exists()
    assert Path(payload["manifest_path"]).exists()

    rc = main(
        [
            "research",
            "evaluate",
            "poly-eval",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--scope",
            "smc",
            "--run-label",
            "scope-smc",
            "--seed",
            "7",
            "--n-particles",
            "64",
            "--synthetic-n",
            "36",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["category"] == "poly_eval"
    assert payload["scope_label"] == "smc"
    smc_summary = json.loads(Path(payload["summary_path"]).read_text(encoding="utf-8"))
    assert smc_summary["rows"] == 36
    assert smc_summary["category"] == "poly_eval"

    rc = main(
        [
            "research",
            "evaluate",
            "poly-eval",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--scope",
            "copula_risk",
            "--run-label",
            "scope-copula",
            "--input",
            str(copula_input),
            "--cols",
            "xrp_ret,sol_ret,btc_ret",
            "--n-sim",
            "2000",
            "--seed",
            "7",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["category"] == "poly_eval"
    assert payload["scope_label"] == "copula_risk"
    copula_summary = json.loads(Path(payload["summary_path"]).read_text(encoding="utf-8"))
    assert copula_summary["rows_used"] == 160
    assert copula_summary["category"] == "poly_eval"

    rc = main(
        [
            "research",
            "evaluate",
            "poly-eval",
            "--market",
            "sol",
            "--profile",
            "deep_otm",
            "--scope",
            "production_stack",
            "--run-label",
            "scope-stack",
            "--seed",
            "7",
            "--n-steps",
            "40",
            "--n-particles",
            "64",
            "--copula-n-sim",
            "5000",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["category"] == "poly_eval"
    assert payload["scope_label"] == "production_stack"
    stack_summary = json.loads(Path(payload["summary_path"]).read_text(encoding="utf-8"))
    assert "layer1_data_ingestion" in stack_summary["layers"]
    assert stack_summary["category"] == "poly_eval"


def test_data_show_layout(capsys) -> None:
    rc = main(["data", "show-layout", "--market", "btc", "--cycle", "15m", "--surface", "backtest"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "btc"
    assert payload["cycle"] == "15m"
    assert payload["surface"] == "backtest"


def test_data_show_config(capsys) -> None:
    rc = main(
        [
            "data",
            "show-config",
            "--market",
            "sol",
            "--cycle",
            "15m",
            "--surface",
            "live",
            "--market-depth",
            "2",
            "--market-start-offset",
            "7",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"] == "sol"
    assert payload["market_depth"] == 2
    assert payload["market_start_offset"] == 7
    assert payload["surface"] == "live"


def test_data_sync_market_catalog_help(capsys) -> None:
    try:
        main(["data", "sync", "market-catalog", "--help"])
    except SystemExit as exc:
        assert exc.code == 0
    out = capsys.readouterr().out
    assert "market-catalog" in out


def test_data_record_orderbooks_help(capsys) -> None:
    try:
        main(["data", "record", "orderbooks", "--help"])
    except SystemExit as exc:
        assert exc.code == 0
    out = capsys.readouterr().out
    assert "orderbooks" in out


def test_data_run_orderbook_fleet(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.data.cli.run_orderbook_recorder_fleet",
        lambda markets="btc,eth,sol,xrp", cycle="15m", surface="live", poll_interval_sec=0.35, orderbook_timeout_sec=1.2, recent_window_minutes=15, market_depth=1, market_start_offset=0, iterations=1, loop=False, sleep_sec=None: {
            "domain": "data",
            "dataset": "orderbook_recorder_fleet",
            "status": "ok",
            "markets": markets.split(","),
            "recent_window_minutes": recent_window_minutes,
            "poll_interval_sec": poll_interval_sec,
            "market_start_offset": market_start_offset,
        },
    )
    rc = main(
        [
            "data",
            "run",
            "orderbook-fleet",
            "--markets",
            "btc,eth,sol,xrp",
            "--recent-window-minutes",
            "9",
            "--market-start-offset",
            "7",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "orderbook_recorder_fleet"
    assert payload["markets"] == ["btc", "eth", "sol", "xrp"]
    assert payload["recent_window_minutes"] == 9
    assert payload["market_start_offset"] == 7


def test_data_build_truth_help(capsys) -> None:
    try:
        main(["data", "build", "truth-15m", "--help"])
    except SystemExit as exc:
        assert exc.code == 0
    out = capsys.readouterr().out
    assert "truth-15m" in out


def test_data_export_truth_help(capsys) -> None:
    try:
        main(["data", "export", "truth-15m", "--help"])
    except SystemExit as exc:
        assert exc.code == 0
    out = capsys.readouterr().out
    assert "truth-15m" in out


def test_data_sync_direct_oracle_help(capsys) -> None:
    try:
        main(["data", "sync", "direct-oracle-prices", "--help"])
    except SystemExit as exc:
        assert exc.code == 0
    out = capsys.readouterr().out
    assert "direct-oracle-prices" in out
