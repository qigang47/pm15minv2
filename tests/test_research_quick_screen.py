from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from pm15min.research.automation import quick_screen as quick_screen_module
from pm15min.research.automation.quick_screen import (
    build_profitable_offset_pool_frame,
    build_quick_screen_summary,
    ensure_training_and_bundle,
    profitable_offset_pool_cache_paths,
    quick_screen_rank_tuple,
    run_bundle_quick_screen,
)
from pm15min.research.config import ResearchConfig


def test_build_quick_screen_summary_counts_price_band_and_trade_hits() -> None:
    decisions = pd.DataFrame(
        [
            {
                "resolved": True,
                "winner_side": "UP",
                "quote_status": "ok",
                "quote_up_ask": 0.15,
                "quote_down_ask": 0.82,
                "predicted_side": "UP",
                "policy_action": "trade",
                "policy_reason": "trade",
                "decision_source": "primary",
            },
            {
                "resolved": True,
                "winner_side": "DOWN",
                "quote_status": "ok",
                "quote_up_ask": 0.77,
                "quote_down_ask": 0.20,
                "predicted_side": "UP",
                "policy_action": "reject",
                "policy_reason": "policy_low_confidence",
                "decision_source": "primary",
            },
            {
                "resolved": True,
                "winner_side": "UP",
                "quote_status": "ok",
                "quote_up_ask": 0.45,
                "quote_down_ask": 0.60,
                "predicted_side": "UP",
                "policy_action": "trade",
                "policy_reason": "trade",
                "decision_source": "primary",
            },
            {
                "resolved": False,
                "winner_side": "",
                "quote_status": "missing",
                "quote_up_ask": None,
                "quote_down_ask": None,
                "predicted_side": "DOWN",
                "policy_action": "reject",
                "policy_reason": "unresolved_label",
                "decision_source": "primary",
            },
        ]
    )

    summary = build_quick_screen_summary(
        decisions,
        entry_price_min=0.01,
        entry_price_max=0.30,
    )

    assert summary["rows"] == 4
    assert summary["resolved_rows"] == 3
    assert summary["quote_ready_rows"] == 3
    assert summary["winner_in_band_rows"] == 2
    assert summary["backed_winner_rows"] == 2
    assert summary["trade_rows"] == 2
    assert summary["traded_winner_rows"] == 2
    assert summary["backed_winner_in_band_rows"] == 1
    assert summary["traded_winner_in_band_rows"] == 1
    assert summary["reject_reason_counts"] == {
        "policy_low_confidence": 1,
        "unresolved_label": 1,
    }
    assert summary["profitable_pool_rows"] == 2
    assert summary["profitable_pool_correct_side_rows"] == 1
    assert summary["profitable_pool_capture_rows"] == 1
    assert summary["profitable_pool_coverage_ratio"] == pytest.approx(0.5)
    assert summary["profitable_pool_status_counts"] == {
        "captured": 1,
        "correct_side_no_trade": 0,
        "missed": 1,
        "traded_wrong_side": 0,
    }


def test_build_profitable_offset_pool_frame_marks_strict_tradeable_captures() -> None:
    decisions = pd.DataFrame(
        [
            {
                "decision_ts": "2026-04-01T00:07:00Z",
                "cycle_start_ts": "2026-04-01T00:00:00Z",
                "cycle_end_ts": "2026-04-01T00:15:00Z",
                "offset": 7,
                "resolved": True,
                "winner_side": "UP",
                "quote_status": "ok",
                "quote_up_ask": 0.22,
                "quote_down_ask": 0.81,
                "predicted_side": "UP",
                "policy_action": "trade",
                "policy_reason": "trade",
            },
            {
                "decision_ts": "2026-04-01T00:08:00Z",
                "cycle_start_ts": "2026-04-01T00:00:00Z",
                "cycle_end_ts": "2026-04-01T00:15:00Z",
                "offset": 8,
                "resolved": True,
                "winner_side": "DOWN",
                "quote_status": "ok",
                "quote_up_ask": 0.75,
                "quote_down_ask": 0.18,
                "predicted_side": "DOWN",
                "policy_action": "reject",
                "policy_reason": "policy_low_confidence",
            },
            {
                "decision_ts": "2026-04-01T00:09:00Z",
                "cycle_start_ts": "2026-04-01T00:00:00Z",
                "cycle_end_ts": "2026-04-01T00:15:00Z",
                "offset": 9,
                "resolved": True,
                "winner_side": "DOWN",
                "quote_status": "ok",
                "quote_up_ask": 0.79,
                "quote_down_ask": 0.21,
                "predicted_side": "UP",
                "policy_action": "trade",
                "policy_reason": "trade",
            },
        ]
    )

    pool = build_profitable_offset_pool_frame(
        decisions,
        entry_price_min=0.01,
        entry_price_max=0.30,
    )

    assert pool["profitable_pool_window"].tolist() == [True, True, True]
    assert pool["profitable_pool_capture"].tolist() == [True, False, False]
    assert pool["profitable_pool_status"].tolist() == [
        "captured",
        "correct_side_no_trade",
        "traded_wrong_side",
    ]


def test_quick_screen_rank_tuple_prefers_tradeable_band_hits() -> None:
    better = {
        "profitable_pool_coverage_ratio": 0.70,
        "profitable_pool_capture_rows": 14,
        "profitable_pool_correct_side_rows": 17,
        "profitable_pool_rows": 20,
        "traded_winner_in_band_rows": 6,
        "backed_winner_in_band_rows": 10,
        "trade_rows": 14,
        "backed_winner_rows": 20,
        "winner_in_band_rows": 25,
    }
    worse = {
        "profitable_pool_coverage_ratio": 0.55,
        "profitable_pool_capture_rows": 11,
        "profitable_pool_correct_side_rows": 18,
        "profitable_pool_rows": 20,
        "traded_winner_in_band_rows": 3,
        "backed_winner_in_band_rows": 11,
        "trade_rows": 18,
        "backed_winner_rows": 22,
        "winner_in_band_rows": 25,
    }

    assert quick_screen_rank_tuple(better) > quick_screen_rank_tuple(worse)


def test_run_bundle_quick_screen_scopes_inputs_before_replay_build(tmp_path: Path, monkeypatch) -> None:
    bundle_dir = tmp_path / "bundle"
    for offset in (7, 8):
        offset_dir = bundle_dir / "offsets" / f"offset={offset}"
        offset_dir.mkdir(parents=True, exist_ok=True)
        (offset_dir / "bundle_config.json").write_text(
            json.dumps({"feature_columns": ["feature_a"]}, ensure_ascii=False),
            encoding="utf-8",
        )

    features = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-27T23:59:00Z",
                "cycle_start_ts": "2026-03-27T23:45:00Z",
                "cycle_end_ts": "2026-03-28T00:00:00Z",
                "offset": 7,
                "feature_a": 1.0,
                "extra_feature": 99.0,
            },
            {
                "decision_ts": "2026-03-28T00:07:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
                "feature_a": 2.0,
                "extra_feature": 98.0,
            },
            {
                "decision_ts": "2026-03-28T00:23:00Z",
                "cycle_start_ts": "2026-03-28T00:15:00Z",
                "cycle_end_ts": "2026-03-28T00:30:00Z",
                "offset": 8,
                "feature_a": 3.0,
                "extra_feature": 97.0,
            },
            {
                "decision_ts": "2026-03-28T00:40:00Z",
                "cycle_start_ts": "2026-03-28T00:30:00Z",
                "cycle_end_ts": "2026-03-28T00:45:00Z",
                "offset": 10,
                "feature_a": 4.0,
                "extra_feature": 96.0,
            },
        ]
    )
    labels = pd.DataFrame(
        [
            {
                "cycle_start_ts": 1_774_656_000,
                "cycle_end_ts": 1_774_656_900,
                "label_set": "truth",
                "resolved": True,
                "winner_side": "UP",
                "label_source": "settlement_truth",
                "settlement_source": "settlement_truth",
            },
            {
                "cycle_start_ts": 1_774_656_900,
                "cycle_end_ts": 1_774_657_800,
                "label_set": "truth",
                "resolved": True,
                "winner_side": "DOWN",
                "label_source": "settlement_truth",
                "settlement_source": "settlement_truth",
            },
            {
                "cycle_start_ts": 1_774_655_100,
                "cycle_end_ts": 1_774_656_000,
                "label_set": "truth",
                "resolved": True,
                "winner_side": "UP",
                "label_source": "settlement_truth",
                "settlement_source": "settlement_truth",
            },
        ]
    )
    cfg = ResearchConfig.build(
        market="btc",
        cycle="15m",
        profile="deep_otm_baseline",
        source_surface="backtest",
        feature_set="bs_q_replace_direction",
        label_set="truth",
        target="direction",
        model_family="deep_otm",
        root=tmp_path,
    )

    seen: dict[str, object] = {}

    def _fake_load_feature_frame(_cfg, *, feature_set=None, columns=None):
        seen["feature_columns"] = list(columns) if columns is not None else None
        selected = features.copy()
        if columns is not None:
            selected = selected.loc[:, [column for column in columns if column in selected.columns]]
        return selected

    def _fake_load_label_frame(_cfg, *, label_set=None, columns=None):
        seen["label_columns"] = list(columns) if columns is not None else None
        selected = labels.copy()
        if columns is not None:
            selected = selected.loc[:, [column for column in columns if column in selected.columns]]
        return selected

    def _fake_build_bundle_replay(*, bundle_dir, features, labels):
        assert list(features.columns) == ["decision_ts", "cycle_start_ts", "cycle_end_ts", "offset", "feature_a"]
        assert len(features) == 2
        assert set(pd.to_numeric(features["offset"], errors="coerce").astype(int).tolist()) == {7, 8}
        assert len(labels) == 2
        replay = pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-28T00:07:00Z",
                    "cycle_start_ts": "2026-03-28T00:00:00Z",
                    "cycle_end_ts": "2026-03-28T00:15:00Z",
                    "offset": 7,
                    "resolved": True,
                    "winner_side": "UP",
                }
            ]
        )
        return replay, SimpleNamespace(merged_rows=1, ready_rows=1), [7, 8]

    monkeypatch.setattr(quick_screen_module, "load_feature_frame", _fake_load_feature_frame)
    monkeypatch.setattr(quick_screen_module, "load_label_frame", _fake_load_label_frame)
    monkeypatch.setattr(quick_screen_module, "_build_bundle_replay", _fake_build_bundle_replay)
    monkeypatch.setattr(
        quick_screen_module,
        "attach_canonical_quote_surface",
        lambda *, replay, data_cfg: (
            replay.assign(
                quote_status="ok",
                quote_up_ask=0.12,
                quote_down_ask=0.88,
            ),
            SimpleNamespace(quote_ready_rows=len(replay), quote_missing_rows=0),
        ),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "resolve_backtest_profile_spec",
        lambda **kwargs: SimpleNamespace(entry_price_min=0.01, entry_price_max=0.30),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "build_profile_decision_engine_parity_config",
        lambda **kwargs: {},
    )
    monkeypatch.setattr(
        quick_screen_module,
        "apply_decision_engine_parity",
        lambda replay, config, up_price_columns, down_price_columns: replay.assign(predicted_side="UP"),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "build_policy_decisions",
        lambda decisions, config, model_source: decisions.assign(policy_action="trade", policy_reason="trade"),
    )

    summary, decisions = run_bundle_quick_screen(
        cfg=cfg,
        bundle_dir=bundle_dir,
        profile="deep_otm_baseline",
        target="direction",
        decision_start="2026-03-28",
        decision_end="2026-03-28",
        parity=SimpleNamespace(),
    )

    assert seen["feature_columns"] == ["decision_ts", "cycle_start_ts", "cycle_end_ts", "offset", "feature_a"]
    assert seen["label_columns"] == [
        "cycle_start_ts",
        "cycle_end_ts",
        "label_set",
        "resolved",
        "winner_side",
        "label_source",
        "settlement_source",
        "full_truth",
    ]
    assert summary["rows"] == 1
    assert decisions["policy_action"].tolist() == ["trade"]


def test_run_bundle_quick_screen_writes_profitable_pool_cache(tmp_path: Path, monkeypatch) -> None:
    bundle_dir = tmp_path / "bundle"
    offset_dir = bundle_dir / "offsets" / "offset=7"
    offset_dir.mkdir(parents=True, exist_ok=True)
    (offset_dir / "bundle_config.json").write_text(
        json.dumps({"feature_columns": ["feature_a"]}, ensure_ascii=False),
        encoding="utf-8",
    )
    cfg = ResearchConfig.build(
        market="btc",
        cycle="15m",
        profile="deep_otm_baseline",
        source_surface="backtest",
        feature_set="bs_q_replace_direction",
        label_set="truth",
        target="direction",
        model_family="deep_otm",
        root=tmp_path,
    )

    monkeypatch.setattr(
        quick_screen_module,
        "load_feature_frame",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "decision_ts": "2026-04-01T00:07:00Z",
                    "cycle_start_ts": "2026-04-01T00:00:00Z",
                    "cycle_end_ts": "2026-04-01T00:15:00Z",
                    "offset": 7,
                    "feature_a": 1.0,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "load_label_frame",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "cycle_start_ts": 1_775_001_600,
                    "cycle_end_ts": 1_775_002_500,
                    "label_set": "truth",
                    "resolved": True,
                    "winner_side": "UP",
                    "label_source": "settlement_truth",
                    "settlement_source": "settlement_truth",
                    "full_truth": True,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "_build_bundle_replay",
        lambda **_kwargs: (
            pd.DataFrame(
                [
                    {
                        "decision_ts": "2026-04-01T00:07:00Z",
                        "cycle_start_ts": "2026-04-01T00:00:00Z",
                        "cycle_end_ts": "2026-04-01T00:15:00Z",
                        "offset": 7,
                        "resolved": True,
                        "winner_side": "UP",
                    }
                ]
            ),
            SimpleNamespace(merged_rows=1, ready_rows=1),
            [7],
        ),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "attach_canonical_quote_surface",
        lambda *, replay, data_cfg: (
            replay.assign(
                quote_status="ok",
                quote_up_ask=0.12,
                quote_down_ask=0.88,
            ),
            SimpleNamespace(quote_ready_rows=len(replay), quote_missing_rows=0),
        ),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "resolve_backtest_profile_spec",
        lambda **kwargs: SimpleNamespace(entry_price_min=0.01, entry_price_max=0.30),
    )
    monkeypatch.setattr(quick_screen_module, "build_profile_decision_engine_parity_config", lambda **kwargs: {})
    monkeypatch.setattr(
        quick_screen_module,
        "apply_decision_engine_parity",
        lambda replay, config, up_price_columns, down_price_columns: replay.assign(predicted_side="UP"),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "build_policy_decisions",
        lambda decisions, config, model_source: decisions.assign(policy_action="trade", policy_reason="trade"),
    )

    summary, _decisions = run_bundle_quick_screen(
        cfg=cfg,
        bundle_dir=bundle_dir,
        profile="deep_otm_baseline",
        target="direction",
        decision_start="2026-04-01",
        decision_end="2026-04-15",
        parity=SimpleNamespace(),
    )

    data_path, manifest_path = profitable_offset_pool_cache_paths(
        cfg=cfg,
        profile="deep_otm_baseline",
        decision_start="2026-04-01",
        decision_end="2026-04-15",
        stake_label="2usd",
    )
    assert data_path.exists()
    assert manifest_path.exists()
    cached = pd.read_parquet(data_path)
    assert len(cached) == 1
    assert summary["profitable_pool_rows"] == 1
    assert summary["profitable_pool_capture_rows"] == 1


def test_ensure_training_and_bundle_defaults_to_parallel_offset_quick_screen_training(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cfg = ResearchConfig.build(
        market="btc",
        cycle="15m",
        profile="deep_otm_baseline",
        source_surface="backtest",
        feature_set="bs_q_replace_direction",
        label_set="truth",
        target="reversal",
        model_family="deep_otm",
        root=tmp_path,
    )
    market_spec = SimpleNamespace(
        market="btc",
        profile="deep_otm_baseline",
        feature_set="bs_q_replace_direction",
        label_set="truth",
        target="reversal",
        model_family="deep_otm",
        window=SimpleNamespace(label="2026-03"),
        offsets=(7, 8, 9),
        weight_variant_label="default",
        balance_classes=None,
        weight_by_vol=None,
        inverse_vol=None,
        contrarian_weight=None,
        contrarian_quantile=None,
        contrarian_return_col=None,
        winner_in_band_weight=None,
        offset_weight_overrides=None,
    )
    captured: dict[str, object] = {}

    def _fake_train_research_run(_cfg, spec):
        captured["parallel_workers"] = spec.parallel_workers
        run_dir = _cfg.layout.training_run_dir(
            model_family=spec.model_family,
            target=spec.target,
            run_label_text=spec.run_label,
        )
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "summary.json").write_text("{}", encoding="utf-8")
        return {"run_dir": str(run_dir), "summary_path": str(run_dir / "summary.json")}

    def _fake_build_model_bundle(_cfg, spec):
        bundle_dir = _cfg.layout.bundle_dir(
            profile=spec.profile,
            target=spec.target,
            bundle_label_text=spec.bundle_label,
        )
        bundle_dir.mkdir(parents=True, exist_ok=True)
        (bundle_dir / "summary.json").write_text("{}", encoding="utf-8")
        return {"bundle_dir": str(bundle_dir), "summary_path": str(bundle_dir / "summary.json")}

    monkeypatch.setattr(quick_screen_module, "train_research_run", _fake_train_research_run)
    monkeypatch.setattr(quick_screen_module, "build_model_bundle", _fake_build_model_bundle)

    ensure_training_and_bundle(
        cfg=cfg,
        market_spec=market_spec,
        training_run_label="demo-train",
        bundle_label="demo-bundle",
    )

    assert captured["parallel_workers"] == 3


def test_run_bundle_quick_screen_reuses_cached_profitable_pool(tmp_path: Path, monkeypatch) -> None:
    bundle_dir = tmp_path / "bundle"
    offset_dir = bundle_dir / "offsets" / "offset=7"
    offset_dir.mkdir(parents=True, exist_ok=True)
    (offset_dir / "bundle_config.json").write_text(
        json.dumps({"feature_columns": ["feature_a"]}, ensure_ascii=False),
        encoding="utf-8",
    )
    cfg = ResearchConfig.build(
        market="btc",
        cycle="15m",
        profile="deep_otm_baseline",
        source_surface="backtest",
        feature_set="bs_q_replace_direction",
        label_set="truth",
        target="direction",
        model_family="deep_otm",
        root=tmp_path,
    )

    monkeypatch.setattr(
        quick_screen_module,
        "load_feature_frame",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "decision_ts": "2026-04-01T00:07:00Z",
                    "cycle_start_ts": "2026-04-01T00:00:00Z",
                    "cycle_end_ts": "2026-04-01T00:15:00Z",
                    "offset": 7,
                    "feature_a": 1.0,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "load_label_frame",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "cycle_start_ts": 1_775_001_600,
                    "cycle_end_ts": 1_775_002_500,
                    "label_set": "truth",
                    "resolved": True,
                    "winner_side": "UP",
                    "label_source": "settlement_truth",
                    "settlement_source": "settlement_truth",
                    "full_truth": True,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "_build_bundle_replay",
        lambda **_kwargs: (
            pd.DataFrame(
                [
                    {
                        "decision_ts": "2026-04-01T00:07:00Z",
                        "cycle_start_ts": "2026-04-01T00:00:00Z",
                        "cycle_end_ts": "2026-04-01T00:15:00Z",
                        "offset": 7,
                        "resolved": True,
                        "winner_side": "UP",
                    }
                ]
            ),
            SimpleNamespace(merged_rows=1, ready_rows=1),
            [7],
        ),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "attach_canonical_quote_surface",
        lambda *, replay, data_cfg: (
            replay.assign(
                quote_status="ok",
                quote_up_ask=0.12,
                quote_down_ask=0.88,
            ),
            SimpleNamespace(quote_ready_rows=len(replay), quote_missing_rows=0),
        ),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "resolve_backtest_profile_spec",
        lambda **kwargs: SimpleNamespace(entry_price_min=0.01, entry_price_max=0.30),
    )
    monkeypatch.setattr(quick_screen_module, "build_profile_decision_engine_parity_config", lambda **kwargs: {})
    monkeypatch.setattr(
        quick_screen_module,
        "apply_decision_engine_parity",
        lambda replay, config, up_price_columns, down_price_columns: replay.assign(predicted_side="UP"),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "build_policy_decisions",
        lambda decisions, config, model_source: decisions.assign(policy_action="trade", policy_reason="trade"),
    )

    run_bundle_quick_screen(
        cfg=cfg,
        bundle_dir=bundle_dir,
        profile="deep_otm_baseline",
        target="direction",
        decision_start="2026-04-01",
        decision_end="2026-04-15",
        parity=SimpleNamespace(),
    )

    monkeypatch.setattr(
        quick_screen_module,
        "build_profitable_offset_pool_frame",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should reuse cache")),
    )

    summary, _decisions = run_bundle_quick_screen(
        cfg=cfg,
        bundle_dir=bundle_dir,
        profile="deep_otm_baseline",
        target="direction",
        decision_start="2026-04-01",
        decision_end="2026-04-15",
        parity=SimpleNamespace(),
    )

    assert summary["profitable_pool_rows"] == 1
    assert summary["profitable_pool_capture_rows"] == 1
