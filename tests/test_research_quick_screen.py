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
    compact_quick_screen_artifacts,
    ensure_training_and_bundle,
    profitable_offset_pool_cache_paths,
    resolve_profitable_offset_pool_frame,
    quick_screen_artifact_retention_decision,
    quick_screen_rank_tuple,
    run_bundle_quick_screen,
)
from pm15min.research.config import ResearchConfig


def _patch_formal_quick_screen_runtime(monkeypatch) -> None:
    def _fake_build_decision_depth_runtime(*, replay, data_cfg, fill_config):
        return (
            pd.DataFrame(),
            SimpleNamespace(
                snapshot_rows=0,
                replay_rows_with_snapshots=0,
                replay_rows_without_snapshots=len(replay),
            ),
            {},
        )

    def _fake_build_guarded_policy_decisions(
        *,
        replay,
        market,
        profile,
        profile_spec,
        model_source,
        depth_replay,
        fill_config,
    ):
        decisions = replay.copy()
        decisions["predicted_side"] = "UP"
        decisions["policy_action"] = "trade"
        decisions["policy_reason"] = "trade"
        if "quote_status" not in decisions.columns:
            decisions["quote_status"] = "ok"
        if "quote_up_ask" not in decisions.columns:
            decisions["quote_up_ask"] = 0.12
        if "quote_down_ask" not in decisions.columns:
            decisions["quote_down_ask"] = 0.88
        return decisions, SimpleNamespace(), SimpleNamespace(raw_depth_rows=0)

    monkeypatch.setattr(
        quick_screen_module,
        "_build_decision_depth_runtime",
        _fake_build_decision_depth_runtime,
    )
    monkeypatch.setattr(
        quick_screen_module,
        "_build_guarded_policy_decisions",
        _fake_build_guarded_policy_decisions,
    )
    monkeypatch.setattr(
        quick_screen_module,
        "_resolve_fill_depth_runtime",
        lambda **_kwargs: (pd.DataFrame(), {}),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "build_canonical_fills",
        lambda accepted, **_kwargs: (
            accepted.assign(fill_valid=True, fill_reason="filled").reset_index(drop=True),
            pd.DataFrame(columns=["reason"]),
        ),
    )
    monkeypatch.setattr(
        quick_screen_module,
        "settle_trade_fills",
        lambda fills: fills.copy().reset_index(drop=True),
    )


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
    assert summary["signal_trade_rows"] == 2
    assert summary["metric_semantics"]["trade_rows"] == "quick_screen_policy_signal_rows"
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
    assert summary["density_bottleneck"]["primary_bottleneck"] == "low_trade_density"
    assert summary["density_bottleneck"]["recommended_route"] == "feature_width_or_family_rework"


def test_build_quick_screen_summary_uses_final_fills_when_available() -> None:
    decisions = pd.DataFrame(
        [
            {
                "decision_ts": f"2026-04-01T00:{minute:02d}:00Z",
                "cycle_start_ts": "2026-04-01T00:00:00Z",
                "cycle_end_ts": "2026-04-01T00:15:00Z",
                "offset": 7,
                "resolved": True,
                "winner_side": "UP",
                "quote_status": "ok",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.82,
                "predicted_side": "UP",
                "policy_action": "trade",
                "policy_reason": "trade",
            }
            for minute in range(10)
        ]
    )
    trades = decisions.iloc[[0, 1]].copy()
    rejects = pd.DataFrame(
        [
            {
                "decision_ts": f"2026-04-01T00:{minute:02d}:00Z",
                "cycle_start_ts": "2026-04-01T00:00:00Z",
                "cycle_end_ts": "2026-04-01T00:15:00Z",
                "offset": 7,
                "reason": "repriced_entry_price_max",
            }
            for minute in range(2, 10)
        ]
    )

    summary = build_quick_screen_summary(
        decisions,
        entry_price_min=0.01,
        entry_price_max=0.30,
        final_trades=trades,
        rejects=rejects,
    )

    assert summary["signal_trade_rows"] == 10
    assert summary["trade_rows"] == 2
    assert summary["profitable_pool_capture_rows"] == 2
    assert summary["metric_semantics"]["trade_rows"] == "quick_screen_formal_filled_trade_rows"
    assert summary["reject_reason_counts"] == {"repriced_entry_price_max": 8}


def test_build_quick_screen_summary_uses_final_fills_for_pool_statuses() -> None:
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
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.82,
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
                "quote_up_ask": 0.78,
                "quote_down_ask": 0.18,
                "predicted_side": "UP",
                "policy_action": "trade",
                "policy_reason": "trade",
            },
        ]
    )
    trades = decisions.iloc[[0]].copy()

    summary = build_quick_screen_summary(
        decisions,
        entry_price_min=0.01,
        entry_price_max=0.30,
        final_trades=trades,
        rejects=pd.DataFrame(columns=["reason"]),
    )

    assert summary["signal_trade_rows"] == 2
    assert summary["trade_rows"] == 1
    assert summary["profitable_pool_capture_rows"] == 1
    assert summary["profitable_pool_status_counts"] == {
        "captured": 1,
        "correct_side_no_trade": 0,
        "missed": 1,
        "traded_wrong_side": 0,
    }


def test_build_quick_screen_summary_does_not_copy_full_decisions_frame(monkeypatch) -> None:
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
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.82,
                "predicted_side": "UP",
                "policy_action": "trade",
                "policy_reason": "trade",
            },
        ]
    )
    original_copy = pd.DataFrame.copy

    def _guard_decision_copy(self, *args, **kwargs):
        if self is decisions:
            raise AssertionError("quick-screen summary should not copy the full decisions frame")
        return original_copy(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "copy", _guard_decision_copy)

    summary = build_quick_screen_summary(
        decisions,
        entry_price_min=0.01,
        entry_price_max=0.30,
        rejects=pd.DataFrame(columns=["reason"]),
    )

    assert summary["trade_rows"] == 1
    assert "profitable_pool_status" not in decisions.columns


def test_build_quick_screen_summary_reports_density_bottleneck() -> None:
    decisions = pd.DataFrame(
        [
            {
                "resolved": True,
                "winner_side": "UP",
                "quote_status": "ok",
                "quote_up_ask": 0.18,
                "quote_down_ask": 0.82,
                "predicted_side": "UP",
                "policy_action": "reject",
                "policy_reason": "direction_prob",
            },
            {
                "resolved": True,
                "winner_side": "DOWN",
                "quote_status": "ok",
                "quote_up_ask": 0.88,
                "quote_down_ask": 0.16,
                "predicted_side": "DOWN",
                "policy_action": "reject",
                "policy_reason": "direction_prob",
            },
            {
                "resolved": True,
                "winner_side": "UP",
                "quote_status": "ok",
                "quote_up_ask": 0.22,
                "quote_down_ask": 0.79,
                "predicted_side": "UP",
                "policy_action": "trade",
                "policy_reason": "trade",
            },
            {
                "resolved": True,
                "winner_side": "DOWN",
                "quote_status": "ok",
                "quote_up_ask": 0.73,
                "quote_down_ask": 0.40,
                "predicted_side": "DOWN",
                "policy_action": "reject",
                "policy_reason": "entry_price_max",
            },
        ]
    )

    summary = build_quick_screen_summary(
        decisions,
        entry_price_min=0.01,
        entry_price_max=0.30,
    )

    assert summary["density_bottleneck"] == {
        "primary_bottleneck": "probability_gate",
        "recommended_route": "model_or_calibration_rework",
        "sparse_density": True,
        "dominant_reject_reason": "direction_prob",
        "dominant_reject_rows": 2,
        "reject_rows": 3,
        "quote_missing_rows": 0,
        "trade_rows": 1,
        "profitable_pool_rows": 3,
        "profitable_pool_capture_rows": 1,
        "profitable_pool_correct_side_rows": 3,
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


def test_apply_final_trade_captures_updates_cached_pool_statuses() -> None:
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
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.82,
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
                "quote_up_ask": 0.78,
                "quote_down_ask": 0.18,
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
    trades = decisions.iloc[[0]].copy()

    updated = quick_screen_module._apply_final_trade_captures(
        profitable_pool_frame=pool,
        decisions=decisions,
        final_trades=trades,
    )

    assert updated["profitable_pool_capture"].tolist() == [True, False]
    assert updated["profitable_pool_status"].tolist() == ["captured", "missed"]


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


def test_quick_screen_artifact_retention_compacts_sparse_candidates() -> None:
    decision = quick_screen_artifact_retention_decision(
        {
            "trade_rows": 12,
            "profitable_pool_capture_rows": 0,
            "density_bottleneck": {"sparse_density": True},
        },
        mode="compact_rejects",
        retain_min_trades=56,
    )

    assert decision["artifacts_retained"] is False
    assert decision["retention_reason"] == "below_trade_floor"
    assert decision["trade_rows"] == 12


def test_quick_screen_artifact_retention_keeps_dense_candidates() -> None:
    decision = quick_screen_artifact_retention_decision(
        {
            "trade_rows": 56,
            "profitable_pool_capture_rows": 0,
            "density_bottleneck": {"sparse_density": False},
        },
        mode="compact_rejects",
        retain_min_trades=56,
    )

    assert decision["artifacts_retained"] is True
    assert decision["retention_reason"] == "trade_floor_met"


def test_compact_quick_screen_artifacts_removes_sparse_candidate_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("PM15MIN_QUICK_SCREEN_ARTIFACT_RETENTION", "compact_rejects")
    monkeypatch.setenv("PM15MIN_QUICK_SCREEN_CLEAN_FEATURE_FRAMES", "1")
    monkeypatch.setenv("PM15MIN_QUICK_SCREEN_CLEAN_TRAINING_SETS", "1")
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm_baseline",
        source_surface="backtest",
        feature_set="candidate_sparse",
        label_set="truth",
        target="direction",
        model_family="deep_otm",
        root=tmp_path,
    )
    market_spec = SimpleNamespace(
        feature_set="candidate_sparse",
        label_set="truth",
        target="direction",
        window=SimpleNamespace(label="2026-03-01_2026-03-31"),
        offsets=(7, 8),
    )
    training_run_dir = cfg.layout.training_run_dir(
        model_family="deep_otm",
        target="direction",
        run_label_text="candidate-train",
    )
    bundle_dir = cfg.layout.bundle_dir(
        profile="deep_otm_baseline",
        target="direction",
        bundle_label_text="candidate-bundle",
    )
    feature_frame_dir = cfg.layout.feature_frame_dir("candidate_sparse", source_surface="backtest")
    training_set_dirs = [
        cfg.layout.training_set_dir(
            feature_set="candidate_sparse",
            label_set="truth",
            target="direction",
            window="2026-03-01_2026-03-31",
            offset=offset,
        )
        for offset in (7, 8)
    ]
    for path in [training_run_dir, bundle_dir, feature_frame_dir, *training_set_dirs]:
        path.mkdir(parents=True, exist_ok=True)
        (path / "sentinel.txt").write_text("keep until compacted", encoding="utf-8")

    cleanup = compact_quick_screen_artifacts(
        cfg=cfg,
        market_spec=market_spec,
        train_result={"run_dir": str(training_run_dir)},
        bundle_result={"bundle_dir": str(bundle_dir)},
        quick_summary={
            "trade_rows": 3,
            "profitable_pool_capture_rows": 0,
            "density_bottleneck": {"sparse_density": True},
        },
    )

    assert cleanup["artifacts_retained"] is False
    assert not training_run_dir.exists()
    assert not bundle_dir.exists()
    assert not feature_frame_dir.exists()
    assert all(not path.exists() for path in training_set_dirs)
    assert cleanup["removed_path_count"] == 5


def test_compact_quick_screen_artifacts_keeps_dense_candidate_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("PM15MIN_QUICK_SCREEN_ARTIFACT_RETENTION", "compact_rejects")
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm_baseline",
        source_surface="backtest",
        feature_set="candidate_dense",
        label_set="truth",
        target="direction",
        model_family="deep_otm",
        root=tmp_path,
    )
    market_spec = SimpleNamespace(
        feature_set="candidate_dense",
        label_set="truth",
        target="direction",
        window=SimpleNamespace(label="2026-03-01_2026-03-31"),
        offsets=(7,),
    )
    training_run_dir = cfg.layout.training_run_dir(
        model_family="deep_otm",
        target="direction",
        run_label_text="candidate-train",
    )
    bundle_dir = cfg.layout.bundle_dir(
        profile="deep_otm_baseline",
        target="direction",
        bundle_label_text="candidate-bundle",
    )
    for path in (training_run_dir, bundle_dir):
        path.mkdir(parents=True, exist_ok=True)

    cleanup = compact_quick_screen_artifacts(
        cfg=cfg,
        market_spec=market_spec,
        train_result={"run_dir": str(training_run_dir)},
        bundle_result={"bundle_dir": str(bundle_dir)},
        quick_summary={
            "trade_rows": 80,
            "profitable_pool_capture_rows": 0,
            "density_bottleneck": {"sparse_density": False},
        },
    )

    assert cleanup["artifacts_retained"] is True
    assert training_run_dir.exists()
    assert bundle_dir.exists()


def test_compact_quick_screen_artifacts_dry_run_reports_without_removing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("PM15MIN_QUICK_SCREEN_ARTIFACT_RETENTION", "compact_rejects")
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm_baseline",
        source_surface="backtest",
        feature_set="candidate_sparse",
        label_set="truth",
        target="direction",
        model_family="deep_otm",
        root=tmp_path,
    )
    market_spec = SimpleNamespace(
        feature_set="candidate_sparse",
        label_set="truth",
        target="direction",
        window=SimpleNamespace(label="2026-03-01_2026-03-31"),
        offsets=(7,),
    )
    training_run_dir = cfg.layout.training_run_dir(
        model_family="deep_otm",
        target="direction",
        run_label_text="candidate-train",
    )
    bundle_dir = cfg.layout.bundle_dir(
        profile="deep_otm_baseline",
        target="direction",
        bundle_label_text="candidate-bundle",
    )
    for path in (training_run_dir, bundle_dir):
        path.mkdir(parents=True, exist_ok=True)

    cleanup = compact_quick_screen_artifacts(
        cfg=cfg,
        market_spec=market_spec,
        train_result={"run_dir": str(training_run_dir)},
        bundle_result={"bundle_dir": str(bundle_dir)},
        quick_summary={"trade_rows": 1, "profitable_pool_capture_rows": 0},
        apply=False,
    )

    assert cleanup["artifacts_retained"] is False
    assert cleanup["removed_path_count"] == 0
    assert cleanup["would_remove_path_count"] >= 2
    assert training_run_dir.exists()
    assert bundle_dir.exists()


def test_resolve_profitable_offset_pool_frame_reuses_freshly_built_pool_without_second_merge(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm_baseline",
        source_surface="backtest",
        feature_set="candidate_sparse",
        label_set="truth",
        target="direction",
        model_family="deep_otm",
        root=tmp_path,
    )
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
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.82,
                "predicted_side": "UP",
                "policy_action": "trade",
                "policy_reason": "trade",
            }
        ]
    )

    monkeypatch.setattr(
        quick_screen_module,
        "_apply_cached_profitable_pool",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("freshly built pool should not be merged a second time")
        ),
    )

    pool_frame, pool_cache = resolve_profitable_offset_pool_frame(
        cfg=cfg,
        profile="deep_otm_baseline",
        decision_start="2026-04-01",
        decision_end="2026-04-15",
        decisions=decisions,
        entry_price_min=0.01,
        entry_price_max=0.30,
        stake_label="2usd",
    )

    assert pool_cache["cache_status"] == "built"
    assert pool_frame["profitable_pool_capture"].tolist() == [True]


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

    def _fake_load_feature_frame(_cfg, *, feature_set=None, columns=None, filters=None):
        seen["feature_columns"] = list(columns) if columns is not None else None
        seen["feature_filters"] = filters
        selected = features.copy()
        if columns is not None:
            selected = selected.loc[:, [column for column in columns if column in selected.columns]]
        return selected

    def _fake_load_label_frame(_cfg, *, label_set=None, columns=None, filters=None):
        seen["label_columns"] = list(columns) if columns is not None else None
        seen["label_filters"] = filters
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
    _patch_formal_quick_screen_runtime(monkeypatch)
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
    assert seen["feature_filters"] == [
        ("decision_ts", ">=", pd.Timestamp("2026-03-28T00:00:00Z")),
        ("decision_ts", "<", pd.Timestamp("2026-03-29T00:00:00Z")),
    ]
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
    assert seen["label_filters"] == [
        ("cycle_start_ts", ">=", int(pd.Timestamp("2026-03-28T00:00:00Z").timestamp())),
        ("cycle_end_ts", "<=", int(pd.Timestamp("2026-03-28T00:30:00Z").timestamp())),
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
    _patch_formal_quick_screen_runtime(monkeypatch)
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

    summary, decisions = run_bundle_quick_screen(
        cfg=cfg,
        bundle_dir=bundle_dir,
        profile="deep_otm_baseline",
        target="direction",
        decision_start="2026-04-01",
        decision_end="2026-04-15",
        parity=SimpleNamespace(),
        return_decisions=False,
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
    assert decisions.empty


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
    _patch_formal_quick_screen_runtime(monkeypatch)
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
