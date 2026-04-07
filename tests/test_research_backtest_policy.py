from __future__ import annotations

import pandas as pd

from pm15min.research.backtests.hybrid import apply_hybrid_fallback
from pm15min.research.backtests.policy import DecisionPolicyConfig, apply_decision_policy, build_policy_reject_frame
from pm15min.research.backtests.reports import (
    build_backtest_summary,
    build_policy_breakdown_frame,
    build_reject_reason_counts,
    build_reject_summary_frame,
    render_backtest_report,
)


def test_apply_decision_policy_marks_trades_and_rejects() -> None:
    rows = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "resolved": True,
                "winner_side": "UP",
                "score_valid": True,
                "score_reason": "",
                "p_up": 0.73,
                "p_down": 0.27,
            },
            {
                "decision_ts": "2026-03-01T00:16:00Z",
                "cycle_start_ts": "2026-03-01T00:15:00Z",
                "cycle_end_ts": "2026-03-01T00:30:00Z",
                "offset": 7,
                "resolved": True,
                "winner_side": "DOWN",
                "score_valid": False,
                "score_reason": "missing_reversal_anchor",
                "p_up": 0.51,
                "p_down": 0.49,
            },
        ]
    )
    out = apply_decision_policy(rows, cfg=DecisionPolicyConfig(min_confidence=0.60, min_probability_gap=0.05))
    assert out["policy_action"].tolist() == ["trade", "reject"]
    assert out["policy_reason"].tolist() == ["trade", "missing_reversal_anchor"]
    rejects = build_policy_reject_frame(out)
    assert rejects["policy_reason"].tolist() == ["missing_reversal_anchor"]


def test_apply_hybrid_fallback_uses_secondary_trade() -> None:
    primary = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "policy_action": "reject",
                "policy_reason": "direction_prob",
                "decision_source": "primary",
                "predicted_side": "UP",
                "predicted_prob": 0.54,
                "probability_gap": 0.08,
            }
        ]
    )
    secondary = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "policy_action": "trade",
                "policy_reason": "trade",
                "decision_source": "secondary",
                "predicted_side": "DOWN",
                "predicted_prob": 0.66,
                "probability_gap": 0.32,
            }
        ]
    )
    out = apply_hybrid_fallback(primary, secondary, fallback_reasons=["direction_prob"])
    assert out.loc[0, "policy_action"] == "trade"
    assert out.loc[0, "policy_reason"] == "hybrid_fallback_trade"
    assert out.loc[0, "decision_source"] == "secondary"
    assert out.loc[0, "predicted_side"] == "DOWN"


def test_apply_decision_policy_uses_decision_engine_side_and_reject_reason() -> None:
    rows = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "resolved": True,
                "winner_side": "DOWN",
                "score_valid": True,
                "score_reason": "",
                "p_up": 0.58,
                "p_down": 0.42,
                "decision_engine_action": "trade",
                "decision_engine_reason": "trade",
                "decision_engine_side": "DOWN",
                "decision_engine_prob": 0.42,
                "decision_engine_probability_gap": 0.16,
            },
            {
                "decision_ts": "2026-03-01T00:16:00Z",
                "cycle_start_ts": "2026-03-01T00:15:00Z",
                "cycle_end_ts": "2026-03-01T00:30:00Z",
                "offset": 7,
                "resolved": True,
                "winner_side": "UP",
                "score_valid": True,
                "score_reason": "",
                "p_up": 0.61,
                "p_down": 0.39,
                "decision_engine_action": "reject",
                "decision_engine_reason": "direction_prob",
            },
        ]
    )

    out = apply_decision_policy(rows, cfg=DecisionPolicyConfig(min_confidence=0.40, min_probability_gap=0.05))

    assert out["policy_action"].tolist() == ["trade", "reject"]
    assert out["predicted_side"].tolist() == ["DOWN", "UP"]
    assert out["predicted_prob"].tolist() == [0.42, 0.61]
    assert out["policy_reason"].tolist() == ["trade", "direction_prob"]


def test_report_builders_render_summary() -> None:
    trades = pd.DataFrame(
        [
            {"decision_source": "primary", "win": True, "stake": 2.0, "pnl": 1.0},
            {"decision_source": "secondary", "win": False, "stake": 1.0, "pnl": -1.0},
        ]
    )
    rejects = pd.DataFrame(
        [
            {"decision_source": "primary", "reason": "policy_low_confidence"},
            {"decision_source": "primary", "reason": "unresolved_label"},
        ]
    )
    scored = pd.DataFrame(
        [
            {
                "decision_source": "primary",
                "policy_action": "trade",
                "policy_reason": "trade",
                "pre_submit_orderbook_retry_armed": False,
                "pre_submit_orderbook_retry_reason": "",
            },
            {"decision_source": "secondary", "policy_action": "trade", "policy_reason": "hybrid_fallback_trade"},
            {
                "decision_source": "primary",
                "policy_action": "reject",
                "policy_reason": "policy_low_confidence",
                "pre_submit_orderbook_retry_armed": False,
                "pre_submit_orderbook_retry_reason": "",
            },
            {
                "decision_source": "primary",
                "policy_action": "reject",
                "policy_reason": "orderbook_limit_reject",
                "pre_submit_orderbook_retry_armed": True,
                "pre_submit_orderbook_retry_reason": "orderbook_limit_reject",
            },
        ]
    )
    summary = build_backtest_summary(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        spec_name="baseline_truth",
        target="direction",
        bundle_dir="/tmp/bundle",
        feature_set="deep_otm_v1",
        label_set="truth",
        available_offsets=[7, 8],
        decision_quote_summary={
            "raw_depth_rows": 3,
            "repriced_rows": 2,
            "limit_reject_rows": 1,
            "orderbook_missing_rows": 0,
        },
        retry_contract_summary={
            "pre_submit_depth_retry_max": 5,
            "pre_submit_depth_retry_interval_sec": 0.2,
            "post_submit_order_retry_max": 3,
            "post_submit_fast_retry_interval_sec": 1.0,
            "post_submit_fak_immediate_retry_max": 3,
        },
        label_runtime_summary={
            "status": "truth_table_ready",
            "truth_table_rows": 12,
            "truth_source_counts": {"settlement_truth": 12},
            "oracle_table_rows": 16,
            "oracle_has_both_rows": 14,
        },
        truth_runtime_summary={
            "truth_runtime_foundation_status": "degraded",
            "truth_runtime_foundation_reason": "oracle_direct_rate_limited",
            "truth_runtime_foundation_issue_codes": ["oracle_direct_rate_limited"],
            "truth_runtime_foundation_run_started_at": "2026-03-20T00:00:00+00:00",
            "truth_runtime_foundation_last_completed_at": "2026-03-20T00:00:05+00:00",
            "truth_runtime_foundation_finished_at": "2026-03-20T00:00:05+00:00",
            "truth_runtime_foundation_completed_iterations": 1,
            "truth_runtime_recent_refresh_status": "fail_open",
            "truth_runtime_recent_refresh_interpretation": "recent_refresh_degraded_but_existing_oracle_table_is_still_serving_reads",
            "truth_runtime_direct_oracle_fail_open": True,
            "truth_runtime_truth_table_status": "ok",
            "truth_runtime_truth_table_freshness_max": "2026-03-20T00:00:00+00:00",
            "truth_runtime_truth_table_freshness_state": "fresh",
            "truth_runtime_truth_table_recent_refresh_status": "fresh",
            "truth_runtime_oracle_prices_table_status": "ok",
            "truth_runtime_oracle_prices_table_freshness_max": "2026-03-20T00:00:00+00:00",
            "truth_runtime_oracle_prices_table_freshness_state": "fresh",
            "truth_runtime_oracle_prices_table_recent_refresh_status": "fresh",
            "truth_runtime_direct_oracle_source_status": "ok",
            "truth_runtime_direct_oracle_source_freshness_max": "2026-03-20T00:00:00+00:00",
            "truth_runtime_direct_oracle_source_freshness_state": "stale",
            "truth_runtime_direct_oracle_source_recent_refresh_status": "stale",
            "truth_runtime_settlement_truth_source_status": "ok",
            "truth_runtime_streams_source_status": "ok",
            "truth_runtime_datafeeds_source_status": "missing",
        },
        regime_summary={
            "liquidity_proxy_mode": "spot_kline_mirror",
            "liquidity_available_rows": 3,
            "liquidity_missing_rows": 0,
            "liquidity_degraded_rows": 1,
            "regime_state_counts": {"NORMAL": 2, "DEFENSE": 1},
            "regime_pressure_counts": {"neutral": 2, "down": 1},
        },
        scored=scored,
        trades=trades,
        rejects=rejects,
    )
    reject_summary = build_reject_summary_frame(rejects)
    policy_breakdown = build_policy_breakdown_frame(scored)
    market_summary = pd.DataFrame([{"market_id": "m-1", "trades": 2, "wins": 1, "pnl_sum": 0.0}])
    report = render_backtest_report(
        summary,
        reject_summary=reject_summary,
        policy_breakdown=policy_breakdown,
        market_summary=market_summary,
    )
    assert summary["reject_reason_counts"] == build_reject_reason_counts(rejects)
    assert summary["decision_source_counts"] == {"primary": 3, "secondary": 1}
    assert summary["decision_quote_raw_depth_rows"] == 3
    assert summary["decision_quote_repriced_rows"] == 2
    assert summary["pre_submit_orderbook_retry_rows"] == 1
    assert summary["pre_submit_orderbook_retry_reason_counts"] == {"orderbook_limit_reject": 1}
    assert summary["retry_contract_post_submit_fak_immediate_retry_max"] == 3
    assert summary["label_runtime_status"] == "truth_table_ready"
    assert summary["truth_runtime_status"] == "fail_open"
    assert summary["truth_runtime_reason"] == "oracle_direct_rate_limited"
    assert summary["truth_runtime_truth_status"] == "fresh"
    assert summary["truth_runtime_oracle_status"] == "fail_open"
    assert summary["truth_runtime_window_refresh_status"] == "fail_open"
    assert summary["truth_runtime_foundation_status"] == "degraded"
    assert summary["liquidity_proxy_mode"] == "spot_kline_mirror"
    assert summary["regime_state_counts"] == {"NORMAL": 2, "DEFENSE": 1}
    assert "Backtest Summary" in report
    assert "Decision Surface" in report
    assert "Retry Contract" in report
    assert "Truth Runtime" in report
    assert "decision_quote_limit_reject_rows" in report
    assert "pre_submit_orderbook_retry_rows" in report
    assert "Regime / Liquidity" in report
    assert "label_runtime_truth_table_rows" in report
    assert "truth_runtime_status" in report
    assert "truth_runtime_window_refresh_status" in report
    assert "truth_runtime_foundation_status" in report
    assert "truth_runtime_recent_refresh_status" in report
    assert "truth_runtime_foundation_last_completed_at" in report
    assert "truth_runtime_oracle_prices_table_freshness_state" in report
    assert "spot_kline_mirror" in report
    assert "policy_low_confidence" in report
    assert "secondary" in report


def test_backtest_summary_surfaces_truth_source_diagnostics() -> None:
    scored = pd.DataFrame(
        [
            {
                "decision_source": "primary",
                "label_source": "streams",
                "settlement_source": "chainlink_streams",
                "price_to_beat": 120.0,
                "final_price": 121.0,
            },
            {
                "decision_source": "primary",
                "label_source": "datafeeds",
                "settlement_source": "chainlink_datafeeds",
                "price_to_beat": 122.0,
                "final_price": 121.5,
            },
            {
                "decision_source": "secondary",
                "label_source": "settlement_truth",
                "settlement_source": "settlement_truth",
                "price_to_beat": pd.NA,
                "final_price": pd.NA,
            },
        ]
    )

    summary = build_backtest_summary(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        spec_name="baseline_truth",
        target="direction",
        bundle_dir="/tmp/bundle",
        feature_set="deep_otm_v1",
        label_set="truth",
        available_offsets=[7, 8],
        scored=scored,
        trades=pd.DataFrame(),
        rejects=pd.DataFrame(),
    )
    report = render_backtest_report(summary)

    assert summary["label_source_counts"] == {
        "datafeeds": 1,
        "settlement_truth": 1,
        "streams": 1,
    }
    assert summary["settlement_source_counts"] == {
        "chainlink_datafeeds": 1,
        "chainlink_streams": 1,
        "settlement_truth": 1,
    }
    assert summary["price_to_beat_rows"] == 2
    assert summary["final_price_rows"] == 2
    assert "Source Of Truth" in report
    assert "chainlink_streams" in report
    assert "chainlink_datafeeds" in report


def test_render_backtest_report_falls_back_without_tabulate(monkeypatch) -> None:
    summary = build_backtest_summary(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        spec_name="baseline_truth",
        target="direction",
        bundle_dir="/tmp/bundle",
        feature_set="deep_otm_v1",
        label_set="truth",
        available_offsets=[7],
        trades=pd.DataFrame(),
        rejects=pd.DataFrame([{"decision_source": "primary", "reason": "policy_low_confidence"}]),
    )
    reject_summary = build_reject_summary_frame(pd.DataFrame([{"decision_source": "primary", "reason": "policy_low_confidence"}]))

    def _raise_import_error(self, *args, **kwargs):
        raise ImportError("Missing optional dependency 'tabulate'")

    monkeypatch.setattr(pd.DataFrame, "to_markdown", _raise_import_error)

    report = render_backtest_report(
        summary,
        reject_summary=reject_summary,
        policy_breakdown=pd.DataFrame([{"decision_source": "primary", "policy_action": "skip", "policy_reason": "policy_low_confidence", "count": 1}]),
        market_summary=pd.DataFrame([{"market_id": "m-1", "trades": 0, "wins": 0, "pnl_sum": 0.0}]),
    )

    assert "# Backtest Summary" in report
    assert "| decision_source | reason | count |" in report
    assert "| market_id | trades | wins | pnl_sum |" in report


def test_backtest_summary_normalizes_legacy_truth_runtime_keys() -> None:
    summary = build_backtest_summary(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        spec_name="baseline_truth",
        target="direction",
        bundle_dir="/tmp/bundle",
        feature_set="deep_otm_v1",
        label_set="truth",
        available_offsets=[7],
        truth_runtime_summary={
            "truth_runtime_foundation_status": "ok",
            "truth_runtime_truth_table_status": "ok",
            "truth_runtime_oracle_prices_table_status": "ok",
        },
        trades=pd.DataFrame(),
        rejects=pd.DataFrame(),
    )

    assert summary["truth_runtime_status"] == "fresh"
    assert summary["truth_runtime_truth_status"] == "fresh"
    assert summary["truth_runtime_oracle_status"] == "fresh"
    assert summary["truth_runtime_window_refresh_status"] == "fresh"


def test_backtest_summary_tracks_depth_usage_and_fallbacks() -> None:
    trades = pd.DataFrame(
        [
            {
                "decision_source": "primary",
                "fill_model": "canonical_depth",
                "depth_status": "ok",
                "depth_reason": "",
                "depth_fill_ratio": 1.0,
                "depth_candidate_count": 1,
                "depth_candidate_progress_count": 1,
                "depth_chain_mode": "single_snapshot",
                "depth_queue_turnover_count": 0,
                "win": True,
                "stake": 1.0,
                "pnl": 0.5,
            },
            {
                "decision_source": "primary",
                "fill_model": "canonical_quote",
                "depth_status": "blocked",
                "depth_reason": "depth_fill_ratio_below_threshold",
                "depth_fill_ratio": 0.4,
                "depth_candidate_count": 2,
                "depth_candidate_progress_count": 1,
                "depth_chain_mode": "single_snapshot",
                "depth_queue_turnover_count": 0,
                "win": False,
                "stake": 1.0,
                "pnl": -0.5,
            },
            {
                "decision_source": "secondary",
                "fill_model": "canonical_depth_quote",
                "depth_status": "partial",
                "depth_reason": "queue_path_stalled",
                "depth_fill_ratio": 0.8,
                "depth_candidate_count": 3,
                "depth_candidate_progress_count": 2,
                "depth_chain_mode": "queue_growth",
                "depth_queue_turnover_count": 1,
                "win": True,
                "stake": 1.0,
                "pnl": 0.3,
            },
            {
                "decision_source": "primary",
                "fill_model": "canonical_depth",
                "depth_status": "ok",
                "depth_reason": "",
                "depth_fill_ratio": 1.0,
                "depth_candidate_count": 2,
                "depth_candidate_progress_count": 2,
                "depth_chain_mode": "time_turnover",
                "depth_queue_turnover_count": 0,
                "depth_time_turnover_count": 1,
                "win": True,
                "stake": 0.8,
                "pnl": 0.2,
            },
            {
                "decision_source": "primary",
                "fill_model": "canonical_depth",
                "depth_status": "ok",
                "depth_reason": "",
                "depth_fill_ratio": 1.0,
                "depth_candidate_count": 2,
                "depth_candidate_progress_count": 2,
                "depth_chain_mode": "refresh_retry",
                "depth_queue_turnover_count": 0,
                "depth_time_turnover_count": 0,
                "depth_retry_refresh_count": 1,
                "depth_retry_trigger_reason": "depth_fill_unavailable",
                "depth_retry_stage": "pre_submit_orderbook_recheck",
                "depth_retry_exit_reason": "filled_target",
                "depth_retry_budget_source": "orderbook_fast_retry_max",
                "depth_retry_snapshot_unchanged_count": 0,
                "win": True,
                "stake": 1.0,
                "pnl": 0.1,
            },
        ]
    )

    summary = build_backtest_summary(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        spec_name="baseline_truth",
        target="direction",
        bundle_dir="/tmp/bundle",
        feature_set="deep_otm_v1",
        label_set="truth",
        available_offsets=[7, 8],
        trades=trades,
        rejects=pd.DataFrame(),
    )
    report = render_backtest_report(summary)

    assert summary["depth_fill_model_counts"] == {
        "canonical_depth": 3,
        "canonical_depth_quote": 1,
        "canonical_quote": 1,
    }
    assert summary["depth_canonical_depth_rows"] == 3
    assert summary["depth_quote_fallback_rows"] == 1
    assert summary["depth_quote_completion_rows"] == 1
    assert summary["depth_partial_fill_rows"] == 1
    assert summary["depth_queue_growth_rows"] == 1
    assert summary["depth_price_path_rows"] == 0
    assert summary["depth_queue_turnover_rows"] == 1
    assert summary["depth_time_turnover_rows"] == 1
    assert summary["depth_retry_refresh_rows"] == 1
    assert summary["depth_retry_trigger_reason_counts"] == {"depth_fill_unavailable": 1}
    assert summary["depth_retry_stage_counts"] == {"pre_submit_orderbook_recheck": 1}
    assert summary["depth_retry_exit_reason_counts"] == {"filled_target": 1}
    assert summary["depth_retry_snapshot_unchanged_rows"] == 0
    assert summary["depth_multi_snapshot_rows"] == 4
    assert summary["depth_multi_snapshot_progress_rows"] == 3
    assert summary["depth_chain_mode_counts"] == {
        "refresh_retry": 1,
        "queue_growth": 1,
        "time_turnover": 1,
        "single_snapshot": 2,
    }
    assert summary["depth_stop_reason_counts"] == {
        "depth_fill_ratio_below_threshold": 1,
        "filled_target": 3,
        "queue_path_stalled": 1,
    }
    assert "Depth Usage" in report
    assert "canonical_depth" in report
    assert "canonical_depth_quote" in report
    assert "depth_queue_turnover_rows" in report
    assert "depth_time_turnover_rows" in report
    assert "depth_retry_refresh_rows" in report
    assert "depth_retry_trigger_reason_counts" in report
    assert "depth_retry_stage_counts" in report
    assert "depth_retry_exit_reason_counts" in report
    assert "depth_chain_mode_counts" in report
    assert "depth_fill_ratio_below_threshold" in report
    assert "queue_path_stalled" in report
