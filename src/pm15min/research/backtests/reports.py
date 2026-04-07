from __future__ import annotations

import json
from collections.abc import Sequence

import pandas as pd

from pm15min.research.labels.sources import summarize_label_sources
from pm15min.research.labels.runtime_visibility import summarize_truth_runtime_visibility


def build_reject_reason_counts(rejects: pd.DataFrame) -> dict[str, int]:
    if rejects.empty:
        return {}
    reason_col = "reason" if "reason" in rejects.columns else "policy_reason"
    counts = rejects[reason_col].astype(str).value_counts().sort_index()
    return {str(index): int(value) for index, value in counts.items()}


def build_reject_summary_frame(rejects: pd.DataFrame) -> pd.DataFrame:
    if rejects.empty:
        return pd.DataFrame(columns=["decision_source", "reason", "count"])
    source_col = "decision_source" if "decision_source" in rejects.columns else "model_source"
    reason_col = "reason" if "reason" in rejects.columns else "policy_reason"
    frame = (
        rejects.assign(
            decision_source=rejects.get(source_col, "primary").astype(str),
            reason=rejects[reason_col].astype(str),
        )
        .groupby(["decision_source", "reason"], dropna=False)
        .size()
        .rename("count")
        .reset_index()
        .sort_values(["count", "decision_source", "reason"], ascending=[False, True, True])
        .reset_index(drop=True)
    )
    return frame


def build_policy_breakdown_frame(scored: pd.DataFrame) -> pd.DataFrame:
    if scored.empty:
        return pd.DataFrame(columns=["decision_source", "policy_action", "policy_reason", "count"])
    source_col = "decision_source" if "decision_source" in scored.columns else "model_source"
    frame = (
        scored.assign(
            decision_source=scored.get(source_col, "primary").astype(str),
            policy_action=scored.get("policy_action", "unknown").astype(str),
            policy_reason=scored.get("policy_reason", scored.get("reject_reason", "")).astype(str),
        )
        .groupby(["decision_source", "policy_action", "policy_reason"], dropna=False)
        .size()
        .rename("count")
        .reset_index()
        .sort_values(["count", "decision_source", "policy_action", "policy_reason"], ascending=[False, True, True, True])
        .reset_index(drop=True)
    )
    return frame


def build_backtest_summary(
    *,
    market: str,
    cycle: str,
    profile: str,
    spec_name: str,
    target: str,
    bundle_dir: str,
    feature_set: str,
    label_set: str,
    available_offsets: list[int],
    replay_summary: dict[str, object] | None = None,
    depth_replay_summary: dict[str, object] | None = None,
    decision_quote_summary: dict[str, object] | None = None,
    retry_contract_summary: dict[str, object] | None = None,
    label_runtime_summary: dict[str, object] | None = None,
    truth_runtime_summary: dict[str, object] | None = None,
    quote_summary: dict[str, object] | None = None,
    guard_summary: dict[str, object] | None = None,
    regime_summary: dict[str, object] | None = None,
    scored: pd.DataFrame | None = None,
    trades: pd.DataFrame,
    rejects: pd.DataFrame,
    variant_label: str = "default",
    secondary_bundle_label: str | None = None,
    stake_usd: float | None = None,
    max_notional_usd: float | None = None,
    fallback_reasons: Sequence[str] = (),
    parity: dict[str, object] | None = None,
    orderbook_preflight_summary: dict[str, object] | None = None,
) -> dict[str, object]:
    reject_counts = build_reject_reason_counts(rejects)
    stake_sum = float(_numeric_series(trades, "stake").fillna(0.0).sum()) if not trades.empty else 0.0
    pnl_sum = float(_numeric_series(trades, "pnl").fillna(0.0).sum()) if not trades.empty else 0.0
    decision_source_counts = _build_decision_source_counts(trades=trades, rejects=rejects, scored=scored)
    depth_summary = build_depth_usage_summary(trades)
    pre_submit_retry_summary = build_pre_submit_retry_summary(scored)
    truth_summary = build_truth_source_summary(scored)
    truth_runtime_visibility = summarize_truth_runtime_visibility(truth_runtime_summary)
    return {
        "market": market,
        "cycle": cycle,
        "profile": profile,
        "spec_name": spec_name,
        "target": target,
        "bundle_dir": bundle_dir,
        "feature_set": feature_set,
        "label_set": label_set,
        "available_offsets": available_offsets,
        **{
            f"orderbook_preflight_{key}": value
            for key, value in dict(orderbook_preflight_summary or {}).items()
        },
        **dict(replay_summary or {}),
        **{
            f"raw_depth_{key}": value
            for key, value in dict(depth_replay_summary or {}).items()
        },
        **{
            f"decision_quote_{key}": value
            for key, value in dict(decision_quote_summary or {}).items()
        },
        **{
            f"retry_contract_{key}": value
            for key, value in dict(retry_contract_summary or {}).items()
        },
        **{
            f"label_runtime_{key}": value
            for key, value in dict(label_runtime_summary or {}).items()
        },
        **dict(truth_runtime_summary or {}),
        **truth_runtime_visibility,
        **dict(quote_summary or {}),
        **{
            f"guard_{key}": value
            for key, value in dict(guard_summary or {}).items()
        },
        **dict(regime_summary or {}),
        "variant_label": str(variant_label or "default"),
        "secondary_bundle_label": secondary_bundle_label,
        "stake_usd": None if stake_usd is None else float(stake_usd),
        "max_notional_usd": None if max_notional_usd is None else float(max_notional_usd),
        "fallback_reasons": [str(reason) for reason in fallback_reasons if str(reason)],
        "parity": dict(parity or {}),
        "trades": int(len(trades)),
        "rejects": int(len(rejects)),
        "wins": int(_bool_series(trades, "win").sum()) if not trades.empty else 0,
        "losses": int(len(trades) - int(_bool_series(trades, "win").sum())) if not trades.empty else 0,
        "pnl_sum": pnl_sum,
        "stake_sum": stake_sum,
        "avg_roi_pct": float(_numeric_series(trades, "roi_pct").fillna(0.0).mean()) if not trades.empty else 0.0,
        "roi_pct": float((pnl_sum / stake_sum) * 100.0) if stake_sum > 0.0 else 0.0,
        "reject_reason_counts": reject_counts,
        "decision_source_counts": decision_source_counts,
        **pre_submit_retry_summary,
        **truth_summary,
        **depth_summary,
    }


def render_backtest_report(
    summary: dict[str, object],
    *,
    reject_summary: pd.DataFrame | None = None,
    policy_breakdown: pd.DataFrame | None = None,
    market_summary: pd.DataFrame | None = None,
) -> str:
    lines = [
        "# Backtest Summary",
        "",
        f"- market: `{summary.get('market')}`",
        f"- cycle: `{summary.get('cycle')}`",
        f"- profile: `{summary.get('profile')}`",
        f"- spec_name: `{summary.get('spec_name')}`",
        f"- target: `{summary.get('target')}`",
        f"- variant_label: `{summary.get('variant_label')}`",
        f"- stake_usd: `{summary.get('stake_usd')}`",
        f"- max_notional_usd: `{summary.get('max_notional_usd')}`",
        f"- secondary_bundle_label: `{summary.get('secondary_bundle_label')}`",
        f"- fallback_reasons: `{summary.get('fallback_reasons', [])}`",
        f"- feature_set: `{summary.get('feature_set')}`",
        f"- label_set: `{summary.get('label_set')}`",
        f"- available_offsets: `{summary.get('available_offsets')}`",
        f"- trades: `{summary.get('trades')}`",
        f"- rejects: `{summary.get('rejects')}`",
        f"- pnl_sum: `{summary.get('pnl_sum')}`",
        f"- stake_sum: `{summary.get('stake_sum')}`",
        f"- roi_pct: `{summary.get('roi_pct')}`",
        f"- decision_source_counts: `{summary.get('decision_source_counts', {})}`",
        "",
        "## Orderbook Preflight",
        "",
        f"- orderbook_preflight_requested_date_count: `{summary.get('orderbook_preflight_requested_date_count', 0)}`",
        f"- orderbook_preflight_ready_date_count: `{summary.get('orderbook_preflight_ready_date_count', 0)}`",
        f"- orderbook_preflight_rebuilt_date_count: `{summary.get('orderbook_preflight_rebuilt_date_count', 0)}`",
        f"- orderbook_preflight_refreshed_date_count: `{summary.get('orderbook_preflight_refreshed_date_count', 0)}`",
        f"- orderbook_preflight_missing_depth_date_count: `{summary.get('orderbook_preflight_missing_depth_date_count', 0)}`",
        f"- orderbook_preflight_empty_depth_source_date_count: `{summary.get('orderbook_preflight_empty_depth_source_date_count', 0)}`",
        f"- orderbook_preflight_partial_market_coverage_date_count: `{summary.get('orderbook_preflight_partial_market_coverage_date_count', 0)}`",
        f"- orderbook_preflight_index_missing_date_count: `{summary.get('orderbook_preflight_index_missing_date_count', 0)}`",
        f"- orderbook_preflight_status_counts: `{summary.get('orderbook_preflight_status_counts', {})}`",
        f"- orderbook_preflight_missing_depth_dates: `{summary.get('orderbook_preflight_missing_depth_dates', [])}`",
        f"- orderbook_preflight_empty_depth_source_dates: `{summary.get('orderbook_preflight_empty_depth_source_dates', [])}`",
        f"- orderbook_preflight_partial_market_coverage_dates: `{summary.get('orderbook_preflight_partial_market_coverage_dates', [])}`",
        f"- orderbook_preflight_index_missing_dates: `{summary.get('orderbook_preflight_index_missing_dates', [])}`",
        f"- orderbook_preflight_used_live_surface_dates: `{summary.get('orderbook_preflight_used_live_surface_dates', [])}`",
        "",
        "## Replay Coverage",
        "",
        f"- merged_rows: `{summary.get('merged_rows')}`",
        f"- score_covered_rows: `{summary.get('score_covered_rows')}`",
        f"- score_missing_rows: `{summary.get('score_missing_rows')}`",
        f"- unresolved_label_rows: `{summary.get('unresolved_label_rows')}`",
        f"- bundle_offset_missing_rows: `{summary.get('bundle_offset_missing_rows')}`",
        f"- ready_rows: `{summary.get('ready_rows')}`",
        f"- raw_depth_snapshot_rows: `{summary.get('raw_depth_snapshot_rows', 0)}`",
        f"- raw_depth_complete_snapshot_rows: `{summary.get('raw_depth_complete_snapshot_rows', 0)}`",
        f"- raw_depth_partial_snapshot_rows: `{summary.get('raw_depth_partial_snapshot_rows', 0)}`",
        f"- raw_depth_replay_rows_with_snapshots: `{summary.get('raw_depth_replay_rows_with_snapshots', 0)}`",
        f"- raw_depth_replay_rows_without_snapshots: `{summary.get('raw_depth_replay_rows_without_snapshots', 0)}`",
        f"- quote_ready_rows: `{summary.get('quote_ready_rows')}`",
        f"- quote_missing_rows: `{summary.get('quote_missing_rows')}`",
        f"- guard_blocked_rows: `{summary.get('guard_blocked_rows')}`",
        "",
        "## Decision Surface",
        "",
        f"- decision_quote_raw_depth_rows: `{summary.get('decision_quote_raw_depth_rows', 0)}`",
        f"- decision_quote_repriced_rows: `{summary.get('decision_quote_repriced_rows', 0)}`",
        f"- decision_quote_limit_reject_rows: `{summary.get('decision_quote_limit_reject_rows', 0)}`",
        f"- decision_quote_orderbook_missing_rows: `{summary.get('decision_quote_orderbook_missing_rows', 0)}`",
        f"- pre_submit_orderbook_retry_rows: `{summary.get('pre_submit_orderbook_retry_rows', 0)}`",
        f"- pre_submit_orderbook_retry_reason_counts: `{summary.get('pre_submit_orderbook_retry_reason_counts', {})}`",
        "",
        "## Retry Contract",
        "",
        f"- retry_contract_pre_submit_depth_retry_max: `{summary.get('retry_contract_pre_submit_depth_retry_max')}`",
        f"- retry_contract_pre_submit_depth_retry_interval_sec: `{summary.get('retry_contract_pre_submit_depth_retry_interval_sec')}`",
        f"- retry_contract_pre_submit_depth_retry_state_key: `{summary.get('retry_contract_pre_submit_depth_retry_state_key')}`",
        f"- retry_contract_post_submit_order_retry_max: `{summary.get('retry_contract_post_submit_order_retry_max')}`",
        f"- retry_contract_post_submit_fast_retry_interval_sec: `{summary.get('retry_contract_post_submit_fast_retry_interval_sec')}`",
        f"- retry_contract_post_submit_order_retry_state_keys: `{summary.get('retry_contract_post_submit_order_retry_state_keys', [])}`",
        f"- retry_contract_post_submit_fak_immediate_retry_max: `{summary.get('retry_contract_post_submit_fak_immediate_retry_max')}`",
        f"- retry_contract_post_submit_fak_response_driven: `{summary.get('retry_contract_post_submit_fak_response_driven', False)}`",
        "",
        "## Source Of Truth",
        "",
        f"- label_sources: `{summary.get('label_sources', [])}`",
        f"- label_source_counts: `{summary.get('label_source_counts', {})}`",
        f"- settlement_source_counts: `{summary.get('settlement_source_counts', {})}`",
        f"- price_to_beat_rows: `{summary.get('price_to_beat_rows', 0)}`",
        f"- final_price_rows: `{summary.get('final_price_rows', 0)}`",
        f"- label_runtime_status: `{summary.get('label_runtime_status')}`",
        f"- label_runtime_truth_table_rows: `{summary.get('label_runtime_truth_table_rows', 0)}`",
        f"- label_runtime_truth_source_counts: `{summary.get('label_runtime_truth_source_counts', {})}`",
        f"- label_runtime_oracle_table_rows: `{summary.get('label_runtime_oracle_table_rows', 0)}`",
        f"- label_runtime_oracle_has_both_rows: `{summary.get('label_runtime_oracle_has_both_rows', 0)}`",
        "",
        "## Truth Runtime",
        "",
        f"- truth_runtime_status: `{summary.get('truth_runtime_status', '')}`",
        f"- truth_runtime_reason: `{summary.get('truth_runtime_reason', '')}`",
        f"- truth_runtime_truth_status: `{summary.get('truth_runtime_truth_status', '')}`",
        f"- truth_runtime_oracle_status: `{summary.get('truth_runtime_oracle_status', '')}`",
        f"- truth_runtime_window_refresh_status: `{summary.get('truth_runtime_window_refresh_status', '')}`",
        f"- truth_runtime_window_refresh_reason: `{summary.get('truth_runtime_window_refresh_reason', '')}`",
        f"- truth_runtime_last_completed_at: `{summary.get('truth_runtime_last_completed_at', '')}`",
        f"- truth_runtime_truth_freshness_max: `{summary.get('truth_runtime_truth_freshness_max', '')}`",
        f"- truth_runtime_oracle_freshness_max: `{summary.get('truth_runtime_oracle_freshness_max', '')}`",
        f"- truth_runtime_foundation_status: `{summary.get('truth_runtime_foundation_status', '')}`",
        f"- truth_runtime_foundation_reason: `{summary.get('truth_runtime_foundation_reason', '')}`",
        f"- truth_runtime_foundation_issue_codes: `{summary.get('truth_runtime_foundation_issue_codes', [])}`",
        f"- truth_runtime_foundation_run_started_at: `{summary.get('truth_runtime_foundation_run_started_at', '')}`",
        f"- truth_runtime_foundation_last_completed_at: `{summary.get('truth_runtime_foundation_last_completed_at', '')}`",
        f"- truth_runtime_foundation_finished_at: `{summary.get('truth_runtime_foundation_finished_at', '')}`",
        f"- truth_runtime_foundation_completed_iterations: `{summary.get('truth_runtime_foundation_completed_iterations', 0)}`",
        f"- truth_runtime_recent_refresh_status: `{summary.get('truth_runtime_recent_refresh_status', '')}`",
        f"- truth_runtime_recent_refresh_interpretation: `{summary.get('truth_runtime_recent_refresh_interpretation', '')}`",
        f"- truth_runtime_direct_oracle_fail_open: `{summary.get('truth_runtime_direct_oracle_fail_open', False)}`",
        f"- truth_runtime_truth_table_status: `{summary.get('truth_runtime_truth_table_status', '')}`",
        f"- truth_runtime_truth_table_freshness_max: `{summary.get('truth_runtime_truth_table_freshness_max', '')}`",
        f"- truth_runtime_truth_table_freshness_state: `{summary.get('truth_runtime_truth_table_freshness_state', '')}`",
        f"- truth_runtime_truth_table_recent_refresh_status: `{summary.get('truth_runtime_truth_table_recent_refresh_status', '')}`",
        f"- truth_runtime_oracle_prices_table_status: `{summary.get('truth_runtime_oracle_prices_table_status', '')}`",
        f"- truth_runtime_oracle_prices_table_freshness_max: `{summary.get('truth_runtime_oracle_prices_table_freshness_max', '')}`",
        f"- truth_runtime_oracle_prices_table_freshness_state: `{summary.get('truth_runtime_oracle_prices_table_freshness_state', '')}`",
        f"- truth_runtime_oracle_prices_table_recent_refresh_status: `{summary.get('truth_runtime_oracle_prices_table_recent_refresh_status', '')}`",
        f"- truth_runtime_direct_oracle_source_status: `{summary.get('truth_runtime_direct_oracle_source_status', '')}`",
        f"- truth_runtime_direct_oracle_source_freshness_max: `{summary.get('truth_runtime_direct_oracle_source_freshness_max', '')}`",
        f"- truth_runtime_direct_oracle_source_freshness_state: `{summary.get('truth_runtime_direct_oracle_source_freshness_state', '')}`",
        f"- truth_runtime_direct_oracle_source_recent_refresh_status: `{summary.get('truth_runtime_direct_oracle_source_recent_refresh_status', '')}`",
        f"- truth_runtime_settlement_truth_source_status: `{summary.get('truth_runtime_settlement_truth_source_status', '')}`",
        f"- truth_runtime_streams_source_status: `{summary.get('truth_runtime_streams_source_status', '')}`",
        f"- truth_runtime_datafeeds_source_status: `{summary.get('truth_runtime_datafeeds_source_status', '')}`",
        "",
        "## Regime / Liquidity",
        "",
        f"- liquidity_proxy_mode: `{summary.get('liquidity_proxy_mode', 'off')}`",
        f"- liquidity_available_rows: `{summary.get('liquidity_available_rows', 0)}`",
        f"- liquidity_missing_rows: `{summary.get('liquidity_missing_rows', 0)}`",
        f"- liquidity_degraded_rows: `{summary.get('liquidity_degraded_rows', 0)}`",
        f"- regime_state_counts: `{summary.get('regime_state_counts', {})}`",
        f"- regime_pressure_counts: `{summary.get('regime_pressure_counts', {})}`",
        "",
        "## Depth Usage",
        "",
        f"- depth_fill_model_counts: `{summary.get('depth_fill_model_counts', {})}`",
        f"- depth_canonical_depth_rows: `{summary.get('depth_canonical_depth_rows', 0)}`",
        f"- depth_quote_fallback_rows: `{summary.get('depth_quote_fallback_rows', 0)}`",
        f"- depth_quote_completion_rows: `{summary.get('depth_quote_completion_rows', 0)}`",
        f"- depth_partial_fill_rows: `{summary.get('depth_partial_fill_rows', 0)}`",
        f"- depth_queue_growth_rows: `{summary.get('depth_queue_growth_rows', 0)}`",
        f"- depth_price_path_rows: `{summary.get('depth_price_path_rows', 0)}`",
        f"- depth_queue_turnover_rows: `{summary.get('depth_queue_turnover_rows', 0)}`",
        f"- depth_time_turnover_rows: `{summary.get('depth_time_turnover_rows', 0)}`",
        f"- depth_retry_refresh_rows: `{summary.get('depth_retry_refresh_rows', 0)}`",
        f"- depth_retry_budget_exhausted_rows: `{summary.get('depth_retry_budget_exhausted_rows', 0)}`",
        f"- depth_retry_trigger_reason_counts: `{summary.get('depth_retry_trigger_reason_counts', {})}`",
        f"- depth_retry_stage_counts: `{summary.get('depth_retry_stage_counts', {})}`",
        f"- depth_retry_exit_reason_counts: `{summary.get('depth_retry_exit_reason_counts', {})}`",
        f"- depth_retry_snapshot_unchanged_rows: `{summary.get('depth_retry_snapshot_unchanged_rows', 0)}`",
        f"- depth_multi_snapshot_rows: `{summary.get('depth_multi_snapshot_rows', 0)}`",
        f"- depth_multi_snapshot_progress_rows: `{summary.get('depth_multi_snapshot_progress_rows', 0)}`",
        f"- depth_chain_mode_counts: `{summary.get('depth_chain_mode_counts', {})}`",
        f"- depth_stop_reason_counts: `{summary.get('depth_stop_reason_counts', {})}`",
        "",
        "## Reject Reasons",
        "",
    ]
    if reject_summary is not None and not reject_summary.empty:
        lines.append(_render_markdown_table(reject_summary))
    else:
        reject_counts = summary.get("reject_reason_counts", {})
        if isinstance(reject_counts, dict) and reject_counts:
            frame = pd.DataFrame(
                [{"reason": str(reason), "count": int(count)} for reason, count in reject_counts.items()]
            ).sort_values(["count", "reason"], ascending=[False, True])
            lines.append(_render_markdown_table(frame))
        else:
            lines.append("No rejects.")
    lines.extend(["", "## Policy Breakdown", ""])
    if policy_breakdown is not None and not policy_breakdown.empty:
        lines.append(_render_markdown_table(policy_breakdown))
    else:
        lines.append("No policy breakdown available.")
    lines.extend(["", "## Markets", ""])
    if market_summary is not None and not market_summary.empty:
        lines.append(_render_markdown_table(market_summary))
    else:
        lines.append("No market summary available.")
    lines.append("")
    return "\n".join(lines)


def build_stake_sweep_frame(*, summary: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "profile": summary.get("profile"),
                "spec_name": summary.get("spec_name"),
                "run_label": summary.get("run_label"),
                "target": summary.get("target"),
                "variant_label": summary.get("variant_label"),
                "stake_usd": summary.get("stake_usd"),
                "max_notional_usd": summary.get("max_notional_usd"),
                "trades": summary.get("trades"),
                "wins": summary.get("wins"),
                "losses": summary.get("losses"),
                "pnl_sum": summary.get("pnl_sum"),
                "stake_sum": summary.get("stake_sum"),
                "avg_roi_pct": summary.get("avg_roi_pct"),
                "roi_pct": summary.get("roi_pct"),
                "secondary_bundle_label": summary.get("secondary_bundle_label"),
                "fallback_reasons_json": json.dumps(summary.get("fallback_reasons", []), ensure_ascii=False, sort_keys=True),
                "parity_json": json.dumps(summary.get("parity", {}), ensure_ascii=False, sort_keys=True),
                "is_actual_run": True,
            }
        ]
    )


def build_offset_summary_frame(
    *,
    decisions: pd.DataFrame,
    trades: pd.DataFrame,
    rejects: pd.DataFrame,
    available_offsets: Sequence[int],
) -> pd.DataFrame:
    all_offsets = sorted({int(value) for value in available_offsets})
    if not all_offsets:
        all_offsets = sorted(
            {
                *[int(value) for value in pd.to_numeric(decisions.get("offset", pd.Series(dtype=float)), errors="coerce").dropna().astype(int).tolist()],
                *[int(value) for value in pd.to_numeric(trades.get("offset", pd.Series(dtype=float)), errors="coerce").dropna().astype(int).tolist()],
                *[int(value) for value in pd.to_numeric(rejects.get("offset", pd.Series(dtype=float)), errors="coerce").dropna().astype(int).tolist()],
            }
        )
    rows: list[dict[str, object]] = []
    decision_offsets = (
        pd.to_numeric(decisions.get("offset", pd.Series(dtype=float)), errors="coerce")
        if not decisions.empty
        else pd.Series(dtype=float)
    )
    reject_offsets = (
        pd.to_numeric(rejects.get("offset", pd.Series(dtype=float)), errors="coerce")
        if not rejects.empty
        else pd.Series(dtype=float)
    )
    trade_offsets = (
        pd.to_numeric(trades.get("offset", pd.Series(dtype=float)), errors="coerce")
        if not trades.empty
        else pd.Series(dtype=float)
    )
    for offset in all_offsets:
        decision_mask = decision_offsets.eq(offset) if not decisions.empty else pd.Series(dtype=bool)
        reject_mask = reject_offsets.eq(offset) if not rejects.empty else pd.Series(dtype=bool)
        trade_mask = trade_offsets.eq(offset) if not trades.empty else pd.Series(dtype=bool)
        trade_slice = trades.loc[trade_mask].copy() if not trades.empty else pd.DataFrame()
        rows.append(
            {
                "offset": int(offset),
                "scored_rows": int(decision_mask.sum()) if not decisions.empty else 0,
                "trade_decisions": int(
                    decisions.loc[decision_mask].get("policy_action", pd.Series("", index=decisions.loc[decision_mask].index)).eq("trade").sum()
                )
                if not decisions.empty
                else 0,
                "reject_rows": int(reject_mask.sum()) if not rejects.empty else 0,
                "trades": int(len(trade_slice)),
                "wins": int(_bool_series(trade_slice, "win").sum()) if not trade_slice.empty else 0,
                "pnl_sum": float(_numeric_series(trade_slice, "pnl").fillna(0.0).sum()) if not trade_slice.empty else 0.0,
                "stake_sum": float(_numeric_series(trade_slice, "stake").fillna(0.0).sum()) if not trade_slice.empty else 0.0,
                "avg_roi_pct": float(_numeric_series(trade_slice, "roi_pct").fillna(0.0).mean()) if not trade_slice.empty else 0.0,
            }
        )
    if not rows:
        return pd.DataFrame(columns=["offset", "scored_rows", "trade_decisions", "reject_rows", "trades", "wins", "pnl_sum", "stake_sum", "avg_roi_pct"])
    return pd.DataFrame(rows).sort_values("offset").reset_index(drop=True)


def build_factor_pnl_frame(
    *,
    decisions: pd.DataFrame,
    trades: pd.DataFrame,
    factor_columns: Sequence[str],
) -> pd.DataFrame:
    if decisions.empty or trades.empty or not factor_columns:
        return pd.DataFrame(
            columns=[
                "feature",
                "trade_count",
                "pnl_sum",
                "avg_pnl",
                "avg_roi_pct",
                "mean_feature_value",
                "pnl_correlation",
                "abs_pnl_correlation",
                "positive_pnl_trades",
                "negative_pnl_trades",
            ]
        )
    join_keys = [
        column
        for column in ("decision_ts", "offset", "market_id", "condition_id", "decision_source")
        if column in decisions.columns and column in trades.columns
    ]
    if not join_keys:
        return pd.DataFrame(
            columns=[
                "feature",
                "trade_count",
                "pnl_sum",
                "avg_pnl",
                "avg_roi_pct",
                "mean_feature_value",
                "pnl_correlation",
                "abs_pnl_correlation",
                "positive_pnl_trades",
                "negative_pnl_trades",
            ]
        )
    selected_factors = [column for column in factor_columns if column in decisions.columns]
    if not selected_factors:
        return pd.DataFrame(
            columns=[
                "feature",
                "trade_count",
                "pnl_sum",
                "avg_pnl",
                "avg_roi_pct",
                "mean_feature_value",
                "pnl_correlation",
                "abs_pnl_correlation",
                "positive_pnl_trades",
                "negative_pnl_trades",
            ]
        )
    merged = decisions.loc[:, [*join_keys, *selected_factors]].merge(
        trades.loc[:, [*join_keys, "pnl", "roi_pct"]],
        on=join_keys,
        how="inner",
    )
    rows: list[dict[str, object]] = []
    pnl = _numeric_series(merged, "pnl").fillna(0.0)
    roi = _numeric_series(merged, "roi_pct").fillna(0.0)
    for factor in selected_factors:
        values = _numeric_series(merged, factor)
        mask = values.notna()
        if not mask.any():
            continue
        factor_pnl = pnl.loc[mask]
        factor_values = values.loc[mask]
        pnl_correlation = factor_values.corr(factor_pnl)
        rows.append(
            {
                "feature": str(factor),
                "trade_count": int(mask.sum()),
                "pnl_sum": float(factor_pnl.sum()),
                "avg_pnl": float(factor_pnl.mean()),
                "avg_roi_pct": float(roi.loc[mask].mean()),
                "mean_feature_value": float(factor_values.mean()),
                "pnl_correlation": 0.0 if pd.isna(pnl_correlation) else float(pnl_correlation),
                "abs_pnl_correlation": 0.0 if pd.isna(pnl_correlation) else float(abs(float(pnl_correlation))),
                "positive_pnl_trades": int(factor_pnl.gt(0.0).sum()),
                "negative_pnl_trades": int(factor_pnl.lt(0.0).sum()),
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "feature",
                "trade_count",
                "pnl_sum",
                "avg_pnl",
                "avg_roi_pct",
                "mean_feature_value",
                "pnl_correlation",
                "abs_pnl_correlation",
                "positive_pnl_trades",
                "negative_pnl_trades",
            ]
        )
    return pd.DataFrame(rows).sort_values(
        ["abs_pnl_correlation", "pnl_sum", "feature"],
        ascending=[False, False, True],
    ).reset_index(drop=True)


def _build_decision_source_counts(
    *,
    trades: pd.DataFrame,
    rejects: pd.DataFrame,
    scored: pd.DataFrame | None,
) -> dict[str, int]:
    sources: list[pd.Series] = []
    if not trades.empty:
        source_col = "decision_source" if "decision_source" in trades.columns else "model_source"
        if source_col in trades.columns:
            sources.append(trades[source_col].astype(str))
    if not rejects.empty:
        source_col = "decision_source" if "decision_source" in rejects.columns else "model_source"
        if source_col in rejects.columns:
            sources.append(rejects[source_col].astype(str))
    if not sources and scored is not None and not scored.empty:
        source_col = "decision_source" if "decision_source" in scored.columns else "model_source"
        if source_col in scored.columns:
            sources.append(scored[source_col].astype(str))
    if not sources:
        return {}
    combined = pd.concat(sources, ignore_index=True)
    counts = combined.value_counts().sort_index()
    return {str(index): int(value) for index, value in counts.items()}


def build_depth_usage_summary(trades: pd.DataFrame) -> dict[str, object]:
    if trades.empty:
        return {
            "depth_fill_model_counts": {},
            "depth_canonical_depth_rows": 0,
            "depth_quote_fallback_rows": 0,
            "depth_quote_completion_rows": 0,
            "depth_partial_fill_rows": 0,
            "depth_queue_growth_rows": 0,
            "depth_price_path_rows": 0,
            "depth_queue_turnover_rows": 0,
            "depth_time_turnover_rows": 0,
            "depth_retry_refresh_rows": 0,
            "depth_retry_budget_exhausted_rows": 0,
            "depth_retry_trigger_reason_counts": {},
            "depth_retry_stage_counts": {},
            "depth_retry_exit_reason_counts": {},
            "depth_retry_snapshot_unchanged_rows": 0,
            "depth_multi_snapshot_rows": 0,
            "depth_multi_snapshot_progress_rows": 0,
            "depth_chain_mode_counts": {},
            "depth_stop_reason_counts": {},
        }
    fill_models = trades.get("fill_model", pd.Series("", index=trades.index, dtype="string")).astype("string").fillna("")
    fill_model_counts = fill_models.value_counts().sort_index()
    depth_status = trades.get("depth_status", pd.Series("", index=trades.index, dtype="string")).astype("string").fillna("")
    depth_reason = trades.get("depth_reason", pd.Series("", index=trades.index, dtype="string")).astype("string").fillna("")
    depth_fill_ratio = _numeric_series(trades, "depth_fill_ratio")
    depth_candidate_count = _numeric_series(trades, "depth_candidate_count")
    depth_candidate_progress_count = _numeric_series(trades, "depth_candidate_progress_count")
    depth_chain_modes = trades.get("depth_chain_mode", pd.Series("", index=trades.index, dtype="string")).astype("string").fillna("")
    depth_queue_turnover_count = _numeric_series(trades, "depth_queue_turnover_count")
    depth_time_turnover_count = _numeric_series(trades, "depth_time_turnover_count")
    depth_retry_refresh_count = _numeric_series(trades, "depth_retry_refresh_count")
    depth_retry_budget_exhausted = _bool_series(trades, "depth_retry_budget_exhausted")
    depth_retry_trigger_reason = trades.get("depth_retry_trigger_reason", pd.Series("", index=trades.index, dtype="string")).astype("string").fillna("")
    depth_retry_stage = trades.get("depth_retry_stage", pd.Series("", index=trades.index, dtype="string")).astype("string").fillna("")
    depth_retry_exit_reason = trades.get("depth_retry_exit_reason", pd.Series("", index=trades.index, dtype="string")).astype("string").fillna("")
    depth_retry_snapshot_unchanged_count = _numeric_series(trades, "depth_retry_snapshot_unchanged_count")
    depth_executed_mask = fill_models.isin(["canonical_depth", "canonical_depth_quote"])
    canonical_depth_mask = fill_models.eq("canonical_depth")
    quote_fallback_mask = fill_models.eq("canonical_quote") & depth_status.isin(["blocked", "missing"])
    quote_completion_mask = fill_models.eq("canonical_depth_quote")
    partial_fill_mask = depth_status.eq("partial") | quote_completion_mask | (canonical_depth_mask & depth_fill_ratio.lt(1.0))
    queue_growth_mask = depth_chain_modes.isin(["queue_growth", "queue_price_path", "queue_time_turnover", "queue_price_time_turnover"])
    price_path_mask = depth_chain_modes.isin(["price_path", "queue_price_path", "price_time_turnover", "queue_price_time_turnover"])
    queue_turnover_mask = depth_queue_turnover_count.gt(0)
    time_turnover_mask = depth_time_turnover_count.gt(0)
    retry_refresh_mask = depth_retry_refresh_count.gt(0) | depth_chain_modes.eq("refresh_retry")
    multi_snapshot_mask = depth_candidate_count.gt(1)
    multi_snapshot_progress_mask = depth_candidate_progress_count.gt(1)

    stop_reasons = pd.Series("", index=trades.index, dtype="string")
    stop_reasons.loc[depth_reason.ne("")] = depth_reason.loc[depth_reason.ne("")]
    stop_reasons.loc[stop_reasons.eq("") & depth_executed_mask & ~partial_fill_mask] = "filled_target"
    stop_reasons.loc[stop_reasons.eq("") & partial_fill_mask] = "partial_fill"
    stop_reason_counts = stop_reasons.value_counts().sort_index()
    stop_reason_counts = stop_reason_counts[stop_reason_counts.index != ""]
    chain_mode_counts = depth_chain_modes.value_counts().sort_index()
    chain_mode_counts = chain_mode_counts[chain_mode_counts.index != ""]
    retry_trigger_counts = depth_retry_trigger_reason.value_counts().sort_index()
    retry_trigger_counts = retry_trigger_counts[retry_trigger_counts.index != ""]
    retry_stage_counts = depth_retry_stage.value_counts().sort_index()
    retry_stage_counts = retry_stage_counts[retry_stage_counts.index != ""]
    retry_exit_reason_counts = depth_retry_exit_reason.value_counts().sort_index()
    retry_exit_reason_counts = retry_exit_reason_counts[retry_exit_reason_counts.index != ""]

    return {
        "depth_fill_model_counts": {str(index): int(value) for index, value in fill_model_counts.items() if str(index)},
        "depth_canonical_depth_rows": int(canonical_depth_mask.sum()),
        "depth_quote_fallback_rows": int(quote_fallback_mask.sum()),
        "depth_quote_completion_rows": int(quote_completion_mask.sum()),
        "depth_partial_fill_rows": int(partial_fill_mask.sum()),
        "depth_queue_growth_rows": int(queue_growth_mask.sum()),
        "depth_price_path_rows": int(price_path_mask.sum()),
        "depth_queue_turnover_rows": int(queue_turnover_mask.sum()),
        "depth_time_turnover_rows": int(time_turnover_mask.sum()),
        "depth_retry_refresh_rows": int(retry_refresh_mask.sum()),
        "depth_retry_budget_exhausted_rows": int(depth_retry_budget_exhausted.sum()),
        "depth_retry_trigger_reason_counts": {str(index): int(value) for index, value in retry_trigger_counts.items()},
        "depth_retry_stage_counts": {str(index): int(value) for index, value in retry_stage_counts.items()},
        "depth_retry_exit_reason_counts": {str(index): int(value) for index, value in retry_exit_reason_counts.items()},
        "depth_retry_snapshot_unchanged_rows": int(depth_retry_snapshot_unchanged_count.gt(0).sum()),
        "depth_multi_snapshot_rows": int(multi_snapshot_mask.sum()),
        "depth_multi_snapshot_progress_rows": int(multi_snapshot_progress_mask.sum()),
        "depth_chain_mode_counts": {str(index): int(value) for index, value in chain_mode_counts.items()},
        "depth_stop_reason_counts": {str(index): int(value) for index, value in stop_reason_counts.items()},
    }


def build_pre_submit_retry_summary(scored: pd.DataFrame | None) -> dict[str, object]:
    if scored is None or scored.empty:
        return {
            "pre_submit_orderbook_retry_rows": 0,
            "pre_submit_orderbook_retry_reason_counts": {},
        }
    armed = _bool_series(scored, "pre_submit_orderbook_retry_armed")
    reasons = (
        scored.get("pre_submit_orderbook_retry_reason", pd.Series("", index=scored.index, dtype="string"))
        .astype("string")
        .fillna("")
    )
    reason_counts = reasons.loc[armed].value_counts().sort_index()
    reason_counts = reason_counts[reason_counts.index != ""]
    return {
        "pre_submit_orderbook_retry_rows": int(armed.sum()),
        "pre_submit_orderbook_retry_reason_counts": {str(index): int(value) for index, value in reason_counts.items()},
    }


def build_truth_source_summary(scored: pd.DataFrame | None) -> dict[str, object]:
    if scored is None or scored.empty:
        return {
            "label_source_count": 0,
            "label_sources": [],
            "label_source_counts": {},
            "settlement_source_counts": {},
            "price_to_beat_rows": 0,
            "final_price_rows": 0,
        }
    payload = summarize_label_sources(scored.get("label_source", pd.Series(dtype="string")))
    settlement = (
        scored.get("settlement_source", pd.Series("", index=scored.index, dtype="string"))
        .astype("string")
        .fillna("")
    )
    settlement_counts = settlement.value_counts().sort_index()
    settlement_counts = settlement_counts[settlement_counts.index != ""]
    payload["settlement_source_counts"] = {str(index): int(value) for index, value in settlement_counts.items()}
    payload["price_to_beat_rows"] = int(_numeric_series(scored, "price_to_beat").notna().sum())
    payload["final_price_rows"] = int(_numeric_series(scored, "final_price").notna().sum())
    return payload


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series(0.0, index=frame.index, dtype=float)
    return pd.to_numeric(values, errors="coerce")


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series(False, index=frame.index, dtype="boolean")
    return values.astype("boolean").fillna(False).astype(bool)


def _render_markdown_table(frame: pd.DataFrame) -> str:
    rendered = frame.astype("object").where(frame.notna(), "")
    try:
        return rendered.to_markdown(index=False)
    except ImportError:
        return _render_markdown_table_fallback(rendered)


def _render_markdown_table_fallback(frame: pd.DataFrame) -> str:
    columns = [str(column) for column in frame.columns.tolist()]
    if not columns:
        return ""
    header = "| " + " | ".join(_markdown_cell(column) for column in columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    rows = [
        "| " + " | ".join(_markdown_cell(value) for value in row) + " |"
        for row in frame.itertuples(index=False, name=None)
    ]
    return "\n".join([header, divider, *rows])


def _markdown_cell(value: object) -> str:
    text = "" if value is None else str(value)
    return text.replace("\n", "<br>").replace("|", "\\|")
