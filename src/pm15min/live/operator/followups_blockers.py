from __future__ import annotations


def append_foundation_warning_followups(
    *,
    actions: list[str],
    foundation_reason: str,
    foundation_issue_codes: set[str],
) -> None:
    if "oracle_direct_rate_limited" in foundation_issue_codes:
        actions.append("foundation is degraded by direct oracle rate limiting; inspect operator_summary.foundation_reason before retrying")
        actions.append("treat the latest oracle_prices_table as fail-open fallback and retry after the rate limit window if you need green readiness")
    else:
        actions.append("inspect latest foundation summary and degraded task list before enabling side effects")
    if foundation_reason:
        actions.append("use operator_summary.foundation_reason to identify the degraded foundation task without opening raw logs")


def append_decision_not_accept_followups(
    *,
    actions: list[str],
    reject_category: str,
    reject_interpretation: str,
    reject_reasons: list[object],
) -> None:
    if reject_category == "quote_inputs_missing":
        actions.append("inspect latest quote snapshot and orderbook_index coverage for the rejected market before retrying")
        actions.append("inspect operator_summary.orderbook_hot_cache_summary to see whether recent orderbook cache is missing, empty, or stale")
        actions.append("rerun data run live-foundation or data record orderbooks if quote inputs are still missing")
    elif reject_category == "confidence_threshold":
        actions.append("inspect latest decision trigger metric vs threshold and active bundle output before retrying")
    elif reject_category == "liquidity_guard":
        actions.append("run live sync-liquidity-state and inspect liquidity guard reason codes before retrying")
    elif reject_category == "regime_guard":
        if {"max_trades_per_offset", "regime_trade_count_cap"} & set(reject_reasons):
            actions.append("inspect operator_summary.capital_usage_summary.focus_market and regime_context before retrying")
            actions.append("reduce existing open orders / positions for the focus market or wait for market rollover before retrying")
        else:
            actions.append("inspect latest regime state and regime guard reasons before retrying")
    elif reject_category == "entry_or_quote_threshold":
        if reject_interpretation == "market_priced_through_signal":
            actions.append("latest quotes already price the selected side above live entry cap and above the model trigger price; keep side effects disabled for this cycle")
            actions.append("inspect operator_summary.decision_reject_diagnostics.best_rejected_offset before changing live profile thresholds")
        elif reject_interpretation == "entry_price_above_live_cap":
            actions.append("latest quotes price the selected side above live entry_price_max; wait for a cheaper entry instead of forcing side effects")
            actions.append("inspect operator_summary.decision_reject_diagnostics.rejected_offsets to confirm every live offset is still above the entry cap")
        elif reject_interpretation == "negative_quote_edge":
            actions.append("compare p_side vs quote ask on operator_summary.decision_reject_diagnostics.best_rejected_offset before changing live thresholds")
        else:
            actions.append("inspect latest quote entry price cap and orderbook-derived entry price before retrying")
    elif reject_category == "tail_space_guard":
        actions.append("inspect ret_from_strike / move_z tail-space guards before retrying")
    else:
        actions.append("inspect latest decision snapshot and top reject reasons before retrying")


def append_execution_not_plan_followups(
    *,
    actions: list[str],
    execution_block_category: str,
    execution_reason: str,
) -> None:
    if execution_block_category == "orderbook_depth":
        actions.append("inspect latest execution depth_plan and orderbook fill_ratio before retrying")
        actions.append("rerun data record orderbooks or live-foundation if depth snapshots are stale or missing")
    elif execution_block_category == "repriced_quote_threshold":
        actions.append("inspect latest execution repriced_metrics and compare repriced entry/edge/roi vs live profile thresholds")
    elif execution_block_category == "execution_inputs_missing":
        actions.append("inspect latest execution snapshot and selected decision/quote row for missing market/token/price/notional inputs")
    elif execution_block_category == "regime_budget_blocked":
        actions.append("inspect operator_summary.capital_usage_summary.execution_budget and regime_context before retrying")
    elif execution_block_category == "decision_reject":
        actions.append("inspect latest decision snapshot and decision reject diagnostics before retrying execution")
    else:
        actions.append("inspect latest execution snapshot for blocked/no_action reason before retrying")
        if execution_reason:
            actions.append("use operator_summary.execution_reason and execution_reasons to narrow the execution block cause")
