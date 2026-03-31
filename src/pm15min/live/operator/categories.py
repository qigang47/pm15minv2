from __future__ import annotations


def categorize_decision_reject_reasons(reasons: list[object]) -> str | None:
    tokens = [str(reason or "").strip().lower() for reason in reasons if str(reason or "").strip()]
    if not tokens:
        return None
    token_set = set(tokens)
    if (
        "signal_not_ready" in token_set
        or "offset_not_yet_open" in token_set
        or "offset_window_expired" in token_set
        or "missing_score_row" in token_set
        or any(token.startswith("signal_") for token in token_set)
    ):
        return "signal_not_ready"
    if "quote_missing_inputs" in token_set or any(token.startswith("quote_") for token in token_set):
        return "quote_inputs_missing"
    if (
        "confidence_below_threshold" in token_set
        or "regime_direction_prob" in token_set
        or "up_lcb_below_threshold" in token_set
        or "up_ucb_above_threshold" in token_set
        or "up_raw_not_above_midpoint" in token_set
        or "down_raw_not_below_midpoint" in token_set
        or "confidence_missing" in token_set
    ):
        return "confidence_threshold"
    if "liquidity_guard_blocked" in token_set or any(token.startswith("liquidity_") for token in token_set):
        return "liquidity_guard"
    if "max_trades_per_offset" in token_set or "regime_trade_count_cap" in token_set or any(token.startswith("regime_") for token in token_set):
        return "regime_guard"
    if "tail_space_too_far" in token_set:
        return "tail_space_guard"
    if (
        "entry_price_missing" in token_set
        or "entry_price_min" in token_set
        or "entry_price_max" in token_set
        or "net_edge_below_quote_threshold" in token_set
        or "roi_net_below_threshold" in token_set
    ):
        return "entry_or_quote_threshold"
    return "other"


def categorize_execution_block_reasons(
    *,
    execution_reason: object,
    execution_reasons: list[object],
) -> str | None:
    tokens = [
        str(token or "").strip().lower()
        for token in [execution_reason, *list(execution_reasons or [])]
        if str(token or "").strip()
    ]
    if not tokens:
        return None
    token_set = set(tokens)
    if "decision_reject" in token_set:
        return "decision_reject"
    if "regime_stake_nonpositive" in token_set:
        return "regime_budget_blocked"
    if any(token.startswith("repriced_") for token in token_set):
        return "repriced_quote_threshold"
    if (
        "depth_fill_ratio_below_threshold" in token_set
        or "depth_fill_unavailable" in token_set
        or "depth_snapshot_missing" in token_set
        or "l1_ask_size_missing" in token_set
    ):
        return "orderbook_depth"
    if (
        "selected_side_missing" in token_set
        or "entry_price_missing" in token_set
        or "requested_notional_missing" in token_set
        or "submitted_shares_missing" in token_set
        or "market_id_missing" in token_set
        or "token_id_missing" in token_set
    ):
        return "execution_inputs_missing"
    return "other"
