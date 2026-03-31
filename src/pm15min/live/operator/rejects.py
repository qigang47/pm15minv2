from __future__ import annotations

from .utils import float_or_none, int_or_none


def build_decision_reject_diagnostics(*, last_iteration: dict[str, object]) -> dict[str, object] | None:
    decision_payload = last_iteration.get("decision_payload") or {}
    if not isinstance(decision_payload, dict):
        return None
    accepted_offsets = [
        row
        for row in (decision_payload.get("accepted_offsets") or [])
        if isinstance(row, dict)
    ]
    rejected_offsets = [
        compact_rejected_offset_summary(row)
        for row in (decision_payload.get("rejected_offsets") or [])
        if isinstance(row, dict)
    ]
    if not accepted_offsets and not rejected_offsets:
        return None
    guard_reason_counts: dict[str, int] = {}
    for row in rejected_offsets:
        for reason in row["guard_reasons"]:
            guard_reason_counts[reason] = guard_reason_counts.get(reason, 0) + 1
    dominant_guard_reasons = [
        {"reason": reason, "count": count}
        for reason, count in sorted(guard_reason_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    shared_guard_reasons = shared_guard_reasons_for_offsets(rejected_offsets)
    best_rejected_offset = max(
        rejected_offsets,
        key=lambda row: (
            float("-inf") if row["confidence"] is None else float(row["confidence"]),
            float("-inf") if row["entry_price"] is None else -float(row["entry_price"]),
        ),
        default=None,
    )
    return {
        "accepted_offset_count": int(len(accepted_offsets)),
        "rejected_offset_count": int(len(rejected_offsets)),
        "shared_guard_reasons": shared_guard_reasons,
        "dominant_guard_reasons": dominant_guard_reasons,
        "best_rejected_offset": best_rejected_offset,
        "rejected_offsets": rejected_offsets,
        "interpretation": classify_decision_reject_interpretation(
            rejected_offsets=rejected_offsets,
            shared_guard_reasons=shared_guard_reasons,
        ),
    }


def compact_rejected_offset_summary(row: dict[str, object]) -> dict[str, object]:
    quote_metrics = row.get("quote_metrics") or {}
    quote_row = row.get("quote_row") or {}
    payload = {
        "offset": int_or_none(row.get("offset")),
        "decision_ts": row.get("decision_ts"),
        "side": str(row.get("recommended_side") or "") or None,
        "confidence": float_or_none(row.get("confidence")),
        "market_id": quote_row.get("market_id"),
        "condition_id": quote_row.get("condition_id"),
        "entry_price": float_or_none(quote_metrics.get("entry_price")),
        "entry_price_min": float_or_none(quote_metrics.get("entry_price_min")),
        "entry_price_max": float_or_none(quote_metrics.get("entry_price_max")),
        "p_side": float_or_none(quote_metrics.get("p_side")),
        "edge_vs_quote": float_or_none(quote_metrics.get("edge_vs_quote")),
        "min_net_edge_required": float_or_none(quote_metrics.get("min_net_edge_required")),
        "roi_net_vs_quote": float_or_none(quote_metrics.get("roi_net_vs_quote")),
        "roi_threshold_required": float_or_none(quote_metrics.get("roi_threshold_required")),
        "quote_market_id": quote_metrics.get("quote_market_id") or quote_row.get("market_id"),
        "guard_reasons": [str(reason) for reason in (row.get("guard_reasons") or []) if str(reason)],
    }
    trigger_metric = str(row.get("trigger_metric") or "") or None
    trigger_probability = float_or_none(row.get("trigger_probability"))
    p_up_raw = float_or_none(row.get("p_up_raw"))
    p_up_lcb = float_or_none(row.get("p_up_lcb"))
    p_up_ucb = float_or_none(row.get("p_up_ucb"))
    if trigger_metric is not None:
        payload["trigger_metric"] = trigger_metric
    if trigger_probability is not None:
        payload["trigger_probability"] = trigger_probability
    if p_up_raw is not None:
        payload["p_up_raw"] = p_up_raw
    if p_up_lcb is not None:
        payload["p_up_lcb"] = p_up_lcb
    if p_up_ucb is not None:
        payload["p_up_ucb"] = p_up_ucb
    return payload


def shared_guard_reasons_for_offsets(rejected_offsets: list[dict[str, object]]) -> list[str]:
    if not rejected_offsets:
        return []
    ordered = list(rejected_offsets[0].get("guard_reasons") or [])
    shared = [reason for reason in ordered if all(reason in set(row.get("guard_reasons") or []) for row in rejected_offsets[1:])]
    return [str(reason) for reason in shared if str(reason)]


def classify_decision_reject_interpretation(
    *,
    rejected_offsets: list[dict[str, object]],
    shared_guard_reasons: list[str],
) -> str | None:
    if not rejected_offsets:
        return None
    shared = set(str(reason or "") for reason in shared_guard_reasons)
    comparable_rows = [
        row
        for row in rejected_offsets
        if row.get("entry_price") is not None and row.get("p_side") is not None
    ]
    if "entry_price_max" in shared and comparable_rows and all(float(row["entry_price"]) > float(row["p_side"]) for row in comparable_rows):
        return "market_priced_through_signal"
    if "entry_price_max" in shared:
        return "entry_price_above_live_cap"
    if "net_edge_below_quote_threshold" in shared and "roi_net_below_threshold" in shared:
        return "negative_quote_edge"
    if "net_edge_below_quote_threshold" in shared:
        return "edge_below_threshold"
    if "roi_net_below_threshold" in shared:
        return "roi_below_threshold"
    return None
