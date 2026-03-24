from __future__ import annotations

from typing import Any

from ..profiles import LiveProfileSpec


def quote_guard_reasons(
    *,
    profile_spec: LiveProfileSpec,
    signal_row: dict[str, Any],
    quote_row: dict[str, Any] | None,
    metrics_override: dict[str, Any] | None = None,
) -> tuple[list[str], dict[str, Any]]:
    reasons: list[str] = []
    metrics: dict[str, Any] = {} if metrics_override is None else dict(metrics_override)
    if quote_row is None:
        return reasons, metrics

    metrics.setdefault("quote_status", str(quote_row.get("status") or ""))
    metrics.setdefault("quote_reasons", list(quote_row.get("reasons") or []))
    metrics.setdefault("quote_market_id", quote_row.get("market_id"))

    if str(quote_row.get("status") or "") != "ok":
        reasons.append("quote_missing_inputs")
        for reason in quote_row.get("reasons") or []:
            reasons.append(f"quote_{reason}")
        return reasons, metrics

    if bool(metrics.get("depth_enforced")):
        depth_reason = str(metrics.get("depth_reason") or "").strip()
        if depth_reason:
            reasons.append(depth_reason)
            return reasons, metrics

    side = str(signal_row.get("recommended_side") or "").upper()
    offset = int(signal_row["offset"])
    entry_price = float_or_none(metrics.get("entry_price"))
    if entry_price is None:
        entry_price = float_or_none(quote_row.get("quote_up_ask") if side == "UP" else quote_row.get("quote_down_ask"))
    metrics["entry_price"] = entry_price
    metrics.setdefault("entry_side", side)
    if entry_price is None or entry_price <= 0.0:
        reasons.append("entry_price_missing")
        return reasons, metrics

    price_min = profile_spec.entry_price_min if "entry_price_min" not in metrics else metrics.get("entry_price_min")
    price_max = profile_spec.entry_price_max if "entry_price_max" not in metrics else metrics.get("entry_price_max")
    metrics["entry_price_min"] = price_min
    metrics["entry_price_max"] = price_max
    if price_min is not None and entry_price < float(price_min):
        reasons.append("entry_price_min")
    if price_max is not None and entry_price > float(price_max):
        reasons.append("entry_price_max")

    p_side = float_or_none(metrics.get("p_side"))
    if p_side is None:
        p_side = float_or_none(signal_row.get("p_up") if side == "UP" else signal_row.get("p_down"))
    if p_side is None:
        p_side = float_or_none(signal_row.get("confidence"))
    metrics["p_side"] = p_side
    if p_side is None:
        reasons.append("side_probability_missing")
        return reasons, metrics

    raw_edge = float_or_none(metrics.get("edge_vs_quote"))
    if raw_edge is None:
        raw_edge = float(p_side) - float(entry_price)
    min_net_edge = float_or_none(metrics.get("min_net_edge_required"))
    if min_net_edge is None:
        min_net_edge = profile_spec.min_net_edge_for(offset=offset, entry_price=entry_price)
    metrics["edge_vs_quote"] = raw_edge
    metrics["min_net_edge_required"] = min_net_edge
    if raw_edge < min_net_edge:
        reasons.append("net_edge_below_quote_threshold")

    slippage_bps = float_or_none(metrics.get("slippage_bps"))
    if slippage_bps is None:
        slippage_bps = float(profile_spec.slippage_bps)
    slip = max(0.0, float(slippage_bps)) / 10000.0
    effective_price = float_or_none(metrics.get("effective_entry_price"))
    if effective_price is None:
        effective_price = float(entry_price) * (1.0 + slip)
    fee_rate = float_or_none(metrics.get("fee_rate"))
    if fee_rate is None:
        fee_rate = profile_spec.fee_rate(price=effective_price)
    roi_net = float_or_none(metrics.get("roi_net_vs_quote"))
    if roi_net is None:
        roi_net = float(p_side) / max(effective_price, 1e-9) - 1.0 - fee_rate
    roi_threshold = float_or_none(metrics.get("roi_threshold_required"))
    if roi_threshold is None:
        roi_threshold = profile_spec.roi_threshold_for(offset=offset)
    metrics["slippage_bps"] = float(slippage_bps)
    metrics["fee_rate"] = fee_rate
    metrics["effective_entry_price"] = effective_price
    metrics["roi_net_vs_quote"] = roi_net
    metrics["roi_threshold_required"] = roi_threshold
    if roi_net < roi_threshold:
        reasons.append("roi_net_below_threshold")

    return reasons, metrics


def float_or_none(value) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out
