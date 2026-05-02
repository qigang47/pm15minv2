from __future__ import annotations


def normalize_fee_model(model: str | None) -> str:
    return str(model or "flat_bps").strip().lower()


def fee_collection(model: str | None) -> str:
    if normalize_fee_model(model) == "polymarket_curve":
        return "shares"
    return "cash"


def resolve_fee_rate(
    *,
    model: str | None,
    price: float,
    fee_bps: float = 0.0,
    fee_curve_k: float = 0.0,
) -> float:
    token = normalize_fee_model(model)
    bounded_price = max(0.0, min(float(price), 1.0))
    if token == "polymarket_curve":
        # Polymarket taker buy fees are proportional to p * (1 - p) in USDC.
        # Relative to buy-side notional this simplifies to r * (1 - p).
        return max(0.0, float(fee_curve_k)) * max(0.0, 1.0 - bounded_price)
    return max(0.0, float(fee_bps)) / 10_000.0


def fee_paid_from_notional(*, notional: float, fee_rate: float) -> float:
    return max(0.0, float(notional)) * max(0.0, float(fee_rate))


def net_shares_after_entry_fee(
    *,
    gross_shares: float,
    fee_rate: float,
    collection: str,
) -> float:
    shares = max(0.0, float(gross_shares))
    if str(collection or "").strip().lower() == "shares":
        return shares * max(0.0, 1.0 - float(fee_rate))
    return shares


def expected_resolution_roi(
    *,
    probability: float,
    price: float,
    model: str | None,
    fee_bps: float = 0.0,
    fee_curve_k: float = 0.0,
) -> float:
    win_probability = max(0.0, min(1.0, float(probability)))
    entry_price = max(1e-9, min(1.0, float(price)))
    rate = resolve_fee_rate(
        model=model,
        price=entry_price,
        fee_bps=fee_bps,
        fee_curve_k=fee_curve_k,
    )
    if fee_collection(model) == "shares":
        return win_probability / entry_price * max(0.0, 1.0 - rate) - 1.0
    return win_probability / entry_price - 1.0 - rate


def max_quote_price_for_target_roi(
    *,
    probability: float,
    roi_target: float,
    model: str | None,
    fee_bps: float = 0.0,
    fee_curve_k: float = 0.0,
    slippage_bps: float = 0.0,
) -> float:
    win_probability = max(0.0, min(1.0, float(probability)))
    target = float(roi_target)
    slip = max(0.0, float(slippage_bps)) / 10_000.0

    def roi_at_quote(quote_price: float) -> float:
        effective_price = max(1e-9, min(1.0, float(quote_price) * (1.0 + slip)))
        return expected_resolution_roi(
            probability=win_probability,
            price=effective_price,
            model=model,
            fee_bps=fee_bps,
            fee_curve_k=fee_curve_k,
        )

    lower = 1e-6
    upper = 1.0
    if roi_at_quote(lower) < target:
        return lower
    if roi_at_quote(upper) >= target:
        return upper
    for _ in range(60):
        midpoint = (lower + upper) / 2.0
        if roi_at_quote(midpoint) >= target:
            lower = midpoint
        else:
            upper = midpoint
    return lower
