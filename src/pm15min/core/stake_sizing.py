from __future__ import annotations

from typing import Any


def resolve_tiered_kelly_stake(
    *,
    probability: object,
    entry_price: object,
    base_stake: float,
    max_stake: float | None,
    enabled: bool = False,
    kelly_fraction: float = 0.25,
    medium_fraction_threshold: float = 0.05392156862745098,
    strong_fraction_threshold: float = 0.07843137254901962,
    medium_stake: float = 5.0,
    strong_stake: float = 10.0,
) -> tuple[float, dict[str, Any]]:
    base = max(0.0, float(base_stake))
    cap = float(max_stake) if max_stake is not None else float("inf")
    cap = max(0.0, cap)
    fixed_stake = min(base, cap)
    context: dict[str, Any] = {
        "stake_source": "fixed_profile",
        "kelly_enabled": bool(enabled),
        "kelly_probability": _float_or_none(probability),
        "kelly_entry_price": _float_or_none(entry_price),
        "kelly_edge": None,
        "kelly_full": 0.0,
        "kelly_fractional": 0.0,
        "kelly_fraction": float(kelly_fraction),
    }
    if not enabled:
        return fixed_stake, context

    prob = _float_or_none(probability)
    price = _float_or_none(entry_price)
    if prob is None or price is None or price <= 0.0 or price >= 1.0:
        context["stake_source"] = "kelly_tier_invalid"
        return fixed_stake, context

    edge = float(prob) - float(price)
    kelly_full = max(0.0, edge / max(1.0 - float(price), 1e-9))
    fractional = max(0.0, kelly_full * max(0.0, float(kelly_fraction)))
    context.update(
        {
            "kelly_edge": float(edge),
            "kelly_full": float(kelly_full),
            "kelly_fractional": float(fractional),
        }
    )

    medium_threshold = max(0.0, float(medium_fraction_threshold))
    strong_threshold = max(medium_threshold, float(strong_fraction_threshold))
    if fractional >= strong_threshold:
        context["stake_source"] = "kelly_tier_strong"
        return min(max(base, float(strong_stake)), cap), context
    if fractional > medium_threshold:
        context["stake_source"] = "kelly_tier_scaled"
        if strong_threshold <= medium_threshold:
            stake = float(strong_stake)
        else:
            scale = (fractional - medium_threshold) / (strong_threshold - medium_threshold)
            stake = base + max(0.0, min(float(scale), 1.0)) * (float(strong_stake) - base)
        return min(max(base, stake), cap), context
    context["stake_source"] = "kelly_tier_base"
    return fixed_stake, context


def _float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out
