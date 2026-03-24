from __future__ import annotations

from dataclasses import dataclass


def _clip_prob(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


@dataclass(frozen=True)
class TakerDecision:
    """Decision summary for a YES/NO taker order."""

    side: str
    market_price: float
    effective_price: float
    win_probability: float
    expected_value: float
    expected_roi: float
    break_even_yes_prob: float
    edge_bps: float
    should_trade: bool

    def as_dict(self) -> dict[str, float | bool | str]:
        return {
            "side": self.side,
            "market_price": float(self.market_price),
            "effective_price": float(self.effective_price),
            "win_probability": float(self.win_probability),
            "expected_value": float(self.expected_value),
            "expected_roi": float(self.expected_roi),
            "break_even_yes_prob": float(self.break_even_yes_prob),
            "edge_bps": float(self.edge_bps),
            "should_trade": bool(self.should_trade),
        }


def evaluate_taker_trade(
    *,
    yes_probability: float,
    side: str,
    market_price: float,
    fee_rate_entry: float = 0.0,
    fee_rate_exit: float = 0.0,
    half_spread: float = 0.0,
    extra_slippage: float = 0.0,
    min_ev: float = 0.0,
    min_roi: float = 0.0,
) -> TakerDecision:
    """Evaluate expected value of a taker order for a YES or NO token."""

    q_yes = _clip_prob(yes_probability)
    normalized_side = str(side).strip().upper()
    if normalized_side not in {"YES", "NO"}:
        raise ValueError(f"side must be YES or NO, got {side}")

    base_price = _clip_prob(market_price)
    entry_fee = max(0.0, float(fee_rate_entry))
    exit_fee = max(0.0, float(fee_rate_exit))
    spread = max(0.0, float(half_spread))
    slippage = max(0.0, float(extra_slippage))

    effective_price = _clip_prob(base_price + spread + slippage)
    total_cost = effective_price * (1.0 + entry_fee)
    win_prob = q_yes if normalized_side == "YES" else 1.0 - q_yes

    expected_payout = win_prob * (1.0 - exit_fee)
    expected_value = expected_payout - total_cost
    expected_roi = expected_value / total_cost if total_cost > 0.0 else 0.0

    denom = max(1e-12, 1.0 - exit_fee)
    if normalized_side == "YES":
        break_even_yes = total_cost / denom
    else:
        break_even_yes = 1.0 - (total_cost / denom)
    break_even_yes = _clip_prob(break_even_yes)

    return TakerDecision(
        side=normalized_side,
        market_price=base_price,
        effective_price=effective_price,
        win_probability=win_prob,
        expected_value=expected_value,
        expected_roi=expected_roi,
        break_even_yes_prob=break_even_yes,
        edge_bps=expected_value * 10_000.0,
        should_trade=(expected_value >= float(min_ev) and expected_roi >= float(min_roi)),
    )


def evaluate_two_sided_taker(
    *,
    yes_probability: float,
    yes_price: float,
    no_price: float,
    fee_rate_entry: float = 0.0,
    fee_rate_exit: float = 0.0,
    half_spread: float = 0.0,
    extra_slippage: float = 0.0,
    min_ev: float = 0.0,
    min_roi: float = 0.0,
) -> tuple[TakerDecision, TakerDecision, TakerDecision]:
    """Evaluate YES and NO taker trades and return (yes, no, best)."""

    yes_decision = evaluate_taker_trade(
        yes_probability=yes_probability,
        side="YES",
        market_price=yes_price,
        fee_rate_entry=fee_rate_entry,
        fee_rate_exit=fee_rate_exit,
        half_spread=half_spread,
        extra_slippage=extra_slippage,
        min_ev=min_ev,
        min_roi=min_roi,
    )
    no_decision = evaluate_taker_trade(
        yes_probability=yes_probability,
        side="NO",
        market_price=no_price,
        fee_rate_entry=fee_rate_entry,
        fee_rate_exit=fee_rate_exit,
        half_spread=half_spread,
        extra_slippage=extra_slippage,
        min_ev=min_ev,
        min_roi=min_roi,
    )
    best = yes_decision if yes_decision.expected_value >= no_decision.expected_value else no_decision
    return yes_decision, no_decision, best


def maker_quote_from_fair_prob(
    *,
    yes_probability: float,
    target_edge: float,
    inventory_skew: float = 0.0,
) -> tuple[float, float]:
    """Build simple YES bid/ask quotes around a fair probability."""

    fair_yes = _clip_prob(yes_probability - float(inventory_skew))
    edge = max(0.0, float(target_edge))
    bid = _clip_prob(fair_yes - edge)
    ask = _clip_prob(fair_yes + edge)
    if ask < bid:
        ask = bid
    return bid, ask
