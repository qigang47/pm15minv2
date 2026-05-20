from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest

from pm15min.core.fees import fee_paid_from_notional, max_quote_price_for_target_roi, resolve_fee_rate
from pm15min.live.guards.quote import quote_guard_reasons
from pm15min.live.profiles import resolve_live_profile_spec
from pm15min.live.signal.decision import _build_decision_quote_metrics
from pm15min.research.backtests.fills import build_fill_plan_frame
from pm15min.research.backtests.settlement import settle_fill_frame


def _crypto_profile_spec():
    return replace(
        resolve_live_profile_spec("deep_otm"),
        slippage_bps=0.0,
        min_dir_prob_default=0.0,
        min_dir_prob_by_offset={},
        min_net_edge_default=0.0,
        min_net_edge_by_offset={},
        min_net_edge_entry_price_le_0p10_bonus=0.0,
        min_net_edge_entry_price_le_0p05_bonus=0.0,
        roi_threshold_default=0.0,
        roi_threshold_by_offset={},
        fee_model="polymarket_curve",
        fee_curve_k=0.072,
    )


def test_polymarket_crypto_fee_matches_official_example() -> None:
    fee_rate = resolve_fee_rate(
        model="polymarket_curve",
        price=0.20,
        fee_curve_k=0.072,
    )

    assert fee_rate == pytest.approx(0.0576)
    assert fee_paid_from_notional(notional=20.0, fee_rate=fee_rate) == pytest.approx(1.152)


def test_quote_guard_uses_share_collected_fee_for_roi() -> None:
    profile_spec = replace(_crypto_profile_spec(), roi_threshold_default=0.43)
    signal_row = {
        "offset": 7,
        "recommended_side": "UP",
        "p_up": 0.30,
        "p_down": 0.70,
    }
    quote_row = {
        "status": "ok",
        "quote_up_ask": 0.20,
        "quote_down_ask": 0.80,
    }

    reasons, metrics = quote_guard_reasons(
        profile_spec=profile_spec,
        signal_row=signal_row,
        quote_row=quote_row,
    )

    assert "roi_net_below_threshold" in reasons
    assert float(metrics["fee_rate"]) == pytest.approx(0.0576)
    assert float(metrics["roi_net_vs_quote"]) == pytest.approx(0.4136)


def test_decision_quote_metrics_use_official_price_cap_formula() -> None:
    profile_spec = replace(_crypto_profile_spec(), roi_threshold_default=0.40)
    signal_row = {
        "offset": 7,
        "decision_ts": "2026-03-19T08:23:00+00:00",
        "recommended_side": "UP",
        "p_up": 0.30,
        "p_down": 0.70,
    }
    quote_row = {
        "status": "ok",
        "market_id": "m-1",
        "token_up": "token-up",
        "token_down": "token-down",
        "quote_up_ask": 0.20,
        "quote_up_ask_size_1": 50.0,
    }

    metrics = _build_decision_quote_metrics(
        market="btc",
        cycle="15m",
        rewrite_root=None,
        profile_spec=profile_spec,
        signal_row=signal_row,
        quote_row=quote_row,
        regime_state=None,
        account_state=None,
    )

    assert metrics is not None
    assert float(metrics["fee_rate"]) == pytest.approx(0.0576)
    assert float(metrics["roi_net_vs_quote"]) == pytest.approx(0.4136)
    assert float(metrics["price_cap"]) == pytest.approx(
        max_quote_price_for_target_roi(
            probability=0.30,
            roi_target=0.40,
            model="polymarket_curve",
            fee_curve_k=0.072,
            slippage_bps=0.0,
        )
    )


def test_build_fill_plan_frame_applies_polymarket_fee_to_net_shares() -> None:
    out = build_fill_plan_frame(
        pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-01T00:01:00Z",
                    "offset": 7,
                    "p_up": 0.80,
                    "p_down": 0.20,
                    "quote_prob_up": 0.20,
                }
            ]
        ),
        base_stake=2.0,
        max_stake=2.0,
        high_conf_threshold=0.99,
        high_conf_multiplier=1.0,
        profile_spec=_crypto_profile_spec(),
    )

    assert out.iloc[0]["fee_collection"] == "shares"
    assert float(out.iloc[0]["fee_rate"]) == pytest.approx(0.0576)
    assert float(out.iloc[0]["fee_paid"]) == pytest.approx(0.1152)
    assert float(out.iloc[0]["shares"]) == pytest.approx(9.424)


def test_settle_fill_frame_keeps_share_collected_fee_out_of_second_deduction() -> None:
    trades = settle_fill_frame(
        pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-01T00:01:00Z",
                    "cycle_start_ts": "2026-03-01T00:00:00Z",
                    "cycle_end_ts": "2026-03-01T00:15:00Z",
                    "offset": 7,
                    "market_id": "m-1",
                    "condition_id": "c-1",
                    "predicted_side": "UP",
                    "predicted_prob": 0.80,
                    "winner_side": "UP",
                    "entry_price": 0.20,
                    "stake": 2.0,
                    "shares": 9.424,
                    "fee_rate": 0.0576,
                    "fee_paid": 0.1152,
                    "fee_collection": "shares",
                    "fill_model": "canonical_quote",
                    "decision_source": "primary",
                }
            ]
        )
    )

    assert float(trades.iloc[0]["payout"]) == pytest.approx(9.424)
    assert float(trades.iloc[0]["pnl"]) == pytest.approx(7.424)


def test_settle_fill_frame_subtracts_cash_collected_fee_from_entry_cost() -> None:
    trades = settle_fill_frame(
        pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-01T00:01:00Z",
                    "cycle_start_ts": "2026-03-01T00:00:00Z",
                    "cycle_end_ts": "2026-03-01T00:15:00Z",
                    "offset": 7,
                    "market_id": "m-1",
                    "condition_id": "c-1",
                    "predicted_side": "UP",
                    "predicted_prob": 0.80,
                    "winner_side": "UP",
                    "entry_price": 0.20,
                    "stake": 2.0,
                    "shares": 10.0,
                    "fee_rate": 0.05,
                    "fee_paid": 0.10,
                    "fee_collection": "cash",
                    "fill_model": "canonical_quote",
                    "decision_source": "primary",
                }
            ]
        )
    )

    assert float(trades.iloc[0]["payout"]) == pytest.approx(10.0)
    assert float(trades.iloc[0]["pnl"]) == pytest.approx(7.9)
