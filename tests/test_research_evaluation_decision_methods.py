from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_eval.decision import (
    evaluate_taker_trade as legacy_evaluate_taker_trade,
    evaluate_two_sided_taker as legacy_evaluate_two_sided_taker,
    maker_quote_from_fair_prob as legacy_maker_quote_from_fair_prob,
)
from pm15min.research.evaluation.methods.decision import (
    evaluate_taker_trade,
    evaluate_two_sided_taker,
    maker_quote_from_fair_prob,
)


def _assert_decision_equal(actual: object, expected: object) -> None:
    actual_payload = actual.as_dict()
    expected_payload = expected.as_dict()
    assert actual_payload["side"] == expected_payload["side"]
    assert actual_payload["should_trade"] is expected_payload["should_trade"]
    for key in [
        "market_price",
        "effective_price",
        "win_probability",
        "expected_value",
        "expected_roi",
        "break_even_yes_prob",
        "edge_bps",
    ]:
        assert actual_payload[key] == pytest.approx(expected_payload[key])


def test_evaluate_taker_trade_matches_legacy_for_yes_and_no() -> None:
    params = {
        "yes_probability": 0.63,
        "market_price": 0.41,
        "fee_rate_entry": 0.015,
        "fee_rate_exit": 0.02,
        "half_spread": 0.01,
        "extra_slippage": 0.005,
        "min_ev": 0.01,
        "min_roi": 0.02,
    }

    actual_yes = evaluate_taker_trade(side="YES", **params)
    expected_yes = legacy_evaluate_taker_trade(side="YES", **params)
    _assert_decision_equal(actual_yes, expected_yes)

    actual_no = evaluate_taker_trade(side="NO", **params)
    expected_no = legacy_evaluate_taker_trade(side="NO", **params)
    _assert_decision_equal(actual_no, expected_no)


def test_evaluate_two_sided_taker_matches_legacy_best_side_selection() -> None:
    params = {
        "yes_probability": 0.58,
        "yes_price": 0.47,
        "no_price": 0.38,
        "fee_rate_entry": 0.01,
        "fee_rate_exit": 0.01,
        "half_spread": 0.002,
        "extra_slippage": 0.001,
        "min_ev": 0.0,
        "min_roi": 0.0,
    }

    actual_yes, actual_no, actual_best = evaluate_two_sided_taker(**params)
    expected_yes, expected_no, expected_best = legacy_evaluate_two_sided_taker(**params)

    _assert_decision_equal(actual_yes, expected_yes)
    _assert_decision_equal(actual_no, expected_no)
    _assert_decision_equal(actual_best, expected_best)


def test_maker_quote_from_fair_prob_matches_legacy() -> None:
    actual = maker_quote_from_fair_prob(yes_probability=0.67, target_edge=0.04, inventory_skew=0.03)
    expected = legacy_maker_quote_from_fair_prob(yes_probability=0.67, target_edge=0.04, inventory_skew=0.03)

    assert actual == pytest.approx(expected)
    assert actual[0] <= actual[1]
