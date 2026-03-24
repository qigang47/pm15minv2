from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_eval.abm import ABMConfig as LegacyABMConfig
from poly_eval.abm import PredictionMarketABM as LegacyPredictionMarketABM
from poly_eval.abm import run_abm_simulation as legacy_run_abm_simulation
from poly_eval.abm import sweep_informed_noise_ratio as legacy_sweep_informed_noise_ratio
from pm15min.research.evaluation.methods.abm import (
    ABMConfig,
    PredictionMarketABM,
    run_abm_simulation,
    sweep_informed_noise_ratio,
)


def _assert_summary_matches(
    actual: dict[str, float | int | str],
    expected: dict[str, float | int | str],
) -> None:
    assert actual.keys() == expected.keys()
    for key, expected_value in expected.items():
        actual_value = actual[key]
        if isinstance(expected_value, str):
            assert actual_value == expected_value
        elif isinstance(expected_value, int) and not isinstance(expected_value, bool):
            assert actual_value == expected_value
        elif math.isnan(float(expected_value)):
            assert math.isnan(float(actual_value))
        else:
            assert float(actual_value) == pytest.approx(float(expected_value))


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"true_prob": 0.0}, "true_prob must be in (0,1), got 0.0"),
        ({"init_price": 1.0}, "init_price must be in (0,1), got 1.0"),
        ({"n_noise": -1}, "n_noise must be >=0, got -1"),
        ({"n_informed": 0, "n_noise": 0, "n_mm": 0}, "Total agents must be positive"),
    ],
)
def test_abm_config_validation_matches_legacy_poly_eval(kwargs: dict[str, float | int], message: str) -> None:
    with pytest.raises(ValueError) as actual_error:
        ABMConfig(**kwargs)
    assert str(actual_error.value) == message
    with pytest.raises(ValueError) as expected_error:
        LegacyABMConfig(**kwargs)
    assert str(expected_error.value) == message


def test_prediction_market_abm_matches_legacy_poly_eval() -> None:
    actual_config = ABMConfig(
        true_prob=0.63,
        init_price=0.48,
        n_informed=12,
        n_noise=43,
        n_mm=4,
        informed_signal_sigma=0.03,
        informed_threshold=0.015,
        informed_max_size=0.11,
        informed_sensitivity=1.8,
        noise_size_scale=0.018,
        noise_max_size=0.17,
        mm_min_spread=0.015,
        mm_base_spread=0.045,
        mm_decay_volume_scale=90.0,
        impact_floor=0.015,
        impact_noise_scale=0.12,
        price_floor=0.02,
        price_cap=0.98,
    )
    expected_config = LegacyABMConfig(
        true_prob=0.63,
        init_price=0.48,
        n_informed=12,
        n_noise=43,
        n_mm=4,
        informed_signal_sigma=0.03,
        informed_threshold=0.015,
        informed_max_size=0.11,
        informed_sensitivity=1.8,
        noise_size_scale=0.018,
        noise_max_size=0.17,
        mm_min_spread=0.015,
        mm_base_spread=0.045,
        mm_decay_volume_scale=90.0,
        impact_floor=0.015,
        impact_noise_scale=0.12,
        price_floor=0.02,
        price_cap=0.98,
    )

    actual = PredictionMarketABM(config=actual_config, seed=17)
    expected = LegacyPredictionMarketABM(config=expected_config, seed=17)

    actual_run = actual.run(n_steps=250)
    expected_run = expected.run(n_steps=250)

    pdt.assert_series_equal(pd.Series(actual_run), pd.Series(expected_run), check_names=False)
    assert actual.convergence_time(epsilon=0.03, hold_steps=20) == expected.convergence_time(epsilon=0.03, hold_steps=20)
    pdt.assert_frame_equal(actual.to_frame(), expected.to_frame())
    _assert_summary_matches(actual.summary(epsilon=0.03, hold_steps=20), expected.summary(epsilon=0.03, hold_steps=20))


def test_run_abm_simulation_matches_legacy_poly_eval() -> None:
    actual_config = ABMConfig(true_prob=0.67, init_price=0.52, n_informed=9, n_noise=38, n_mm=6)
    expected_config = LegacyABMConfig(true_prob=0.67, init_price=0.52, n_informed=9, n_noise=38, n_mm=6)

    actual_frame, actual_summary = run_abm_simulation(config=actual_config, n_steps=400, seed=123)
    expected_frame, expected_summary = legacy_run_abm_simulation(config=expected_config, n_steps=400, seed=123)

    pdt.assert_frame_equal(actual_frame, expected_frame)
    _assert_summary_matches(actual_summary, expected_summary)


def test_sweep_informed_noise_ratio_matches_legacy_poly_eval() -> None:
    actual = sweep_informed_noise_ratio(
        true_prob=0.65,
        init_price=0.5,
        informed_values=[2, 8],
        noise_values=[30, 45],
        n_mm=5,
        n_steps=120,
        seeds=[3, 7],
    )
    expected = legacy_sweep_informed_noise_ratio(
        true_prob=0.65,
        init_price=0.5,
        informed_values=[2, 8],
        noise_values=[30, 45],
        n_mm=5,
        n_steps=120,
        seeds=[3, 7],
    )

    pdt.assert_frame_equal(actual, expected)
