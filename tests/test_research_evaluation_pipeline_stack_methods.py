from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas.testing as pdt
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_eval.abm import (
    ABMConfig as LegacyABMConfig,
    run_abm_simulation as legacy_run_abm_simulation,
    sweep_informed_noise_ratio as legacy_sweep_informed_noise_ratio,
)
from poly_eval.control_variate import (
    bs_digital_call_probability as legacy_bs_digital_call_probability,
    control_variate_estimate as legacy_control_variate_estimate,
    estimate_probability_with_cv_sv_vs_gaussian as legacy_estimate_probability_with_cv_sv_vs_gaussian,
)
from poly_eval.events import (
    EventSpec as LegacyEventSpec,
    build_event_fn as legacy_build_event_fn,
    last_n_comeback_event as legacy_last_n_comeback_event,
    terminal_cross_event as legacy_terminal_cross_event,
)
from poly_eval.path_models import GaussianRandomWalk as LegacyGaussianRandomWalk
from poly_eval.pipeline import (
    EstimationConfig as LegacyEstimationConfig,
    MarketConfig as LegacyMarketConfig,
    brier_from_backtest as legacy_brier_from_backtest,
    estimate_event_probability as legacy_estimate_event_probability,
    render_pipeline_markdown as legacy_render_pipeline_markdown,
    run_deep_otm_pipeline as legacy_run_deep_otm_pipeline,
)
from poly_eval.production_stack import (
    ProductionStackConfig as LegacyProductionStackConfig,
    render_production_stack_markdown as legacy_render_production_stack_markdown,
    run_production_stack_demo as legacy_run_production_stack_demo,
)
from poly_eval.types import ProbabilityEstimate as LegacyProbabilityEstimate
from pm15min.research.evaluation.methods.abm import ABMConfig, run_abm_simulation, sweep_informed_noise_ratio
from pm15min.research.evaluation.methods.control_variate import (
    bs_digital_call_probability,
    control_variate_estimate,
    estimate_probability_with_cv_sv_vs_gaussian,
)
from pm15min.research.evaluation.methods.events import (
    EventSpec,
    build_event_fn,
    last_n_comeback_event,
    terminal_cross_event,
)
from pm15min.research.evaluation.methods.pipeline import (
    EstimationConfig,
    MarketConfig,
    brier_from_backtest,
    estimate_event_probability,
    render_pipeline_markdown,
    run_deep_otm_pipeline,
)
from pm15min.research.evaluation.methods.probability.path_models import GaussianRandomWalk
from pm15min.research.evaluation.methods.probability.types import ProbabilityEstimate
from pm15min.research.evaluation.methods.production_stack import (
    ProductionStackConfig,
    render_production_stack_markdown,
    run_production_stack_demo,
)


def _assert_estimate_matches(actual: ProbabilityEstimate, expected: LegacyProbabilityEstimate) -> None:
    assert actual.method == expected.method
    assert actual.p_hat == pytest.approx(expected.p_hat)
    assert actual.stderr == pytest.approx(expected.stderr)
    assert actual.n_paths == expected.n_paths
    assert actual.hit_rate == pytest.approx(expected.hit_rate)
    assert actual.ess == pytest.approx(expected.ess) if expected.ess is not None else actual.ess is None
    assert actual.gamma == pytest.approx(expected.gamma) if expected.gamma is not None else actual.gamma is None
    assert actual.diagnostics.keys() == expected.diagnostics.keys()
    for key, value in expected.diagnostics.items():
        if math.isnan(value):
            assert math.isnan(actual.diagnostics[key])
        else:
            assert actual.diagnostics[key] == pytest.approx(value)
    assert actual.ci95() == pytest.approx(expected.ci95())
    assert actual.as_dict() == pytest.approx(expected.as_dict())


def _assert_summary_dict_matches(
    actual: dict[str, dict[str, float | int | str]],
    expected: dict[str, dict[str, float | int | str]],
) -> None:
    assert actual.keys() == expected.keys()
    for layer_name, expected_layer in expected.items():
        actual_layer = actual[layer_name]
        assert actual_layer.keys() == expected_layer.keys()
        for key, expected_value in expected_layer.items():
            actual_value = actual_layer[key]
            if isinstance(expected_value, float):
                if math.isnan(expected_value):
                    assert isinstance(actual_value, float) and math.isnan(actual_value)
                else:
                    assert actual_value == pytest.approx(expected_value)
            else:
                assert actual_value == expected_value


def _assert_decision_dict_matches(
    actual: dict[str, float | bool | str],
    expected: dict[str, float | bool | str],
) -> None:
    assert actual.keys() == expected.keys()
    for key, expected_value in expected.items():
        actual_value = actual[key]
        if isinstance(expected_value, float):
            assert actual_value == pytest.approx(expected_value)
        else:
            assert actual_value == expected_value


def test_event_helpers_match_legacy_poly_eval() -> None:
    paths = np.array(
        [
            [0.0, -0.5, 0.2, 0.6],
            [0.0, -1.4, -1.1, 0.1],
            [0.0, -0.2, -0.4, -0.3],
        ],
        dtype=float,
    )

    np.testing.assert_array_equal(
        terminal_cross_event(paths, threshold=0.0, direction="ge"),
        legacy_terminal_cross_event(paths, threshold=0.0, direction="ge"),
    )
    np.testing.assert_array_equal(
        last_n_comeback_event(paths, lookback_steps=3, min_deficit=-1.0, recovery_level=0.0),
        legacy_last_n_comeback_event(paths, lookback_steps=3, min_deficit=-1.0, recovery_level=0.0),
    )

    terminal_spec = EventSpec(kind="terminal_cross", threshold=0.5, direction="ge")
    legacy_terminal_spec = LegacyEventSpec(kind="terminal_cross", threshold=0.5, direction="ge")
    np.testing.assert_array_equal(build_event_fn(terminal_spec)(paths), legacy_build_event_fn(legacy_terminal_spec)(paths))

    comeback_spec = EventSpec(
        kind="last_n_comeback",
        lookback_steps=3,
        min_deficit=-1.0,
        recovery_level=0.0,
    )
    legacy_comeback_spec = LegacyEventSpec(
        kind="last_n_comeback",
        lookback_steps=3,
        min_deficit=-1.0,
        recovery_level=0.0,
    )
    np.testing.assert_array_equal(build_event_fn(comeback_spec)(paths), legacy_build_event_fn(legacy_comeback_spec)(paths))


def test_control_variate_helpers_match_legacy_poly_eval() -> None:
    assert bs_digital_call_probability(s0=100.0, strike=105.0, vol=0.2, maturity=1.5, drift=0.03) == pytest.approx(
        legacy_bs_digital_call_probability(s0=100.0, strike=105.0, vol=0.2, maturity=1.5, drift=0.03)
    )

    target = np.array([1.0, 0.0, 1.0, 1.0, 0.0], dtype=float)
    control = np.array([0.8, 0.1, 0.9, 0.7, 0.2], dtype=float)
    actual = control_variate_estimate(
        target_samples=target,
        control_samples=control,
        control_mean_exact=0.55,
        method_name="cv_demo",
    )
    expected = legacy_control_variate_estimate(
        target_samples=target,
        control_samples=control,
        control_mean_exact=0.55,
        method_name="cv_demo",
    )
    _assert_estimate_matches(actual, expected)

    actual_model = GaussianRandomWalk(step_mean=-0.01, step_std=1.0, start=0.0)
    expected_model = LegacyGaussianRandomWalk(step_mean=-0.01, step_std=1.0, start=0.0)
    actual_raw, actual_cv = estimate_probability_with_cv_sv_vs_gaussian(
        base_model=actual_model,
        threshold=2.5,
        n_paths=4096,
        n_steps=20,
        vol_of_vol=0.45,
        seed=11,
    )
    expected_raw, expected_cv = legacy_estimate_probability_with_cv_sv_vs_gaussian(
        base_model=expected_model,
        threshold=2.5,
        n_paths=4096,
        n_steps=20,
        vol_of_vol=0.45,
        seed=11,
    )
    _assert_estimate_matches(actual_raw, expected_raw)
    _assert_estimate_matches(actual_cv, expected_cv)


def test_pipeline_methods_match_legacy_poly_eval() -> None:
    actual_model = GaussianRandomWalk(step_mean=-0.03, step_std=1.0, start=0.0)
    expected_model = LegacyGaussianRandomWalk(step_mean=-0.03, step_std=1.0, start=0.0)
    actual_event_spec = EventSpec(kind="terminal_cross", threshold=4.5, direction="ge")
    expected_event_spec = LegacyEventSpec(kind="terminal_cross", threshold=4.5, direction="ge")
    actual_estimation = EstimationConfig(method="cv_sv", n_paths=6000, n_steps=30, vol_of_vol=0.35)
    expected_estimation = LegacyEstimationConfig(method="cv_sv", n_paths=6000, n_steps=30, vol_of_vol=0.35)

    actual_estimate = estimate_event_probability(
        model=actual_model,
        event_spec=actual_event_spec,
        config=actual_estimation,
        seed=13,
    )
    expected_estimate = legacy_estimate_event_probability(
        model=expected_model,
        event_spec=expected_event_spec,
        config=expected_estimation,
        seed=13,
    )
    _assert_estimate_matches(actual_estimate, expected_estimate)

    actual_market = MarketConfig(
        yes_ask=0.03,
        no_ask=0.97,
        fee_rate_entry=0.01,
        half_spread=0.002,
        extra_slippage=0.001,
        min_ev=0.0,
        min_roi=0.0,
    )
    expected_market = LegacyMarketConfig(
        yes_ask=0.03,
        no_ask=0.97,
        fee_rate_entry=0.01,
        half_spread=0.002,
        extra_slippage=0.001,
        min_ev=0.0,
        min_roi=0.0,
    )

    actual_result = run_deep_otm_pipeline(
        model=actual_model,
        event_spec=actual_event_spec,
        estimation=actual_estimation,
        market=actual_market,
        seed=13,
    )
    expected_result = legacy_run_deep_otm_pipeline(
        model=expected_model,
        event_spec=expected_event_spec,
        estimation=expected_estimation,
        market=expected_market,
        seed=13,
    )

    _assert_estimate_matches(actual_result["estimate"], expected_result["estimate"])
    _assert_decision_dict_matches(actual_result["yes_decision"].as_dict(), expected_result["yes_decision"].as_dict())
    _assert_decision_dict_matches(actual_result["no_decision"].as_dict(), expected_result["no_decision"].as_dict())
    _assert_decision_dict_matches(actual_result["best_decision"].as_dict(), expected_result["best_decision"].as_dict())
    assert render_pipeline_markdown(actual_result) == legacy_render_pipeline_markdown(expected_result)

    predicted_probs = np.array([0.1, 0.2, 0.8, 0.9], dtype=float)
    outcomes = np.array([0.0, 0.0, 1.0, 1.0], dtype=float)
    assert brier_from_backtest(predicted_probs=predicted_probs, outcomes=outcomes) == pytest.approx(
        legacy_brier_from_backtest(predicted_probs=predicted_probs, outcomes=outcomes)
    )


def test_abm_helpers_match_legacy_poly_eval() -> None:
    actual_cfg = ABMConfig(true_prob=0.65, init_price=0.50, n_informed=10, n_noise=50, n_mm=5)
    expected_cfg = LegacyABMConfig(true_prob=0.65, init_price=0.50, n_informed=10, n_noise=50, n_mm=5)

    actual_frame, actual_summary = run_abm_simulation(config=actual_cfg, n_steps=120, seed=42)
    expected_frame, expected_summary = legacy_run_abm_simulation(config=expected_cfg, n_steps=120, seed=42)
    pdt.assert_frame_equal(actual_frame, expected_frame)
    assert actual_summary == pytest.approx(expected_summary)

    actual_sweep = sweep_informed_noise_ratio(
        true_prob=0.65,
        init_price=0.50,
        informed_values=[2, 8],
        noise_values=[40],
        n_mm=5,
        n_steps=150,
        seeds=[1, 2],
    )
    expected_sweep = legacy_sweep_informed_noise_ratio(
        true_prob=0.65,
        init_price=0.50,
        informed_values=[2, 8],
        noise_values=[40],
        n_mm=5,
        n_steps=150,
        seeds=[1, 2],
    )
    pdt.assert_frame_equal(actual_sweep, expected_sweep)


def test_production_stack_methods_match_legacy_poly_eval() -> None:
    actual_cfg = ProductionStackConfig(
        true_prob=0.62,
        init_price=0.50,
        n_steps=60,
        seed=11,
        n_informed=10,
        n_noise=55,
        n_mm=6,
        n_particles=256,
        copula_n_sim=5000,
    )
    expected_cfg = LegacyProductionStackConfig(
        true_prob=0.62,
        init_price=0.50,
        n_steps=60,
        seed=11,
        n_informed=10,
        n_noise=55,
        n_mm=6,
        n_particles=256,
        copula_n_sim=5000,
    )

    actual = run_production_stack_demo(actual_cfg)
    expected = legacy_run_production_stack_demo(expected_cfg)

    pdt.assert_frame_equal(actual.layer1_feed, expected.layer1_feed)
    pdt.assert_frame_equal(actual.layer2_probs, expected.layer2_probs)
    pdt.assert_frame_equal(actual.layer3_pairwise_tail, expected.layer3_pairwise_tail)
    _assert_summary_dict_matches(actual.layer_summaries, expected.layer_summaries)
    assert render_production_stack_markdown(actual) == legacy_render_production_stack_markdown(expected)
