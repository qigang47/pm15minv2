from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas.testing as pdt
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_eval.importance_sampling import (
    estimate_is_exponential_tilting as legacy_estimate_is_exponential_tilting,
    exponential_tilt_log_weights as legacy_exponential_tilt_log_weights,
    is_estimate_with_auto_gamma as legacy_is_estimate_with_auto_gamma,
    tune_gamma_for_target_hit_rate as legacy_tune_gamma_for_target_hit_rate,
)
from poly_eval.mc_convergence import (
    bernoulli_mc_stderr as legacy_bernoulli_mc_stderr,
    bernoulli_mc_variance as legacy_bernoulli_mc_variance,
    required_mc_samples_for_margin as legacy_required_mc_samples_for_margin,
    simulate_running_p_hat as legacy_simulate_running_p_hat,
    worst_case_bernoulli_variance as legacy_worst_case_bernoulli_variance,
)
from poly_eval.mc_estimators import (
    estimate_antithetic_probability as legacy_estimate_antithetic_probability,
    estimate_crude_probability as legacy_estimate_crude_probability,
    estimate_stratified_probability as legacy_estimate_stratified_probability,
)
from poly_eval.path_models import (
    GaussianRandomWalk as LegacyGaussianRandomWalk,
    gbm_terminal_price as legacy_gbm_terminal_price,
    simulate_gbm_terminal_prices as legacy_simulate_gbm_terminal_prices,
)
from poly_eval.smc import ParticleFilterConfig as LegacyParticleFilterConfig
from poly_eval.smc import run_particle_filter as legacy_run_particle_filter
from poly_eval.types import ProbabilityEstimate as LegacyProbabilityEstimate
from pm15min.research.evaluation.methods.probability.importance_sampling import (
    estimate_is_exponential_tilting,
    exponential_tilt_log_weights,
    is_estimate_with_auto_gamma,
    tune_gamma_for_target_hit_rate,
)
from pm15min.research.evaluation.methods.probability.mc_convergence import (
    bernoulli_mc_stderr,
    bernoulli_mc_variance,
    required_mc_samples_for_margin,
    simulate_running_p_hat,
    worst_case_bernoulli_variance,
)
from pm15min.research.evaluation.methods.probability.mc_estimators import (
    estimate_antithetic_probability,
    estimate_crude_probability,
    estimate_stratified_probability,
)
from pm15min.research.evaluation.methods.probability.path_models import (
    GaussianRandomWalk,
    gbm_terminal_price,
    simulate_gbm_terminal_prices,
)
from pm15min.research.evaluation.methods.probability.types import ProbabilityEstimate
from pm15min.research.evaluation.methods.smc.particle_filter import ParticleFilterConfig, run_particle_filter


def _terminal_cross(paths: np.ndarray) -> np.ndarray:
    return paths[:, -1] >= 4.5


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
        assert actual.diagnostics[key] == pytest.approx(value)
    assert actual.ci95() == pytest.approx(expected.ci95())
    assert actual.as_dict() == pytest.approx(expected.as_dict())


def test_probability_estimate_helpers_match_legacy_poly_eval() -> None:
    actual = ProbabilityEstimate(
        method="is_exp_tilt",
        p_hat=0.031,
        stderr=0.004,
        n_paths=5000,
        hit_rate=0.17,
        ess=814.5,
        gamma=1.25,
        diagnostics={"weight_cv": 2.3},
    )
    expected = LegacyProbabilityEstimate(
        method="is_exp_tilt",
        p_hat=0.031,
        stderr=0.004,
        n_paths=5000,
        hit_rate=0.17,
        ess=814.5,
        gamma=1.25,
        diagnostics={"weight_cv": 2.3},
    )

    _assert_estimate_matches(actual, expected)


def test_path_models_match_legacy_poly_eval() -> None:
    actual_model = GaussianRandomWalk(step_mean=-0.03, step_std=1.5, start=0.25)
    expected_model = LegacyGaussianRandomWalk(step_mean=-0.03, step_std=1.5, start=0.25)
    normals = np.array([[0.1, -0.2, 0.3], [-0.4, 0.5, -0.6]], dtype=float)

    actual_paths, actual_increments = actual_model.simulate_paths(
        n_paths=2,
        n_steps=3,
        rng=np.random.default_rng(0),
        normals=normals,
    )
    expected_paths, expected_increments = expected_model.simulate_paths(
        n_paths=2,
        n_steps=3,
        rng=np.random.default_rng(0),
        normals=normals,
    )

    np.testing.assert_array_equal(actual_paths, expected_paths)
    np.testing.assert_array_equal(actual_increments, expected_increments)
    actual_tilted = actual_model.tilted(0.7)
    expected_tilted = expected_model.tilted(0.7)
    assert actual_tilted.step_mean == pytest.approx(expected_tilted.step_mean)
    assert actual_tilted.step_std == pytest.approx(expected_tilted.step_std)
    assert actual_tilted.start == pytest.approx(expected_tilted.start)
    assert actual_model.log_mgf(0.7) == pytest.approx(expected_model.log_mgf(0.7))
    assert actual_model.mgf(0.7) == pytest.approx(expected_model.mgf(0.7))
    assert actual_model.lundberg_root() == pytest.approx(expected_model.lundberg_root())
    assert actual_model.terminal_prob_ge(n_steps=12, threshold=3.5) == pytest.approx(
        expected_model.terminal_prob_ge(n_steps=12, threshold=3.5)
    )

    actual_terminal = gbm_terminal_price(s0=100.0, mu=0.05, sigma=0.2, maturity=1.5, z=np.array([0.0, 1.0]))
    expected_terminal = legacy_gbm_terminal_price(s0=100.0, mu=0.05, sigma=0.2, maturity=1.5, z=np.array([0.0, 1.0]))
    np.testing.assert_array_equal(actual_terminal, expected_terminal)

    actual_samples = simulate_gbm_terminal_prices(
        s0=100.0,
        mu=0.05,
        sigma=0.2,
        maturity=1.5,
        n_paths=4,
        normals=np.array([0.0, -0.5, 0.5, 1.0]),
    )
    expected_samples = legacy_simulate_gbm_terminal_prices(
        s0=100.0,
        mu=0.05,
        sigma=0.2,
        maturity=1.5,
        n_paths=4,
        normals=np.array([0.0, -0.5, 0.5, 1.0]),
    )
    np.testing.assert_array_equal(actual_samples, expected_samples)


def test_mc_convergence_helpers_match_legacy_poly_eval() -> None:
    assert bernoulli_mc_variance(p=0.35, n_samples=1200) == pytest.approx(
        legacy_bernoulli_mc_variance(p=0.35, n_samples=1200)
    )
    assert bernoulli_mc_stderr(p=0.35, n_samples=1200) == pytest.approx(
        legacy_bernoulli_mc_stderr(p=0.35, n_samples=1200)
    )
    assert worst_case_bernoulli_variance() == pytest.approx(legacy_worst_case_bernoulli_variance())
    assert required_mc_samples_for_margin(p=0.35, epsilon=0.02, z_value=2.1) == legacy_required_mc_samples_for_margin(
        p=0.35,
        epsilon=0.02,
        z_value=2.1,
    )

    actual_ns, actual_p_hats = simulate_running_p_hat(p=0.8, N_max=50, seed=123)
    expected_ns, expected_p_hats = legacy_simulate_running_p_hat(p=0.8, N_max=50, seed=123)
    np.testing.assert_array_equal(actual_ns, expected_ns)
    np.testing.assert_array_equal(actual_p_hats, expected_p_hats)


def test_mc_estimators_match_legacy_poly_eval() -> None:
    actual_model = GaussianRandomWalk(step_mean=-0.03, step_std=1.0, start=0.0)
    expected_model = LegacyGaussianRandomWalk(step_mean=-0.03, step_std=1.0, start=0.0)

    actual_crude = estimate_crude_probability(
        model=actual_model,
        event_fn=_terminal_cross,
        n_paths=4096,
        n_steps=30,
        seed=17,
    )
    expected_crude = legacy_estimate_crude_probability(
        model=expected_model,
        event_fn=_terminal_cross,
        n_paths=4096,
        n_steps=30,
        seed=17,
    )
    _assert_estimate_matches(actual_crude, expected_crude)

    actual_antithetic = estimate_antithetic_probability(
        model=actual_model,
        event_fn=_terminal_cross,
        n_pairs=2048,
        n_steps=30,
        seed=17,
    )
    expected_antithetic = legacy_estimate_antithetic_probability(
        model=expected_model,
        event_fn=_terminal_cross,
        n_pairs=2048,
        n_steps=30,
        seed=17,
    )
    _assert_estimate_matches(actual_antithetic, expected_antithetic)

    actual_stratified = estimate_stratified_probability(
        model=actual_model,
        event_fn=_terminal_cross,
        n_paths=6000,
        n_steps=30,
        n_strata=12,
        use_neyman=True,
        pilot_paths_per_stratum=30,
        seed=17,
    )
    expected_stratified = legacy_estimate_stratified_probability(
        model=expected_model,
        event_fn=_terminal_cross,
        n_paths=6000,
        n_steps=30,
        n_strata=12,
        use_neyman=True,
        pilot_paths_per_stratum=30,
        seed=17,
    )
    _assert_estimate_matches(actual_stratified, expected_stratified)


def test_importance_sampling_methods_match_legacy_poly_eval() -> None:
    actual_model = GaussianRandomWalk(step_mean=-0.03, step_std=1.0, start=0.0)
    expected_model = LegacyGaussianRandomWalk(step_mean=-0.03, step_std=1.0, start=0.0)
    terminal = np.array([2.0, 3.5, 4.5], dtype=float)

    np.testing.assert_array_equal(
        exponential_tilt_log_weights(model=actual_model, terminal_values=terminal, n_steps=30, gamma=1.1),
        legacy_exponential_tilt_log_weights(model=expected_model, terminal_values=terminal, n_steps=30, gamma=1.1),
    )

    actual_is = estimate_is_exponential_tilting(
        model=actual_model,
        event_fn=_terminal_cross,
        n_paths=4096,
        n_steps=30,
        gamma=1.1,
        seed=23,
    )
    expected_is = legacy_estimate_is_exponential_tilting(
        model=expected_model,
        event_fn=_terminal_cross,
        n_paths=4096,
        n_steps=30,
        gamma=1.1,
        seed=23,
    )
    _assert_estimate_matches(actual_is, expected_is)

    actual_gamma, actual_trace = tune_gamma_for_target_hit_rate(
        model=actual_model,
        event_fn=_terminal_cross,
        n_steps=30,
        target_hit_rate=0.15,
        n_probe_paths=512,
        max_iter=8,
        seed=19,
    )
    expected_gamma, expected_trace = legacy_tune_gamma_for_target_hit_rate(
        model=expected_model,
        event_fn=_terminal_cross,
        n_steps=30,
        target_hit_rate=0.15,
        n_probe_paths=512,
        max_iter=8,
        seed=19,
    )
    assert actual_gamma == pytest.approx(expected_gamma)
    assert actual_trace == pytest.approx(expected_trace)

    actual_auto = is_estimate_with_auto_gamma(
        model=actual_model,
        event_fn=_terminal_cross,
        n_paths=4096,
        n_steps=30,
        target_hit_rate=0.15,
        seed=19,
        self_normalized=True,
    )
    expected_auto = legacy_is_estimate_with_auto_gamma(
        model=expected_model,
        event_fn=_terminal_cross,
        n_paths=4096,
        n_steps=30,
        target_hit_rate=0.15,
        seed=19,
        self_normalized=True,
    )
    _assert_estimate_matches(actual_auto, expected_auto)


def test_particle_filter_matches_legacy_poly_eval() -> None:
    observations = np.array([0.42, 0.55, np.nan, 0.61, 0.48, 0.52], dtype=float)
    actual_config = ParticleFilterConfig(
        n_particles=512,
        process_sigma=0.12,
        obs_sigma=0.05,
        resample_ess_ratio=0.5,
        prior_yes_prob=0.5,
        prior_logit_std=1.0,
    )
    expected_config = LegacyParticleFilterConfig(
        n_particles=512,
        process_sigma=0.12,
        obs_sigma=0.05,
        resample_ess_ratio=0.5,
        prior_yes_prob=0.5,
        prior_logit_std=1.0,
    )

    actual = run_particle_filter(observations=observations, config=actual_config, seed=29)
    expected = legacy_run_particle_filter(observations=observations, config=expected_config, seed=29)

    pdt.assert_frame_equal(actual, expected)
