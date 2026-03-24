from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas.testing as pdt
import pytest
from scipy import stats

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_eval.copula_risk import CopulaRiskConfig as LegacyCopulaRiskConfig
from poly_eval.copula_risk import run_copula_tail_risk as legacy_run_copula_tail_risk
from poly_eval.copulas import (
    apply_sklar_inverse_cdfs as legacy_apply_sklar_inverse_cdfs,
    empirical_pairwise_tail_dependence as legacy_empirical_pairwise_tail_dependence,
    fit_clayton_theta_from_tau as legacy_fit_clayton_theta_from_tau,
    fit_gaussian_copula as legacy_fit_gaussian_copula,
    fit_gumbel_theta_from_tau as legacy_fit_gumbel_theta_from_tau,
    fit_t_copula as legacy_fit_t_copula,
    make_correlation_psd as legacy_make_correlation_psd,
    pseudo_observations as legacy_pseudo_observations,
    simulate_clayton_copula as legacy_simulate_clayton_copula,
    simulate_gaussian_copula as legacy_simulate_gaussian_copula,
    simulate_gumbel_copula as legacy_simulate_gumbel_copula,
    simulate_t_copula as legacy_simulate_t_copula,
    tail_dependence_clayton as legacy_tail_dependence_clayton,
    tail_dependence_gaussian as legacy_tail_dependence_gaussian,
    tail_dependence_gumbel as legacy_tail_dependence_gumbel,
    tail_dependence_t as legacy_tail_dependence_t,
    vine_pair_copula_count as legacy_vine_pair_copula_count,
)
from pm15min.research.evaluation.methods.copula_risk import CopulaRiskConfig, run_copula_tail_risk
from pm15min.research.evaluation.methods.copulas import (
    apply_sklar_inverse_cdfs,
    empirical_pairwise_tail_dependence,
    fit_clayton_theta_from_tau,
    fit_gaussian_copula,
    fit_gumbel_theta_from_tau,
    fit_t_copula,
    make_correlation_psd,
    pseudo_observations,
    simulate_clayton_copula,
    simulate_gaussian_copula,
    simulate_gumbel_copula,
    simulate_t_copula,
    tail_dependence_clayton,
    tail_dependence_gaussian,
    tail_dependence_gumbel,
    tail_dependence_t,
    vine_pair_copula_count,
)


def _assert_float_mapping_equal(
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


def test_copula_helper_functions_match_legacy_poly_eval() -> None:
    rng = np.random.default_rng(123)
    data = rng.normal(size=(400, 3))
    corr = np.array([[1.0, 0.4, -0.2], [0.4, 1.0, 0.3], [-0.2, 0.3, 1.0]])

    actual_u = pseudo_observations(data)
    expected_u = legacy_pseudo_observations(data)
    assert np.allclose(actual_u, expected_u)

    actual_psd = make_correlation_psd(corr)
    expected_psd = legacy_make_correlation_psd(corr)
    assert np.allclose(actual_psd, expected_psd)

    actual_gaussian_fit = fit_gaussian_copula(actual_u)
    expected_gaussian_fit = legacy_fit_gaussian_copula(expected_u)
    assert np.allclose(actual_gaussian_fit.corr, expected_gaussian_fit.corr)

    actual_t_fit = fit_t_copula(actual_u, nu=4.0)
    expected_t_fit = legacy_fit_t_copula(expected_u, nu=4.0)
    assert actual_t_fit.nu == pytest.approx(expected_t_fit.nu)
    assert actual_t_fit.loglik == pytest.approx(expected_t_fit.loglik)
    assert np.allclose(actual_t_fit.corr, expected_t_fit.corr)

    actual_clayton_fit = fit_clayton_theta_from_tau(0.35)
    expected_clayton_fit = legacy_fit_clayton_theta_from_tau(0.35)
    assert actual_clayton_fit.theta == pytest.approx(expected_clayton_fit.theta)
    assert actual_clayton_fit.avg_kendall_tau == pytest.approx(expected_clayton_fit.avg_kendall_tau)

    actual_gumbel_fit = fit_gumbel_theta_from_tau(0.35)
    expected_gumbel_fit = legacy_fit_gumbel_theta_from_tau(0.35)
    assert actual_gumbel_fit.theta == pytest.approx(expected_gumbel_fit.theta)
    assert actual_gumbel_fit.avg_kendall_tau == pytest.approx(expected_gumbel_fit.avg_kendall_tau)

    assert simulate_gaussian_copula(corr, n_samples=256, seed=7) == pytest.approx(
        legacy_simulate_gaussian_copula(corr, n_samples=256, seed=7)
    )
    assert simulate_t_copula(corr, nu=5.0, n_samples=256, seed=7) == pytest.approx(
        legacy_simulate_t_copula(corr, nu=5.0, n_samples=256, seed=7)
    )
    assert simulate_clayton_copula(theta=2.0, dim=3, n_samples=256, seed=7) == pytest.approx(
        legacy_simulate_clayton_copula(theta=2.0, dim=3, n_samples=256, seed=7)
    )
    assert simulate_gumbel_copula(theta=2.0, dim=3, n_samples=256, seed=7) == pytest.approx(
        legacy_simulate_gumbel_copula(theta=2.0, dim=3, n_samples=256, seed=7)
    )

    assert tail_dependence_t(rho=0.6, nu=4.0) == pytest.approx(legacy_tail_dependence_t(rho=0.6, nu=4.0))
    assert tail_dependence_gaussian(rho=0.6) == pytest.approx(legacy_tail_dependence_gaussian(rho=0.6))
    assert tail_dependence_clayton(theta=2.0) == pytest.approx(legacy_tail_dependence_clayton(theta=2.0))
    assert tail_dependence_gumbel(theta=2.0) == pytest.approx(legacy_tail_dependence_gumbel(theta=2.0))
    assert vine_pair_copula_count(5) == legacy_vine_pair_copula_count(5)

    uniforms = np.array([[0.25, 0.75], [0.50, 0.50]])
    actual_inverse = apply_sklar_inverse_cdfs(uniforms, inverse_cdfs=[stats.norm.ppf, stats.expon.ppf])
    expected_inverse = legacy_apply_sklar_inverse_cdfs(uniforms, inverse_cdfs=[stats.norm.ppf, stats.expon.ppf])
    assert np.allclose(actual_inverse, expected_inverse)

    actual_tail = empirical_pairwise_tail_dependence(actual_u, col_names=["a", "b", "c"], tail_q=0.95)
    expected_tail = legacy_empirical_pairwise_tail_dependence(expected_u, col_names=["a", "b", "c"], tail_q=0.95)
    pdt.assert_frame_equal(actual_tail, expected_tail)


def test_run_copula_tail_risk_matches_legacy_for_t_family() -> None:
    rng = np.random.default_rng(42)
    x1 = rng.standard_t(df=4, size=1200)
    x2 = 0.6 * x1 + np.sqrt(1.0 - 0.6**2) * rng.standard_t(df=4, size=1200)
    x3 = 0.5 * x1 + np.sqrt(1.0 - 0.5**2) * rng.standard_t(df=4, size=1200)
    data = np.column_stack([x1, x2, x3])

    actual_cfg = CopulaRiskConfig(
        family="t",
        tail="lower",
        n_sim=8000,
        quantile=0.05,
        alpha=0.99,
        tail_q=0.95,
        seed=9,
    )
    expected_cfg = LegacyCopulaRiskConfig(
        family="t",
        tail="lower",
        n_sim=8000,
        quantile=0.05,
        alpha=0.99,
        tail_q=0.95,
        seed=9,
    )

    actual = run_copula_tail_risk(data=data, col_names=["x1", "x2", "x3"], config=actual_cfg)
    expected = legacy_run_copula_tail_risk(data=data, col_names=["x1", "x2", "x3"], config=expected_cfg)

    _assert_float_mapping_equal(actual.summary, expected.summary)
    _assert_float_mapping_equal(actual.fit_params, expected.fit_params)
    _assert_float_mapping_equal(actual.event_thresholds, expected.event_thresholds)
    _assert_float_mapping_equal(actual.event_probs, expected.event_probs)
    pdt.assert_frame_equal(actual.pairwise_tail, expected.pairwise_tail)


def test_run_copula_tail_risk_matches_legacy_for_clayton_family() -> None:
    rng = np.random.default_rng(77)
    data = rng.normal(size=(1000, 2))

    actual_cfg = CopulaRiskConfig(
        family="clayton",
        tail="lower",
        n_sim=6000,
        quantile=0.05,
        alpha=0.99,
        theta=2.0,
        seed=5,
    )
    expected_cfg = LegacyCopulaRiskConfig(
        family="clayton",
        tail="lower",
        n_sim=6000,
        quantile=0.05,
        alpha=0.99,
        theta=2.0,
        seed=5,
    )

    actual = run_copula_tail_risk(
        data=data,
        col_names=["x1", "x2"],
        config=actual_cfg,
        event_probs=[0.2, 0.3],
    )
    expected = legacy_run_copula_tail_risk(
        data=data,
        col_names=["x1", "x2"],
        config=expected_cfg,
        event_probs=[0.2, 0.3],
    )

    _assert_float_mapping_equal(actual.summary, expected.summary)
    _assert_float_mapping_equal(actual.fit_params, expected.fit_params)
    _assert_float_mapping_equal(actual.event_thresholds, expected.event_thresholds)
    _assert_float_mapping_equal(actual.event_probs, expected.event_probs)
    pdt.assert_frame_equal(actual.pairwise_tail, expected.pairwise_tail)
