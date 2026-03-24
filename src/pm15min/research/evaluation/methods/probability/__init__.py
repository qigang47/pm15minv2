"""Probability estimation helpers migrated from the legacy poly_eval package."""

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
    simulate_stochastic_vol_paths,
)
from pm15min.research.evaluation.methods.probability.types import ProbabilityEstimate

__all__ = [
    "GaussianRandomWalk",
    "ProbabilityEstimate",
    "bernoulli_mc_stderr",
    "bernoulli_mc_variance",
    "estimate_antithetic_probability",
    "estimate_crude_probability",
    "estimate_is_exponential_tilting",
    "estimate_stratified_probability",
    "exponential_tilt_log_weights",
    "gbm_terminal_price",
    "is_estimate_with_auto_gamma",
    "required_mc_samples_for_margin",
    "simulate_gbm_terminal_prices",
    "simulate_running_p_hat",
    "simulate_stochastic_vol_paths",
    "tune_gamma_for_target_hit_rate",
    "worst_case_bernoulli_variance",
]
