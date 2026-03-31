from __future__ import annotations

import sys

from pm15min.research.evaluation.methods import *  # noqa: F401,F403
from pm15min.research.evaluation.methods import __all__ as _METHODS_ALL
from pm15min.research.evaluation.methods import binary_metrics as _binary_metrics
from pm15min.research.evaluation.methods import control_variate as _control_variate
from pm15min.research.evaluation.methods import copula_risk as _copula_risk
from pm15min.research.evaluation.methods import copulas as _copulas
from pm15min.research.evaluation.methods import decision as _decision
from pm15min.research.evaluation.methods import events as _events
from pm15min.research.evaluation.methods import pipeline as _pipeline
from pm15min.research.evaluation.methods import production_stack as _production_stack
from pm15min.research.evaluation.methods import time_slices as _time_slices
from pm15min.research.evaluation.methods.abm import simulation as _abm
from pm15min.research.evaluation.methods.probability import importance_sampling as _importance_sampling
from pm15min.research.evaluation.methods.probability import mc_convergence as _mc_convergence
from pm15min.research.evaluation.methods.probability import mc_estimators as _mc_estimators
from pm15min.research.evaluation.methods.probability import path_models as _path_models
from pm15min.research.evaluation.methods.probability import types as _types
from pm15min.research.evaluation.methods.smc import particle_filter as _smc


_MODULE_ALIASES = {
    "abm": _abm,
    "brier_score": _binary_metrics,
    "control_variate": _control_variate,
    "copula_risk": _copula_risk,
    "copulas": _copulas,
    "decision": _decision,
    "events": _events,
    "importance_sampling": _importance_sampling,
    "mc_convergence": _mc_convergence,
    "mc_estimators": _mc_estimators,
    "path_models": _path_models,
    "pipeline": _pipeline,
    "production_stack": _production_stack,
    "smc": _smc,
    "time_slices": _time_slices,
    "types": _types,
}

for _name, _module in _MODULE_ALIASES.items():
    sys.modules[f"{__name__}.{_name}"] = _module
    globals()[_name] = _module

__all__ = [*_METHODS_ALL, *_MODULE_ALIASES.keys()]
