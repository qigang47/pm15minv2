from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any

from pm15min.research._contracts_frames import DateWindow, FeatureFrameSpec, LabelFrameSpec
from pm15min.research._contracts_runs import (
    BacktestRunSpec,
    EvaluationRunSpec,
    ExperimentRunSpec,
    ExperimentSuiteSpec,
    ModelBundleSpec,
)
from pm15min.research._contracts_training import TrainingRunSpec, TrainingSetSpec
from pm15min.research.layout import normalize_source_surface, normalize_target, slug_token, window_label

for _spec_cls in (
    DateWindow,
    FeatureFrameSpec,
    LabelFrameSpec,
    TrainingSetSpec,
    TrainingRunSpec,
    ModelBundleSpec,
    BacktestRunSpec,
    ExperimentSuiteSpec,
    ExperimentRunSpec,
    EvaluationRunSpec,
):
    _spec_cls.__module__ = __name__
del _spec_cls
