from __future__ import annotations

import argparse

from pm15min.research.backtests.engine import run_research_backtest
from pm15min.research.bundles.builder import build_model_bundle
from pm15min.research.cli_handlers import (
    ResearchCliDeps,
    _build_config as _build_config_impl,
    _parse_offsets as _parse_offsets_impl,
    run_research_command as _run_research_command_impl,
)
from pm15min.research.cli_parser import attach_research_subcommands as _attach_research_subcommands_impl
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import (
    BacktestRunSpec,
    DateWindow,
    EvaluationRunSpec,
    ModelBundleSpec,
    TrainingRunSpec,
    TrainingSetSpec,
)
from pm15min.research.datasets.feature_frames import build_feature_frame_dataset
from pm15min.research.datasets.training_sets import build_training_set_dataset
from pm15min.research.evaluation.calibration import run_calibration_evaluation
from pm15min.research.evaluation.drift import run_drift_evaluation
from pm15min.research.evaluation.poly_eval import run_poly_eval_report
from pm15min.research.experiments.runner import run_experiment_suite
from pm15min.research.labels.datasets import build_label_frame_dataset
from pm15min.research.service import (
    activate_model_bundle,
    describe_research_runtime,
    get_active_bundle_selection,
    list_model_bundles,
    list_training_runs,
)
from pm15min.research.training.runner import train_research_run
from pm15min.research.workflows import run_research_backfill_followups


def _build_cli_deps() -> ResearchCliDeps:
    return ResearchCliDeps(
        ResearchConfig=ResearchConfig,
        DateWindow=DateWindow,
        TrainingSetSpec=TrainingSetSpec,
        TrainingRunSpec=TrainingRunSpec,
        ModelBundleSpec=ModelBundleSpec,
        BacktestRunSpec=BacktestRunSpec,
        EvaluationRunSpec=EvaluationRunSpec,
        describe_research_runtime=describe_research_runtime,
        list_training_runs=list_training_runs,
        list_model_bundles=list_model_bundles,
        get_active_bundle_selection=get_active_bundle_selection,
        activate_model_bundle=activate_model_bundle,
        build_feature_frame_dataset=build_feature_frame_dataset,
        build_label_frame_dataset=build_label_frame_dataset,
        build_training_set_dataset=build_training_set_dataset,
        train_research_run=train_research_run,
        build_model_bundle=build_model_bundle,
        run_research_backtest=run_research_backtest,
        run_experiment_suite=run_experiment_suite,
        run_calibration_evaluation=run_calibration_evaluation,
        run_drift_evaluation=run_drift_evaluation,
        run_poly_eval_report=run_poly_eval_report,
        run_research_backfill_followups=run_research_backfill_followups,
    )


def attach_research_subcommands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    _attach_research_subcommands_impl(subparsers)


def run_research_command(args: argparse.Namespace) -> int:
    return _run_research_command_impl(args, deps=_build_cli_deps())


def _build_config(args: argparse.Namespace) -> ResearchConfig:
    return _build_config_impl(args, deps=_build_cli_deps())


def _parse_offsets(raw: str) -> tuple[int, ...]:
    return _parse_offsets_impl(raw)
