from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

import pandas as pd

from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.backtests.engine import run_research_backtest
from pm15min.research.bundles.builder import build_model_bundle
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import BacktestRunSpec, ModelBundleSpec, TrainingRunSpec
from pm15min.research.datasets.feature_frames import build_feature_frame_dataset
from pm15min.research.labels.datasets import build_label_frame_dataset
from pm15min.research.manifests import build_manifest, write_manifest
from pm15min.research.training.runner import train_research_run

from .cache import ExperimentSharedCache, normalize_offsets as shared_normalize_offsets
from .compare_policy import build_variant_compare_frame
from .leaderboard import build_leaderboard
from .orchestration import (
    ExperimentRunState,
    build_case_row_prefix,
    build_execution_groups,
    build_failed_case_row,
)
from .reports import (
    build_matrix_summary_frame,
    build_experiment_compare_frame,
    build_experiment_summary,
    render_experiment_report,
)
from .specs import ExperimentRuntimePolicy, load_suite_definition


class ExperimentReporter(Protocol):
    def __call__(
        self,
        summary: str,
        *,
        current: int | None = None,
        total: int | None = None,
        current_stage: str | None = None,
        progress_pct: int | None = None,
        heartbeat: str | None = None,
    ) -> None: ...


@dataclass(frozen=True)
class _ExperimentCaseResult:
    case_key: str
    case_position: int
    case_label: str
    training_row: dict[str, object] | None
    backtest_row: dict[str, object] | None
    failed_row: dict[str, object] | None
    log_event: dict[str, object]
    success: bool


def run_experiment_suite(
    *,
    cfg: ResearchConfig,
    suite_name: str,
    run_label: str,
    reporter: ExperimentReporter | None = None,
) -> dict[str, object]:
    suite_path = _resolve_suite_spec_path(cfg, suite_name)
    suite = load_suite_definition(suite_path)
    run_dir = cfg.layout.storage.experiment_run_dir(suite.suite_name, run_label)
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / "suite.jsonl"

    shared_cache = ExperimentSharedCache.load_for_storage(cfg.layout.storage)
    run_state = ExperimentRunState.load(run_dir)
    runtime_policy = _suite_runtime_policy(suite)
    prepared_datasets = _seed_prepared_datasets(
        rows=run_state.training_rows(),
        cycle=suite.cycle,
        rewrite_root=str(cfg.layout.storage.rewrite_root),
    )
    prepared_datasets.update(set(shared_cache.prepared_datasets.keys()))
    training_cache = _seed_shared_training_cache(shared_cache)
    training_cache.update(_seed_training_cache(run_state.training_rows()))
    bundle_cache = _seed_shared_bundle_cache(shared_cache)
    bundle_cache.update(_seed_bundle_cache(run_state.training_rows()))
    execution_groups = build_execution_groups(suite.markets)
    total_cases = sum(len(group.market_specs) for group in execution_groups)
    total_groups = len(execution_groups)
    completed_cases = 0
    _emit_experiment_progress(
        reporter,
        summary=f"Starting experiment suite {suite.suite_name}",
        current=0,
        total=total_cases,
        current_stage="experiment_suite",
        progress_pct=0,
    )

    for group_position, execution_group in enumerate(execution_groups, start=1):
        _emit_experiment_progress(
            reporter,
            summary=f"Group {group_position}/{len(execution_groups)}: {execution_group.group_label} ({len(execution_group.market_specs)} cases)",
            current=group_position,
            total=total_groups,
            current_stage="experiment_group",
            progress_pct=_suite_progress_pct(completed_cases, total_cases),
        )
        _append_suite_log(
            log_path,
            {
                "event": "execution_group_started",
                "group_key": execution_group.group_key,
                "group_label": execution_group.group_label,
                "cases": len(execution_group.market_specs),
            },
        )
        queued_case_contexts: list[dict[str, object]] = []
        group_base_completed = completed_cases
        for group_index, market_spec in enumerate(execution_group.market_specs, start=1):
            case_position = group_base_completed + group_index
            case_label = f"{market_spec.market}/{_run_name(market_spec)}"
            case_progress_markers = _case_progress_markers(has_secondary=bool(market_spec.hybrid_secondary_target))
            case_key = _case_key(market_spec)
            case_row_prefix = build_case_row_prefix(market_spec, case_key=case_key)
            case_plan = run_state.plan_case(
                market_spec=market_spec,
                case_key=case_key,
                runtime_policy=runtime_policy,
            )
            _emit_case_progress(
                reporter,
                case_position=case_position,
                total_cases=total_cases,
                summary=f"Running case {case_position}/{total_cases}: {case_label}",
                case_progress=0.0,
            )
            case_reporter = _case_nested_reporter(
                reporter,
                case_position=case_position,
                total_cases=total_cases,
                case_label=case_label,
                case_progress_start=case_progress_markers["backtest"][0],
                case_progress_end=case_progress_markers["backtest"][1],
            )
            if case_plan.action == "resume_existing":
                run_state.apply_planned_case(case_plan)
                _append_suite_log(
                    log_path,
                    {
                        "event": "market_resumed",
                        "market": market_spec.market,
                        "case_key": case_key,
                        "group_name": _group_name(market_spec),
                        "run_name": _run_name(market_spec),
                        "variant_label": market_spec.variant_label,
                        "summary_path": None if case_plan.backtest_row is None else case_plan.backtest_row.get("summary_path"),
                    },
                )
                _persist_runtime_state(
                    cfg=cfg,
                    suite=suite,
                    suite_path=suite_path,
                    run_label=run_label,
                    run_dir=run_dir,
                    log_path=log_path,
                    shared_cache=shared_cache,
                    run_state=run_state,
                    cycle=suite.cycle,
                    rewrite_root=str(cfg.layout.storage.rewrite_root),
                )
                completed_cases += 1
                _emit_case_progress(
                    reporter,
                    case_position=completed_cases,
                    total_cases=total_cases,
                    summary=f"Resumed case {case_position}/{total_cases}: {case_label}",
                    case_progress=1.0,
                )
                continue
            if case_plan.action == "retain_failed":
                run_state.apply_planned_case(case_plan)
                _append_suite_log(
                    log_path,
                    {
                        "event": "market_failed_retained",
                        "market": market_spec.market,
                        "case_key": case_key,
                        "group_name": _group_name(market_spec),
                        "run_name": _run_name(market_spec),
                        "variant_label": market_spec.variant_label,
                        "failure_stage": None if case_plan.failed_row is None else case_plan.failed_row.get("failure_stage"),
                        "error_type": None if case_plan.failed_row is None else case_plan.failed_row.get("error_type"),
                        "error_message": None if case_plan.failed_row is None else case_plan.failed_row.get("error_message"),
                    },
                )
                _persist_runtime_state(
                    cfg=cfg,
                    suite=suite,
                    suite_path=suite_path,
                    run_label=run_label,
                    run_dir=run_dir,
                    log_path=log_path,
                    shared_cache=shared_cache,
                    run_state=run_state,
                    cycle=suite.cycle,
                    rewrite_root=str(cfg.layout.storage.rewrite_root),
                )
                completed_cases += 1
                _emit_case_progress(
                    reporter,
                    case_position=case_position,
                    total_cases=total_cases,
                    summary=f"Retained failed case {case_position}/{total_cases}: {case_label}",
                    case_progress=1.0,
                )
                continue
            if runtime_policy.parallel_case_workers > 1:
                queued_case_contexts.append(
                    {
                        "market_spec": market_spec,
                        "case_position": case_position,
                        "case_label": case_label,
                        "case_progress_markers": case_progress_markers,
                        "case_key": case_key,
                        "case_row_prefix": case_row_prefix,
                    }
                )
                continue

            train_summary = None
            bundle_summary = None
            secondary_train_summary = None
            secondary_bundle_summary = None
            training_reused = False
            bundle_reused = False
            secondary_training_reused = False
            secondary_bundle_reused = False
            case_succeeded = False
            failure_stage = "prepare_market_cfg"
            try:
                market_cfg = _build_market_cfg(
                    root_cfg=cfg,
                    cycle=suite.cycle,
                    market_spec=market_spec,
                    target=market_spec.target,
                )

                failure_stage = "prepare_datasets"
                _ensure_market_datasets(market_cfg=market_cfg, prepared_datasets=prepared_datasets)
                _emit_case_progress(
                    reporter,
                    case_position=case_position,
                    total_cases=total_cases,
                    summary=f"Prepared datasets for {case_label}",
                    case_progress=float(case_progress_markers["datasets_ready"]),
                )
                failure_stage = "train_primary"
                train_summary, training_reused = _resolve_training_summary(
                    market_cfg=market_cfg,
                    market_spec=market_spec,
                    run_label=run_label,
                    target=market_spec.target,
                    offsets=market_spec.offsets,
                    training_cache=training_cache,
                    reporter=_case_nested_reporter(
                        reporter,
                        case_position=case_position,
                        total_cases=total_cases,
                        case_label=case_label,
                        case_progress_start=case_progress_markers["primary_training"][0],
                        case_progress_end=case_progress_markers["primary_training"][1],
                    ),
                )
                _emit_case_progress(
                    reporter,
                    case_position=case_position,
                    total_cases=total_cases,
                    summary=f"Resolved primary training for {case_label}",
                    case_progress=float(case_progress_markers["primary_training_done"]),
                )
                failure_stage = "bundle_primary"
                bundle_summary, bundle_reused = _resolve_bundle_summary(
                    market_cfg=market_cfg,
                    market_spec=market_spec,
                    run_label=run_label,
                    target=market_spec.target,
                    offsets=market_spec.offsets,
                    train_summary=train_summary,
                    bundle_cache=bundle_cache,
                )
                _emit_case_progress(
                    reporter,
                    case_position=case_position,
                    total_cases=total_cases,
                    summary=f"Prepared primary bundle for {case_label}",
                    case_progress=float(case_progress_markers["primary_bundle_done"]),
                )
                if market_spec.hybrid_secondary_target:
                    secondary_target = str(market_spec.hybrid_secondary_target)
                    secondary_offsets = market_spec.hybrid_secondary_offsets or market_spec.offsets
                    failure_stage = "prepare_secondary_cfg"
                    secondary_cfg = _build_market_cfg(
                        root_cfg=cfg,
                        cycle=suite.cycle,
                        market_spec=market_spec,
                        target=secondary_target,
                    )
                    failure_stage = "prepare_secondary_datasets"
                    _ensure_market_datasets(market_cfg=secondary_cfg, prepared_datasets=prepared_datasets)
                    failure_stage = "train_secondary"
                    secondary_train_summary, secondary_training_reused = _resolve_training_summary(
                        market_cfg=secondary_cfg,
                        market_spec=market_spec,
                        run_label=run_label,
                        target=secondary_target,
                        offsets=secondary_offsets,
                        training_cache=training_cache,
                        reporter=_case_nested_reporter(
                            reporter,
                            case_position=case_position,
                            total_cases=total_cases,
                            case_label=case_label,
                            case_progress_start=case_progress_markers["secondary_training"][0],
                            case_progress_end=case_progress_markers["secondary_training"][1],
                        ),
                    )
                    _emit_case_progress(
                        reporter,
                        case_position=case_position,
                        total_cases=total_cases,
                        summary=f"Resolved secondary training for {case_label}",
                        case_progress=float(case_progress_markers["secondary_training_done"]),
                    )
                    failure_stage = "bundle_secondary"
                    secondary_bundle_summary, secondary_bundle_reused = _resolve_bundle_summary(
                        market_cfg=secondary_cfg,
                        market_spec=market_spec,
                        run_label=run_label,
                        target=secondary_target,
                        offsets=secondary_offsets,
                        train_summary=secondary_train_summary,
                        bundle_cache=bundle_cache,
                    )
                    _emit_case_progress(
                        reporter,
                        case_position=case_position,
                        total_cases=total_cases,
                        summary=f"Prepared secondary bundle for {case_label}",
                        case_progress=float(case_progress_markers["secondary_bundle_done"]),
                    )
                failure_stage = "backtest"
                _emit_case_progress(
                    reporter,
                    case_position=case_position,
                    total_cases=total_cases,
                    summary=f"Starting backtest for {case_label}",
                    case_progress=float(case_progress_markers["backtest"][0]),
                )
                backtest_spec = BacktestRunSpec(
                    profile=market_spec.profile,
                    spec_name=market_spec.backtest_spec,
                    run_label=_backtest_run_label(run_label=run_label, market_spec=market_spec, case_key=case_key),
                    target=market_spec.target,
                    bundle_label=str(bundle_summary["bundle_label"]),
                    secondary_target=market_spec.hybrid_secondary_target,
                    secondary_bundle_label=(
                        None if secondary_bundle_summary is None else str(secondary_bundle_summary["bundle_label"])
                    ),
                    fallback_reasons=market_spec.hybrid_fallback_reasons,
                    variant_label=market_spec.variant_label,
                    variant_notes=market_spec.variant_notes,
                    stake_usd=_stake_usd(market_spec),
                    max_notional_usd=_max_notional_usd(market_spec),
                    parity=market_spec.parity,
                )
                try:
                    backtest_summary = run_research_backtest(
                        market_cfg,
                        backtest_spec,
                        reporter=case_reporter,
                    )
                except TypeError:
                    backtest_summary = run_research_backtest(
                        market_cfg,
                        backtest_spec,
                    )
                _emit_case_progress(
                    reporter,
                    case_position=case_position,
                    total_cases=total_cases,
                    summary=f"Finished backtest for {case_label}",
                    case_progress=float(case_progress_markers["backtest_done"]),
                )
                parity_payload = market_spec.parity.to_dict()
                run_state.set_training_row(
                    {
                        **case_row_prefix,
                        "feature_set": market_spec.feature_set,
                        "label_set": market_spec.label_set,
                        "model_family": market_spec.model_family,
                        "window": market_spec.window.label,
                        "offsets": list(market_spec.offsets),
                        "training_run_dir": train_summary["run_dir"],
                        "bundle_dir": bundle_summary["bundle_dir"],
                        "training_reused": bool(training_reused),
                        "bundle_reused": bool(bundle_reused),
                        "resumed_from_existing": False,
                        "secondary_target": market_spec.hybrid_secondary_target,
                        "secondary_training_run_dir": None if secondary_train_summary is None else secondary_train_summary["run_dir"],
                        "secondary_bundle_dir": None if secondary_bundle_summary is None else secondary_bundle_summary["bundle_dir"],
                        "secondary_training_reused": bool(secondary_training_reused),
                        "secondary_bundle_reused": bool(secondary_bundle_reused),
                        "parity_spec_json": _json_text(parity_payload),
                    }
                )
                run_state.set_backtest_row(
                    {
                        **case_row_prefix,
                        "feature_set": market_spec.feature_set,
                        "bundle_dir": bundle_summary["bundle_dir"],
                        "secondary_bundle_dir": None if secondary_bundle_summary is None else secondary_bundle_summary["bundle_dir"],
                        "backtest_run_dir": backtest_summary["run_dir"],
                        "summary_path": backtest_summary["summary_path"],
                        "resumed_from_existing": False,
                        "parity_spec_json": _json_text(parity_payload),
                        **_load_backtest_metrics(Path(backtest_summary["summary_path"])),
                    }
                )
                run_state.drop_failed_case(case_key=case_key)
                _append_suite_log(
                    log_path,
                    {
                        "event": "market_completed",
                        "market": market_spec.market,
                        "case_key": case_key,
                        "group_name": _group_name(market_spec),
                        "run_name": _run_name(market_spec),
                        "training_run_dir": train_summary["run_dir"],
                        "bundle_dir": bundle_summary["bundle_dir"],
                        "training_reused": bool(training_reused),
                        "bundle_reused": bool(bundle_reused),
                        "secondary_training_run_dir": None if secondary_train_summary is None else secondary_train_summary["run_dir"],
                        "secondary_bundle_dir": None if secondary_bundle_summary is None else secondary_bundle_summary["bundle_dir"],
                        "backtest_run_dir": backtest_summary["run_dir"],
                        "parity_spec": parity_payload,
                    },
                )
                case_succeeded = True
            except Exception as exc:
                if train_summary is not None or bundle_summary is not None:
                    run_state.set_training_row(
                        {
                            **case_row_prefix,
                            "feature_set": market_spec.feature_set,
                            "label_set": market_spec.label_set,
                            "model_family": market_spec.model_family,
                            "window": market_spec.window.label,
                            "offsets": list(market_spec.offsets),
                            "training_run_dir": None if train_summary is None else train_summary.get("run_dir"),
                            "bundle_dir": None if bundle_summary is None else bundle_summary.get("bundle_dir"),
                            "training_reused": bool(training_reused),
                            "bundle_reused": bool(bundle_reused),
                            "resumed_from_existing": False,
                            "secondary_target": market_spec.hybrid_secondary_target,
                            "secondary_training_run_dir": None if secondary_train_summary is None else secondary_train_summary.get("run_dir"),
                            "secondary_bundle_dir": None if secondary_bundle_summary is None else secondary_bundle_summary.get("bundle_dir"),
                            "secondary_training_reused": bool(secondary_training_reused),
                            "secondary_bundle_reused": bool(secondary_bundle_reused),
                            "parity_spec_json": _json_text(market_spec.parity.to_dict()),
                        }
                    )
                run_state.set_failed_row(
                    build_failed_case_row(
                        market_spec,
                        case_key=case_key,
                        failure_stage=failure_stage,
                        error=exc,
                        train_summary=train_summary,
                        bundle_summary=bundle_summary,
                        secondary_train_summary=secondary_train_summary,
                        secondary_bundle_summary=secondary_bundle_summary,
                    ),
                )
                _append_suite_log(
                    log_path,
                    {
                        "event": "market_failed",
                        "market": market_spec.market,
                        "case_key": case_key,
                        "group_name": _group_name(market_spec),
                        "run_name": _run_name(market_spec),
                        "variant_label": market_spec.variant_label,
                        "failure_stage": failure_stage,
                        "error_type": exc.__class__.__name__,
                        "error_message": str(exc),
                    },
                )
            _persist_runtime_state(
                cfg=cfg,
                suite=suite,
                suite_path=suite_path,
                run_label=run_label,
                run_dir=run_dir,
                log_path=log_path,
                shared_cache=shared_cache,
                run_state=run_state,
                cycle=suite.cycle,
                rewrite_root=str(cfg.layout.storage.rewrite_root),
            )
            completed_cases += 1
            _emit_case_progress(
                reporter,
                case_position=completed_cases,
                total_cases=total_cases,
                summary=(
                    f"Completed case {case_position}/{total_cases}: {case_label}"
                    if case_succeeded
                    else f"Failed case {case_position}/{total_cases}: {case_label}"
                ),
                case_progress=1.0,
            )
        if queued_case_contexts:
            first_context, *remaining_contexts = queued_case_contexts
            first_result = _execute_experiment_case(
                cfg=cfg,
                suite=suite,
                run_label=run_label,
                market_spec=first_context["market_spec"],
                case_position=int(first_context["case_position"]),
                total_cases=total_cases,
                case_label=str(first_context["case_label"]),
                case_progress_markers=first_context["case_progress_markers"],
                case_key=str(first_context["case_key"]),
                case_row_prefix=first_context["case_row_prefix"],
                prepared_datasets=prepared_datasets,
                training_cache=training_cache,
                bundle_cache=bundle_cache,
                reporter=reporter,
            )
            completed_cases = _apply_experiment_case_result(
                completed_cases=completed_cases,
                result=first_result,
                cfg=cfg,
                suite=suite,
                suite_path=suite_path,
                run_label=run_label,
                run_dir=run_dir,
                log_path=log_path,
                shared_cache=shared_cache,
                run_state=run_state,
                cycle=suite.cycle,
                rewrite_root=str(cfg.layout.storage.rewrite_root),
                reporter=reporter,
                total_cases=total_cases,
            )
            if remaining_contexts and first_result.success:
                worker_count = min(int(runtime_policy.parallel_case_workers), len(remaining_contexts))
                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    future_map = {
                        executor.submit(
                            _execute_experiment_case,
                            cfg=cfg,
                            suite=suite,
                            run_label=run_label,
                            market_spec=context["market_spec"],
                            case_position=int(context["case_position"]),
                            total_cases=total_cases,
                            case_label=str(context["case_label"]),
                            case_progress_markers=context["case_progress_markers"],
                            case_key=str(context["case_key"]),
                            case_row_prefix=context["case_row_prefix"],
                            prepared_datasets=prepared_datasets,
                            training_cache=training_cache,
                            bundle_cache=bundle_cache,
                            reporter=reporter,
                        ): context
                        for context in remaining_contexts
                    }
                    for future in as_completed(future_map):
                        completed_cases = _apply_experiment_case_result(
                            completed_cases=completed_cases,
                            result=future.result(),
                            cfg=cfg,
                            suite=suite,
                            suite_path=suite_path,
                            run_label=run_label,
                            run_dir=run_dir,
                            log_path=log_path,
                            shared_cache=shared_cache,
                            run_state=run_state,
                            cycle=suite.cycle,
                            rewrite_root=str(cfg.layout.storage.rewrite_root),
                            reporter=reporter,
                            total_cases=total_cases,
                        )
        _append_suite_log(
            log_path,
            {
                "event": "execution_group_completed",
                "group_key": execution_group.group_key,
                "group_label": execution_group.group_label,
                "cases": len(execution_group.market_specs),
            },
        )

    outputs = _persist_runtime_state(
        cfg=cfg,
        suite=suite,
        suite_path=suite_path,
        run_label=run_label,
        run_dir=run_dir,
        log_path=log_path,
        shared_cache=shared_cache,
        run_state=run_state,
        cycle=suite.cycle,
        rewrite_root=str(cfg.layout.storage.rewrite_root),
    )
    _emit_experiment_progress(
        reporter,
        summary=f"Experiment suite {suite.suite_name} completed",
        current=total_cases,
        total=total_cases,
        current_stage="finished",
        progress_pct=100,
    )
    return {
        "dataset": "experiment_run",
        "suite_name": suite.suite_name,
        "cycle": suite.cycle,
        "markets": [market.market for market in suite.markets],
        "run_label": run_label,
        "runtime_policy": _suite_runtime_policy_payload(suite),
        **outputs,
    }


def _execute_experiment_case(
    *,
    cfg: ResearchConfig,
    suite,
    run_label: str,
    market_spec,
    case_position: int,
    total_cases: int,
    case_label: str,
    case_progress_markers: dict[str, object],
    case_key: str,
    case_row_prefix: dict[str, object],
    prepared_datasets: set[str],
    training_cache: dict[str, dict[str, object]],
    bundle_cache: dict[str, dict[str, object]],
    reporter: ExperimentReporter | None,
) -> _ExperimentCaseResult:
    case_reporter = _case_nested_reporter(
        reporter,
        case_position=case_position,
        total_cases=total_cases,
        case_label=case_label,
        case_progress_start=case_progress_markers["backtest"][0],
        case_progress_end=case_progress_markers["backtest"][1],
    )
    train_summary = None
    bundle_summary = None
    secondary_train_summary = None
    secondary_bundle_summary = None
    training_reused = False
    bundle_reused = False
    secondary_training_reused = False
    secondary_bundle_reused = False
    failure_stage = "prepare_market_cfg"
    try:
        market_cfg = _build_market_cfg(
            root_cfg=cfg,
            cycle=suite.cycle,
            market_spec=market_spec,
            target=market_spec.target,
        )
        failure_stage = "prepare_datasets"
        _ensure_market_datasets(market_cfg=market_cfg, prepared_datasets=prepared_datasets)
        _emit_case_progress(
            reporter,
            case_position=case_position,
            total_cases=total_cases,
            summary=f"Prepared datasets for {case_label}",
            case_progress=float(case_progress_markers["datasets_ready"]),
        )
        failure_stage = "train_primary"
        train_summary, training_reused = _resolve_training_summary(
            market_cfg=market_cfg,
            market_spec=market_spec,
            run_label=run_label,
            target=market_spec.target,
            offsets=market_spec.offsets,
            training_cache=training_cache,
            reporter=_case_nested_reporter(
                reporter,
                case_position=case_position,
                total_cases=total_cases,
                case_label=case_label,
                case_progress_start=case_progress_markers["primary_training"][0],
                case_progress_end=case_progress_markers["primary_training"][1],
            ),
        )
        _emit_case_progress(
            reporter,
            case_position=case_position,
            total_cases=total_cases,
            summary=f"Resolved primary training for {case_label}",
            case_progress=float(case_progress_markers["primary_training_done"]),
        )
        failure_stage = "bundle_primary"
        bundle_summary, bundle_reused = _resolve_bundle_summary(
            market_cfg=market_cfg,
            market_spec=market_spec,
            run_label=run_label,
            target=market_spec.target,
            offsets=market_spec.offsets,
            train_summary=train_summary,
            bundle_cache=bundle_cache,
        )
        _emit_case_progress(
            reporter,
            case_position=case_position,
            total_cases=total_cases,
            summary=f"Prepared primary bundle for {case_label}",
            case_progress=float(case_progress_markers["primary_bundle_done"]),
        )
        if market_spec.hybrid_secondary_target:
            secondary_target = str(market_spec.hybrid_secondary_target)
            secondary_offsets = market_spec.hybrid_secondary_offsets or market_spec.offsets
            failure_stage = "prepare_secondary_cfg"
            secondary_cfg = _build_market_cfg(
                root_cfg=cfg,
                cycle=suite.cycle,
                market_spec=market_spec,
                target=secondary_target,
            )
            failure_stage = "prepare_secondary_datasets"
            _ensure_market_datasets(market_cfg=secondary_cfg, prepared_datasets=prepared_datasets)
            failure_stage = "train_secondary"
            secondary_train_summary, secondary_training_reused = _resolve_training_summary(
                market_cfg=secondary_cfg,
                market_spec=market_spec,
                run_label=run_label,
                target=secondary_target,
                offsets=secondary_offsets,
                training_cache=training_cache,
                reporter=_case_nested_reporter(
                    reporter,
                    case_position=case_position,
                    total_cases=total_cases,
                    case_label=case_label,
                    case_progress_start=case_progress_markers["secondary_training"][0],
                    case_progress_end=case_progress_markers["secondary_training"][1],
                ),
            )
            _emit_case_progress(
                reporter,
                case_position=case_position,
                total_cases=total_cases,
                summary=f"Resolved secondary training for {case_label}",
                case_progress=float(case_progress_markers["secondary_training_done"]),
            )
            failure_stage = "bundle_secondary"
            secondary_bundle_summary, secondary_bundle_reused = _resolve_bundle_summary(
                market_cfg=secondary_cfg,
                market_spec=market_spec,
                run_label=run_label,
                target=secondary_target,
                offsets=secondary_offsets,
                train_summary=secondary_train_summary,
                bundle_cache=bundle_cache,
            )
            _emit_case_progress(
                reporter,
                case_position=case_position,
                total_cases=total_cases,
                summary=f"Prepared secondary bundle for {case_label}",
                case_progress=float(case_progress_markers["secondary_bundle_done"]),
            )
        failure_stage = "backtest"
        _emit_case_progress(
            reporter,
            case_position=case_position,
            total_cases=total_cases,
            summary=f"Starting backtest for {case_label}",
            case_progress=float(case_progress_markers["backtest"][0]),
        )
        backtest_spec = BacktestRunSpec(
            profile=market_spec.profile,
            spec_name=market_spec.backtest_spec,
            run_label=_backtest_run_label(run_label=run_label, market_spec=market_spec, case_key=case_key),
            target=market_spec.target,
            bundle_label=str(bundle_summary["bundle_label"]),
            secondary_target=market_spec.hybrid_secondary_target,
            secondary_bundle_label=(
                None if secondary_bundle_summary is None else str(secondary_bundle_summary["bundle_label"])
            ),
            fallback_reasons=market_spec.hybrid_fallback_reasons,
            variant_label=market_spec.variant_label,
            variant_notes=market_spec.variant_notes,
            stake_usd=_stake_usd(market_spec),
            max_notional_usd=_max_notional_usd(market_spec),
            parity=market_spec.parity,
        )
        try:
            backtest_summary = run_research_backtest(
                market_cfg,
                backtest_spec,
                reporter=case_reporter,
            )
        except TypeError:
            backtest_summary = run_research_backtest(
                market_cfg,
                backtest_spec,
            )
        _emit_case_progress(
            reporter,
            case_position=case_position,
            total_cases=total_cases,
            summary=f"Finished backtest for {case_label}",
            case_progress=float(case_progress_markers["backtest_done"]),
        )
        parity_payload = market_spec.parity.to_dict()
        return _ExperimentCaseResult(
            case_key=case_key,
            case_position=case_position,
            case_label=case_label,
            training_row={
                **case_row_prefix,
                "feature_set": market_spec.feature_set,
                "label_set": market_spec.label_set,
                "model_family": market_spec.model_family,
                "window": market_spec.window.label,
                "offsets": list(market_spec.offsets),
                "training_run_dir": train_summary["run_dir"],
                "bundle_dir": bundle_summary["bundle_dir"],
                "training_reused": bool(training_reused),
                "bundle_reused": bool(bundle_reused),
                "resumed_from_existing": False,
                "secondary_target": market_spec.hybrid_secondary_target,
                "secondary_training_run_dir": None if secondary_train_summary is None else secondary_train_summary["run_dir"],
                "secondary_bundle_dir": None if secondary_bundle_summary is None else secondary_bundle_summary["bundle_dir"],
                "secondary_training_reused": bool(secondary_training_reused),
                "secondary_bundle_reused": bool(secondary_bundle_reused),
                "parity_spec_json": _json_text(parity_payload),
            },
            backtest_row={
                **case_row_prefix,
                "feature_set": market_spec.feature_set,
                "bundle_dir": bundle_summary["bundle_dir"],
                "secondary_bundle_dir": None if secondary_bundle_summary is None else secondary_bundle_summary["bundle_dir"],
                "backtest_run_dir": backtest_summary["run_dir"],
                "summary_path": backtest_summary["summary_path"],
                "resumed_from_existing": False,
                "parity_spec_json": _json_text(parity_payload),
                **_load_backtest_metrics(Path(backtest_summary["summary_path"])),
            },
            failed_row=None,
            log_event={
                "event": "market_completed",
                "market": market_spec.market,
                "case_key": case_key,
                "group_name": _group_name(market_spec),
                "run_name": _run_name(market_spec),
                "training_run_dir": train_summary["run_dir"],
                "bundle_dir": bundle_summary["bundle_dir"],
                "training_reused": bool(training_reused),
                "bundle_reused": bool(bundle_reused),
                "secondary_training_run_dir": None if secondary_train_summary is None else secondary_train_summary["run_dir"],
                "secondary_bundle_dir": None if secondary_bundle_summary is None else secondary_bundle_summary["bundle_dir"],
                "backtest_run_dir": backtest_summary["run_dir"],
                "parity_spec": parity_payload,
            },
            success=True,
        )
    except Exception as exc:
        training_row = None
        if train_summary is not None or bundle_summary is not None:
            training_row = {
                **case_row_prefix,
                "feature_set": market_spec.feature_set,
                "label_set": market_spec.label_set,
                "model_family": market_spec.model_family,
                "window": market_spec.window.label,
                "offsets": list(market_spec.offsets),
                "training_run_dir": None if train_summary is None else train_summary.get("run_dir"),
                "bundle_dir": None if bundle_summary is None else bundle_summary.get("bundle_dir"),
                "training_reused": bool(training_reused),
                "bundle_reused": bool(bundle_reused),
                "resumed_from_existing": False,
                "secondary_target": market_spec.hybrid_secondary_target,
                "secondary_training_run_dir": None if secondary_train_summary is None else secondary_train_summary.get("run_dir"),
                "secondary_bundle_dir": None if secondary_bundle_summary is None else secondary_bundle_summary.get("bundle_dir"),
                "secondary_training_reused": bool(secondary_training_reused),
                "secondary_bundle_reused": bool(secondary_bundle_reused),
                "parity_spec_json": _json_text(market_spec.parity.to_dict()),
            }
        return _ExperimentCaseResult(
            case_key=case_key,
            case_position=case_position,
            case_label=case_label,
            training_row=training_row,
            backtest_row=None,
            failed_row=build_failed_case_row(
                market_spec,
                case_key=case_key,
                failure_stage=failure_stage,
                error=exc,
                train_summary=train_summary,
                bundle_summary=bundle_summary,
                secondary_train_summary=secondary_train_summary,
                secondary_bundle_summary=secondary_bundle_summary,
            ),
            log_event={
                "event": "market_failed",
                "market": market_spec.market,
                "case_key": case_key,
                "group_name": _group_name(market_spec),
                "run_name": _run_name(market_spec),
                "variant_label": market_spec.variant_label,
                "failure_stage": failure_stage,
                "error_type": exc.__class__.__name__,
                "error_message": str(exc),
            },
            success=False,
        )


def _apply_experiment_case_result(
    *,
    completed_cases: int,
    result: _ExperimentCaseResult,
    cfg: ResearchConfig,
    suite,
    suite_path: Path,
    run_label: str,
    run_dir: Path,
    log_path: Path,
    shared_cache: ExperimentSharedCache,
    run_state: ExperimentRunState,
    cycle: str,
    rewrite_root: str,
    reporter: ExperimentReporter | None,
    total_cases: int,
) -> int:
    if result.training_row is not None:
        run_state.set_training_row(result.training_row)
    if result.backtest_row is not None:
        run_state.set_backtest_row(result.backtest_row)
        run_state.drop_failed_case(case_key=result.case_key)
    if result.failed_row is not None:
        run_state.set_failed_row(result.failed_row)
    _append_suite_log(log_path, result.log_event)
    _persist_runtime_state(
        cfg=cfg,
        suite=suite,
        suite_path=suite_path,
        run_label=run_label,
        run_dir=run_dir,
        log_path=log_path,
        shared_cache=shared_cache,
        run_state=run_state,
        cycle=cycle,
        rewrite_root=rewrite_root,
    )
    new_completed_cases = completed_cases + 1
    _emit_case_progress(
        reporter,
        case_position=result.case_position,
        total_cases=total_cases,
        summary=(
            f"Completed case {result.case_position}/{total_cases}: {result.case_label}"
            if result.success
            else f"Failed case {result.case_position}/{total_cases}: {result.case_label}"
        ),
        case_progress=1.0,
    )
    return new_completed_cases


def _emit_experiment_progress(
    reporter: ExperimentReporter | None,
    *,
    summary: str,
    current: int | None,
    total: int | None,
    current_stage: str,
    progress_pct: int,
    heartbeat: str | None = None,
) -> None:
    if reporter is None:
        return
    reporter(
        summary=summary,
        current=current,
        total=total,
        current_stage=current_stage,
        progress_pct=progress_pct,
        heartbeat=heartbeat,
    )


def _suite_progress_pct(current: int, total: int) -> int:
    if total <= 0:
        return 100
    return int(max(0, min(100, round((float(current) / float(total)) * 100))))


def _active_case_current(completed_cases: int, total_cases: int) -> int:
    if total_cases <= 0:
        return 0
    return min(max(int(completed_cases) + 1, 1), int(total_cases))


def _case_progress_pct(*, case_position: int, total_cases: int, case_progress: float) -> int:
    if total_cases <= 0:
        return 100
    bounded_case_position = min(max(int(case_position), 1), int(total_cases))
    bounded_case_progress = max(0.0, min(1.0, float(case_progress)))
    suite_ratio = ((bounded_case_position - 1) + bounded_case_progress) / float(total_cases)
    return int(max(0, min(100, round(suite_ratio * 100.0))))


def _emit_case_progress(
    reporter: ExperimentReporter | None,
    *,
    case_position: int,
    total_cases: int,
    summary: str,
    case_progress: float,
    current_stage: str = "experiment_cases",
    heartbeat: str | None = None,
) -> None:
    _emit_experiment_progress(
        reporter,
        summary=summary,
        current=min(max(int(case_position), 1), int(total_cases)) if total_cases > 0 else 0,
        total=total_cases,
        current_stage=current_stage,
        progress_pct=_case_progress_pct(
            case_position=case_position,
            total_cases=total_cases,
            case_progress=case_progress,
        ),
        heartbeat=heartbeat,
    )


def _case_progress_markers(*, has_secondary: bool) -> dict[str, object]:
    if has_secondary:
        return {
            "datasets_ready": 0.05,
            "primary_training": (0.05, 0.30),
            "primary_training_done": 0.30,
            "primary_bundle_done": 0.40,
            "secondary_training": (0.45, 0.65),
            "secondary_training_done": 0.65,
            "secondary_bundle_done": 0.75,
            "backtest": (0.80, 0.98),
            "backtest_done": 0.98,
        }
    return {
        "datasets_ready": 0.10,
        "primary_training": (0.10, 0.55),
        "primary_training_done": 0.55,
        "primary_bundle_done": 0.65,
        "secondary_training": None,
        "secondary_training_done": None,
        "secondary_bundle_done": None,
        "backtest": (0.70, 0.98),
        "backtest_done": 0.98,
    }


def _case_nested_reporter(
    reporter: ExperimentReporter | None,
    *,
    case_position: int,
    total_cases: int,
    case_label: str,
    case_progress_start: float,
    case_progress_end: float,
) -> ExperimentReporter | None:
    if reporter is None:
        return None

    bounded_start = max(0.0, min(1.0, float(case_progress_start)))
    bounded_end = max(bounded_start, min(1.0, float(case_progress_end)))

    def _report(
        summary: str,
        *,
        current: int | None = None,
        total: int | None = None,
        current_stage: str | None = None,
        progress_pct: int | None = None,
        heartbeat: str | None = None,
    ) -> None:
        del current, total
        nested_ratio = 0.0 if progress_pct is None else max(0.0, min(1.0, float(progress_pct) / 100.0))
        case_progress = bounded_start + ((bounded_end - bounded_start) * nested_ratio)
        reporter(
            summary=f"{case_label}: {summary}",
            current=_active_case_current(case_position - 1, total_cases),
            total=total_cases,
            current_stage=current_stage or "experiment_case_detail",
            progress_pct=_case_progress_pct(
                case_position=case_position,
                total_cases=total_cases,
                case_progress=case_progress,
            ),
            heartbeat=heartbeat,
        )

    return _report


def _resolve_suite_spec_path(cfg: ResearchConfig, suite_name: str) -> Path:
    candidate = Path(suite_name)
    if candidate.exists():
        return candidate
    path = cfg.layout.storage.suite_spec_path(suite_name)
    if not path.exists():
        raise FileNotFoundError(f"Experiment suite spec not found: {path}")
    return path


def _build_market_cfg(
    *,
    root_cfg: ResearchConfig,
    cycle: str,
    market_spec,
    target: str,
) -> ResearchConfig:
    return ResearchConfig.build(
        market=market_spec.market,
        cycle=cycle,
        profile=market_spec.profile,
        source_surface="backtest",
        feature_set=market_spec.feature_set,
        label_set=market_spec.label_set,
        target=target,
        model_family=market_spec.model_family,
        root=root_cfg.layout.storage.rewrite_root,
    )


def _load_backtest_metrics(summary_path: Path) -> dict[str, object]:
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    return {
        "trades": int(payload.get("trades", 0)),
        "rejects": int(payload.get("rejects", 0)),
        "wins": int(payload.get("wins", 0)),
        "losses": int(payload.get("losses", 0)),
        "pnl_sum": float(payload.get("pnl_sum", 0.0)),
        "stake_sum": float(payload.get("stake_sum", 0.0)),
        "roi_pct": float(payload.get("roi_pct", 0.0)),
    }


def _append_suite_log(path: Path, event: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False, sort_keys=True))
        fh.write("\n")


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _seed_prepared_datasets(
    *,
    rows,
    cycle: str,
    rewrite_root: str,
) -> set[tuple[str, ...]]:
    prepared: set[tuple[str, ...]] = set()
    for row in rows:
        market = str(row.get("market") or "").strip()
        profile = str(row.get("profile") or "").strip()
        feature_set = str(row.get("feature_set") or "").strip()
        label_set = str(row.get("label_set") or "").strip()
        if not market or not profile or not feature_set or not label_set:
            continue
        prepared.add((market, cycle, profile, feature_set, label_set, rewrite_root))
    return prepared


def _seed_training_cache(rows) -> dict[str, dict[str, object]]:
    cache: dict[str, dict[str, object]] = {}
    for row in rows:
        training_run_dir = str(row.get("training_run_dir") or "").strip()
        if not training_run_dir:
            continue
        offsets = _normalize_offsets(row.get("offsets"))
        if not offsets:
            continue
        key = _training_cache_key(
            market=str(row.get("market") or ""),
            profile=str(row.get("profile") or ""),
            model_family=str(row.get("model_family") or ""),
            feature_set=str(row.get("feature_set") or ""),
            label_set=str(row.get("label_set") or ""),
            target=str(row.get("target") or ""),
            window_label=str(row.get("window") or ""),
            offsets=offsets,
        )
        cache[key] = {
            "run_dir": training_run_dir,
            "run_label": Path(training_run_dir).name,
        }
    return cache


def _seed_bundle_cache(rows) -> dict[str, dict[str, object]]:
    cache: dict[str, dict[str, object]] = {}
    for row in rows:
        bundle_dir = str(row.get("bundle_dir") or "").strip()
        training_run_dir = str(row.get("training_run_dir") or "").strip()
        if not bundle_dir or not training_run_dir:
            continue
        offsets = _normalize_offsets(row.get("offsets"))
        if not offsets:
            continue
        key = _bundle_cache_key(
            market=str(row.get("market") or ""),
            profile=str(row.get("profile") or ""),
            target=str(row.get("target") or ""),
            offsets=offsets,
            training_run_label=Path(training_run_dir).name,
        )
        cache[key] = {
            "bundle_dir": bundle_dir,
            "bundle_label": Path(bundle_dir).name,
        }
    return cache


def _ensure_market_datasets(*, market_cfg: ResearchConfig, prepared_datasets: set[tuple[str, ...]]) -> None:
    key = (
        market_cfg.asset.slug,
        market_cfg.cycle,
        market_cfg.profile,
        market_cfg.feature_set,
        market_cfg.label_set,
        str(market_cfg.layout.storage.rewrite_root),
    )
    if key in prepared_datasets:
        return
    build_feature_frame_dataset(market_cfg)
    build_label_frame_dataset(market_cfg)
    prepared_datasets.add(key)


def _normalize_offsets(raw: object) -> tuple[int, ...]:
    return shared_normalize_offsets(raw)


def _seed_shared_training_cache(shared_cache: ExperimentSharedCache) -> dict[str, dict[str, object]]:
    return {
        str(cache_key): {
            "run_dir": str(record.get("run_dir") or ""),
            "run_label": str(record.get("run_label") or ""),
        }
        for cache_key, record in shared_cache.training_reuse.items()
        if str(record.get("run_dir") or "").strip() and str(record.get("run_label") or "").strip()
    }


def _seed_shared_bundle_cache(shared_cache: ExperimentSharedCache) -> dict[str, dict[str, object]]:
    return {
        str(cache_key): {
            "bundle_dir": str(record.get("bundle_dir") or ""),
            "bundle_label": str(record.get("bundle_label") or ""),
        }
        for cache_key, record in shared_cache.bundle_reuse.items()
        if str(record.get("bundle_dir") or "").strip() and str(record.get("bundle_label") or "").strip()
    }


def _persist_shared_cache(
    *,
    shared_cache: ExperimentSharedCache,
    training_rows: list[dict[str, object]],
    cycle: str,
    rewrite_root: str,
    suite_name: str,
    run_label: str,
) -> None:
    shared_cache.ingest_training_runs(
        pd.DataFrame(training_rows),
        cycle=cycle,
        rewrite_root=rewrite_root,
        source_suite_name=suite_name,
        source_run_label=run_label,
    )
    shared_cache.save()


def _resolve_training_summary(
    *,
    market_cfg: ResearchConfig,
    market_spec,
    run_label: str,
    target: str,
    offsets: tuple[int, ...],
    training_cache: dict[str, dict[str, object]],
    reporter: ExperimentReporter | None = None,
) -> tuple[dict[str, object], bool]:
    training_run_label = _training_run_label(
        run_label=run_label,
        market=market_spec.market,
        target=target,
        offsets=offsets,
        cache_key=_training_cache_key(
            market=market_spec.market,
            profile=market_spec.profile,
            model_family=market_spec.model_family,
            feature_set=market_spec.feature_set,
            label_set=market_spec.label_set,
            target=target,
            window_label=market_spec.window.label,
            offsets=offsets,
        ),
    )
    cache_key = _training_cache_key(
        market=market_spec.market,
        profile=market_spec.profile,
        model_family=market_spec.model_family,
        feature_set=market_spec.feature_set,
        label_set=market_spec.label_set,
        target=target,
        window_label=market_spec.window.label,
        offsets=offsets,
    )
    cached = training_cache.get(cache_key)
    if cached is not None:
        return cached, True
    spec = TrainingRunSpec(
        model_family=market_spec.model_family,
        feature_set=market_spec.feature_set,
        label_set=market_spec.label_set,
        target=target,
        window=market_spec.window,
        run_label=training_run_label,
        offsets=offsets,
    )
    try:
        summary = train_research_run(
            market_cfg,
            spec,
            reporter=reporter,
        )
    except TypeError:
        summary = train_research_run(
            market_cfg,
            spec,
        )
    normalized = dict(summary)
    normalized.setdefault("run_label", training_run_label)
    training_cache[cache_key] = normalized
    return normalized, False


def _resolve_bundle_summary(
    *,
    market_cfg: ResearchConfig,
    market_spec,
    run_label: str,
    target: str,
    offsets: tuple[int, ...],
    train_summary: dict[str, object],
    bundle_cache: dict[str, dict[str, object]],
) -> tuple[dict[str, object], bool]:
    cache_key = _bundle_cache_key(
        market=market_spec.market,
        profile=market_spec.profile,
        target=target,
        offsets=offsets,
        training_run_label=str(train_summary.get("run_label") or ""),
    )
    cached = bundle_cache.get(cache_key)
    if cached is not None:
        return cached, True
    summary = build_model_bundle(
        market_cfg,
        ModelBundleSpec(
            profile=market_spec.profile,
            target=target,
            bundle_label=_bundle_label(
                run_label=run_label,
                market=market_spec.market,
                target=target,
                offsets=offsets,
                cache_key=cache_key,
            ),
            offsets=offsets,
            source_training_run=str(train_summary["run_label"]),
        ),
    )
    bundle_cache[cache_key] = summary
    return summary, False


def _dedupe_case_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    if "case_key" in frame.columns:
        frame = frame.drop_duplicates(subset=["case_key"], keep="last")
    order = [
        column
        for column in (
            "market",
            "group_name",
            "matrix_parent_run_name",
            "run_name",
            "variant_label",
            "stake_usd",
            "target",
            "case_key",
        )
        if column in frame.columns
    ]
    if order:
        frame = frame.sort_values(order, kind="stable")
    return frame.reset_index(drop=True)


def _persist_experiment_outputs(
    *,
    cfg: ResearchConfig,
    suite,
    suite_path: Path,
    run_label: str,
    run_dir: Path,
    log_path: Path,
    training_rows: list[dict[str, object]],
    backtest_rows: list[dict[str, object]],
    failed_rows: list[dict[str, object]],
) -> dict[str, object]:
    training_df = _dedupe_case_rows(pd.DataFrame(training_rows))
    backtest_df = _dedupe_case_rows(pd.DataFrame(backtest_rows))
    failed_df = _dedupe_case_rows(pd.DataFrame(failed_rows))
    leaderboard_df = build_leaderboard(backtest_df)
    compare_df = build_experiment_compare_frame(
        training_runs=training_df,
        backtest_runs=backtest_df,
        failed_cases=failed_df,
    )
    matrix_summary_df = build_matrix_summary_frame(compare_df)
    variant_compare_df = build_variant_compare_frame(
        compare_df,
        reference_variant_labels=_suite_reference_variant_labels(suite),
    )
    summary_payload = build_experiment_summary(
        suite_name=suite.suite_name,
        run_label=run_label,
        training_runs=training_df,
        backtest_runs=backtest_df,
        leaderboard=leaderboard_df,
        compare_frame=compare_df,
        failed_cases=failed_df,
    )

    training_runs_path = run_dir / "training_runs.parquet"
    backtest_runs_path = run_dir / "backtest_runs.parquet"
    failed_cases_parquet_path = run_dir / "failed_cases.parquet"
    failed_cases_csv_path = run_dir / "failed_cases.csv"
    leaderboard_parquet_path = run_dir / "leaderboard.parquet"
    leaderboard_csv_path = run_dir / "leaderboard.csv"
    compare_parquet_path = run_dir / "compare.parquet"
    compare_csv_path = run_dir / "compare.csv"
    matrix_summary_parquet_path = run_dir / "matrix_summary.parquet"
    matrix_summary_csv_path = run_dir / "matrix_summary.csv"
    variant_compare_parquet_path = run_dir / "variant_compare.parquet"
    variant_compare_csv_path = run_dir / "variant_compare.csv"
    summary_path = run_dir / "summary.json"
    report_path = run_dir / "report.md"
    manifest_path = run_dir / "manifest.json"

    write_parquet_atomic(training_df, training_runs_path)
    write_parquet_atomic(backtest_df, backtest_runs_path)
    write_parquet_atomic(failed_df, failed_cases_parquet_path)
    failed_df.to_csv(failed_cases_csv_path, index=False)
    write_parquet_atomic(leaderboard_df, leaderboard_parquet_path)
    leaderboard_df.to_csv(leaderboard_csv_path, index=False)
    write_parquet_atomic(compare_df, compare_parquet_path)
    compare_df.to_csv(compare_csv_path, index=False)
    write_parquet_atomic(matrix_summary_df, matrix_summary_parquet_path)
    matrix_summary_df.to_csv(matrix_summary_csv_path, index=False)
    write_parquet_atomic(variant_compare_df, variant_compare_parquet_path)
    variant_compare_df.to_csv(variant_compare_csv_path, index=False)
    summary_path.write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    report_path.write_text(
        render_experiment_report(
            summary_payload,
            leaderboard=leaderboard_df,
            compare_frame=compare_df,
            failed_cases=failed_df,
        ),
        encoding="utf-8",
    )

    manifest = build_manifest(
        object_type="experiment_run",
        object_id=f"experiment_run:{suite.suite_name}:{run_label}",
        market=cfg.asset.slug,
        cycle=suite.cycle,
        path=run_dir,
        spec={
            "suite_name": suite.suite_name,
            "run_label": run_label,
            "suite_spec_path": str(suite_path),
            "markets": [market.to_dict() for market in suite.markets],
            "compare_policy": _suite_compare_policy_payload(suite),
            "runtime_policy": _suite_runtime_policy_payload(suite),
        },
        inputs=[
            {"kind": "suite_spec", "path": str(suite_path)},
        ],
        outputs=[
            {"kind": "training_runs_parquet", "path": str(training_runs_path)},
            {"kind": "backtest_runs_parquet", "path": str(backtest_runs_path)},
            {"kind": "failed_cases_parquet", "path": str(failed_cases_parquet_path)},
            {"kind": "failed_cases_csv", "path": str(failed_cases_csv_path)},
            {"kind": "leaderboard_parquet", "path": str(leaderboard_parquet_path)},
            {"kind": "leaderboard_csv", "path": str(leaderboard_csv_path)},
            {"kind": "compare_parquet", "path": str(compare_parquet_path)},
            {"kind": "compare_csv", "path": str(compare_csv_path)},
            {"kind": "matrix_summary_parquet", "path": str(matrix_summary_parquet_path)},
            {"kind": "matrix_summary_csv", "path": str(matrix_summary_csv_path)},
            {"kind": "variant_compare_parquet", "path": str(variant_compare_parquet_path)},
            {"kind": "variant_compare_csv", "path": str(variant_compare_csv_path)},
            {"kind": "summary_json", "path": str(summary_path)},
            {"kind": "report_md", "path": str(report_path)},
            {"kind": "suite_log", "path": str(log_path)},
        ],
        metadata={
            "markets": [market.market for market in suite.markets],
            "training_runs": int(len(training_df)),
            "backtest_runs": int(len(backtest_df)),
            "failed_cases": int(len(failed_df)),
            "compare_rows": int(len(compare_df)),
            "matrix_summary_rows": int(len(matrix_summary_df)),
            "variant_compare_rows": int(len(variant_compare_df)),
            "resumed_cases": int(summary_payload.get("resumed_cases", 0)),
        },
    )
    write_manifest(manifest_path, manifest)
    return {
        "run_dir": str(run_dir),
        "leaderboard_csv_path": str(leaderboard_csv_path),
        "compare_csv_path": str(compare_csv_path),
        "matrix_summary_csv_path": str(matrix_summary_csv_path),
        "variant_compare_csv_path": str(variant_compare_csv_path),
        "failed_cases_csv_path": str(failed_cases_csv_path),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "manifest_path": str(manifest_path),
    }


def _persist_runtime_state(
    *,
    cfg: ResearchConfig,
    suite,
    suite_path: Path,
    run_label: str,
    run_dir: Path,
    log_path: Path,
    shared_cache: ExperimentSharedCache,
    run_state: ExperimentRunState,
    cycle: str,
    rewrite_root: str,
) -> dict[str, object]:
    outputs = _persist_experiment_outputs(
        cfg=cfg,
        suite=suite,
        suite_path=suite_path,
        run_label=run_label,
        run_dir=run_dir,
        log_path=log_path,
        training_rows=run_state.training_rows(),
        backtest_rows=run_state.backtest_rows(),
        failed_rows=run_state.failed_rows(),
    )
    _persist_shared_cache(
        shared_cache=shared_cache,
        training_rows=run_state.training_rows(),
        cycle=cycle,
        rewrite_root=rewrite_root,
        suite_name=suite.suite_name,
        run_label=run_label,
    )
    return outputs


def _case_key(market_spec) -> str:
    payload = {
        "market": market_spec.market,
        "group_name": _group_name(market_spec),
        "run_name": _run_name(market_spec),
        "matrix_parent_run_name": _matrix_parent_run_name(market_spec),
        "matrix_stake_label": _matrix_stake_label(market_spec),
        "profile": market_spec.profile,
        "model_family": market_spec.model_family,
        "feature_set": market_spec.feature_set,
        "label_set": market_spec.label_set,
        "target": market_spec.target,
        "offsets": [int(value) for value in market_spec.offsets],
        "window": market_spec.window.label,
        "backtest_spec": market_spec.backtest_spec,
        "variant_label": market_spec.variant_label,
        "variant_notes": market_spec.variant_notes,
        "stake_usd": _stake_usd(market_spec),
        "max_notional_usd": _max_notional_usd(market_spec),
        "hybrid_secondary_target": market_spec.hybrid_secondary_target,
        "hybrid_secondary_offsets": None if market_spec.hybrid_secondary_offsets is None else [int(value) for value in market_spec.hybrid_secondary_offsets],
        "hybrid_fallback_reasons": [str(value) for value in market_spec.hybrid_fallback_reasons],
        "parity": market_spec.parity.to_dict(),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:20]


def _group_name(market_spec) -> str:
    return str(getattr(market_spec, "group_name", "") or "")


def _run_name(market_spec) -> str:
    return str(getattr(market_spec, "run_name", "") or "")


def _tags(market_spec) -> tuple[str, ...]:
    raw = getattr(market_spec, "tags", ()) or ()
    return tuple(str(tag) for tag in raw if str(tag))


def _matrix_parent_run_name(market_spec) -> str:
    return str(getattr(market_spec, "matrix_parent_run_name", "") or "")


def _matrix_stake_label(market_spec) -> str:
    return str(getattr(market_spec, "matrix_stake_label", "") or "")


def _stake_usd(market_spec) -> float | None:
    raw = getattr(market_spec, "stake_usd", None)
    return None if raw in {None, ""} else float(raw)


def _max_notional_usd(market_spec) -> float | None:
    raw = getattr(market_spec, "max_notional_usd", None)
    return None if raw in {None, ""} else float(raw)


def _suite_reference_variant_labels(suite) -> tuple[str, ...]:
    policy = getattr(suite, "compare_policy", None)
    raw = getattr(policy, "reference_variant_labels", ()) if policy is not None else ()
    labels = tuple(str(label).strip().lower() for label in raw if str(label).strip())
    return labels


def _suite_compare_policy_payload(suite) -> dict[str, object]:
    policy = getattr(suite, "compare_policy", None)
    to_dict = getattr(policy, "to_dict", None)
    if callable(to_dict):
        payload = to_dict()
        if isinstance(payload, dict):
            return payload
    labels = _suite_reference_variant_labels(suite) or ("default", "baseline", "control")
    return {"reference_variant_labels": list(labels)}


def _suite_runtime_policy(suite) -> ExperimentRuntimePolicy:
    policy = getattr(suite, "runtime_policy", None)
    if isinstance(policy, ExperimentRuntimePolicy):
        return policy
    from_mapping = getattr(ExperimentRuntimePolicy, "from_mapping", None)
    if callable(from_mapping):
        try:
            return ExperimentRuntimePolicy.from_mapping(policy)
        except Exception:
            return ExperimentRuntimePolicy()
    return ExperimentRuntimePolicy()


def _suite_runtime_policy_payload(suite) -> dict[str, object]:
    policy = _suite_runtime_policy(suite)
    return policy.to_dict()


def _training_cache_key(
    *,
    market: str,
    profile: str,
    model_family: str,
    feature_set: str,
    label_set: str,
    target: str,
    window_label: str,
    offsets: tuple[int, ...],
) -> str:
    payload = {
        "market": market,
        "profile": profile,
        "model_family": model_family,
        "feature_set": feature_set,
        "label_set": label_set,
        "target": target,
        "window": window_label,
        "offsets": [int(value) for value in offsets],
    }
    return _stable_key(payload)


def _bundle_cache_key(
    *,
    market: str,
    profile: str,
    target: str,
    offsets: tuple[int, ...],
    training_run_label: str,
) -> str:
    return _stable_key(
        {
            "market": market,
            "profile": profile,
            "target": target,
            "offsets": [int(value) for value in offsets],
            "training_run_label": training_run_label,
        }
    )


def _training_run_label(*, run_label: str, market: str, target: str, offsets: tuple[int, ...], cache_key: str) -> str:
    return _slug(f"{run_label}-{market}-{target}-train-{_offset_label(offsets)}-{cache_key[:8]}")


def _bundle_label(*, run_label: str, market: str, target: str, offsets: tuple[int, ...], cache_key: str) -> str:
    return _slug(f"{run_label}-{market}-{target}-bundle-{_offset_label(offsets)}-{cache_key[:8]}")


def _backtest_run_label(*, run_label: str, market_spec, case_key: str) -> str:
    tokens = [run_label, market_spec.market]
    group_name = _group_name(market_spec)
    run_name = _run_name(market_spec)
    if group_name:
        tokens.append(group_name)
    if run_name:
        tokens.append(run_name)
    variant = str(market_spec.variant_label or "default")
    if variant and variant != "default":
        tokens.append(variant)
    tokens.extend(["backtest", case_key[:8]])
    return _slug("-".join(tokens))


def _offset_label(offsets: tuple[int, ...]) -> str:
    return "off" + "-".join(str(int(value)) for value in offsets)


def _stable_key(payload: dict[str, object]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def _slug(value: str) -> str:
    chars: list[str] = []
    for char in str(value or "").strip().lower():
        chars.append(char if char.isalnum() or char in {"-", "_"} else "-")
    token = "".join(chars).strip("-")
    return token or "planned"
