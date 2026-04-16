from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
import json
import shutil
from pathlib import Path

import joblib
import pandas as pd

from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import TrainingRunSpec, TrainingSetSpec
from pm15min.research._contracts_training import offset_weight_overrides_payload
from pm15min.research.datasets.training_sets import build_training_set_dataset
from pm15min.research.freshness import prepare_research_artifacts
from pm15min.research.manifests import build_manifest, write_manifest
from pm15min.research.training.metrics import blend_weights_from_brier, classification_metrics, feature_schema_rows
from pm15min.research.training.calibration import build_reliability_bins
from pm15min.research.training.explainability import (
    build_factor_correlation_frame,
    build_factor_direction_summary,
    build_lgb_feature_importance,
    build_logreg_coefficients,
)
from pm15min.research.training.probes import build_final_model_probe
from pm15min.research.training.reports import render_offset_training_report, render_training_run_report
from pm15min.research.training.trainers import (
    TrainerConfig,
    fit_lgbm,
    fit_logreg,
    generate_oof_predictions,
    prepare_training_matrix,
    report_training_progress,
    TrainingProgressReporter,
)
from pm15min.research.training.splits import build_purged_time_series_splits
from pm15min.research.training.weights import compute_sample_weights


def train_research_run(
    cfg: ResearchConfig,
    spec: TrainingRunSpec,
    *,
    reporter: TrainingProgressReporter | None = None,
    dependency_mode: str = "auto_repair",
) -> dict[str, object]:
    prepare_research_artifacts(
        cfg,
        feature_set=spec.feature_set,
        label_set=spec.label_set,
        mode=dependency_mode,
    )
    run_dir = cfg.layout.training_run_dir(
        model_family=spec.model_family,
        target=spec.target,
        run_label_text=spec.run_label,
    )
    if run_dir.exists():
        shutil.rmtree(run_dir)
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    event_log_path = logs_dir / "train.jsonl"

    summaries: list[dict[str, object]] = []
    offset_metrics: dict[int, dict[str, object]] = {}
    base_trainer_cfg = TrainerConfig(
        parallel_workers=max(1, int(spec.parallel_workers or 1)),
        balance_classes=TrainerConfig.balance_classes if spec.balance_classes is None else bool(spec.balance_classes),
        weight_by_vol=TrainerConfig.weight_by_vol if spec.weight_by_vol is None else bool(spec.weight_by_vol),
        inverse_vol=TrainerConfig.inverse_vol if spec.inverse_vol is None else bool(spec.inverse_vol),
        contrarian_weight=(
            TrainerConfig.contrarian_weight if spec.contrarian_weight is None else float(spec.contrarian_weight)
        ),
        contrarian_quantile=(
            TrainerConfig.contrarian_quantile if spec.contrarian_quantile is None else float(spec.contrarian_quantile)
        ),
        contrarian_return_col=(
            TrainerConfig.contrarian_return_col
            if spec.contrarian_return_col in {None, ""}
            else str(spec.contrarian_return_col)
        ),
        winner_in_band_weight=(
            TrainerConfig.winner_in_band_weight
            if spec.winner_in_band_weight is None
            else float(spec.winner_in_band_weight)
        ),
        feature_set_root=cfg.layout.storage.rewrite_root,
    )
    input_paths: list[dict[str, str]] = []
    total_offsets = len(spec.offsets)
    parallel_workers = max(1, int(spec.parallel_workers or 1))
    if parallel_workers <= 1 or total_offsets <= 1:
        for index, offset in enumerate(spec.offsets, start=1):
            report_training_progress(
                reporter,
                summary=f"Preparing offset {offset} ({index}/{total_offsets})",
                current=index - 1,
                total=total_offsets,
                current_stage="training_prepare",
                progress_pct=_progress_pct(index - 1, total_offsets),
            )
            report_training_progress(
                reporter,
                summary=f"Training offset {offset} ({index}/{total_offsets})",
                current=index - 1,
                total=total_offsets,
                current_stage="training_offsets",
                progress_pct=_progress_pct(index - 1, total_offsets),
            )
            training_set_path, result = _execute_training_offset(
                cfg=cfg,
                spec=spec,
                offset=offset,
                run_dir=run_dir,
                trainer_cfg=base_trainer_cfg,
                reporter=_offset_reporter(
                    reporter,
                    offset=offset,
                    completed_offsets=index - 1,
                    total_offsets=total_offsets,
                ),
            )
            input_paths.append({"kind": "training_set", "path": str(training_set_path)})
            offset_metrics[offset] = result
            summaries.append(_training_summary_row(offset=offset, result=result))
            report_training_progress(
                reporter,
                summary=f"Completed offset {offset} ({index}/{total_offsets})",
                current=index,
                total=total_offsets,
                current_stage="training_offsets",
                progress_pct=_progress_pct(index, total_offsets),
            )
            _append_train_log(
                event_log_path,
                {
                    "event": "offset_trained",
                    "offset": offset,
                    "rows": result["rows"],
                    "positive_rate": result["positive_rate"],
                    "output_dir": str(run_dir / "offsets" / f"offset={offset}"),
                },
            )
    else:
        future_map = {}
        with ThreadPoolExecutor(max_workers=min(parallel_workers, total_offsets)) as executor:
            for index, offset in enumerate(spec.offsets, start=1):
                report_training_progress(
                    reporter,
                    summary=f"Preparing offset {offset} ({index}/{total_offsets})",
                    current=0,
                    total=total_offsets,
                    current_stage="training_prepare",
                    progress_pct=0,
                )
                report_training_progress(
                    reporter,
                    summary=f"Training offset {offset} ({index}/{total_offsets})",
                    current=0,
                    total=total_offsets,
                    current_stage="training_offsets",
                    progress_pct=0,
                )
                future = executor.submit(
                    _execute_training_offset,
                    cfg=cfg,
                    spec=spec,
                    offset=offset,
                    run_dir=run_dir,
                    trainer_cfg=base_trainer_cfg,
                    reporter=_offset_reporter(
                        reporter,
                        offset=offset,
                        completed_offsets=0,
                        total_offsets=total_offsets,
                    ),
                )
                future_map[future] = int(offset)
            completed_offsets = 0
            for future in as_completed(future_map):
                offset = future_map[future]
                training_set_path, result = future.result()
                completed_offsets += 1
                input_paths.append({"kind": "training_set", "path": str(training_set_path)})
                offset_metrics[offset] = result
                summaries.append(_training_summary_row(offset=offset, result=result))
                report_training_progress(
                    reporter,
                    summary=f"Completed offset {offset} ({completed_offsets}/{total_offsets})",
                    current=completed_offsets,
                    total=total_offsets,
                    current_stage="training_offsets",
                    progress_pct=_progress_pct(completed_offsets, total_offsets),
                )
                _append_train_log(
                    event_log_path,
                    {
                        "event": "offset_trained",
                        "offset": offset,
                        "rows": result["rows"],
                        "positive_rate": result["positive_rate"],
                        "output_dir": str(run_dir / "offsets" / f"offset={offset}"),
                    },
                )

    summary_df = pd.DataFrame(summaries).sort_values("offset").reset_index(drop=True)
    report_training_progress(
        reporter,
        summary="Writing training outputs",
        current=total_offsets,
        total=total_offsets,
        current_stage="training_finalize",
        progress_pct=100,
    )
    summary_payload = {
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "model_family": spec.model_family,
        "feature_set": spec.feature_set,
        "label_set": spec.label_set,
        "label_source": spec.label_source,
        "parallel_workers": spec.parallel_workers,
        "target": spec.target,
        "window": spec.window.label,
        "run_label": spec.run_label,
        "offsets": list(spec.offsets),
        "weight_variant_label": spec.weight_variant_label,
        "balance_classes": base_trainer_cfg.balance_classes,
        "weight_by_vol": base_trainer_cfg.weight_by_vol,
        "inverse_vol": base_trainer_cfg.inverse_vol,
        "contrarian_weight": base_trainer_cfg.contrarian_weight,
        "contrarian_quantile": base_trainer_cfg.contrarian_quantile,
        "contrarian_return_col": base_trainer_cfg.contrarian_return_col,
        "winner_in_band_weight": base_trainer_cfg.winner_in_band_weight,
        "offset_weight_overrides": offset_weight_overrides_payload(spec.offset_weight_overrides),
        "offset_summaries": summary_df.to_dict(orient="records"),
    }
    (run_dir / "summary.json").write_text(
        json.dumps(summary_payload, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    report_path = run_dir / "report.md"
    report_path.write_text(render_training_run_report(summary_payload), encoding="utf-8")

    manifest = build_manifest(
        object_type="training_run",
        object_id=spec.object_id,
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        path=run_dir,
        spec=spec.to_dict(),
        inputs=input_paths,
        outputs=[
            {"kind": "training_run_dir", "path": str(run_dir)},
            {"kind": "summary_json", "path": str(run_dir / "summary.json")},
            {"kind": "report_md", "path": str(report_path)},
            {"kind": "log_jsonl", "path": str(event_log_path)},
        ],
        metadata={
            "offset_count": len(spec.offsets),
            "rows_total": int(sum(int(item["rows"]) for item in summaries)),
        },
    )
    write_manifest(run_dir / "manifest.json", manifest)
    return {
        "dataset": "training_run",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "model_family": spec.model_family,
        "feature_set": spec.feature_set,
        "label_set": spec.label_set,
        "target": spec.target,
        "window": spec.window.label,
        "run_label": spec.run_label,
        "offsets": list(spec.offsets),
        "parallel_workers": spec.parallel_workers,
        "weight_variant_label": spec.weight_variant_label,
        "balance_classes": base_trainer_cfg.balance_classes,
        "weight_by_vol": base_trainer_cfg.weight_by_vol,
        "inverse_vol": base_trainer_cfg.inverse_vol,
        "contrarian_weight": base_trainer_cfg.contrarian_weight,
        "contrarian_quantile": base_trainer_cfg.contrarian_quantile,
        "contrarian_return_col": base_trainer_cfg.contrarian_return_col,
        "offset_weight_overrides": offset_weight_overrides_payload(spec.offset_weight_overrides),
        "run_dir": str(run_dir),
        "summary_path": str(run_dir / "summary.json"),
        "report_path": str(report_path),
        "manifest_path": str(run_dir / "manifest.json"),
    }


def _train_single_offset(
    df: pd.DataFrame,
    *,
    offset: int,
    run_dir: Path,
    trainer_cfg: TrainerConfig,
    market: str,
    feature_set: str,
    weight_variant_label: str,
    reporter: TrainingProgressReporter | None = None,
) -> dict[str, object]:
    X, y, pruning_plan = prepare_training_matrix(
        df,
        market=market,
        feature_set=feature_set,
        cfg=trainer_cfg,
    )
    if len(X) < 6:
        raise ValueError(f"offset={offset} has too few rows for training: {len(X)}")
    if X.shape[1] == 0:
        raise ValueError(f"offset={offset} has no remaining features after pruning")
    if len(y.unique()) < 2:
        raise ValueError(f"offset={offset} requires at least two classes")

    sample_weight = compute_sample_weights(
        df.loc[X.index],
        y,
        balance_classes=trainer_cfg.balance_classes,
        weight_by_vol=trainer_cfg.weight_by_vol,
        inverse_vol=trainer_cfg.inverse_vol,
        contrarian_weight=trainer_cfg.contrarian_weight,
        contrarian_quantile=trainer_cfg.contrarian_quantile,
        contrarian_return_col=trainer_cfg.contrarian_return_col,
        winner_in_band_weight=trainer_cfg.winner_in_band_weight,
    )
    split_rows = build_purged_time_series_splits(
        df.loc[X.index, "decision_ts"],
        n_splits=min(int(trainer_cfg.n_splits), max(2, len(X) // 4)),
        purge_minutes=trainer_cfg.purge_minutes,
        embargo_minutes=trainer_cfg.embargo_minutes,
    )
    oof = generate_oof_predictions(
        X,
        y,
        decision_ts=df.loc[X.index, "decision_ts"],
        raw_frame=df.loc[X.index],
        cfg=trainer_cfg,
        reporter=reporter,
    )
    logreg = fit_logreg(X, y, cfg=trainer_cfg, sample_weight=sample_weight)
    lgbm = fit_lgbm(X, y, cfg=trainer_cfg, sample_weight=sample_weight)

    report_training_progress(
        reporter,
        summary=f"Offset {offset}: writing artifacts",
        current_stage="training_artifacts",
    )

    offset_dir = run_dir / "offsets" / f"offset={offset}"
    models_dir = offset_dir / "models"
    calibration_dir = offset_dir / "calibration"
    reports_dir = offset_dir / "reports"
    models_dir.mkdir(parents=True, exist_ok=True)
    calibration_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    joblib.dump(logreg, models_dir / "logreg_sigmoid.joblib")
    joblib.dump(lgbm, models_dir / "lgbm_sigmoid.joblib")
    joblib.dump(list(X.columns), offset_dir / "feature_cols.joblib")

    feature_schema = feature_schema_rows(X)
    (offset_dir / "feature_schema.json").write_text(
        json.dumps(feature_schema, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    if oof.empty:
        p_lr = logreg.predict_proba(X)[:, 1].astype(float)
        p_lgb = lgbm.predict_proba(X)[:, 1].astype(float)
        eval_y = y.to_numpy(dtype=int)
        oof_to_write = pd.DataFrame(
            {
                "row_number": X.index.astype(int),
                "fold": -1,
                "y": eval_y,
                "p_lgb": p_lgb,
                "p_lr": p_lr,
            }
        )
    else:
        oof_to_write = oof.copy()
        eval_y = oof_to_write["y"].to_numpy(dtype=int)
        p_lgb = oof_to_write["p_lgb"].to_numpy(dtype=float)
        p_lr = oof_to_write["p_lr"].to_numpy(dtype=float)

    blend_weights = blend_weights_from_brier(
        brier_lgb=classification_metrics(eval_y, p_lgb)["brier"],
        brier_lr=classification_metrics(eval_y, p_lr)["brier"],
    )
    p_blend = blend_weights["w_lgb"] * p_lgb + blend_weights["w_lr"] * p_lr
    full_p_lgb = lgbm.predict_proba(X)[:, 1].astype(float)
    full_p_lr = logreg.predict_proba(X)[:, 1].astype(float)
    full_p_blend = blend_weights["w_lgb"] * full_p_lgb + blend_weights["w_lr"] * full_p_lr
    metrics = {
        "lgbm": classification_metrics(eval_y, p_lgb),
        "logreg": classification_metrics(eval_y, p_lr),
        "blend": classification_metrics(eval_y, p_blend),
    }
    reliability_payload = {
        "lgbm": build_reliability_bins(eval_y, p_lgb),
        "logreg": build_reliability_bins(eval_y, p_lr),
        "blend": build_reliability_bins(eval_y, p_blend),
    }
    probe = build_final_model_probe(
        X=X,
        y=y,
        p_lgb=full_p_lgb,
        p_lr=full_p_lr,
        p_blend=full_p_blend,
    )
    logreg_coefficients = build_logreg_coefficients(feature_names=X.columns, model=logreg)
    lgb_feature_importance = build_lgb_feature_importance(feature_names=X.columns, model=lgbm)
    factor_correlations = build_factor_correlation_frame(
        X=X,
        y=y,
        p_lgb=full_p_lgb,
        p_lr=full_p_lr,
        p_blend=full_p_blend,
    )
    factor_direction_summary = build_factor_direction_summary(
        X=X,
        y=y,
        logreg_coefficients=logreg_coefficients,
        lgb_importance=lgb_feature_importance,
    )

    write_parquet_atomic(oof_to_write, offset_dir / "oof_predictions.parquet")
    write_parquet_atomic(factor_correlations, offset_dir / "factor_correlations.parquet")
    (calibration_dir / "blend_weights.json").write_text(
        json.dumps(
            {
                **blend_weights,
                "method": "inverse_brier_weight",
            },
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    for name, rows in reliability_payload.items():
        (calibration_dir / f"reliability_bins_{name}.json").write_text(
            json.dumps(rows, indent=2, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
    (calibration_dir / "reliability_bins.json").write_text(
        json.dumps(reliability_payload, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    weight_summary = {
        "rows": int(len(sample_weight)),
        "min_weight": float(sample_weight.min()) if len(sample_weight) else None,
        "max_weight": float(sample_weight.max()) if len(sample_weight) else None,
        "mean_weight": float(sample_weight.mean()) if len(sample_weight) else None,
        "sum_weight": float(sample_weight.sum()) if len(sample_weight) else None,
        "weight_variant_label": str(weight_variant_label),
        "balance_classes": bool(trainer_cfg.balance_classes),
        "weight_by_vol": bool(trainer_cfg.weight_by_vol),
        "inverse_vol": bool(trainer_cfg.inverse_vol),
        "contrarian_weight": float(trainer_cfg.contrarian_weight),
        "contrarian_quantile": float(trainer_cfg.contrarian_quantile),
        "contrarian_return_col": str(trainer_cfg.contrarian_return_col),
        "winner_in_band_weight": float(trainer_cfg.winner_in_band_weight),
    }
    split_summary = {
        "folds_requested": int(min(int(trainer_cfg.n_splits), max(2, len(X) // 4))),
        "folds_built": int(len(split_rows)),
        "folds_used": int(oof["fold"].nunique()) if not oof.empty else 0,
        "purge_minutes": int(trainer_cfg.purge_minutes),
        "embargo_minutes": int(trainer_cfg.embargo_minutes),
    }
    explainability_preview = {
        "top_logreg_coefficients": list(logreg_coefficients.get("rows", []))[:5],
        "top_lgb_importance": list(lgb_feature_importance.get("rows", []))[:5],
        "top_positive_factors": list(factor_direction_summary.get("top_positive_factors", []))[:5],
        "top_negative_factors": list(factor_direction_summary.get("top_negative_factors", []))[:5],
    }
    offset_summary = {
        "offset": int(offset),
        "rows": int(len(X)),
        "positive_rate": float(y.mean()),
        "feature_count": int(X.shape[1]),
        "feature_set": str(feature_set),
        "market": str(market),
        "weight_variant_label": str(weight_variant_label),
        "metrics": metrics,
        "pruning": pruning_plan.to_dict(),
        "weight_summary": weight_summary,
        "split_summary": split_summary,
        "probe": probe,
        "explainability": explainability_preview,
    }
    (offset_dir / "metrics.json").write_text(
        json.dumps(offset_summary, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    (offset_dir / "summary.json").write_text(
        json.dumps(offset_summary, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    (offset_dir / "feature_pruning.json").write_text(
        json.dumps(pruning_plan.to_dict(), indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    (offset_dir / "logreg_coefficients.json").write_text(
        json.dumps(logreg_coefficients, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    (offset_dir / "lgb_feature_importance.json").write_text(
        json.dumps(lgb_feature_importance, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    (offset_dir / "factor_direction_summary.json").write_text(
        json.dumps(factor_direction_summary, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    (offset_dir / "probe.json").write_text(
        json.dumps(probe, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    offset_report = render_offset_training_report(
        offset=offset,
        rows=int(len(X)),
        positive_rate=float(y.mean()),
        feature_count=int(X.shape[1]),
        dropped_features=pruning_plan.dropped_columns,
        metrics=metrics,
        explainability=explainability_preview,
    )
    (offset_dir / "report.md").write_text(offset_report, encoding="utf-8")
    (reports_dir / "offset_report.md").write_text(offset_report, encoding="utf-8")
    return {
        "rows": int(len(X)),
        "positive_rate": float(y.mean()),
        "metrics": metrics,
        "dropped_features": list(pruning_plan.dropped_columns),
        "weight_summary": weight_summary,
        "split_summary": split_summary,
        "explainability": explainability_preview,
    }


def _execute_training_offset(
    *,
    cfg: ResearchConfig,
    spec: TrainingRunSpec,
    offset: int,
    run_dir: Path,
    trainer_cfg: TrainerConfig,
    reporter: TrainingProgressReporter | None,
) -> tuple[Path, dict[str, object]]:
    trainer_cfg = _trainer_cfg_for_offset(base_cfg=trainer_cfg, spec=spec, offset=offset)
    training_set_spec = TrainingSetSpec(
        feature_set=spec.feature_set,
        label_set=spec.label_set,
        target=spec.target,
        window=spec.window,
        offset=offset,
    )
    build_training_set_dataset(cfg, training_set_spec, skip_freshness=True)
    training_set_path = cfg.layout.training_set_path(
        feature_set=spec.feature_set,
        label_set=spec.label_set,
        target=spec.target,
        window=spec.window.label,
        offset=offset,
    )
    df = pd.read_parquet(training_set_path)
    result = _train_single_offset(
        df,
        offset=offset,
        run_dir=run_dir,
        trainer_cfg=trainer_cfg,
        market=cfg.asset.slug,
        feature_set=spec.feature_set,
        weight_variant_label=spec.weight_variant_label,
        reporter=reporter,
    )
    return training_set_path, result


def _training_summary_row(*, offset: int, result: dict[str, object]) -> dict[str, object]:
    return {
        "offset": int(offset),
        "rows": result["rows"],
        "positive_rate": result["positive_rate"],
        "dropped_features": result.get("dropped_features", []),
        "brier_lgb": result["metrics"]["lgbm"]["brier"],
        "brier_lr": result["metrics"]["logreg"]["brier"],
        "brier_blend": result["metrics"]["blend"]["brier"],
        "auc_lgb": result["metrics"]["lgbm"]["auc"],
        "auc_lr": result["metrics"]["logreg"]["auc"],
        "auc_blend": result["metrics"]["blend"]["auc"],
        "explainability": result["explainability"],
    }


def _trainer_cfg_for_offset(
    *,
    base_cfg: TrainerConfig,
    spec: TrainingRunSpec,
    offset: int,
) -> TrainerConfig:
    override = dict((spec.offset_weight_overrides or {}).get(int(offset), {}))
    if not override:
        return base_cfg
    return replace(
        base_cfg,
        balance_classes=base_cfg.balance_classes if override.get("balance_classes") is None else bool(override["balance_classes"]),
        weight_by_vol=base_cfg.weight_by_vol if override.get("weight_by_vol") is None else bool(override["weight_by_vol"]),
        inverse_vol=base_cfg.inverse_vol if override.get("inverse_vol") is None else bool(override["inverse_vol"]),
        contrarian_weight=(
            base_cfg.contrarian_weight if override.get("contrarian_weight") is None else float(override["contrarian_weight"])
        ),
        contrarian_quantile=(
            base_cfg.contrarian_quantile
            if override.get("contrarian_quantile") is None
            else float(override["contrarian_quantile"])
        ),
        contrarian_return_col=(
            base_cfg.contrarian_return_col
            if override.get("contrarian_return_col") in {None, ""}
            else str(override["contrarian_return_col"])
        ),
        winner_in_band_weight=(
            base_cfg.winner_in_band_weight
            if override.get("winner_in_band_weight") is None
            else float(override["winner_in_band_weight"])
        ),
    )


def _progress_pct(current: int, total: int) -> int:
    if total <= 0:
        return 100
    bounded_current = min(max(int(current), 0), int(total))
    return int(round((bounded_current / int(total)) * 100))


def _offset_reporter(
    reporter: TrainingProgressReporter | None,
    *,
    offset: int,
    completed_offsets: int,
    total_offsets: int,
) -> TrainingProgressReporter | None:
    if reporter is None:
        return None

    def _report(
        *,
        summary: str,
        current: int | None = None,
        total: int | None = None,
        current_stage: str | None = None,
        progress_pct: int | None = None,
        heartbeat: str | None = None,
    ) -> None:
        del current, total, progress_pct
        report_training_progress(
            reporter,
            summary=f"Offset {offset}: {summary}",
            current=completed_offsets,
            total=total_offsets,
            current_stage=current_stage or "training_offsets",
            progress_pct=_progress_pct(completed_offsets, total_offsets),
            heartbeat=heartbeat,
        )

    return _report


def _append_train_log(path: Path, event: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, ensure_ascii=False, sort_keys=True))
        fh.write("\n")
