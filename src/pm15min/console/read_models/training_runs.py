from __future__ import annotations

from pathlib import Path
from typing import Any

from pm15min.research.config import ResearchConfig
from pm15min.research.manifests import read_manifest
from pm15min.research.service import list_training_runs as _list_training_runs

from .common import json_ready, read_json_object


def list_console_training_runs(
    *,
    market: str,
    cycle: str | int = "15m",
    model_family: str | None = None,
    target: str | None = None,
    prefix: str | None = None,
    root: Path | None = None,
) -> list[dict[str, object]]:
    cfg = _build_cfg(market=market, cycle=cycle, root=root)
    rows = _list_training_runs(
        cfg,
        model_family=model_family,
        target=target,
        prefix=prefix,
    )
    return json_ready([_build_training_run_row(Path(str(row["path"]))) for row in rows])


def load_console_training_run(
    *,
    market: str,
    cycle: str | int = "15m",
    model_family: str | None = None,
    target: str | None = None,
    run_label: str | None = None,
    run_dir: str | Path | None = None,
    root: Path | None = None,
) -> dict[str, object]:
    cfg = _build_cfg(
        market=market,
        cycle=cycle,
        root=root,
        model_family=model_family,
        target=target,
    )
    resolved_run_dir = _resolve_training_run_dir(
        cfg,
        model_family=model_family,
        target=target,
        run_label=run_label,
        run_dir=run_dir,
    )
    row = _build_training_run_row(resolved_run_dir)
    offset_details = _build_offset_details(resolved_run_dir)
    bundle_readiness = _training_bundle_readiness(offset_details)
    metric_summary = _training_metric_summary_from_details(offset_details)
    payload: dict[str, object] = {
        "domain": "console",
        "dataset": "console_training_run",
        **row,
        "report_text": _read_text_file(resolved_run_dir / "report.md"),
        "offset_details": offset_details,
        "run_overview": _training_run_overview(
            row,
            bundle_readiness=bundle_readiness,
            metric_summary=metric_summary,
        ),
        "offset_preview": _training_offset_preview(offset_details),
        "explainability_overview": _training_explainability_overview(offset_details),
        "bundle_readiness": bundle_readiness,
        "metric_summary": metric_summary,
    }
    return json_ready(payload)


def _build_cfg(
    *,
    market: str,
    cycle: str | int,
    root: Path | None,
    model_family: str | None = None,
    target: str | None = None,
) -> ResearchConfig:
    return ResearchConfig.build(
        market=market,
        cycle=cycle,
        profile="default",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target=target or "direction",
        model_family=model_family or "deep_otm",
        root=root,
    )


def _resolve_training_run_dir(
    cfg: ResearchConfig,
    *,
    model_family: str | None,
    target: str | None,
    run_label: str | None,
    run_dir: str | Path | None,
) -> Path:
    if run_dir is not None:
        path = Path(run_dir)
        if path.exists():
            return path
        raise FileNotFoundError(f"Training run not found: {path}")

    rows = _list_training_runs(
        cfg,
        model_family=model_family,
        target=target,
        prefix=run_label,
    )
    if not rows:
        raise FileNotFoundError(
            f"No training runs found for market={cfg.asset.slug} cycle={cfg.cycle} "
            f"model_family={model_family or cfg.model_family} target={target or cfg.target}"
        )
    return Path(str(rows[-1]["path"]))


def _build_training_run_row(run_dir: Path) -> dict[str, object]:
    summary_path = run_dir / "summary.json"
    report_path = run_dir / "report.md"
    manifest_path = run_dir / "manifest.json"

    summary = read_json_object(summary_path)
    manifest = _read_manifest_dict(manifest_path)
    spec = dict(manifest.get("spec") or {}) if manifest is not None else {}
    offsets = _resolve_offsets(run_dir, summary=summary)
    market = _string_value(summary, "market") or _string_value(manifest, "market") or _part_value(run_dir, "asset")
    cycle = _string_value(summary, "cycle") or _string_value(manifest, "cycle") or _part_value(run_dir, "cycle")
    model_family = _string_value(summary, "model_family") or _string_value(spec, "model_family") or _part_value(run_dir, "model_family")
    target = _string_value(summary, "target") or _string_value(spec, "target") or _part_value(run_dir, "target")
    run_label = _string_value(summary, "run_label") or _part_value(run_dir, "run")
    feature_set = _string_value(summary, "feature_set") or _string_value(spec, "feature_set")
    label_set = _string_value(summary, "label_set") or _string_value(spec, "label_set")
    label_source = _string_value(summary, "label_source") or _string_value(spec, "label_source")
    window = _window_label(summary=summary, spec=spec)
    offset_metric_rows = _offset_metric_rows(run_dir, summary=summary)
    rows_total = _sum_numeric(offset_metric_rows, "rows")
    positive_rate_avg = _average_numeric(offset_metric_rows, "positive_rate")
    feature_count_range = _numeric_range(offset_metric_rows, "feature_count")
    bundle_readiness = _training_bundle_readiness(_bundle_readiness_rows(run_dir, offsets))
    metric_summary = _training_metric_summary(offset_metric_rows)

    return {
        "object_type": "training_run",
        "market": market,
        "cycle": cycle,
        "model_family": model_family,
        "target": target,
        "run_label": run_label,
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "manifest_path": str(manifest_path),
        "summary_exists": summary_path.exists(),
        "report_exists": report_path.exists(),
        "manifest_exists": manifest_path.exists(),
        "feature_set": feature_set,
        "label_set": label_set,
        "label_source": label_source,
        "window": window,
        "offsets": offsets,
        "offset_count": len(offsets),
        "rows_total": rows_total,
        "positive_rate_avg": positive_rate_avg,
        "feature_count_range": feature_count_range,
        "bundle_readiness": bundle_readiness,
        "metric_summary": metric_summary,
        "parallel_workers": summary.get("parallel_workers") if isinstance(summary, dict) else None,
        "overview_cards": _training_overview_cards(
            summary=summary,
            offsets=offsets,
            feature_set=feature_set,
            label_set=label_set,
            rows_total=rows_total,
            bundle_readiness=bundle_readiness,
        ),
        "action_context": {
            "market": market,
            "cycle": cycle,
            "model_family": model_family,
            "target": target,
            "run_label": run_label,
            "window": window,
            "offsets": offsets,
            "feature_set": feature_set,
            "label_set": label_set,
            "label_source": label_source,
            "run_dir": str(run_dir),
        },
        "summary": summary,
        "manifest": manifest,
    }


def _build_offset_details(run_dir: Path) -> list[dict[str, object]]:
    offsets_root = run_dir / "offsets"
    if not offsets_root.exists():
        return []

    rows: list[dict[str, object]] = []
    for offset_dir in sorted(
        [path for path in offsets_root.glob("offset=*") if path.is_dir()],
        key=lambda path: _offset_value(path),
    ):
        metrics_path = offset_dir / "metrics.json"
        summary_path = offset_dir / "summary.json"
        report_path = offset_dir / "report.md"
        feature_schema_path = offset_dir / "feature_schema.json"
        feature_cols_path = offset_dir / "feature_cols.joblib"
        feature_pruning_path = offset_dir / "feature_pruning.json"
        logreg_coefficients_path = offset_dir / "logreg_coefficients.json"
        lgb_feature_importance_path = offset_dir / "lgb_feature_importance.json"
        factor_direction_summary_path = offset_dir / "factor_direction_summary.json"
        factor_correlations_path = offset_dir / "factor_correlations.parquet"
        probe_path = offset_dir / "probe.json"
        oof_predictions_path = offset_dir / "oof_predictions.parquet"
        blend_weights_path = offset_dir / "calibration" / "blend_weights.json"
        reliability_bins_path = offset_dir / "calibration" / "reliability_bins.json"
        rows.append(
            {
                "offset": _offset_value(offset_dir),
                "offset_dir": str(offset_dir),
                "summary_path": str(summary_path),
                "metrics_path": str(metrics_path),
                "report_path": str(report_path),
                "feature_schema_path": str(feature_schema_path),
                "feature_cols_path": str(feature_cols_path),
                "feature_pruning_path": str(feature_pruning_path),
                "logreg_coefficients_path": str(logreg_coefficients_path),
                "lgb_feature_importance_path": str(lgb_feature_importance_path),
                "factor_direction_summary_path": str(factor_direction_summary_path),
                "factor_correlations_path": str(factor_correlations_path),
                "probe_path": str(probe_path),
                "oof_predictions_path": str(oof_predictions_path),
                "blend_weights_path": str(blend_weights_path),
                "reliability_bins_path": str(reliability_bins_path),
                "summary_exists": summary_path.exists(),
                "metrics_exists": metrics_path.exists(),
                "report_exists": report_path.exists(),
                "feature_schema_exists": feature_schema_path.exists(),
                "feature_cols_exists": feature_cols_path.exists(),
                "feature_pruning_exists": feature_pruning_path.exists(),
                "logreg_coefficients_exists": logreg_coefficients_path.exists(),
                "lgb_feature_importance_exists": lgb_feature_importance_path.exists(),
                "factor_direction_summary_exists": factor_direction_summary_path.exists(),
                "factor_correlations_exists": factor_correlations_path.exists(),
                "probe_exists": probe_path.exists(),
                "oof_predictions_exists": oof_predictions_path.exists(),
                "blend_weights_exists": blend_weights_path.exists(),
                "reliability_bins_exists": reliability_bins_path.exists(),
                "summary": read_json_object(summary_path),
                "metrics": read_json_object(metrics_path),
                "feature_schema": _read_json_value(feature_schema_path),
                "feature_pruning": read_json_object(feature_pruning_path),
                "logreg_coefficients": read_json_object(logreg_coefficients_path),
                "lgb_feature_importance": read_json_object(lgb_feature_importance_path),
                "factor_direction_summary": read_json_object(factor_direction_summary_path),
                "factor_correlations": _artifact_payload(factor_correlations_path),
                "probe": read_json_object(probe_path),
                "blend_weights": read_json_object(blend_weights_path),
                "reliability_bins": _read_json_value(reliability_bins_path),
                "report_text": _read_text_file(report_path),
                "model_files": _sorted_file_names(offset_dir / "models", "*.joblib"),
                "calibration_files": _sorted_file_names(offset_dir / "calibration", "*"),
            }
        )
    return rows


def _resolve_offsets(run_dir: Path, *, summary: dict[str, object] | None) -> list[int]:
    if isinstance(summary, dict):
        raw = summary.get("offsets")
        if isinstance(raw, list):
            values = []
            for item in raw:
                try:
                    values.append(int(item))
                except Exception:
                    continue
            return sorted(dict.fromkeys(values))
    values = []
    offsets_root = run_dir / "offsets"
    if offsets_root.exists():
        for path in offsets_root.glob("offset=*"):
            if path.is_dir():
                values.append(_offset_value(path))
    return sorted(dict.fromkeys(values))


def _training_overview_cards(
    *,
    summary: dict[str, object] | None,
    offsets: list[int],
    feature_set: str | None,
    label_set: str | None,
    rows_total: int | None,
    bundle_readiness: dict[str, object],
) -> list[dict[str, object]]:
    return [
        {"card_id": "offset_count", "label": "Offsets", "value": len(offsets)},
        {
            "card_id": "parallel_workers",
            "label": "Parallel Workers",
            "value": summary.get("parallel_workers") if isinstance(summary, dict) else None,
        },
        {"card_id": "feature_set", "label": "Feature Set", "value": feature_set},
        {"card_id": "label_set", "label": "Label Set", "value": label_set},
        {"card_id": "rows_total", "label": "Rows Total", "value": rows_total},
        {
            "card_id": "bundle_ready_offsets",
            "label": "Bundle Ready Offsets",
            "value": bundle_readiness.get("ready_offset_count"),
        },
    ]


def _training_run_overview(
    row: dict[str, object],
    *,
    bundle_readiness: dict[str, object] | None = None,
    metric_summary: dict[str, object] | None = None,
) -> dict[str, object]:
    readiness = bundle_readiness if isinstance(bundle_readiness, dict) else row.get("bundle_readiness")
    metrics = metric_summary if isinstance(metric_summary, dict) else row.get("metric_summary")
    readiness = readiness if isinstance(readiness, dict) else {}
    metrics = metrics if isinstance(metrics, dict) else {}
    return {
        "run_label": row.get("run_label"),
        "market": row.get("market"),
        "cycle": row.get("cycle"),
        "model_family": row.get("model_family"),
        "target": row.get("target"),
        "feature_set": row.get("feature_set"),
        "label_set": row.get("label_set"),
        "window": row.get("window"),
        "offset_count": row.get("offset_count"),
        "rows_total": row.get("rows_total"),
        "positive_rate_avg": row.get("positive_rate_avg"),
        "feature_count_range": row.get("feature_count_range"),
        "bundle_ready_offset_count": readiness.get("ready_offset_count"),
        "bundle_missing_offset_count": readiness.get("missing_offset_count"),
        "best_auc_offset": _nested_value(metrics, "best_auc_offset", "offset"),
        "best_brier_offset": _nested_value(metrics, "best_brier_offset", "offset"),
        "mean_auc": metrics.get("mean_auc"),
        "mean_brier": metrics.get("mean_brier"),
        "parallel_workers": row.get("parallel_workers"),
    }


def _training_offset_preview(rows: list[dict[str, object]]) -> dict[str, object]:
    preview_rows: list[dict[str, object]] = []
    for row in rows[:5]:
        summary = row.get("summary") if isinstance(row, dict) else {}
        summary = summary if isinstance(summary, dict) else {}
        metrics = summary.get("metrics")
        metrics = metrics if isinstance(metrics, dict) else {}
        split_summary = summary.get("split_summary")
        split_summary = split_summary if isinstance(split_summary, dict) else {}
        explainability = summary.get("explainability")
        explainability = explainability if isinstance(explainability, dict) else {}
        blend_weights = row.get("blend_weights") if isinstance(row, dict) else {}
        blend_weights = blend_weights if isinstance(blend_weights, dict) else {}
        readiness = _bundle_readiness_entry(row)
        preview_rows.append(
            {
                "offset": row.get("offset"),
                "rows": summary.get("rows"),
                "positive_rate": summary.get("positive_rate"),
                "feature_count": summary.get("feature_count"),
                "folds_used": split_summary.get("folds_used"),
                "auc": _metrics_payload_value(metrics, "auc"),
                "brier": _metrics_payload_value(metrics, "brier"),
                "logloss": _metrics_payload_value(metrics, "logloss"),
                "blend_w_lgb": blend_weights.get("w_lgb"),
                "blend_w_lr": blend_weights.get("w_lr"),
                "bundle_ready": readiness.get("is_ready"),
                "missing_bundle_artifacts": list(readiness.get("missing_artifacts") or []),
                "top_logreg_feature": _first_preview_feature(explainability.get("top_logreg_coefficients")),
                "top_lgb_feature": _first_preview_feature(explainability.get("top_lgb_importance")),
                "top_positive_factor": _first_preview_feature(explainability.get("top_positive_factors")),
                "top_negative_factor": _first_preview_feature(explainability.get("top_negative_factors")),
                "has_factor_correlations": bool(row.get("factor_correlations_exists")),
            }
        )
    return {"row_count": len(rows), "rows": preview_rows}


def _training_explainability_overview(rows: list[dict[str, object]]) -> dict[str, object]:
    top_logreg = _collect_explainability_rows(rows, "top_logreg_coefficients")
    top_lgb = _collect_explainability_rows(rows, "top_lgb_importance")
    top_positive = _collect_explainability_rows(rows, "top_positive_factors")
    top_negative = _collect_explainability_rows(rows, "top_negative_factors")
    readiness_rows = [_bundle_readiness_entry(row) for row in rows]
    return {
        "offsets_with_explainability": sum(
            1
            for row in rows
            if bool(row.get("logreg_coefficients_exists"))
            or bool(row.get("lgb_feature_importance_exists"))
            or bool(row.get("factor_correlations_exists"))
        ),
        "offsets_with_logreg_coefficients": sum(1 for row in rows if bool(row.get("logreg_coefficients_exists"))),
        "offsets_with_lgb_importance": sum(1 for row in rows if bool(row.get("lgb_feature_importance_exists"))),
        "offsets_with_factor_direction_summary": sum(1 for row in rows if bool(row.get("factor_direction_summary_exists"))),
        "offsets_with_factor_correlations": sum(1 for row in rows if bool(row.get("factor_correlations_exists"))),
        "bundle_ready_offsets": sum(1 for row in readiness_rows if bool(row.get("is_ready"))),
        "offsets_with_blend_weights": sum(1 for row in rows if bool(row.get("blend_weights_exists"))),
        "unique_top_logreg_feature_count": len({str(item.get("feature") or "").strip() for item in top_logreg if str(item.get("feature") or "").strip()}),
        "unique_top_lgb_feature_count": len({str(item.get("feature") or "").strip() for item in top_lgb if str(item.get("feature") or "").strip()}),
        "top_logreg_coefficients": top_logreg,
        "top_lgb_importance": top_lgb,
        "top_positive_factors": top_positive,
        "top_negative_factors": top_negative,
    }


def _offset_value(path: Path) -> int:
    try:
        return int(path.name.split("=", 1)[1])
    except Exception:
        return -1


def _window_label(*, summary: dict[str, object] | None, spec: dict[str, object]) -> str | None:
    value = _string_value(summary, "window")
    if value:
        return value
    window = spec.get("window")
    if isinstance(window, dict):
        label = window.get("label")
        if label is not None and str(label).strip():
            return str(label)
    return None


def _offset_summaries(summary: dict[str, object] | None) -> list[dict[str, object]]:
    if not isinstance(summary, dict):
        return []
    values = summary.get("offset_summaries")
    if not isinstance(values, list):
        return []
    return [item for item in values if isinstance(item, dict)]


def _offset_metric_rows(run_dir: Path, *, summary: dict[str, object] | None) -> list[dict[str, object]]:
    rows_by_offset: dict[int, dict[str, object]] = {}
    for item in _offset_summaries(summary):
        try:
            offset = int(item.get("offset"))
        except Exception:
            continue
        rows_by_offset[offset] = dict(item)
    offsets_root = run_dir / "offsets"
    if not offsets_root.exists():
        return [rows_by_offset[key] for key in sorted(rows_by_offset)]
    for offset_dir in sorted((path for path in offsets_root.glob("offset=*") if path.is_dir()), key=_offset_value):
        offset = _offset_value(offset_dir)
        current = dict(rows_by_offset.get(offset) or {})
        if all(current.get(key) is not None for key in ("rows", "positive_rate", "feature_count", "metrics")):
            rows_by_offset[offset] = current
            continue
        offset_summary = read_json_object(offset_dir / "summary.json")
        if not isinstance(offset_summary, dict):
            rows_by_offset[offset] = current
            continue
        current.setdefault("offset", offset)
        for key in ("rows", "positive_rate", "feature_count", "metrics"):
            if current.get(key) is None and offset_summary.get(key) is not None:
                current[key] = offset_summary.get(key)
        rows_by_offset[offset] = current
    return [rows_by_offset[key] for key in sorted(rows_by_offset)]


def _bundle_readiness_rows(run_dir: Path, offsets: list[int]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    offsets_root = run_dir / "offsets"
    for offset in offsets:
        offset_dir = offsets_root / f"offset={offset}"
        rows.append(
            {
                "offset": offset,
                "feature_schema_exists": (offset_dir / "feature_schema.json").exists(),
                "feature_cols_exists": (offset_dir / "feature_cols.joblib").exists(),
                "blend_weights_exists": (offset_dir / "calibration" / "blend_weights.json").exists(),
                "model_files": _sorted_file_names(offset_dir / "models", "*.joblib"),
                "calibration_files": _sorted_file_names(offset_dir / "calibration", "*"),
            }
        )
    return rows


def _sum_numeric(rows: list[dict[str, object]], key: str) -> int | None:
    values: list[int] = []
    for row in rows:
        value = row.get(key)
        try:
            values.append(int(value))
        except Exception:
            continue
    return sum(values) if values else None


def _average_numeric(rows: list[dict[str, object]], key: str) -> float | None:
    values: list[float] = []
    for row in rows:
        value = row.get(key)
        try:
            values.append(float(value))
        except Exception:
            continue
    if not values:
        return None
    return sum(values) / len(values)


def _numeric_range(rows: list[dict[str, object]], key: str) -> dict[str, int] | None:
    values: list[int] = []
    for row in rows:
        value = row.get(key)
        try:
            values.append(int(value))
        except Exception:
            continue
    if not values:
        return None
    return {
        "min": min(values),
        "max": max(values),
    }


def _training_metric_summary(rows: list[dict[str, object]]) -> dict[str, object]:
    metric_rows = _metric_rows_from_summary_rows(rows)
    return _metric_summary_from_flat_rows(metric_rows)


def _training_metric_summary_from_details(rows: list[dict[str, object]]) -> dict[str, object]:
    metric_rows = _metric_rows_from_detail_rows(rows)
    return _metric_summary_from_flat_rows(metric_rows)


def _metric_rows_from_summary_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    metric_rows: list[dict[str, object]] = []
    for row in rows:
        metric_rows.append(
            {
                "offset": row.get("offset"),
                "auc": _summary_metric_value(row, "auc"),
                "brier": _summary_metric_value(row, "brier"),
                "logloss": _summary_metric_value(row, "logloss"),
            }
        )
    return metric_rows


def _metric_rows_from_detail_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    metric_rows: list[dict[str, object]] = []
    for row in rows:
        summary = row.get("summary") if isinstance(row, dict) else None
        summary = summary if isinstance(summary, dict) else {}
        metrics = summary.get("metrics")
        metric_rows.append(
            {
                "offset": row.get("offset"),
                "auc": _metrics_payload_value(metrics, "auc"),
                "brier": _metrics_payload_value(metrics, "brier"),
                "logloss": _metrics_payload_value(metrics, "logloss"),
            }
        )
    return metric_rows


def _metric_summary_from_flat_rows(metric_rows: list[dict[str, object]]) -> dict[str, object]:
    auc_rows = [row for row in metric_rows if row.get("auc") is not None]
    brier_rows = [row for row in metric_rows if row.get("brier") is not None]
    logloss_rows = [row for row in metric_rows if row.get("logloss") is not None]
    best_auc = max(auc_rows, key=lambda row: float(row["auc"])) if auc_rows else None
    best_brier = min(brier_rows, key=lambda row: float(row["brier"])) if brier_rows else None
    return {
        "offsets_with_metrics": len(metric_rows),
        "offsets_with_auc": len(auc_rows),
        "offsets_with_brier": len(brier_rows),
        "offsets_with_logloss": len(logloss_rows),
        "mean_auc": _mean_from_rows(auc_rows, "auc"),
        "mean_brier": _mean_from_rows(brier_rows, "brier"),
        "mean_logloss": _mean_from_rows(logloss_rows, "logloss"),
        "best_auc_offset": dict(best_auc) if isinstance(best_auc, dict) else None,
        "best_brier_offset": dict(best_brier) if isinstance(best_brier, dict) else None,
    }


def _training_bundle_readiness(rows: list[dict[str, object]]) -> dict[str, object]:
    readiness_rows = [_bundle_readiness_entry(row) for row in rows]
    ready_rows = [row for row in readiness_rows if bool(row.get("is_ready"))]
    missing_counts: dict[str, int] = {}
    for row in readiness_rows:
        for artifact in row.get("missing_artifacts") or []:
            missing_counts[str(artifact)] = missing_counts.get(str(artifact), 0) + 1
    return {
        "required_artifacts": [
            "feature_schema",
            "feature_cols",
            "blend_weights",
            "logreg_model",
            "lgbm_model",
        ],
        "offset_count": len(readiness_rows),
        "ready_offset_count": len(ready_rows),
        "missing_offset_count": len(readiness_rows) - len(ready_rows),
        "is_ready": len(readiness_rows) > 0 and len(ready_rows) == len(readiness_rows),
        "missing_artifact_counts": missing_counts,
        "offset_rows": readiness_rows,
    }


def _bundle_readiness_entry(row: dict[str, object]) -> dict[str, object]:
    model_files = row.get("model_files") if isinstance(row.get("model_files"), list) else []
    missing: list[str] = []
    if not bool(row.get("feature_schema_exists")):
        missing.append("feature_schema")
    if not bool(row.get("feature_cols_exists")):
        missing.append("feature_cols")
    if not bool(row.get("blend_weights_exists")):
        missing.append("blend_weights")
    if "logreg_sigmoid.joblib" not in model_files:
        missing.append("logreg_model")
    if "lgbm_sigmoid.joblib" not in model_files:
        missing.append("lgbm_model")
    return {
        "offset": row.get("offset"),
        "is_ready": not missing,
        "missing_artifacts": missing,
        "model_file_count": len(model_files),
        "calibration_file_count": len(row.get("calibration_files") if isinstance(row.get("calibration_files"), list) else []),
    }


def _mean_from_rows(rows: list[dict[str, object]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _float_or_none(value: object) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _nested_value(payload: dict[str, object], key: str, nested_key: str) -> object:
    value = payload.get(key)
    if not isinstance(value, dict):
        return None
    return value.get(nested_key)


def _summary_metric_value(row: dict[str, object], key: str) -> float | None:
    metrics = row.get("metrics")
    value = _metrics_payload_value(metrics, key)
    if value is not None:
        return value
    return _float_or_none(row.get(f"{key}_blend"))


def _metrics_payload_value(metrics: object, key: str) -> float | None:
    if not isinstance(metrics, dict):
        return None
    direct = _float_or_none(metrics.get(key))
    if direct is not None:
        return direct
    for model_key in ("blend", "lgbm", "logreg"):
        payload = metrics.get(model_key)
        if isinstance(payload, dict):
            value = _float_or_none(payload.get(key))
            if value is not None:
                return value
    return None


def _collect_explainability_rows(rows: list[dict[str, object]], key: str, *, limit: int = 5) -> list[dict[str, object]]:
    collected: list[dict[str, object]] = []
    seen_features: set[str] = set()
    for row in rows:
        summary = row.get("summary") if isinstance(row, dict) else None
        summary = summary if isinstance(summary, dict) else {}
        explainability = summary.get("explainability")
        explainability = explainability if isinstance(explainability, dict) else {}
        for item in explainability.get(key) or []:
            if not isinstance(item, dict):
                continue
            feature = str(item.get("feature") or "").strip()
            dedupe_key = feature or str(item)
            if dedupe_key in seen_features:
                continue
            seen_features.add(dedupe_key)
            collected.append(dict(item))
            if len(collected) >= max(int(limit), 0):
                return collected
    return collected


def _first_preview_feature(values: object) -> str | None:
    if not isinstance(values, list) or not values:
        return None
    first = values[0]
    if not isinstance(first, dict):
        return None
    feature = first.get("feature")
    if feature is None:
        return None
    text = str(feature).strip()
    return text or None


def _part_value(path: Path, key: str) -> str | None:
    token = f"{key}="
    for part in path.parts:
        if part.startswith(token):
            return part.split("=", 1)[1]
    return None


def _string_value(payload: dict[str, object] | None, key: str) -> str | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _read_manifest_dict(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        return read_manifest(path).to_dict()
    except Exception:
        return None


def _read_text_file(path: Path) -> str | None:
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def _read_json_value(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        import json

        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _sorted_file_names(directory: Path, pattern: str) -> list[str]:
    if not directory.exists():
        return []
    return sorted(path.name for path in directory.glob(pattern) if path.is_file())


def _artifact_payload(path: Path) -> dict[str, object]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "size_bytes": int(path.stat().st_size) if path.exists() else 0,
    }
