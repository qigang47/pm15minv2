from __future__ import annotations

import json
import shutil
from pathlib import Path

import joblib

from pm15min.research.bundles.loader import (
    read_training_run_manifest,
    read_training_run_summary,
    resolve_training_run_dir,
)
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import ModelBundleSpec
from pm15min.research.manifests import build_manifest, write_manifest


_OPTIONAL_OFFSET_ARTIFACTS = (
    ("metrics.json", "diagnostics/metrics.json"),
    ("feature_pruning.json", "diagnostics/feature_pruning.json"),
    ("probe.json", "diagnostics/probe.json"),
    ("logreg_coefficients.json", "diagnostics/logreg_coefficients.json"),
    ("lgb_feature_importance.json", "diagnostics/lgb_feature_importance.json"),
    ("factor_direction_summary.json", "diagnostics/factor_direction_summary.json"),
    ("factor_correlations.parquet", "diagnostics/factor_correlations.parquet"),
    ("oof_predictions.parquet", "diagnostics/oof_predictions.parquet"),
    ("summary.json", "diagnostics/summary.json"),
    ("report.md", "diagnostics/report.md"),
    ("reports/offset_report.md", "reports/offset_report.md"),
)


def build_model_bundle(cfg: ResearchConfig, spec: ModelBundleSpec) -> dict[str, object]:
    training_run_dir = resolve_training_run_dir(
        cfg,
        target=spec.target,
        source_training_run=spec.source_training_run,
    )
    training_manifest = read_training_run_manifest(training_run_dir)
    training_summary = read_training_run_summary(training_run_dir)

    bundle_dir = cfg.layout.bundle_dir(
        profile=spec.profile,
        target=spec.target,
        bundle_label_text=spec.bundle_label,
    )
    bundle_dir.mkdir(parents=True, exist_ok=True)
    summary_path = bundle_dir / "summary.json"
    report_path = bundle_dir / "report.md"

    copied_offsets: list[int] = []
    optional_offset_artifacts: dict[str, list[str]] = {}
    allowed_blacklist_columns: set[str] = set()
    for offset in spec.offsets:
        copied_offset, copied_optional, offset_allowed_blacklist = _copy_offset_bundle(
            training_run_dir=training_run_dir,
            bundle_dir=bundle_dir,
            offset=int(offset),
            profile=spec.profile,
            target=spec.target,
            source_training_run=training_run_dir.name.split("=", 1)[1],
            feature_set=str(training_manifest.spec.get("feature_set") or ""),
        )
        copied_offsets.append(copied_offset)
        optional_offset_artifacts[f"offset={copied_offset}"] = copied_optional
        allowed_blacklist_columns.update(offset_allowed_blacklist)

    summary_payload = _build_bundle_summary(
        cfg=cfg,
        spec=spec,
        bundle_dir=bundle_dir,
        training_run_dir=training_run_dir,
        training_manifest=training_manifest,
        training_summary=training_summary,
        copied_offsets=copied_offsets,
        optional_offset_artifacts=optional_offset_artifacts,
        allowed_blacklist_columns=sorted(allowed_blacklist_columns),
    )
    summary_path.write_text(
        json.dumps(summary_payload, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    report_path.write_text(_render_bundle_report(summary_payload), encoding="utf-8")

    manifest = build_manifest(
        object_type="model_bundle",
        object_id=spec.object_id,
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        path=bundle_dir,
        spec={
            **spec.to_dict(),
            "source_training_run": str(training_run_dir),
            "feature_set": training_manifest.spec.get("feature_set"),
            "label_set": training_manifest.spec.get("label_set"),
            "model_family": training_manifest.spec.get("model_family"),
            "training_window": training_manifest.spec.get("window"),
        },
        inputs=[
            {"kind": "training_run", "path": str(training_run_dir)},
            {"kind": "training_manifest", "path": str(training_run_dir / "manifest.json")},
            {"kind": "training_summary", "path": str(training_run_dir / "summary.json")},
        ],
        outputs=[
            {"kind": "model_bundle_dir", "path": str(bundle_dir)},
            {"kind": "bundle_manifest", "path": str(bundle_dir / "manifest.json")},
            {"kind": "summary_json", "path": str(summary_path)},
            {"kind": "report_md", "path": str(report_path)},
        ],
        metadata={
            "offset_count": len(copied_offsets),
            "offsets": copied_offsets,
            "source_training_object_id": training_manifest.object_id,
            "source_training_offsets": training_summary.get("offsets", []),
            "optional_offset_artifacts": optional_offset_artifacts,
            "allowed_blacklist_columns": sorted(allowed_blacklist_columns),
            "bundle_summary_path": str(summary_path),
        },
    )
    write_manifest(bundle_dir / "manifest.json", manifest)
    return {
        "dataset": "model_bundle",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "profile": spec.profile,
        "target": spec.target,
        "feature_set": training_manifest.spec.get("feature_set"),
        "label_set": training_manifest.spec.get("label_set"),
        "bundle_label": spec.bundle_label,
        "offsets": copied_offsets,
        "source_training_run": str(training_run_dir),
        "bundle_dir": str(bundle_dir),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "manifest_path": str(bundle_dir / "manifest.json"),
    }


def _copy_offset_bundle(
    *,
    training_run_dir: Path,
    bundle_dir: Path,
    offset: int,
    profile: str,
    target: str,
    source_training_run: str,
    feature_set: str,
) -> tuple[int, list[str], list[str]]:
    source_dir = training_run_dir / "offsets" / f"offset={offset}"
    if not source_dir.exists():
        raise FileNotFoundError(f"Training run missing offset dir: {source_dir}")

    dest_dir = bundle_dir / "offsets" / f"offset={offset}"
    models_dir = dest_dir / "models"
    calibration_dir = dest_dir / "calibration"
    models_dir.mkdir(parents=True, exist_ok=True)
    calibration_dir.mkdir(parents=True, exist_ok=True)

    _copy_required(source_dir / "feature_schema.json", dest_dir / "feature_schema.json")
    _copy_required(source_dir / "feature_cols.joblib", dest_dir / "feature_cols.joblib")
    _copy_required(source_dir / "calibration" / "blend_weights.json", calibration_dir / "blend_weights.json")

    for source_model in sorted((source_dir / "models").glob("*.joblib")):
        shutil.copy2(source_model, models_dir / source_model.name)

    copied_optional = _copy_optional_offset_artifacts(source_dir=source_dir, dest_dir=dest_dir)
    copied_optional.extend(
        _copy_optional_calibration_artifacts(
            source_calibration_dir=source_dir / "calibration",
            dest_calibration_dir=calibration_dir,
        )
    )

    feature_columns = joblib.load(source_dir / "feature_cols.joblib")
    allowed_blacklist_columns = _load_allowed_blacklist_columns(source_dir / "feature_pruning.json")
    bundle_config = {
        "offset": int(offset),
        "profile": str(profile),
        "signal_target": str(target),
        "source_training_run": str(source_training_run),
        "feature_set": str(feature_set),
        "feature_columns": list(feature_columns),
        "required_feature_columns": list(feature_columns),
        "missing_feature_fill_value": 0.0,
        "allowed_blacklist_columns": allowed_blacklist_columns,
    }
    (dest_dir / "bundle_config.json").write_text(
        json.dumps(bundle_config, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    return int(offset), sorted(set(copied_optional)), allowed_blacklist_columns


def _copy_required(source: Path, dest: Path) -> None:
    if not source.exists():
        raise FileNotFoundError(f"Missing required bundle input: {source}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, dest)


def _copy_optional_offset_artifacts(*, source_dir: Path, dest_dir: Path) -> list[str]:
    copied: list[str] = []
    for relative_source, relative_dest in _OPTIONAL_OFFSET_ARTIFACTS:
        source_path = source_dir / relative_source
        if not source_path.exists():
            continue
        dest_path = dest_dir / relative_dest
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, dest_path)
        copied.append(relative_dest)
    return copied


def _load_allowed_blacklist_columns(path: Path) -> list[str]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    dropped = payload.get("dropped_columns", [])
    if not isinstance(dropped, list):
        return []
    return [str(item) for item in dropped]


def _copy_optional_calibration_artifacts(*, source_calibration_dir: Path, dest_calibration_dir: Path) -> list[str]:
    if not source_calibration_dir.exists():
        return []
    copied: list[str] = []
    for source_path in sorted(source_calibration_dir.iterdir()):
        if not source_path.is_file():
            continue
        if source_path.name == "blend_weights.json":
            continue
        if source_path.suffix.lower() not in {".json", ".parquet", ".csv", ".md"}:
            continue
        dest_path = dest_calibration_dir / source_path.name
        shutil.copy2(source_path, dest_path)
        copied.append(f"calibration/{source_path.name}")
    return copied


def _build_bundle_summary(
    *,
    cfg: ResearchConfig,
    spec: ModelBundleSpec,
    bundle_dir: Path,
    training_run_dir: Path,
    training_manifest,
    training_summary: dict[str, object],
    copied_offsets: list[int],
    optional_offset_artifacts: dict[str, list[str]],
    allowed_blacklist_columns: list[str],
) -> dict[str, object]:
    offset_lookup = {
        int(item.get("offset")): dict(item)
        for item in training_summary.get("offset_summaries", [])
        if isinstance(item, dict) and item.get("offset") is not None
    }
    return {
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "profile": spec.profile,
        "target": spec.target,
        "bundle_label": spec.bundle_label,
        "bundle_dir": str(bundle_dir),
        "source_training_run_dir": str(training_run_dir),
        "source_training_object_id": training_manifest.object_id,
        "feature_set": training_manifest.spec.get("feature_set"),
        "label_set": training_manifest.spec.get("label_set"),
        "model_family": training_manifest.spec.get("model_family"),
        "training_window": training_manifest.spec.get("window"),
        "offsets": copied_offsets,
        "offset_count": len(copied_offsets),
        "allowed_blacklist_columns": list(allowed_blacklist_columns),
        "offset_summaries": [offset_lookup[offset] for offset in copied_offsets if offset in offset_lookup],
        "optional_offset_artifacts": optional_offset_artifacts,
    }


def _render_bundle_report(summary: dict[str, object]) -> str:
    lines = [
        "# Model Bundle Summary",
        "",
        f"- market: `{summary['market']}`",
        f"- cycle: `{summary['cycle']}`",
        f"- profile: `{summary['profile']}`",
        f"- target: `{summary['target']}`",
        f"- bundle_label: `{summary['bundle_label']}`",
        f"- feature_set: `{summary['feature_set']}`",
        f"- label_set: `{summary['label_set']}`",
        f"- model_family: `{summary['model_family']}`",
        f"- source_training_run_dir: `{summary['source_training_run_dir']}`",
        f"- offsets: `{summary['offsets']}`",
        f"- allowed_blacklist_columns: `{summary.get('allowed_blacklist_columns', [])}`",
        "",
        "## Offset Diagnostics",
        "",
    ]
    offset_summaries = summary.get("offset_summaries", [])
    if isinstance(offset_summaries, list) and offset_summaries:
        lines.append(_offset_metrics_markdown(offset_summaries))
    else:
        lines.append("No offset metrics available.")
    lines.extend(["", "## Copied Optional Artifacts", ""])
    optional_artifacts = summary.get("optional_offset_artifacts", {})
    if isinstance(optional_artifacts, dict) and optional_artifacts:
        for offset_key, artifacts in sorted(optional_artifacts.items()):
            lines.append(f"- {offset_key}: `{artifacts}`")
    else:
        lines.append("No optional artifacts copied.")
    lines.append("")
    return "\n".join(lines)


def _offset_metrics_markdown(rows: list[dict[str, object]]) -> str:
    headers = ["offset", "rows", "positive_rate", "brier_blend", "auc_blend"]
    lines = [
        "| offset | rows | positive_rate | brier_blend | auc_blend |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| {offset} | {rows} | {positive_rate} | {brier_blend} | {auc_blend} |".format(
                offset=row.get("offset", ""),
                rows=row.get("rows", ""),
                positive_rate=row.get("positive_rate", ""),
                brier_blend=row.get("brier_blend", ""),
                auc_blend=row.get("auc_blend", ""),
            )
        )
    return "\n".join(lines)
