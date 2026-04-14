from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib

from pm15min.research.bundles.active_registry import write_active_bundle_selection
from pm15min.research.config import ResearchConfig
from pm15min.research.manifests import build_manifest, utc_manifest_timestamp, write_manifest


REPO_ROOT = Path(__file__).resolve().parents[2]
V2_ROOT = REPO_ROOT / "v2"
RESEARCH_ROOT = V2_ROOT / "research"
QUARANTINE_ROOT = V2_ROOT / "var" / "quarantine" / "model_bundles_reversal_legacy"
LOG_PATH = V2_ROOT / "var" / "research" / "logs" / "direction_bundle_import_20260319.json"


@dataclass(frozen=True)
class ImportSpec:
    asset: str
    profile: str
    bundle_label: str
    usage: str
    label_set: str
    feature_set: str
    source_run_dir: Path
    allowed_blacklist_columns: tuple[str, ...]
    notes: str
    source_group: str
    offsets: tuple[int, ...] = (7, 8, 9)
    target: str = "direction"

    @property
    def bundle_dir(self) -> Path:
        return (
            RESEARCH_ROOT
            / "model_bundles"
            / "cycle=15m"
            / f"asset={self.asset}"
            / f"profile={self.profile}"
            / f"target={self.target}"
            / f"bundle={self.bundle_label}"
        )

    @property
    def object_id(self) -> str:
        return f"model_bundle:{self.profile}:{self.target}:{self.bundle_label}"


IMPORT_SPECS: tuple[ImportSpec, ...] = (
    ImportSpec(
        asset="sol",
        profile="deep_otm_baseline",
        bundle_label="legacy-train_v6_bs_q_replace_sol_20260313_163701",
        usage="baseline_reference",
        label_set="settlement_truth",
        feature_set="bs_q_replace_direction",
        source_run_dir=(
            REPO_ROOT
            / "data/markets/sol/artifacts_runs/by_group/B_bs_replace_direction/bs_replace/bs_q_replace/base/"
            / "train_v6_bs_q_replace_sol_20260313_163701"
        ),
        allowed_blacklist_columns=("delta_rsi_5",),
        notes="SOL deep_otm baseline reference run from March 13, 2026.",
        source_group="B_bs_replace_direction",
    ),
    ImportSpec(
        asset="xrp",
        profile="deep_otm_baseline",
        bundle_label="legacy-train_v6_bs_q_replace_xrp_20260313_163602",
        usage="baseline_reference",
        label_set="settlement_truth",
        feature_set="bs_q_replace_direction",
        source_run_dir=(
            REPO_ROOT
            / "data/markets/xrp/artifacts_runs/by_group/B_bs_replace_direction/bs_replace/bs_q_replace/base/"
            / "train_v6_bs_q_replace_xrp_20260313_163602"
        ),
        allowed_blacklist_columns=("ret_5m", "ma_gap_5"),
        notes="XRP deep_otm baseline reference run from March 13, 2026.",
        source_group="B_bs_replace_direction",
    ),
    ImportSpec(
        asset="sol",
        profile="deep_otm",
        bundle_label="legacy-train_v6_sol_stage2_pool_drop_volume_z_3_end0309_dist_20260317_161410",
        usage="live_current",
        label_set="settlement_truth",
        feature_set="alpha_search_direction_live",
        source_run_dir=(
            REPO_ROOT
            / "data/markets/sol/artifacts_runs/train_v6_sol_stage2_pool_drop_volume_z_3_end0309_dist_20260317_161410"
        ),
        allowed_blacklist_columns=("delta_rsi_5",),
        notes="Current SOL live direction run documented in ALPHA_PROGRESS_20260317.md.",
        source_group="live_current",
    ),
    ImportSpec(
        asset="xrp",
        profile="deep_otm",
        bundle_label="legacy-train_v6_xrp_stage6_q_capacity_cross_drop_vol_price_corr_15_vwap_gap_20_end0309_dist_20260318_162707",
        usage="live_current",
        label_set="settlement_truth",
        feature_set="alpha_search_direction_live",
        source_run_dir=(
            REPO_ROOT
            / "data/markets/xrp/artifacts_runs/alpha_search_stage6_qcentered_capacity_cross_repair_end0309_dist/"
            / "train_v6_xrp_stage6_q_capacity_cross_drop_vol_price_corr_15_vwap_gap_20_end0309_dist_20260318_162707"
        ),
        allowed_blacklist_columns=("ret_5m", "ma_gap_5"),
        notes="Current XRP live direction run documented in ALPHA_PROGRESS_20260317.md.",
        source_group="live_current",
    ),
)


def main() -> None:
    summary: dict[str, Any] = {
        "imported_at": utc_manifest_timestamp(),
        "quarantined_reversal_targets": quarantine_reversal_bundles(),
        "bundles": [],
    }
    for spec in IMPORT_SPECS:
        summary["bundles"].append(import_bundle(spec))
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True))


def quarantine_reversal_bundles() -> list[dict[str, str]]:
    moved: list[dict[str, str]] = []
    for asset in ("sol", "xrp"):
        source_dir = (
            RESEARCH_ROOT
            / "model_bundles"
            / "cycle=15m"
            / f"asset={asset}"
            / "profile=deep_otm"
            / "target=reversal"
        )
        if not source_dir.exists():
            continue
        dest_dir = QUARANTINE_ROOT / "cycle=15m" / f"asset={asset}" / "profile=deep_otm" / "target=reversal"
        if dest_dir.exists():
            shutil.rmtree(dest_dir)
        dest_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source_dir), str(dest_dir))
        moved.append({"asset": asset, "source": str(source_dir), "destination": str(dest_dir)})
    return moved


def import_bundle(spec: ImportSpec) -> dict[str, Any]:
    if not spec.source_run_dir.exists():
        raise FileNotFoundError(f"Missing legacy artifact run: {spec.source_run_dir}")

    bundle_dir = spec.bundle_dir
    if bundle_dir.exists():
        backup_dir = (
            V2_ROOT
            / "var"
            / "quarantine"
            / "bundle_reimports"
            / "cycle=15m"
            / f"asset={spec.asset}"
            / f"profile={spec.profile}"
            / f"target={spec.target}"
            / f"bundle={spec.bundle_label}"
        )
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
        backup_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(bundle_dir), str(backup_dir))

    copied_offsets: list[int] = []
    offset_summaries: list[dict[str, Any]] = []
    for offset in spec.offsets:
        offset_summaries.append(import_offset(spec=spec, offset=int(offset)))
        copied_offsets.append(int(offset))

    manifest = build_manifest(
        object_type="model_bundle",
        object_id=spec.object_id,
        market=spec.asset,
        cycle="15m",
        path=bundle_dir,
        spec={
            "bundle_label": spec.bundle_label,
            "feature_set": spec.feature_set,
            "imported_from_legacy": True,
            "label_set": spec.label_set,
            "offsets": copied_offsets,
            "profile": spec.profile,
            "source_group": spec.source_group,
            "source_kind": "artifacts_runs",
            "source_training_run": str(spec.source_run_dir.resolve()),
            "target": spec.target,
            "usage": spec.usage,
            "notes": spec.notes,
        },
        inputs=[
            {"kind": "legacy_artifact_run", "path": str(spec.source_run_dir.resolve())},
        ],
        outputs=[
            {"kind": "model_bundle_dir", "path": str(bundle_dir.resolve())},
            {"kind": "bundle_manifest", "path": str((bundle_dir / "manifest.json").resolve())},
        ],
        metadata={
            "offset_count": len(copied_offsets),
            "offsets": copied_offsets,
            "allowed_blacklist_columns": list(spec.allowed_blacklist_columns),
            "usage": spec.usage,
        },
    )
    write_manifest(bundle_dir / "manifest.json", manifest)
    registry_cfg = ResearchConfig.build(
        market=spec.asset,
        cycle="15m",
        profile=spec.profile,
        target=spec.target,
        root=V2_ROOT,
    )
    selection_path = write_active_bundle_selection(
        registry_cfg,
        profile=spec.profile,
        target=spec.target,
        bundle_label=spec.bundle_label,
        bundle_dir=bundle_dir.resolve(),
        usage=spec.usage,
        source_run_dir=spec.source_run_dir.resolve(),
        notes=spec.notes,
        metadata={
            "offsets": copied_offsets,
            "source_group": spec.source_group,
            "allowed_blacklist_columns": list(spec.allowed_blacklist_columns),
        },
    )

    return {
        "asset": spec.asset,
        "profile": spec.profile,
        "target": spec.target,
        "usage": spec.usage,
        "source_run_dir": str(spec.source_run_dir.resolve()),
        "bundle_dir": str(bundle_dir.resolve()),
        "active_selection_path": str(selection_path.resolve()),
        "offsets": copied_offsets,
        "offset_summaries": offset_summaries,
    }


def import_offset(*, spec: ImportSpec, offset: int) -> dict[str, Any]:
    source_dir = spec.source_run_dir / f"artifacts_v6_offset{offset}_ensemble"
    if not source_dir.exists():
        raise FileNotFoundError(f"Missing source offset dir: {source_dir}")

    offset_dir = spec.bundle_dir / "offsets" / f"offset={offset}"
    models_dir = offset_dir / "models"
    calibration_dir = offset_dir / "calibration"
    diagnostics_dir = offset_dir / "diagnostics"
    models_dir.mkdir(parents=True, exist_ok=True)
    calibration_dir.mkdir(parents=True, exist_ok=True)
    diagnostics_dir.mkdir(parents=True, exist_ok=True)

    feature_columns = [str(column) for column in joblib.load(source_dir / "feature_cols.joblib")]
    feature_schema = [{"name": column, "dtype": "unknown", "source": "legacy_import"} for column in feature_columns]
    (offset_dir / "feature_schema.json").write_text(
        json.dumps(feature_schema, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    shutil.copy2(source_dir / "feature_cols.joblib", offset_dir / "feature_cols.joblib")

    copied_model_files: list[str] = []
    for model_name in ("lgbm_sigmoid.joblib", "logreg_sigmoid.joblib", "catboost.joblib", "xgb.joblib"):
        src = source_dir / model_name
        if src.exists():
            shutil.copy2(src, models_dir / model_name)
            copied_model_files.append(model_name)

    copied_calibration_files: list[str] = []
    for src in sorted(source_dir.glob("blend_weights.json")):
        shutil.copy2(src, calibration_dir / src.name)
        copied_calibration_files.append(src.name)
    for src in sorted(source_dir.glob("cat_temperature.json")):
        shutil.copy2(src, calibration_dir / src.name)
        copied_calibration_files.append(src.name)
    for src in sorted(source_dir.glob("reliability_bins*.json")):
        shutil.copy2(src, calibration_dir / src.name)
        copied_calibration_files.append(src.name)

    copied_diagnostics: list[str] = []
    for file_name in ("metrics.json", "final_model_probe.json"):
        src = source_dir / file_name
        if src.exists():
            shutil.copy2(src, diagnostics_dir / file_name)
            copied_diagnostics.append(file_name)

    bundle_config = {
        "offset": int(offset),
        "profile": str(spec.profile),
        "signal_target": str(spec.target),
        "source_training_run": str(spec.source_run_dir.resolve()),
        "source_artifact_dir": str(source_dir.resolve()),
        "feature_set": str(spec.feature_set),
        "feature_columns": feature_columns,
        "required_feature_columns": feature_columns,
        "missing_feature_fill_value": 0.0,
        "allowed_blacklist_columns": list(spec.allowed_blacklist_columns),
        "usage": str(spec.usage),
        "label_set": str(spec.label_set),
    }
    (offset_dir / "bundle_config.json").write_text(
        json.dumps(bundle_config, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

    return {
        "offset": offset,
        "source_artifact_dir": str(source_dir.resolve()),
        "bundle_offset_dir": str(offset_dir.resolve()),
        "feature_count": len(feature_columns),
        "copied_model_files": copied_model_files,
        "copied_calibration_files": copied_calibration_files,
        "copied_diagnostics": copied_diagnostics,
    }


if __name__ == "__main__":
    main()
