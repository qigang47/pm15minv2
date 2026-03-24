from __future__ import annotations

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.config import ResearchConfig
from pm15min.research.labels.frames import LABEL_FRAME_COLUMNS, build_label_frame, label_frame_metadata
from pm15min.research.labels.loaders import load_label_inputs
from pm15min.research.labels.runtime import build_label_runtime_summary, build_truth_runtime_summary
from pm15min.research.labels.sources import resolve_label_build_plan
from pm15min.research.manifests import build_manifest, write_manifest


def build_label_frame_dataset(cfg: ResearchConfig) -> dict[str, object]:
    data_cfg = DataConfig.build(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        surface=cfg.source_surface,
        root=cfg.layout.storage.rewrite_root,
    )
    plan = resolve_label_build_plan(cfg.label_set)
    truth_table, oracle_table = load_label_inputs(data_cfg)
    frame = build_label_frame(
        label_set=cfg.label_set,
        truth_table=truth_table,
        oracle_prices_table=oracle_table,
    )
    runtime_summary = build_label_runtime_summary(
        truth_table=truth_table,
        oracle_prices_table=oracle_table,
        truth_path=data_cfg.layout.truth_table_path,
        oracle_path=data_cfg.layout.oracle_prices_table_path,
    )
    truth_runtime_summary = build_truth_runtime_summary(data_cfg)

    data_path = cfg.layout.label_frame_path(cfg.label_set)
    manifest_path = cfg.layout.label_frame_manifest_path(cfg.label_set)
    write_parquet_atomic(frame, data_path)

    manifest = build_manifest(
        object_type="label_frame",
        object_id=f"label_frame:{cfg.asset.slug}:{cfg.label_set}",
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        path=data_path,
        spec={
            "label_set": cfg.label_set,
            "build_plan": plan.to_dict(),
            "columns": list(LABEL_FRAME_COLUMNS),
        },
        inputs=[
            {"path": str(data_cfg.layout.truth_table_path), "kind": "truth_15m"},
            {"path": str(data_cfg.layout.oracle_prices_table_path), "kind": "oracle_prices_15m"},
        ],
        outputs=[
            {"path": str(data_path), "kind": "label_frame_parquet"},
            {"path": str(manifest_path), "kind": "manifest"},
        ],
        metadata=(label_frame_metadata(frame) | runtime_summary | truth_runtime_summary),
    )
    write_manifest(manifest_path, manifest)
    return {
        "dataset": "label_frame",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "source_surface": cfg.source_surface,
        "label_set": cfg.label_set,
        "label_source": plan.label_source or plan.base_label_set,
        "rows_written": int(len(frame)),
        "target_path": str(data_path),
        "manifest_path": str(manifest_path),
    }
