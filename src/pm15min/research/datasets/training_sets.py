from __future__ import annotations

import pandas as pd

from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import TrainingSetSpec
from pm15min.research.datasets.loaders import load_feature_frame, load_label_frame
from pm15min.research.freshness import inspect_research_artifacts_freshness, prepare_research_artifacts
from pm15min.research.layout_helpers import normalize_window_bound, window_bound_is_date_only
from pm15min.research.labels.alignment import merge_feature_and_label_frames
from pm15min.research.labels.direction import build_direction_target
from pm15min.research.labels.reversal import build_reversal_target
from pm15min.research.labels.sources import summarize_label_sources
from pm15min.research.manifests import build_manifest, write_manifest


TRAINING_SET_META_COLUMNS = [
    "decision_ts",
    "cycle_start_ts",
    "cycle_end_ts",
    "offset",
    "target",
    "y",
    "y_direction",
    "current_ret",
    "current_ret_col",
    "current_up",
    "winner_side",
    "resolved",
    "settlement_source",
    "label_source",
    "label_alignment_mode",
    "label_alignment_status",
    "label_alignment_gap_seconds",
    "price_to_beat",
    "final_price",
    "full_truth",
]

TRAINING_SET_LABEL_REQUIRED_COLUMNS = [
    "asset",
    "cycle_start_ts",
    "cycle_end_ts",
    "market_id",
    "condition_id",
    "label_set",
    "settlement_source",
    "label_source",
    "resolved",
    "price_to_beat",
    "final_price",
    "winner_side",
    "direction_up",
    "full_truth",
]


def build_training_set_dataset(
    cfg: ResearchConfig,
    spec: TrainingSetSpec,
    *,
    skip_freshness: bool = False,
    dependency_mode: str = "auto_repair",
) -> dict[str, object]:
    if not skip_freshness:
        freshness = inspect_research_artifacts_freshness(
            cfg,
            feature_set=spec.feature_set,
            label_set=spec.label_set,
        )
        needs_prepare = any(
            str(((freshness.get(name) or {}).get("status")) or "") != "fresh"
            for name in ("feature_frame", "label_frame")
        )
        if needs_prepare:
            prepare_research_artifacts(
                cfg,
                feature_set=spec.feature_set,
                label_set=spec.label_set,
                mode=dependency_mode,
            )
    features = load_feature_frame(
        cfg,
        feature_set=spec.feature_set,
        filters=_training_feature_frame_filters(spec),
    )
    labels = load_label_frame(
        cfg,
        label_set=spec.label_set,
        columns=TRAINING_SET_LABEL_REQUIRED_COLUMNS,
        filters=_training_label_frame_filters(spec),
    )

    merged, alignment_metadata = merge_feature_and_label_frames(features, labels)
    filtered = _filter_training_window(merged, start=spec.window.start, end=spec.window.end, offset=spec.offset)
    dataset, metadata = _build_target_frame(filtered, target=spec.target)
    metadata = {
        **alignment_metadata,
        **metadata,
    }

    data_path = cfg.layout.training_set_path(
        feature_set=spec.feature_set,
        label_set=spec.label_set,
        target=spec.target,
        window=spec.window.label,
        offset=spec.offset,
    )
    manifest_path = cfg.layout.training_set_manifest_path(
        feature_set=spec.feature_set,
        label_set=spec.label_set,
        target=spec.target,
        window=spec.window.label,
        offset=spec.offset,
    )

    write_parquet_atomic(dataset, data_path)
    manifest = build_manifest(
        object_type="training_set",
        object_id=spec.object_id,
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        path=data_path,
        spec=spec.to_dict(),
        inputs=[
            {
                "path": str(cfg.layout.feature_frame_path(spec.feature_set, source_surface=cfg.source_surface)),
                "kind": "feature_frame",
            },
            {
                "path": str(cfg.layout.label_frame_path(spec.label_set)),
                "kind": "label_frame",
            },
        ],
        outputs=[
            {"path": str(data_path), "kind": "training_set_parquet"},
            {"path": str(manifest_path), "kind": "manifest"},
        ],
        metadata=metadata,
    )
    write_manifest(manifest_path, manifest)
    return {
        "dataset": "training_set",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "feature_set": spec.feature_set,
        "label_set": spec.label_set,
        "label_source": _primary_label_source(metadata, fallback=spec.label_source),
        "label_sources": list(metadata.get("label_sources") or []),
        "target": spec.target,
        "window": spec.window.label,
        "offset": spec.offset,
        "rows_written": int(len(dataset)),
        "target_path": str(data_path),
        "manifest_path": str(manifest_path),
    }


def _filter_training_window(frame: pd.DataFrame, *, start: str, end: str, offset: int) -> pd.DataFrame:
    out = frame.copy()
    start_ts = _window_bound_timestamp(start)
    end_ts = _window_bound_timestamp(end)
    end_bound = end_ts + pd.Timedelta(days=1) if window_bound_is_date_only(end) else end_ts
    mask = (
        out["decision_ts"].notna()
        & (out["decision_ts"] >= start_ts)
        & (out["decision_ts"] < end_bound)
        & (pd.to_numeric(out["offset"], errors="coerce") == int(offset))
    )
    return out.loc[mask].copy()


def _window_bound_timestamp(value: str) -> pd.Timestamp:
    ts = pd.Timestamp(normalize_window_bound(value))
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _training_feature_frame_filters(spec: TrainingSetSpec) -> list[tuple[str, str, object]]:
    start_ts, end_bound = _training_window_bounds(spec)
    return [
        ("decision_ts", ">=", start_ts),
        ("decision_ts", "<", end_bound),
        ("offset", "==", int(spec.offset)),
    ]


def _training_label_frame_filters(spec: TrainingSetSpec) -> list[tuple[str, str, object]]:
    start_ts, end_bound = _training_window_bounds(spec)
    return [
        ("cycle_end_ts", ">", int(start_ts.timestamp())),
        ("cycle_start_ts", "<", int(end_bound.timestamp())),
    ]


def _training_window_bounds(spec: TrainingSetSpec) -> tuple[pd.Timestamp, pd.Timestamp]:
    start_ts = _window_bound_timestamp(spec.window.start)
    end_ts = _window_bound_timestamp(spec.window.end)
    end_bound = end_ts + pd.Timedelta(days=1) if window_bound_is_date_only(spec.window.end) else end_ts
    return start_ts, end_bound


def _build_target_frame(frame: pd.DataFrame, *, target: str) -> tuple[pd.DataFrame, dict[str, object]]:
    out = frame.copy()
    out["target"] = str(target)
    out["y_direction"] = build_direction_target(out)
    resolved = out["resolved"] if "resolved" in out.columns else pd.Series(False, index=out.index, dtype="boolean")
    out["resolved"] = resolved.astype("boolean").fillna(False).astype(bool)

    current_ret_col = ""
    if target == "direction":
        out["y"] = out["y_direction"]
        out["current_ret"] = pd.NA
        out["current_up"] = pd.NA
    elif target == "reversal":
        target_values, current_up, current_ret_col = build_reversal_target(out)
        out["y"] = target_values
        out["current_up"] = current_up
        out["current_ret"] = pd.to_numeric(out[current_ret_col], errors="coerce")
    else:
        raise ValueError(f"Unsupported target {target!r}")

    valid = out["resolved"] & out["y"].notna()
    dataset = out.loc[valid].copy()
    dataset["y"] = dataset["y"].astype(int)
    if dataset["y_direction"].notna().any():
        dataset["y_direction"] = dataset["y_direction"].astype("Int64")
    dataset["current_ret_col"] = current_ret_col

    feature_columns = [
        column
        for column in frame.columns
        if column not in {"asset", "label_set", "settlement_source", "market_id", "condition_id"}
    ]
    ordered_columns = TRAINING_SET_META_COLUMNS + [
        column for column in feature_columns if column not in TRAINING_SET_META_COLUMNS
    ]
    dataset = dataset.reindex(columns=ordered_columns)
    dataset = dataset.reset_index(drop=True)

    metadata = {
        "row_count": int(len(dataset)),
        "column_count": int(len(dataset.columns)),
        "positive_rate": float(dataset["y"].mean()) if len(dataset) else None,
        "resolved_rows_seen": int(out["resolved"].sum()),
        "window_rows_seen": int(len(out)),
        "current_ret_col": current_ret_col or None,
        "aligned_rows_in_window": int(out["label_alignment_status"].eq("aligned").sum())
        if "label_alignment_status" in out.columns
        else None,
    }
    metadata.update(summarize_label_sources(dataset.get("label_source", pd.Series(dtype="string"))))
    return dataset, metadata


def _primary_label_source(metadata: dict[str, object], *, fallback: str | None) -> str | None:
    label_sources = metadata.get("label_sources")
    if isinstance(label_sources, list) and len(label_sources) == 1:
        return str(label_sources[0])
    return fallback
