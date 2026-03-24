from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from pm15min.research.labels.sources import summarize_label_sources


@dataclass(frozen=True)
class LabelAlignmentSummary:
    feature_rows: int
    label_rows: int
    matched_rows: int
    missing_label_rows: int
    decision_before_cycle_end_rows: int
    decision_after_cycle_start_rows: int

    def to_dict(self) -> dict[str, int]:
        return {
            "feature_rows": self.feature_rows,
            "label_rows": self.label_rows,
            "matched_rows": self.matched_rows,
            "missing_label_rows": self.missing_label_rows,
            "decision_before_cycle_end_rows": self.decision_before_cycle_end_rows,
            "decision_after_cycle_start_rows": self.decision_after_cycle_start_rows,
        }


def merge_feature_and_label_frames(
    features: pd.DataFrame,
    labels: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, object]]:
    feature_frame = features.copy()
    feature_frame["decision_ts"] = pd.to_datetime(feature_frame["decision_ts"], utc=True, errors="coerce")
    feature_frame["cycle_start_ts"] = pd.to_datetime(feature_frame["cycle_start_ts"], utc=True, errors="coerce")
    feature_frame["cycle_end_ts"] = pd.to_datetime(feature_frame["cycle_end_ts"], utc=True, errors="coerce")
    feature_frame["offset"] = pd.to_numeric(feature_frame["offset"], errors="coerce").astype("Int64")

    label_frame = labels.copy()
    label_frame["cycle_start_ts"] = pd.to_numeric(label_frame["cycle_start_ts"], errors="coerce")
    label_frame["cycle_end_ts"] = pd.to_numeric(label_frame["cycle_end_ts"], errors="coerce")
    label_frame = label_frame.dropna(subset=["cycle_start_ts", "cycle_end_ts"]).copy()
    label_frame["cycle_start_ts"] = pd.to_datetime(label_frame["cycle_start_ts"].astype("int64"), unit="s", utc=True)
    label_frame["cycle_end_ts"] = pd.to_datetime(label_frame["cycle_end_ts"].astype("int64"), unit="s", utc=True)
    label_frame = label_frame.drop_duplicates(subset=["cycle_start_ts", "cycle_end_ts"], keep="last").copy()

    merged = feature_frame.merge(
        label_frame,
        on=["cycle_start_ts", "cycle_end_ts"],
        how="left",
        suffixes=("", "_label"),
    ).sort_values("decision_ts").reset_index(drop=True)
    merged["label_alignment_mode"] = "cycle_boundary_join"
    merged["label_alignment_status"] = "missing_label"
    if "label_set" in merged.columns:
        merged.loc[merged["label_set"].notna(), "label_alignment_status"] = "aligned"
    merged["label_alignment_gap_seconds"] = (
        merged["decision_ts"] - merged["cycle_end_ts"]
    ).dt.total_seconds()

    matched_rows = int(merged.get("label_set", pd.Series(pd.NA, index=merged.index)).notna().sum())
    summary = LabelAlignmentSummary(
        feature_rows=int(len(feature_frame)),
        label_rows=int(len(label_frame)),
        matched_rows=matched_rows,
        missing_label_rows=int(len(merged) - matched_rows),
        decision_before_cycle_end_rows=int(
            (merged["decision_ts"].notna() & merged["cycle_end_ts"].notna() & (merged["decision_ts"] <= merged["cycle_end_ts"])).sum()
        ),
        decision_after_cycle_start_rows=int(
            (merged["decision_ts"].notna() & merged["cycle_start_ts"].notna() & (merged["decision_ts"] >= merged["cycle_start_ts"])).sum()
        ),
    )
    metadata = summary.to_dict()
    metadata["aligned_rows"] = matched_rows
    gap = pd.to_numeric(merged["label_alignment_gap_seconds"], errors="coerce")
    metadata["label_alignment_mode"] = "cycle_boundary_join"
    metadata["label_alignment_gap_seconds_min"] = float(gap.min()) if gap.notna().any() else None
    metadata["label_alignment_gap_seconds_max"] = float(gap.max()) if gap.notna().any() else None
    metadata.update(summarize_label_sources(merged.get("label_source", pd.Series(dtype="string"))))
    return merged, metadata


def align_feature_and_label_frames(
    features: pd.DataFrame,
    labels: pd.DataFrame,
) -> tuple[pd.DataFrame, LabelAlignmentSummary]:
    merged, metadata = merge_feature_and_label_frames(features, labels)
    return (
        merged,
        LabelAlignmentSummary(
            feature_rows=int(metadata["feature_rows"]),
            label_rows=int(metadata["label_rows"]),
            matched_rows=int(metadata["matched_rows"]),
            missing_label_rows=int(metadata["missing_label_rows"]),
            decision_before_cycle_end_rows=int(metadata["decision_before_cycle_end_rows"]),
            decision_after_cycle_start_rows=int(metadata["decision_after_cycle_start_rows"]),
        ),
    )
