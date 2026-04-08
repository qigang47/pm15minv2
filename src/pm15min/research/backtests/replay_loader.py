from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from pm15min.research.labels.alignment import merge_feature_and_label_frames


REPLAY_KEY_COLUMNS = ["decision_ts", "cycle_start_ts", "cycle_end_ts", "offset"]
REPLAY_ENTITY_COLUMNS = ["market_id", "condition_id"]
REPLAY_SCORE_COLUMNS = [
    *REPLAY_KEY_COLUMNS,
    *REPLAY_ENTITY_COLUMNS,
    "window_start_ts",
    "window_end_ts",
    "window_duration_seconds",
    "p_lgb",
    "p_lr",
    "p_signal",
    "p_up",
    "p_down",
    "score_valid",
    "score_reason",
]


@dataclass(frozen=True)
class ReplayLoadSummary:
    feature_rows: int
    label_rows: int
    merged_rows: int
    score_rows: int
    score_covered_rows: int
    score_missing_rows: int
    score_valid_rows: int
    score_invalid_rows: int
    unresolved_label_rows: int
    bundle_offset_missing_rows: int
    ready_rows: int

    def to_dict(self) -> dict[str, int]:
        return {
            "feature_rows": self.feature_rows,
            "label_rows": self.label_rows,
            "merged_rows": self.merged_rows,
            "score_rows": self.score_rows,
            "score_covered_rows": self.score_covered_rows,
            "score_missing_rows": self.score_missing_rows,
            "score_valid_rows": self.score_valid_rows,
            "score_invalid_rows": self.score_invalid_rows,
            "unresolved_label_rows": self.unresolved_label_rows,
            "bundle_offset_missing_rows": self.bundle_offset_missing_rows,
            "ready_rows": self.ready_rows,
        }


def build_score_frame(score_frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not score_frames:
        frame = pd.DataFrame(index=pd.RangeIndex(0))
    else:
        frame = pd.concat(score_frames, ignore_index=True, sort=False)
    for column in ("decision_ts", "cycle_start_ts", "cycle_end_ts", "window_start_ts", "window_end_ts"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
        else:
            frame[column] = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
    if "offset" in frame.columns:
        frame["offset"] = pd.to_numeric(frame["offset"], errors="coerce").astype("Int64")
    else:
        frame["offset"] = pd.Series(pd.NA, index=frame.index, dtype="Int64")
    if "window_duration_seconds" in frame.columns:
        frame["window_duration_seconds"] = pd.to_numeric(frame["window_duration_seconds"], errors="coerce")
    else:
        frame["window_duration_seconds"] = pd.Series(pd.NA, index=frame.index, dtype="Float64")
    for column in REPLAY_ENTITY_COLUMNS:
        frame[column] = _string_series(frame, column)
    frame["score_valid"] = _bool_series(frame, "score_valid")
    frame["score_reason"] = _string_series(frame, "score_reason")
    dedupe_columns = _score_key_columns(frame)
    return (
        frame.reindex(columns=REPLAY_SCORE_COLUMNS)
        .drop_duplicates(subset=dedupe_columns, keep="last")
        .sort_values(dedupe_columns)
        .reset_index(drop=True)
    )


def build_replay_frame(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    score_frames: list[pd.DataFrame],
    available_offsets: list[int],
    scoped_offsets: list[int] | None = None,
) -> tuple[pd.DataFrame, ReplayLoadSummary]:
    scoped_feature_frame = _scope_frame_to_offsets(features, scoped_offsets)
    merged, alignment_metadata = merge_feature_and_label_frames(scoped_feature_frame, labels)
    score_frame = _scope_frame_to_offsets(build_score_frame(score_frames), scoped_offsets)
    replay = merged.merge(score_frame, on=_replay_merge_columns(merged, score_frame), how="left")
    replay["bundle_offset_available"] = pd.to_numeric(replay["offset"], errors="coerce").isin(
        [int(offset) for offset in available_offsets]
    )
    replay["score_present"] = replay["p_up"].notna() & replay["p_down"].notna()
    replay["score_valid"] = _bool_series(replay, "score_valid")
    replay["score_reason"] = _string_series(replay, "score_reason")
    replay["winner_side"] = _string_series(replay, "winner_side").str.upper()
    replay["resolved"] = _bool_series(replay, "resolved")
    replay = _append_replay_window_columns(replay)

    ready_mask = (
        replay["bundle_offset_available"]
        & replay["score_present"]
        & replay["score_valid"]
        & replay["resolved"]
        & replay["winner_side"].isin(["UP", "DOWN"])
    )
    summary = ReplayLoadSummary(
        feature_rows=int(alignment_metadata.get("feature_rows", len(features))),
        label_rows=int(alignment_metadata.get("label_rows", len(labels))),
        merged_rows=int(len(replay)),
        score_rows=int(len(score_frame)),
        score_covered_rows=int(replay["score_present"].sum()),
        score_missing_rows=int((~replay["score_present"]).sum()),
        score_valid_rows=int(replay["score_valid"].sum()),
        score_invalid_rows=int((~replay["score_valid"]).sum()),
        unresolved_label_rows=int((~replay["resolved"]).sum()),
        bundle_offset_missing_rows=int((~replay["bundle_offset_available"]).sum()),
        ready_rows=int(ready_mask.sum()),
    )
    sort_columns = [column for column in [*REPLAY_KEY_COLUMNS, *REPLAY_ENTITY_COLUMNS] if column in replay.columns]
    return replay.sort_values(sort_columns).reset_index(drop=True), summary


def _append_replay_window_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    decision_ts = pd.to_datetime(out.get("decision_ts"), utc=True, errors="coerce")
    cycle_end_ts = pd.to_datetime(out.get("cycle_end_ts"), utc=True, errors="coerce")
    if "window_start_ts" in out.columns:
        window_start_ts = pd.to_datetime(out.get("window_start_ts"), utc=True, errors="coerce")
    else:
        window_start_ts = decision_ts
    duration_seconds = pd.to_numeric(out.get("window_duration_seconds"), errors="coerce")
    duration_seconds = duration_seconds.where(duration_seconds.gt(0.0), 60.0)
    if "window_end_ts" in out.columns:
        window_end_ts = pd.to_datetime(out.get("window_end_ts"), utc=True, errors="coerce")
    else:
        window_end_ts = window_start_ts + pd.to_timedelta(duration_seconds, unit="s")
    valid_cycle_cap = cycle_end_ts.notna() & window_end_ts.notna()
    if bool(valid_cycle_cap.any()):
        window_end_ts.loc[valid_cycle_cap] = window_end_ts.loc[valid_cycle_cap].where(
            window_end_ts.loc[valid_cycle_cap].le(cycle_end_ts.loc[valid_cycle_cap]),
            cycle_end_ts.loc[valid_cycle_cap],
        )
    out["window_start_ts"] = window_start_ts
    out["window_end_ts"] = window_end_ts
    out["window_duration_seconds"] = duration_seconds.astype(float)
    return out


def _scope_frame_to_offsets(frame: pd.DataFrame, scoped_offsets: list[int] | None) -> pd.DataFrame:
    if frame.empty or not scoped_offsets or "offset" not in frame.columns:
        return frame.copy()
    allowed_offsets = {int(value) for value in scoped_offsets}
    offset_values = pd.to_numeric(frame["offset"], errors="coerce")
    return frame.loc[offset_values.isin(allowed_offsets)].copy()


def _score_key_columns(frame: pd.DataFrame) -> list[str]:
    columns = list(REPLAY_KEY_COLUMNS)
    for column in REPLAY_ENTITY_COLUMNS:
        if column in frame.columns and _string_series(frame, column).ne("").any():
            columns.append(column)
    return columns


def _replay_merge_columns(replay: pd.DataFrame, score_frame: pd.DataFrame) -> list[str]:
    columns = list(REPLAY_KEY_COLUMNS)
    for column in REPLAY_ENTITY_COLUMNS:
        if column in replay.columns and column in score_frame.columns and _string_series(score_frame, column).ne("").any():
            columns.append(column)
    return columns


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series(False, index=frame.index, dtype="boolean")
    return values.astype("boolean").fillna(False).astype(bool)


def _string_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series("", index=frame.index, dtype="string")
    return values.astype("string").fillna("").astype(str)
