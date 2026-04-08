from __future__ import annotations

import pandas as pd


_METRIC_COLUMNS_INT = ("trades", "rejects", "wins", "losses")
_METRIC_COLUMNS_FLOAT = ("pnl_sum", "stake_sum")
_BOOL_COLUMNS = (
    "training_reused",
    "bundle_reused",
    "secondary_training_reused",
    "secondary_bundle_reused",
    "resumed_from_existing",
)


def merge_compare_frames(primary: pd.DataFrame, supplemental: pd.DataFrame) -> pd.DataFrame:
    if (primary is None or primary.empty) and (supplemental is None or supplemental.empty):
        return pd.DataFrame()
    if primary is None or primary.empty:
        return _normalize_merged_frame(supplemental.copy())
    if supplemental is None or supplemental.empty:
        return _normalize_merged_frame(primary.copy())

    primary_frame = primary.copy()
    supplemental_frame = supplemental.copy()
    merged = primary_frame.merge(
        supplemental_frame,
        on="case_key",
        how="outer",
        suffixes=("_primary", "_supplemental"),
    )

    out = pd.DataFrame()
    out["case_key"] = merged["case_key"]

    for column in _metadata_columns(primary_frame, supplemental_frame):
        if column == "case_key":
            continue
        out[column] = _coalesce_pair(merged, column)

    for column in _METRIC_COLUMNS_INT:
        out[column] = _sum_pair(merged, column).fillna(0).astype(int)
    for column in _METRIC_COLUMNS_FLOAT:
        out[column] = _sum_pair(merged, column).fillna(0.0).astype(float)

    for column in _BOOL_COLUMNS:
        if column in primary_frame.columns or column in supplemental_frame.columns:
            out[column] = _bool_or_pair(merged, column)

    out["roi_pct"] = out["pnl_sum"].where(out["stake_sum"].ne(0.0), 0.0)
    out["roi_pct"] = (out["pnl_sum"] / out["stake_sum"].where(out["stake_sum"].ne(0.0), 1.0)) * 100.0
    out.loc[out["stake_sum"].eq(0.0), "roi_pct"] = 0.0
    out["status"] = _merge_status(merged)
    out["source_summary_paths"] = [
        _combine_text_values(
            row.get("summary_path_primary"),
            row.get("summary_path_supplemental"),
        )
        for _, row in merged.iterrows()
    ]
    out["source_backtest_run_dirs"] = [
        _combine_text_values(
            row.get("backtest_run_dir_primary"),
            row.get("backtest_run_dir_supplemental"),
        )
        for _, row in merged.iterrows()
    ]
    return _normalize_merged_frame(out)


def _metadata_columns(primary: pd.DataFrame, supplemental: pd.DataFrame) -> list[str]:
    columns: list[str] = []
    for frame in (primary, supplemental):
        for column in frame.columns:
            if column in {"case_key", "roi_pct", *_METRIC_COLUMNS_INT, *_METRIC_COLUMNS_FLOAT, *_BOOL_COLUMNS}:
                continue
            if column not in columns:
                columns.append(column)
    return columns


def _coalesce_pair(frame: pd.DataFrame, column: str) -> pd.Series:
    primary = frame.get(f"{column}_primary", pd.Series(pd.NA, index=frame.index, dtype="object"))
    supplemental = frame.get(f"{column}_supplemental", pd.Series(pd.NA, index=frame.index, dtype="object"))
    return primary.where(primary.notna() & primary.astype("string").fillna("").ne(""), supplemental)


def _sum_pair(frame: pd.DataFrame, column: str) -> pd.Series:
    primary = pd.to_numeric(frame.get(f"{column}_primary", pd.Series(0, index=frame.index)), errors="coerce").fillna(0)
    supplemental = pd.to_numeric(frame.get(f"{column}_supplemental", pd.Series(0, index=frame.index)), errors="coerce").fillna(0)
    return primary + supplemental


def _bool_or_pair(frame: pd.DataFrame, column: str) -> pd.Series:
    primary = frame.get(f"{column}_primary", pd.Series(False, index=frame.index)).astype("boolean").fillna(False).astype(bool)
    supplemental = frame.get(f"{column}_supplemental", pd.Series(False, index=frame.index)).astype("boolean").fillna(False).astype(bool)
    return primary | supplemental


def _merge_status(frame: pd.DataFrame) -> pd.Series:
    primary = frame.get("status_primary", pd.Series("", index=frame.index, dtype="string")).astype("string").fillna("")
    supplemental = frame.get("status_supplemental", pd.Series("", index=frame.index, dtype="string")).astype("string").fillna("")
    out = pd.Series("", index=frame.index, dtype="string")
    out.loc[primary.eq("completed") | supplemental.eq("completed")] = "completed"
    out.loc[out.eq("") & (primary.eq("failed") | supplemental.eq("failed"))] = "failed"
    out.loc[out.eq("") & (primary.ne("") | supplemental.ne(""))] = "pending"
    return out.astype(str)


def _combine_text_values(*values: object) -> list[str]:
    out: list[str] = []
    for value in values:
        token = str(value or "").strip()
        if token and token not in out:
            out.append(token)
    return out


def _normalize_merged_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "roi_pct" in out.columns:
        out["roi_pct"] = pd.to_numeric(out["roi_pct"], errors="coerce").fillna(0.0).astype(float)
    if "pnl_sum" in out.columns:
        out["pnl_sum"] = pd.to_numeric(out["pnl_sum"], errors="coerce").fillna(0.0).astype(float)
    if "stake_sum" in out.columns:
        out["stake_sum"] = pd.to_numeric(out["stake_sum"], errors="coerce").fillna(0.0).astype(float)
    if "trades" in out.columns:
        out["trades"] = pd.to_numeric(out["trades"], errors="coerce").fillna(0).astype(int)
    sort_cols = [col for col in ["market", "group_name", "run_name", "feature_set", "variant_label", "case_key"] if col in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, kind="stable").reset_index(drop=True)
    return out
