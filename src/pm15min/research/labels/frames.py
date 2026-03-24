from __future__ import annotations

import pandas as pd

from pm15min.research.labels.sources import (
    filter_oracle_prices_by_label_source,
    filter_truth_table_by_label_source,
    normalize_label_source_series,
    resolve_label_build_plan,
    summarize_label_sources,
)


LABEL_FRAME_COLUMNS = [
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


def build_label_frame(
    *,
    label_set: str,
    truth_table: pd.DataFrame,
    oracle_prices_table: pd.DataFrame,
) -> pd.DataFrame:
    plan = resolve_label_build_plan(label_set)
    if plan.base_label_set == "oracle_prices":
        oracle_filtered = filter_oracle_prices_by_label_source(oracle_prices_table, label_source=plan.label_source)
        if not oracle_filtered.empty:
            return _build_oracle_label_frame(
                oracle_filtered,
                requested_label_set=plan.requested_label_set,
            )
        return _build_truth_label_frame(
            filter_truth_table_by_label_source(truth_table, label_source=plan.label_source),
            oracle_prices_table,
            requested_label_set=plan.requested_label_set,
        )
    if plan.base_label_set == "truth":
        return _build_truth_label_frame(
            filter_truth_table_by_label_source(truth_table, label_source=plan.label_source),
            oracle_prices_table,
            requested_label_set=plan.requested_label_set,
        )
    raise ValueError(f"Unsupported label_set {label_set!r}")


def _build_oracle_label_frame(oracle_prices_table: pd.DataFrame, *, requested_label_set: str) -> pd.DataFrame:
    if oracle_prices_table.empty:
        return pd.DataFrame(columns=LABEL_FRAME_COLUMNS)
    oracle = oracle_prices_table.copy()
    oracle["price_to_beat"] = pd.to_numeric(oracle["price_to_beat"], errors="coerce")
    oracle["final_price"] = pd.to_numeric(oracle["final_price"], errors="coerce")
    oracle["resolved"] = oracle["has_both"].fillna(False).astype(bool)
    oracle["winner_side"] = pd.Series(pd.NA, index=oracle.index, dtype="object")
    mask = oracle["resolved"] & oracle["price_to_beat"].notna() & oracle["final_price"].notna()
    oracle.loc[mask, "winner_side"] = oracle.loc[mask].apply(
        lambda row: "UP" if float(row["final_price"]) >= float(row["price_to_beat"]) else "DOWN",
        axis=1,
    )
    oracle["direction_up"] = oracle["winner_side"].map({"UP": 1.0, "DOWN": 0.0})
    oracle["label_set"] = requested_label_set
    oracle["settlement_source"] = oracle["source_final_price"].fillna(oracle["source_price_to_beat"]).fillna("")
    oracle["label_source"] = normalize_label_source_series(oracle["settlement_source"], default="oracle_prices")
    oracle["market_id"] = oracle.get("market_id", "").fillna("") if "market_id" in oracle.columns else ""
    oracle["condition_id"] = oracle.get("condition_id", "").fillna("") if "condition_id" in oracle.columns else ""
    oracle["full_truth"] = oracle["resolved"]
    return (
        oracle.reindex(columns=LABEL_FRAME_COLUMNS)
        .sort_values(["cycle_start_ts"])
        .drop_duplicates(subset=["asset", "cycle_start_ts"], keep="last")
        .reset_index(drop=True)
    )


def _build_truth_label_frame(
    truth_table: pd.DataFrame,
    oracle_prices_table: pd.DataFrame,
    *,
    requested_label_set: str,
) -> pd.DataFrame:
    if truth_table.empty:
        return pd.DataFrame(columns=LABEL_FRAME_COLUMNS)
    truth = truth_table.copy()
    truth["winner_side"] = truth["winner_side"].fillna(truth["label_updown"]).astype(str).str.upper()
    truth["direction_up"] = truth["winner_side"].map({"UP": 1.0, "DOWN": 0.0})
    truth["label_set"] = requested_label_set
    truth["settlement_source"] = truth["truth_source"].fillna("")
    truth["label_source"] = normalize_label_source_series(truth["settlement_source"], default="settlement_truth")
    truth["resolved"] = truth["resolved"].fillna(False).astype(bool)
    truth["full_truth"] = truth["full_truth"].fillna(False).astype(bool)

    if oracle_prices_table.empty:
        truth["price_to_beat"] = pd.NA
        truth["final_price"] = pd.NA
    else:
        oracle = oracle_prices_table[["asset", "cycle_start_ts", "cycle_end_ts", "price_to_beat", "final_price"]].copy()
        truth = truth.merge(
            oracle,
            on=["asset", "cycle_start_ts", "cycle_end_ts"],
            how="left",
        )

    return (
        truth.reindex(columns=LABEL_FRAME_COLUMNS)
        .sort_values(["cycle_start_ts"])
        .drop_duplicates(subset=["asset", "cycle_start_ts"], keep="last")
        .reset_index(drop=True)
    )


def label_frame_metadata(frame: pd.DataFrame) -> dict[str, object]:
    payload = {
        "row_count": int(len(frame)),
        "column_count": int(len(frame.columns)),
        "resolved_rows": int(frame.get("resolved", pd.Series(dtype=bool)).fillna(False).sum()),
        "full_truth_rows": int(frame.get("full_truth", pd.Series(dtype=bool)).fillna(False).sum()),
    }
    payload.update(summarize_label_sources(frame.get("label_source", pd.Series(dtype="string"))))
    return payload
