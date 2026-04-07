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
_CONTRACT_HELPER_COLUMNS = ["_contract_scope", "_contract_key"]


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
    oracle["full_truth"] = oracle["resolved"]
    oracle = _with_contract_identity(oracle.reindex(columns=LABEL_FRAME_COLUMNS))
    return _dedupe_contract_rows(oracle)


def _build_truth_label_frame(
    truth_table: pd.DataFrame,
    oracle_prices_table: pd.DataFrame,
    *,
    requested_label_set: str,
) -> pd.DataFrame:
    truth = truth_table.copy()
    if not truth.empty:
        truth["winner_side"] = truth["winner_side"].fillna(truth["label_updown"]).astype(str).str.upper()
        truth["direction_up"] = truth["winner_side"].map({"UP": 1.0, "DOWN": 0.0})
        truth["label_set"] = requested_label_set
        truth["settlement_source"] = truth["truth_source"].fillna("")
        truth["label_source"] = normalize_label_source_series(truth["settlement_source"], default="settlement_truth")
        truth["resolved"] = truth["resolved"].fillna(False).astype(bool)
        truth["full_truth"] = truth["full_truth"].fillna(False).astype(bool)

    if oracle_prices_table.empty:
        if truth.empty:
            return pd.DataFrame(columns=LABEL_FRAME_COLUMNS)
        truth["price_to_beat"] = pd.NA
        truth["final_price"] = pd.NA
        combined = truth.reindex(columns=LABEL_FRAME_COLUMNS).copy()
        combined["label_origin_priority"] = 1
    else:
        if not truth.empty:
            truth = _attach_oracle_prices_to_truth(truth, oracle_prices_table)
            truth = truth.reindex(columns=LABEL_FRAME_COLUMNS)
            truth["label_origin_priority"] = 1
        oracle_fallback = _build_oracle_label_frame(
            oracle_prices_table,
            requested_label_set=requested_label_set,
        )
        if not truth.empty and not oracle_fallback.empty:
            oracle_fallback = _drop_cycle_level_fallback_rows_covered_by_truth(oracle_fallback, truth)
        oracle_fallback["label_origin_priority"] = 0
        combined = pd.concat([truth, oracle_fallback], ignore_index=True, sort=False)

    combined = _with_contract_identity(combined)
    combined["source_priority"] = combined["label_source"].map(
        {"settlement_truth": 3, "streams": 2, "chainlink_mixed": 2, "datafeeds": 2, "oracle_prices": 1}
    ).fillna(0)
    combined["resolved_priority"] = combined["resolved"].fillna(False).astype(int)
    combined["full_truth_priority"] = combined["full_truth"].fillna(False).astype(int)
    combined["winner_side_priority"] = combined["winner_side"].fillna("").astype(str).ne("").astype(int)

    deduped = (
        combined.sort_values(
            [
                "cycle_start_ts",
                "cycle_end_ts",
                "_contract_scope",
                "_contract_key",
                "full_truth_priority",
                "resolved_priority",
                "winner_side_priority",
                "source_priority",
                "label_origin_priority",
            ]
        )
        .drop_duplicates(subset=_contract_dedupe_columns(), keep="last")
    )
    deduped = _drop_shadowed_unresolved_contract_rows(deduped)
    return deduped.drop(
        columns=[
            *_CONTRACT_HELPER_COLUMNS,
            "source_priority",
            "resolved_priority",
            "full_truth_priority",
            "winner_side_priority",
            "label_origin_priority",
        ],
        errors="ignore",
    ).reset_index(drop=True)


def _attach_oracle_prices_to_truth(truth: pd.DataFrame, oracle_prices_table: pd.DataFrame) -> pd.DataFrame:
    out = truth.copy()
    out["market_id"] = _string_column(out, "market_id")
    out["condition_id"] = _string_column(out, "condition_id")
    if "price_to_beat" not in out.columns:
        out["price_to_beat"] = pd.NA
    if "final_price" not in out.columns:
        out["final_price"] = pd.NA
    oracle = _prepare_oracle_price_lookup(oracle_prices_table)
    if oracle.empty:
        return out

    out = _fill_from_oracle_prices(
        out,
        oracle_rows=oracle.loc[oracle["market_id"].ne("")],
        join_columns=["asset", "cycle_start_ts", "cycle_end_ts", "market_id"],
    )
    out = _fill_from_oracle_prices(
        out,
        oracle_rows=oracle.loc[oracle["condition_id"].ne("")],
        join_columns=["asset", "cycle_start_ts", "cycle_end_ts", "condition_id"],
    )
    cycle_fallback = oracle.drop_duplicates(subset=["asset", "cycle_start_ts", "cycle_end_ts"], keep="last")
    return _fill_from_oracle_prices(
        out,
        oracle_rows=cycle_fallback,
        join_columns=["asset", "cycle_start_ts", "cycle_end_ts"],
    )


def _prepare_oracle_price_lookup(oracle_prices_table: pd.DataFrame) -> pd.DataFrame:
    if oracle_prices_table.empty:
        return pd.DataFrame(columns=["asset", "cycle_start_ts", "cycle_end_ts", "market_id", "condition_id", "price_to_beat", "final_price"])
    available_columns = [
        column
        for column in ("asset", "cycle_start_ts", "cycle_end_ts", "market_id", "condition_id", "price_to_beat", "final_price")
        if column in oracle_prices_table.columns
    ]
    oracle = oracle_prices_table.loc[:, available_columns].copy()
    oracle["market_id"] = _string_column(oracle, "market_id")
    oracle["condition_id"] = _string_column(oracle, "condition_id")
    oracle["price_to_beat"] = pd.to_numeric(oracle.get("price_to_beat"), errors="coerce")
    oracle["final_price"] = pd.to_numeric(oracle.get("final_price"), errors="coerce")
    return oracle.dropna(subset=["cycle_start_ts", "cycle_end_ts"]).copy()


def _fill_from_oracle_prices(
    truth: pd.DataFrame,
    *,
    oracle_rows: pd.DataFrame,
    join_columns: list[str],
) -> pd.DataFrame:
    if truth.empty or oracle_rows.empty:
        return truth
    selected_columns = [*join_columns, "price_to_beat", "final_price"]
    oracle_selected = oracle_rows.loc[:, selected_columns].drop_duplicates(subset=join_columns, keep="last")
    merged = truth.merge(
        oracle_selected,
        on=join_columns,
        how="left",
        suffixes=("", "_oracle"),
    )
    for column in ("price_to_beat", "final_price"):
        oracle_column = f"{column}_oracle"
        merged[column] = pd.to_numeric(merged[column], errors="coerce").where(
            pd.to_numeric(merged[column], errors="coerce").notna(),
            pd.to_numeric(merged[oracle_column], errors="coerce"),
        )
        merged = merged.drop(columns=[oracle_column])
    return merged


def _drop_cycle_level_fallback_rows_covered_by_truth(oracle_fallback: pd.DataFrame, truth: pd.DataFrame) -> pd.DataFrame:
    oracle_with_keys = _with_contract_identity(oracle_fallback)
    truth_with_keys = _with_contract_identity(truth)
    truth_cycles = truth_with_keys.loc[
        truth_with_keys["_contract_scope"].ne("cycle")
        & (
            truth_with_keys["resolved"].fillna(False).astype(bool)
            | truth_with_keys["full_truth"].fillna(False).astype(bool)
            | truth_with_keys["winner_side"].fillna("").astype(str).ne("")
        ),
        ["asset", "cycle_start_ts", "cycle_end_ts"],
    ].drop_duplicates()
    if truth_cycles.empty:
        return oracle_fallback.reset_index(drop=True)
    covered = oracle_with_keys.merge(
        truth_cycles.assign(_truth_cycle_seen=True),
        on=["asset", "cycle_start_ts", "cycle_end_ts"],
        how="left",
    )
    keep_mask = ~(
        covered["_contract_scope"].eq("cycle")
        & covered["_truth_cycle_seen"].eq(True)
    )
    return oracle_with_keys.loc[keep_mask].drop(columns=_CONTRACT_HELPER_COLUMNS, errors="ignore").reset_index(drop=True)


def _drop_shadowed_unresolved_contract_rows(frame: pd.DataFrame) -> pd.DataFrame:
    resolved_cycle_rows = frame.loc[
        frame["_contract_scope"].eq("cycle")
        & frame["resolved"].fillna(False).astype(bool)
        & frame["full_truth"].fillna(False).astype(bool),
        ["asset", "cycle_start_ts", "cycle_end_ts"],
    ].drop_duplicates()
    if resolved_cycle_rows.empty:
        return frame
    tagged = frame.merge(
        resolved_cycle_rows.assign(_cycle_resolved_truth=True),
        on=["asset", "cycle_start_ts", "cycle_end_ts"],
        how="left",
    )
    drop_mask = (
        tagged["_cycle_resolved_truth"].eq(True)
        & tagged["_contract_scope"].ne("cycle")
        & ~tagged["resolved"].fillna(False).astype(bool)
        & ~tagged["full_truth"].fillna(False).astype(bool)
    )
    return tagged.loc[~drop_mask].drop(columns=["_cycle_resolved_truth"], errors="ignore")


def _with_contract_identity(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["market_id"] = _string_column(out, "market_id")
    out["condition_id"] = _string_column(out, "condition_id")
    cycle_key = (
        _string_column(out, "asset")
        + "|"
        + out.get("cycle_start_ts", pd.Series(index=out.index, dtype="object")).astype("string").fillna("")
        + "|"
        + out.get("cycle_end_ts", pd.Series(index=out.index, dtype="object")).astype("string").fillna("")
    )
    contract_scope = pd.Series("cycle", index=out.index, dtype="string")
    contract_scope.loc[out["condition_id"].ne("")] = "condition"
    contract_scope.loc[out["market_id"].ne("")] = "market"
    contract_key = out["market_id"].where(out["market_id"].ne(""), out["condition_id"])
    out["_contract_scope"] = contract_scope.astype(str)
    out["_contract_key"] = contract_key.where(contract_key.ne(""), cycle_key).astype(str)
    return out


def _dedupe_contract_rows(frame: pd.DataFrame) -> pd.DataFrame:
    return (
        frame.sort_values(["cycle_start_ts", "cycle_end_ts", "_contract_scope", "_contract_key"])
        .drop_duplicates(subset=_contract_dedupe_columns(), keep="last")
        .drop(columns=_CONTRACT_HELPER_COLUMNS, errors="ignore")
        .reset_index(drop=True)
    )


def _contract_dedupe_columns() -> list[str]:
    return ["asset", "cycle_start_ts", "cycle_end_ts", *_CONTRACT_HELPER_COLUMNS]


def _string_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series("", index=frame.index, dtype="string")
    return frame[column].astype("string").fillna("").str.strip()


def label_frame_metadata(frame: pd.DataFrame) -> dict[str, object]:
    payload = {
        "row_count": int(len(frame)),
        "column_count": int(len(frame.columns)),
        "resolved_rows": int(frame.get("resolved", pd.Series(dtype=bool)).fillna(False).sum()),
        "full_truth_rows": int(frame.get("full_truth", pd.Series(dtype=bool)).fillna(False).sum()),
    }
    payload.update(summarize_label_sources(frame.get("label_source", pd.Series(dtype="string"))))
    return payload
