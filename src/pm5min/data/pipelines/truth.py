from __future__ import annotations

import pandas as pd

from pmshared.io.parquet import write_parquet_atomic

from ..config import DataConfig
from ..queries.loaders import load_market_catalog, load_oracle_prices_table, load_settlement_truth_source


TRUTH_COLUMNS = [
    "asset",
    "cycle_start_ts",
    "cycle_end_ts",
    "market_id",
    "condition_id",
    "winner_side",
    "label_updown",
    "resolved",
    "truth_source",
    "full_truth",
]


def _normalize_side(price_to_beat: float, final_price: float) -> str:
    return "UP" if float(final_price) >= float(price_to_beat) else "DOWN"


def _normalize_oracle_source_label(value: object) -> str:
    token = str(value or "").strip().lower()
    if not token:
        return ""
    if "mixed" in token:
        return "chainlink_mixed"
    if "stream" in token:
        return "streams"
    if "datafeed" in token:
        return "datafeeds"
    if "oracle_prices" in token or "direct_api" in token:
        return "oracle_prices"
    return "oracle_prices"


def _resolve_oracle_truth_source(row: pd.Series) -> str:
    price_source = _normalize_oracle_source_label(row.get("source_price_to_beat"))
    final_source = _normalize_oracle_source_label(row.get("source_final_price"))
    sources = {source for source in (price_source, final_source) if source}
    if not sources:
        return "oracle_prices"
    if len(sources) == 1:
        return next(iter(sources))
    if sources.issubset({"streams", "datafeeds", "chainlink_mixed"}):
        return "chainlink_mixed"
    return "oracle_prices"


def build_truth_table(cfg: DataConfig) -> dict[str, object]:
    markets = load_market_catalog(cfg)
    if markets.empty:
        raise FileNotFoundError(
            f"Missing canonical market catalog: {cfg.layout.market_catalog_table_path}. "
            "Run `pm5min data sync market-catalog` first."
        )

    base_market = markets.copy()
    base_market = base_market[["market_id", "condition_id", "asset", "cycle_start_ts", "cycle_end_ts"]].copy()

    frames: list[pd.DataFrame] = []

    settlement = load_settlement_truth_source(cfg)
    if not settlement.empty:
        st = settlement.copy()
        st["winner_side"] = st["winner_side"].fillna(st["label_updown"]).astype(str).str.upper()
        st["label_updown"] = st["label_updown"].fillna(st["winner_side"]).astype(str).str.upper()
        settlement_has_side = st["winner_side"].ne("") | st["label_updown"].ne("")
        st["resolved"] = settlement_has_side | st["full_truth"].fillna(False).astype(bool)
        st["truth_source"] = "settlement_truth"
        st = st[
            [
                "asset",
                "cycle_start_ts",
                "cycle_end_ts",
                "market_id",
                "condition_id",
                "winner_side",
                "label_updown",
                "resolved",
                "truth_source",
                "full_truth",
            ]
        ].copy()
        frames.append(st)

    oracle = load_oracle_prices_table(cfg)
    if not oracle.empty:
        op = oracle.copy()
        op = op[op["has_both"].fillna(False)].copy()
        if not op.empty:
            op["winner_side"] = op.apply(
                lambda row: _normalize_side(row["price_to_beat"], row["final_price"]),
                axis=1,
            )
            op["label_updown"] = op["winner_side"]
            op["resolved"] = True
            op["truth_source"] = op.apply(_resolve_oracle_truth_source, axis=1)
            op["full_truth"] = True
            op = op.merge(
                base_market,
                on=["asset", "cycle_start_ts", "cycle_end_ts"],
                how="left",
                suffixes=("", "_market"),
            )
            op["market_id"] = op["market_id"].fillna("")
            op["condition_id"] = op["condition_id"].fillna("")
            op = op[
                [
                    "asset",
                    "cycle_start_ts",
                    "cycle_end_ts",
                    "market_id",
                    "condition_id",
                    "winner_side",
                    "label_updown",
                    "resolved",
                    "truth_source",
                    "full_truth",
                ]
            ].copy()
            frames.append(op)

    if not frames:
        out = pd.DataFrame(columns=TRUTH_COLUMNS)
    else:
        out = pd.concat(frames, ignore_index=True, sort=False)
        out["priority"] = out["truth_source"].map({"settlement_truth": 2, "oracle_prices": 1}).fillna(0)
        out["resolved_priority"] = out["resolved"].fillna(False).astype(int)
        out["full_truth_priority"] = out["full_truth"].fillna(False).astype(int)
        out["winner_side_priority"] = out["winner_side"].fillna("").astype(str).ne("").astype(int)
        out = out.sort_values(
            [
                "cycle_end_ts",
                "full_truth_priority",
                "resolved_priority",
                "winner_side_priority",
                "priority",
                "market_id",
            ]
        )
        out = out.drop_duplicates(subset=["asset", "cycle_end_ts"], keep="last")
        out = out.drop(columns=["priority", "resolved_priority", "full_truth_priority", "winner_side_priority"])
        out = out[TRUTH_COLUMNS].reset_index(drop=True)

    write_parquet_atomic(out, cfg.layout.truth_table_path)
    return {
        "dataset": f"truth_{cfg.cycle}",
        "market": cfg.asset.slug,
        "rows_written": int(len(out)),
        "target_path": str(cfg.layout.truth_table_path),
    }


def build_truth_15m(cfg: DataConfig) -> dict[str, object]:
    return build_truth_table(cfg)
