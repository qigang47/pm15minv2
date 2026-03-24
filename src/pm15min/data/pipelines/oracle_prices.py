from __future__ import annotations

import pandas as pd

from ..config import DataConfig
from ..io.parquet import write_parquet_atomic
from ..queries.loaders import load_direct_oracle_source, load_market_catalog, load_streams_source


ORACLE_PRICE_COLUMNS = [
    "asset",
    "cycle_start_ts",
    "cycle_end_ts",
    "price_to_beat",
    "final_price",
    "source_price_to_beat",
    "source_final_price",
    "has_price_to_beat",
    "has_final_price",
    "has_both",
]


def build_oracle_prices_15m(cfg: DataConfig) -> dict[str, object]:
    if cfg.cycle != "15m":
        raise ValueError("oracle_prices_15m currently requires cycle=15m.")

    markets = load_market_catalog(cfg)
    if markets.empty:
        raise FileNotFoundError(
            f"Missing canonical market catalog: {cfg.layout.market_catalog_table_path}. "
            "Run `pm15min data sync market-catalog` first."
        )

    direct = load_direct_oracle_source(cfg)
    streams = load_streams_source(cfg)
    if direct.empty and streams.empty:
        raise FileNotFoundError(
            "Missing direct oracle source and streams source. "
            "Run `pm15min data sync direct-oracle-prices` or `pm15min data sync streams-rpc`."
        )

    mk = markets.copy()
    keep = [
        "asset",
        "market_id",
        "condition_id",
        "cycle_start_ts",
        "cycle_end_ts",
        "slug",
        "question",
    ]
    mk = mk[[column for column in keep if column in mk.columns]].copy()

    out = mk.copy()

    if not direct.empty:
        direct = direct.copy()
        direct["cycle_start_ts"] = pd.to_numeric(direct["cycle_start_ts"], errors="coerce")
        direct["cycle_end_ts"] = pd.to_numeric(direct["cycle_end_ts"], errors="coerce")
        direct["price_to_beat"] = pd.to_numeric(direct["price_to_beat"], errors="coerce")
        direct["final_price"] = pd.to_numeric(direct["final_price"], errors="coerce")
        direct = direct.dropna(subset=["cycle_start_ts", "cycle_end_ts"], how="any").copy()
        direct = direct.sort_values(["cycle_start_ts", "source_priority", "fetched_at"]).drop_duplicates(
            subset=["asset", "cycle_start_ts"], keep="last"
        )
        direct["has_direct_price_to_beat"] = direct["price_to_beat"].notna()
        direct["has_direct_final_price"] = direct["final_price"].notna()
        direct = direct[
            [
                "asset",
                "cycle_start_ts",
                "cycle_end_ts",
                "price_to_beat",
                "final_price",
                "has_price_to_beat",
                "has_final_price",
                "has_both",
                "source",
                "has_direct_price_to_beat",
                "has_direct_final_price",
            ]
        ].copy()
        direct = direct.rename(
            columns={
                "source": "direct_source",
            }
        )
        out = out.merge(direct, on=["asset", "cycle_start_ts", "cycle_end_ts"], how="left")
    else:
        out["price_to_beat"] = pd.NA
        out["final_price"] = pd.NA
        out["has_price_to_beat"] = False
        out["has_final_price"] = False
        out["has_both"] = False
        out["direct_source"] = ""
        out["has_direct_price_to_beat"] = False
        out["has_direct_final_price"] = False

    if not streams.empty:
        streams = streams.copy()
        streams["extra_ts"] = pd.to_numeric(streams["extra_ts"], errors="coerce").astype("Int64")
        streams["price"] = pd.to_numeric(streams["price"], errors="coerce")
        streams = streams.dropna(subset=["extra_ts", "price"]).copy()
        streams["extra_ts"] = streams["extra_ts"].astype("int64")
        streams = streams.sort_values(["extra_ts", "tx_hash", "perform_idx", "value_idx"])
        streams = streams.drop_duplicates(subset=["extra_ts"], keep="last")

        strike = streams[["asset", "extra_ts", "price", "source_file"]].rename(
            columns={
                "extra_ts": "cycle_start_ts",
                "price": "price_to_beat_streams",
                "source_file": "source_price_to_beat_streams",
            }
        )
        final = streams[["asset", "extra_ts", "price", "source_file"]].rename(
            columns={
                "extra_ts": "cycle_end_ts",
                "price": "final_price_streams",
                "source_file": "source_final_price_streams",
            }
        )
        out = out.merge(strike, on=["asset", "cycle_start_ts"], how="left")
        out = out.merge(final, on=["asset", "cycle_end_ts"], how="left")
        out["price_to_beat"] = pd.to_numeric(out["price_to_beat"], errors="coerce").combine_first(
            pd.to_numeric(out.get("price_to_beat_streams"), errors="coerce")
        )
        out["final_price"] = pd.to_numeric(out["final_price"], errors="coerce").combine_first(
            pd.to_numeric(out.get("final_price_streams"), errors="coerce")
        )
        out["source_price_to_beat"] = out["direct_source"].where(
            out["has_direct_price_to_beat"],
            out.get("source_price_to_beat_streams", ""),
        )
        out["source_final_price"] = out["direct_source"].where(
            out["has_direct_final_price"],
            out.get("source_final_price_streams", ""),
        )
    else:
        out["source_price_to_beat"] = out.get("direct_source", "")
        out["source_final_price"] = out.get("direct_source", "")

    out["has_price_to_beat"] = pd.to_numeric(out["price_to_beat"], errors="coerce").notna()
    out["has_final_price"] = pd.to_numeric(out["final_price"], errors="coerce").notna()
    out["has_both"] = out["has_price_to_beat"] & out["has_final_price"]
    out = out[ORACLE_PRICE_COLUMNS]
    out = out.sort_values(["cycle_start_ts"]).drop_duplicates(subset=["asset", "cycle_start_ts"], keep="last")
    out = out.reset_index(drop=True)

    write_parquet_atomic(out, cfg.layout.oracle_prices_table_path)
    return {
        "dataset": "oracle_prices_15m",
        "market": cfg.asset.slug,
        "surface": cfg.surface,
        "rows_written": int(len(out)),
        "target_path": str(cfg.layout.oracle_prices_table_path),
    }
