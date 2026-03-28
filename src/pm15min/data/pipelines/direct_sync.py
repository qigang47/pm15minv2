from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ..config import DataConfig
from ..io.parquet import upsert_parquet
from ..queries.loaders import load_market_catalog, load_streams_source
from ..sources.polymarket_gamma import GammaEventsClient, resolve_winner_side_from_market
from ..sources.chainlink_rpc import ChainlinkRpcSource
from ..sources.polygon_rpc import PolygonRpcClient


def _utc_now_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sync_streams_from_rpc(
    cfg: DataConfig,
    *,
    start_ts: int,
    end_ts: int,
    rpc: PolygonRpcClient | None = None,
    include_block_timestamp: bool = False,
    chunk_blocks: int = 1000,
    sleep_sec: float = 0.02,
) -> dict[str, object]:
    rpc = rpc or PolygonRpcClient()
    source = ChainlinkRpcSource(rpc)
    latest_block = rpc.eth_block_number()
    from_block = rpc.find_first_block_at_or_after_ts(int(start_ts), 1, latest_block)
    to_block = rpc.find_first_block_at_or_after_ts(int(end_ts), from_block, latest_block)

    logs = source.scan_report_verified_logs(
        asset=cfg.asset.slug,
        from_block=from_block,
        to_block=to_block,
        chunk_blocks=chunk_blocks,
        sleep_sec=sleep_sec,
    )
    decoded = source.decode_streams_from_logs(
        asset=cfg.asset.slug,
        logs=logs,
        include_block_timestamp=include_block_timestamp,
    )
    if not decoded:
        return {
            "dataset": "chainlink_streams_rpc",
            "market": cfg.asset.slug,
            "rows_imported": 0,
            "from_block": from_block,
            "to_block": to_block,
            "partitions_written": 0,
        }

    df = pd.DataFrame(decoded)
    df["extra_ts"] = pd.to_numeric(df["extra_ts"], errors="coerce")
    df["observation_ts"] = pd.to_numeric(df["observation_ts"], errors="coerce")
    df["benchmark_price_raw"] = pd.to_numeric(df["benchmark_price_raw"], errors="coerce")
    df = df.dropna(subset=["benchmark_price_raw"])
    df["price"] = df["benchmark_price_raw"].astype(float) / 1e18
    df["source_file"] = "rpc:chainlink_streams"
    df["ingested_at"] = _utc_now_label()
    base_columns = [
        "asset",
        "tx_hash",
        "block_number",
        "observation_ts",
        "extra_ts",
        "benchmark_price_raw",
        "price",
        "report_feed_id",
        "requester",
        "path",
        "perform_idx",
        "value_idx",
        "source_file",
        "ingested_at",
    ]
    df["year"] = pd.to_datetime(df["observation_ts"], unit="s", utc=True, errors="coerce").dt.year.fillna(
        pd.to_datetime(df["extra_ts"], unit="s", utc=True, errors="coerce").dt.year
    ).astype(int)
    df["month"] = pd.to_datetime(df["observation_ts"], unit="s", utc=True, errors="coerce").dt.month.fillna(
        pd.to_datetime(df["extra_ts"], unit="s", utc=True, errors="coerce").dt.month
    ).astype(int)
    partitions_written = 0
    for (year, month), group in df.groupby(["year", "month"], dropna=False):
        upsert_parquet(
            path=cfg.layout.streams_partition_path(int(year), int(month)),
            incoming=group[base_columns].copy(),
            key_columns=["tx_hash", "perform_idx", "value_idx"],
            sort_columns=["observation_ts", "extra_ts", "tx_hash", "perform_idx", "value_idx"],
        )
        partitions_written += 1
    return {
        "dataset": "chainlink_streams_rpc",
        "market": cfg.asset.slug,
        "rows_imported": int(len(df)),
        "logs_matched": int(len(logs)),
        "from_block": from_block,
        "to_block": to_block,
        "partitions_written": partitions_written,
        "target_root": str(cfg.layout.streams_source_root),
    }


def sync_datafeeds_from_rpc(
    cfg: DataConfig,
    *,
    start_ts: int,
    end_ts: int,
    rpc: PolygonRpcClient | None = None,
    chunk_blocks: int = 5000,
    sleep_sec: float = 0.02,
) -> dict[str, object]:
    rpc = rpc or PolygonRpcClient()
    source = ChainlinkRpcSource(rpc)
    latest_block = rpc.eth_block_number()
    from_block = rpc.find_first_block_at_or_after_ts(int(start_ts), 1, latest_block)
    to_block = rpc.find_first_block_at_or_after_ts(int(end_ts), from_block, latest_block)

    rows = source.scan_datafeeds_answer_updated_logs(
        asset=cfg.asset.slug,
        from_block=from_block,
        to_block=to_block,
        chunk_blocks=chunk_blocks,
        sleep_sec=sleep_sec,
    )
    if not rows:
        return {
            "dataset": "chainlink_datafeeds_rpc",
            "market": cfg.asset.slug,
            "rows_imported": 0,
            "from_block": from_block,
            "to_block": to_block,
            "partitions_written": 0,
        }

    df = pd.DataFrame(rows)
    df["updated_at"] = pd.to_numeric(df["updated_at"], errors="coerce")
    df = df.dropna(subset=["updated_at"]).copy()
    df["year"] = pd.to_datetime(df["updated_at"], unit="s", utc=True, errors="coerce").dt.year.astype(int)
    df["month"] = pd.to_datetime(df["updated_at"], unit="s", utc=True, errors="coerce").dt.month.astype(int)
    df["source_file"] = "rpc:chainlink_datafeeds"
    df["ingested_at"] = _utc_now_label()
    base_columns = [
        "asset",
        "feed_name",
        "proxy_address",
        "aggregator_address",
        "decimals",
        "block_number",
        "tx_hash",
        "log_index",
        "round_id",
        "updated_at",
        "updated_at_iso",
        "answer_raw",
        "answer",
        "source_file",
        "ingested_at",
    ]
    partitions_written = 0
    for (year, month), group in df.groupby(["year", "month"], dropna=False):
        upsert_parquet(
            path=cfg.layout.datafeeds_partition_path(int(year), int(month)),
            incoming=group[base_columns].copy(),
            key_columns=["tx_hash", "log_index"],
            sort_columns=["updated_at", "tx_hash", "log_index"],
        )
        partitions_written += 1
    return {
        "dataset": "chainlink_datafeeds_rpc",
        "market": cfg.asset.slug,
        "rows_imported": int(len(df)),
        "from_block": from_block,
        "to_block": to_block,
        "partitions_written": partitions_written,
        "target_root": str(cfg.layout.datafeeds_source_root),
    }


def sync_settlement_truth_from_rpc(
    cfg: DataConfig,
    *,
    rpc: PolygonRpcClient | None = None,
    start_ts: int | None = None,
    end_ts: int | None = None,
    chunk_blocks: int = 3000,
    sleep_sec: float = 0.01,
) -> dict[str, object]:
    if cfg.cycle != "15m":
        raise ValueError("settlement-truth-rpc currently requires cycle=15m.")
    markets = load_market_catalog(cfg)
    if markets.empty:
        raise FileNotFoundError(
            f"Missing canonical market catalog: {cfg.layout.market_catalog_table_path}. "
            "Run `pm15min data sync market-catalog` first."
        )
    streams = load_streams_source(cfg)
    if streams.empty:
        raise FileNotFoundError(
            f"Missing streams source: {cfg.layout.streams_source_root}. "
            "Run `pm15min data sync streams-rpc` first."
        )

    rpc = rpc or PolygonRpcClient()
    latest_block = rpc.eth_block_number()
    min_end_ts = int(markets["cycle_end_ts"].min()) if start_ts is None else int(start_ts)
    max_end_ts = int(markets["cycle_end_ts"].max()) if end_ts is None else int(end_ts)
    from_block = rpc.find_first_block_at_or_after_ts(max(0, min_end_ts - 7 * 86400), 1, latest_block)
    to_block = rpc.find_first_block_at_or_after_ts(max_end_ts + 7 * 86400, from_block, latest_block)

    source = ChainlinkRpcSource(rpc)
    condition_map = source.scan_condition_resolutions(
        from_block=from_block,
        to_block=to_block,
        chunk_blocks=chunk_blocks,
        sleep_sec=sleep_sec,
    )
    condition_df = pd.DataFrame(list(condition_map.values()))

    streams = streams.copy()
    streams["extra_ts"] = pd.to_numeric(streams["extra_ts"], errors="coerce")
    streams["observation_ts"] = pd.to_numeric(streams["observation_ts"], errors="coerce")
    streams["benchmark_price_raw"] = pd.to_numeric(streams["benchmark_price_raw"], errors="coerce")
    streams = streams.dropna(subset=["extra_ts", "benchmark_price_raw"]).copy()
    streams["stream_price"] = streams["benchmark_price_raw"].astype(float) / 1e18
    streams = streams.sort_values(["extra_ts", "tx_hash", "perform_idx", "value_idx"]).drop_duplicates(
        subset=["extra_ts"], keep="last"
    )
    streams = streams.rename(
        columns={
            "extra_ts": "stream_extra_ts",
            "observation_ts": "stream_observation_ts",
            "benchmark_price_raw": "stream_benchmark_price_raw",
            "tx_hash": "stream_tx_hash",
        }
    )
    streams = streams[
        [
            "asset",
            "stream_extra_ts",
            "stream_observation_ts",
            "stream_benchmark_price_raw",
            "stream_price",
            "stream_tx_hash",
        ]
    ]

    base = markets.copy()
    base = base.merge(condition_df, on="condition_id", how="left")
    base = base.merge(streams, left_on=["asset", "cycle_end_ts"], right_on=["asset", "stream_extra_ts"], how="left")
    base["winner_side"] = base["winner_side"].fillna("")
    base["label_updown"] = base["winner_side"]
    base["onchain_resolved"] = base["resolve_tx_hash"].notna()
    base["stream_match_exact"] = base["stream_observation_ts"].notna()
    base["full_truth"] = base["onchain_resolved"] & base["stream_match_exact"]
    base["cycle"] = cfg.cycle
    base["source_file"] = "rpc:settlement_truth"
    base["ingested_at"] = _utc_now_label()

    out = base[
        [
            "market_id",
            "condition_id",
            "asset",
            "cycle",
            "cycle_start_ts",
            "cycle_end_ts",
            "slug",
            "question",
            "resolution_source",
            "winner_side",
            "label_updown",
            "onchain_resolved",
            "stream_match_exact",
            "full_truth",
            "stream_price",
            "stream_extra_ts",
            "source_file",
            "ingested_at",
        ]
    ].copy()

    canonical = upsert_parquet(
        path=cfg.layout.settlement_truth_source_path,
        incoming=out,
        key_columns=["market_id", "cycle_end_ts"],
        sort_columns=["cycle_end_ts", "full_truth", "market_id"],
    )
    return {
        "dataset": "settlement_truth_rpc",
        "market": cfg.asset.slug,
        "rows_imported": int(len(out)),
        "canonical_rows": int(len(canonical)),
        "from_block": from_block,
        "to_block": to_block,
        "target_path": str(cfg.layout.settlement_truth_source_path),
    }


def sync_settlement_truth_from_gamma(
    cfg: DataConfig,
    *,
    client: GammaEventsClient | None = None,
    start_ts: int | None = None,
    end_ts: int | None = None,
    fetched_markets: list[dict[str, object]] | None = None,
    workers: int = 8,
) -> dict[str, object]:
    markets = load_market_catalog(cfg)
    if markets.empty:
        raise FileNotFoundError(
            f"Missing canonical market catalog: {cfg.layout.market_catalog_table_path}. "
            "Run `pm15min data sync market-catalog` first."
        )

    client = client or GammaEventsClient()
    min_end_ts = int(markets["cycle_end_ts"].min()) if start_ts is None else int(start_ts)
    max_end_ts = int(markets["cycle_end_ts"].max()) if end_ts is None else int(end_ts)
    base = markets.copy()
    base["cycle_end_ts"] = pd.to_numeric(base["cycle_end_ts"], errors="coerce")
    base = base.dropna(subset=["cycle_end_ts"]).copy()
    base["cycle_end_ts"] = base["cycle_end_ts"].astype("int64")
    base = base[(base["cycle_end_ts"] >= min_end_ts) & (base["cycle_end_ts"] <= max_end_ts)].copy()
    if base.empty:
        return {
            "dataset": "settlement_truth_gamma",
            "market": cfg.asset.slug,
            "rows_imported": 0,
            "rows_resolved": 0,
            "markets_fetched": 0,
            "matched_markets": 0,
            "canonical_rows": 0,
            "target_path": str(cfg.layout.settlement_truth_source_path),
        }

    if fetched_markets is None:
        fetched: list[dict[str, object]] = []
        market_ids = [str(market_id).strip() for market_id in base["market_id"].astype(str).tolist() if str(market_id).strip()]
        batch_size = 20
        batches = [market_ids[offset : offset + batch_size] for offset in range(0, len(market_ids), batch_size)]
        if max(1, int(workers)) <= 1:
            for batch in batches:
                fetched.extend(client.fetch_markets_by_ids(batch, sleep_sec=cfg.sleep_sec))
        else:
            with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
                future_map = {
                    executor.submit(client.fetch_markets_by_ids, batch, sleep_sec=cfg.sleep_sec): batch for batch in batches
                }
                for future in as_completed(future_map):
                    fetched.extend(future.result())
    else:
        fetched = [item for item in fetched_markets if isinstance(item, dict)]
    by_market_id = {
        str(item.get("id") or "").strip(): item
        for item in fetched
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    }

    base["market_id"] = base["market_id"].astype(str)
    base["winner_side"] = base["market_id"].map(lambda market_id: resolve_winner_side_from_market(by_market_id.get(market_id, {})))
    base["label_updown"] = base["winner_side"]
    base["onchain_resolved"] = base["market_id"].map(
        lambda market_id: str((by_market_id.get(market_id, {}) or {}).get("umaResolutionStatus") or "").strip().lower()
        == "resolved"
    )
    base["full_truth"] = base["winner_side"].ne("")
    base["onchain_resolved"] = base["onchain_resolved"] | base["full_truth"]
    base["stream_match_exact"] = False
    base["stream_price"] = pd.Series([pd.NA] * len(base), index=base.index, dtype="Float64")
    base["stream_extra_ts"] = pd.Series([pd.NA] * len(base), index=base.index, dtype="Int64")
    base["cycle"] = cfg.cycle
    base["source_file"] = "zzz_gamma:markets_outcome_prices"
    base["ingested_at"] = _utc_now_label()

    out = base[
        [
            "market_id",
            "condition_id",
            "asset",
            "cycle",
            "cycle_start_ts",
            "cycle_end_ts",
            "slug",
            "question",
            "resolution_source",
            "winner_side",
            "label_updown",
            "onchain_resolved",
            "stream_match_exact",
            "full_truth",
            "stream_price",
            "stream_extra_ts",
            "source_file",
            "ingested_at",
        ]
    ].copy()

    canonical = upsert_parquet(
        path=cfg.layout.settlement_truth_source_path,
        incoming=out,
        key_columns=["market_id", "cycle_end_ts"],
        sort_columns=["cycle_end_ts", "full_truth", "source_file", "market_id"],
    )
    return {
        "dataset": "settlement_truth_gamma",
        "market": cfg.asset.slug,
        "rows_imported": int(len(out)),
        "rows_resolved": int(out["winner_side"].ne("").sum()),
        "markets_fetched": int(len(fetched)),
        "matched_markets": int(base["market_id"].isin(by_market_id).sum()),
        "canonical_rows": int(len(canonical)),
        "target_path": str(cfg.layout.settlement_truth_source_path),
    }
