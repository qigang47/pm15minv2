from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from pmshared.io.parquet import upsert_parquet, write_parquet_atomic
from pmshared.time import utc_snapshot_label

from ..config import DataConfig
from ..sources.polymarket_gamma import (
    GammaEventsClient,
    build_market_catalog_records,
    build_market_catalog_records_from_markets,
)


MARKET_CATALOG_COLUMNS = [
    "market_id",
    "condition_id",
    "asset",
    "cycle",
    "cycle_start_ts",
    "cycle_end_ts",
    "token_up",
    "token_down",
    "slug",
    "question",
    "resolution_source",
    "event_id",
    "event_slug",
    "event_title",
    "series_slug",
    "closed_ts",
    "source_snapshot_ts",
]


def _frame_from_records(records) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=MARKET_CATALOG_COLUMNS)
    df = pd.DataFrame([record.to_row() for record in records], columns=MARKET_CATALOG_COLUMNS)
    return df.sort_values(["cycle_start_ts", "market_id"]).reset_index(drop=True)


def _write_market_catalog_snapshot(
    *,
    cfg: DataConfig,
    snapshot_df: pd.DataFrame,
    snapshot_ts: str,
    source_mode: str,
    start_ts: int,
    end_ts: int,
    fetched_rows: int,
) -> dict[str, object]:
    snapshot_path = cfg.layout.market_catalog_snapshot_path(snapshot_ts)
    write_parquet_atomic(snapshot_df, snapshot_path)

    canonical_df = upsert_parquet(
        path=cfg.layout.market_catalog_table_path,
        incoming=snapshot_df,
        key_columns=["market_id"],
        sort_columns=["cycle_start_ts", "source_snapshot_ts", "market_id"],
    )

    return {
        "dataset": "market_catalog",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "surface": cfg.surface,
        "source_mode": source_mode,
        "start_ts": int(start_ts),
        "end_ts": int(end_ts),
        "rows_fetched": int(fetched_rows),
        "snapshot_rows": int(len(snapshot_df)),
        "canonical_rows": int(len(canonical_df)),
        "snapshot_path": str(snapshot_path),
        "canonical_path": str(cfg.layout.market_catalog_table_path),
    }


def sync_market_catalog(
    cfg: DataConfig,
    *,
    start_ts: int,
    end_ts: int,
    client: GammaEventsClient | None = None,
    now: datetime | None = None,
    selection_mode: str | None = None,
) -> dict[str, object]:
    snapshot_ts = utc_snapshot_label(now)
    client = client or GammaEventsClient()
    mode = str(selection_mode or "").strip().lower()
    if mode not in {"", "surface_default", "active_markets", "closed_events"}:
        raise ValueError(f"unsupported market catalog selection_mode: {selection_mode}")
    if mode in {"", "surface_default"}:
        mode = "active_markets" if cfg.surface == "live" else "closed_events"
    source_mode = "gamma_closed_events"
    fetched_rows = 0
    if mode == "active_markets":
        markets = client.fetch_active_markets(
            start_ts=start_ts,
            end_ts=end_ts,
            limit=cfg.gamma_limit,
            max_pages=cfg.max_pages,
            sleep_sec=cfg.sleep_sec,
        )
        records = build_market_catalog_records_from_markets(
            markets=markets,
            asset=cfg.asset.slug,
            cycle=cfg.cycle,
            snapshot_ts=snapshot_ts,
        )
        source_mode = "gamma_active_markets"
        fetched_rows = len(markets)
    else:
        events = client.fetch_closed_events(
            start_ts=start_ts,
            end_ts=end_ts,
            limit=cfg.gamma_limit,
            max_pages=cfg.max_pages,
            sleep_sec=cfg.sleep_sec,
        )
        records = build_market_catalog_records(
            events=events,
            asset=cfg.asset.slug,
            cycle=cfg.cycle,
            snapshot_ts=snapshot_ts,
        )
        fetched_rows = len(events)
    snapshot_df = _frame_from_records(records)

    return _write_market_catalog_snapshot(
        cfg=cfg,
        snapshot_df=snapshot_df,
        snapshot_ts=snapshot_ts,
        source_mode=source_mode,
        start_ts=start_ts,
        end_ts=end_ts,
        fetched_rows=fetched_rows,
    )


def backfill_market_catalog_from_closed_markets(
    cfg: DataConfig,
    *,
    start_ts: int,
    end_ts: int,
    window_days: int = 7,
    client: GammaEventsClient | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    client = client or GammaEventsClient()
    window_days = max(1, int(window_days))
    cursor = int(start_ts)
    step = window_days * 86400
    window_results: list[dict[str, object]] = []
    total_rows_fetched = 0
    total_snapshot_rows = 0
    last_canonical_rows = 0
    base_now = now or datetime.now(timezone.utc)

    while cursor <= int(end_ts):
        window_end = min(int(end_ts), cursor + step - 1)
        snapshot_ts = f"{utc_snapshot_label(base_now)}_{int(cursor)}_{int(window_end)}"
        markets = client.fetch_closed_markets(
            start_ts=cursor,
            end_ts=window_end,
            limit=cfg.gamma_limit,
            max_pages=cfg.max_pages,
            sleep_sec=cfg.sleep_sec,
        )
        records = build_market_catalog_records_from_markets(
            markets=markets,
            asset=cfg.asset.slug,
            cycle=cfg.cycle,
            snapshot_ts=snapshot_ts,
            include_closed=True,
        )
        snapshot_df = _frame_from_records(records)
        summary = _write_market_catalog_snapshot(
            cfg=cfg,
            snapshot_df=snapshot_df,
            snapshot_ts=snapshot_ts,
            source_mode="gamma_closed_markets",
            start_ts=cursor,
            end_ts=window_end,
            fetched_rows=len(markets),
        )
        window_results.append(summary)
        total_rows_fetched += int(summary["rows_fetched"])
        total_snapshot_rows += int(summary["snapshot_rows"])
        last_canonical_rows = int(summary["canonical_rows"])
        cursor = window_end + 1

    return {
        "dataset": "market_catalog_backfill",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "surface": cfg.surface,
        "start_ts": int(start_ts),
        "end_ts": int(end_ts),
        "window_days": int(window_days),
        "windows_processed": int(len(window_results)),
        "rows_fetched": int(total_rows_fetched),
        "snapshot_rows": int(total_snapshot_rows),
        "canonical_rows": int(last_canonical_rows),
        "window_results": window_results,
    }
