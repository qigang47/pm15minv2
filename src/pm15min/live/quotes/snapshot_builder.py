from __future__ import annotations

from typing import Any

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.sources.orderbook_provider import OrderbookProvider
from .market import read_market_table, resolve_market_row
from .orderbook import live_provider_orderbook_levels
from .row_builder import build_offset_quote_row_impl
from .snapshot_persistence import persist_quote_snapshot_impl


def build_quote_snapshot_impl(
    *,
    cfg,
    signal_payload: dict[str, Any],
    persist: bool = True,
    now: pd.Timestamp | None = None,
    utc_snapshot_label_fn,
    orderbook_provider: OrderbookProvider | None = None,
) -> dict[str, Any]:
    data_cfg = DataConfig.build(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        surface="live",
        root=cfg.layout.rewrite.root,
    )
    market_table = read_market_table(data_cfg)
    snapshot_ts = utc_snapshot_label_fn()
    now_ts = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz="UTC")
    quote_now = now_ts.tz_convert("UTC") if now_ts.tzinfo is not None else now_ts.tz_localize("UTC")
    if orderbook_provider is not None and not market_table.empty:
        _prefetch_orderbooks_for_signal_rows(
            orderbook_provider=orderbook_provider,
            market_table=market_table,
            signal_payload=signal_payload,
            quote_now=quote_now,
            timeout_sec=data_cfg.orderbook_timeout_sec,
        )
    provider_frame_cache: dict[tuple[str, str, str], pd.DataFrame] = {}
    index_frame_cache: dict[tuple[str, str | None], pd.DataFrame] = {}
    latest_full_snapshot_cache: dict[str, dict[str, object] | None] = {}
    quote_rows = [
        build_offset_quote_row_impl(
            data_cfg=data_cfg,
            market_table=market_table,
            signal_row=row,
            target=str(signal_payload.get("target") or "direction"),
            now=quote_now,
            orderbook_provider=orderbook_provider,
            provider_frame_cache=provider_frame_cache,
            index_frame_cache=index_frame_cache,
            latest_full_snapshot_cache=latest_full_snapshot_cache,
        )
        for row in (signal_payload.get("offset_signals") or [])
    ]
    payload = {
        "domain": "live",
        "dataset": "live_quote_snapshot",
        "snapshot_ts": snapshot_ts,
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": f"{int(cfg.cycle_minutes)}m",
        "target": signal_payload.get("target"),
        "signal_snapshot_ts": signal_payload.get("snapshot_ts"),
        "signal_snapshot_path": signal_payload.get("snapshot_path"),
        "market_catalog_table_path": str(data_cfg.layout.market_catalog_table_path),
        "quote_rows": quote_rows,
        "prerequisites": {
            "market_catalog_exists": bool(data_cfg.layout.market_catalog_table_path.exists()),
        },
    }
    if persist:
        paths = persist_quote_snapshot_impl(rewrite_root=cfg.layout.rewrite.root, payload=payload)
        payload["latest_quote_path"] = str(paths["latest"])
        payload["quote_snapshot_path"] = str(paths["snapshot"])
    return payload


def _prefetch_orderbooks_for_signal_rows(
    *,
    orderbook_provider: OrderbookProvider,
    market_table: pd.DataFrame,
    signal_payload: dict[str, Any],
    quote_now: pd.Timestamp,
    timeout_sec: float,
) -> None:
    tokens: list[str] = []
    target = str(signal_payload.get("target") or "direction")
    for signal_row in list(signal_payload.get("offset_signals") or []):
        if not isinstance(signal_row, dict):
            continue
        window_end_ts = pd.to_datetime(signal_row.get("window_end_ts"), utc=True, errors="coerce")
        if not pd.isna(window_end_ts) and quote_now >= window_end_ts:
            continue
        market_row = resolve_market_row(
            market_table,
            decision_ts=pd.to_datetime(signal_row.get("decision_ts"), utc=True, errors="coerce"),
            cycle_start_ts=pd.to_datetime(signal_row.get("cycle_start_ts"), utc=True, errors="coerce"),
            signal_cycle_end_ts=pd.to_datetime(signal_row.get("cycle_end_ts"), utc=True, errors="coerce"),
            target=target,
            now=quote_now,
        )
        if market_row is None:
            continue
        for token_key in ("token_up", "token_down"):
            token_id = str(market_row.get(token_key) or "").strip()
            if token_id:
                tokens.append(token_id)
    if not tokens:
        return
    try:
        orderbook_provider.sync_subscriptions(
            tokens,
            replace=True,
            prefetch=True,
            levels=live_provider_orderbook_levels(),
            timeout=timeout_sec,
        )
    except Exception:
        return
