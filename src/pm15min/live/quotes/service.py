from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.sources.orderbook_provider import OrderbookProvider
from .row_builder import build_offset_quote_row_impl
from .snapshot_builder import build_quote_snapshot_impl
from .snapshot_persistence import persist_quote_snapshot_impl


def build_quote_snapshot(
    *,
    cfg,
    signal_payload: dict[str, Any],
    persist: bool = True,
    now: pd.Timestamp | None = None,
    utc_snapshot_label_fn,
    orderbook_provider: OrderbookProvider | None = None,
) -> dict[str, Any]:
    return build_quote_snapshot_impl(
        cfg=cfg,
        signal_payload=signal_payload,
        persist=persist,
        now=now,
        utc_snapshot_label_fn=utc_snapshot_label_fn,
        orderbook_provider=orderbook_provider,
    )


def persist_quote_snapshot(*, rewrite_root: Path, payload: dict[str, Any]) -> dict[str, Path]:
    return persist_quote_snapshot_impl(rewrite_root=rewrite_root, payload=payload)


def build_offset_quote_row(
    *,
    data_cfg: DataConfig,
    market_table: pd.DataFrame,
    signal_row: dict[str, Any],
    target: str,
    now: pd.Timestamp,
    orderbook_provider: OrderbookProvider | None = None,
    provider_frame_cache: dict[tuple[str, str, str], pd.DataFrame] | None = None,
) -> dict[str, Any]:
    return build_offset_quote_row_impl(
        data_cfg=data_cfg,
        market_table=market_table,
        signal_row=signal_row,
        target=target,
        now=now,
        orderbook_provider=orderbook_provider,
        provider_frame_cache=provider_frame_cache,
    )
