from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from pm15min.data.layout import utc_snapshot_label
from pm15min.data.sources.orderbook_provider import build_orderbook_provider_from_env
from .orderbook import ORDERBOOK_INDEX_COLUMNS, POST_DECISION_QUOTE_TOLERANCE_MS
from .service import (
    build_offset_quote_row,
    build_quote_snapshot as _build_quote_snapshot_impl,
    persist_quote_snapshot,
)


def build_quote_snapshot(
    *,
    cfg,
    signal_payload: dict[str, Any],
    persist: bool = True,
    now: pd.Timestamp | None = None,
    orderbook_provider=None,
) -> dict[str, Any]:
    if orderbook_provider is None:
        import os

        if (
            str(os.getenv("PM15MIN_ORDERBOOK_HUB_URL") or "").strip()
            or str(os.getenv("PM15MIN_ORDERBOOK_STREAMING") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
        ):
            orderbook_provider = build_orderbook_provider_from_env(
                source_name=f"v2-live-quote:{cfg.asset.slug}:{int(cfg.cycle_minutes)}m",
                subscribe_on_read=True,
            )
    return _build_quote_snapshot_impl(
        cfg=cfg,
        signal_payload=signal_payload,
        persist=persist,
        now=now,
        utc_snapshot_label_fn=utc_snapshot_label,
        orderbook_provider=orderbook_provider,
    )
