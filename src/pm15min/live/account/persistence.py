from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..layout import LiveStateLayout
from ..persistence import write_live_payload_pair


def persist_open_orders_snapshot(*, rewrite_root: Path, payload: dict[str, Any]) -> dict[str, Path]:
    layout = LiveStateLayout.discover(root=rewrite_root)
    latest_path = layout.latest_open_orders_path(market=str(payload["market"]))
    snapshot_path = layout.open_orders_snapshot_path(
        market=str(payload["market"]),
        snapshot_ts=str(payload["snapshot_ts"]),
    )
    return write_live_payload_pair(payload=payload, latest_path=latest_path, snapshot_path=snapshot_path)


def persist_positions_snapshot(*, rewrite_root: Path, payload: dict[str, Any]) -> dict[str, Path]:
    layout = LiveStateLayout.discover(root=rewrite_root)
    latest_path = layout.latest_positions_path(market=str(payload["market"]))
    snapshot_path = layout.positions_snapshot_path(
        market=str(payload["market"]),
        snapshot_ts=str(payload["snapshot_ts"]),
    )
    return write_live_payload_pair(payload=payload, latest_path=latest_path, snapshot_path=snapshot_path)


def load_latest_open_orders_snapshot(*, rewrite_root: Path, market: str) -> dict[str, Any] | None:
    layout = LiveStateLayout.discover(root=rewrite_root)
    path = layout.latest_open_orders_path(market=market)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_latest_positions_snapshot(*, rewrite_root: Path, market: str) -> dict[str, Any] | None:
    layout = LiveStateLayout.discover(root=rewrite_root)
    path = layout.latest_positions_path(market=market)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))
