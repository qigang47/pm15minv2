from __future__ import annotations

from pathlib import Path
from typing import Any

from ..layout import LiveStateLayout
from ..persistence import write_live_payload_pair


def persist_quote_snapshot_impl(*, rewrite_root: Path, payload: dict[str, Any]) -> dict[str, Path]:
    layout = LiveStateLayout.discover(root=rewrite_root)
    latest_path = layout.latest_quote_path(
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        profile=str(payload["profile"]),
        target=str(payload["target"]),
    )
    snapshot_path = layout.quote_snapshot_path(
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        profile=str(payload["profile"]),
        target=str(payload["target"]),
        snapshot_ts=str(payload["snapshot_ts"]),
    )
    return write_live_payload_pair(payload=payload, latest_path=latest_path, snapshot_path=snapshot_path)
