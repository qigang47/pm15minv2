from __future__ import annotations

from pathlib import Path
from typing import Any

from pm15min.data.io.json_files import write_json_atomic


def write_live_payload_pair(
    *,
    payload: dict[str, Any],
    latest_path: Path,
    snapshot_path: Path,
) -> dict[str, Path]:
    # Persist snapshot first so the latest pointer only advances after history exists.
    write_json_atomic(payload, snapshot_path)
    write_json_atomic(payload, latest_path)
    return {"latest": latest_path, "snapshot": snapshot_path}
