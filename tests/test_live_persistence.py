from __future__ import annotations

from pathlib import Path

from pm15min.live.account.persistence import persist_positions_snapshot
from pm15min.live.layout import LiveStateLayout
from pm15min.live.runner import persist_runner_iteration


def test_persist_positions_snapshot_writes_latest_only(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    payload = {
        "domain": "live",
        "dataset": "live_positions_snapshot",
        "snapshot_ts": "2026-03-20T00-14-00Z",
        "market": "sol",
        "status": "ok",
        "reason": None,
        "positions": [],
        "redeem_plan": {},
        "summary": {},
    }

    paths = persist_positions_snapshot(rewrite_root=root, payload=payload)
    layout = LiveStateLayout.discover(root=root)
    history_path = layout.positions_snapshot_path(market="sol", snapshot_ts="2026-03-20T00-14-00Z")

    assert paths["latest"].exists()
    assert paths["snapshot"] == paths["latest"]
    assert not history_path.exists()


def test_persist_runner_iteration_writes_latest_only(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    payload = {
        "domain": "live",
        "dataset": "live_runner_iteration",
        "snapshot_ts": "2026-03-20T00-00-01Z",
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "status": "ok",
    }

    paths = persist_runner_iteration(rewrite_root=root, payload=payload)
    layout = LiveStateLayout.discover(root=root)
    history_path = layout.runner_snapshot_path(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        target="direction",
        snapshot_ts="2026-03-20T00-00-01Z",
    )

    assert paths["latest"].exists()
    assert paths["snapshot"] == paths["latest"]
    assert not history_path.exists()
