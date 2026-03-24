from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.pipelines.orderbook_runtime import run_orderbook_recorder


class _FakeClobClient:
    def fetch_book(self, token_id: str, *, levels: int = 0, timeout_sec: float = 1.2):
        return {
            "timestamp": "2026-03-19T09:00:00Z",
            "asks": [{"price": "0.12", "size": "10"}],
            "bids": [{"price": "0.11", "size": "8"}],
        }


def test_run_orderbook_recorder_writes_state_and_logs(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="xrp", cycle="15m", root=tmp_path / "v2", market_depth=1)
    market_table = pd.DataFrame(
        [
            {
                "market_id": "market-1",
                "condition_id": "cond-1",
                "asset": "xrp",
                "cycle": "15m",
                "cycle_start_ts": 1_710_000_000,
                "cycle_end_ts": 1_910_000_000,
                "token_up": "token-up",
                "token_down": "token-down",
                "slug": "xrp-up-or-down-15m-1710000000",
                "question": "XRP up or down",
                "resolution_source": "https://data.chain.link/streams/xrp-usd",
                "event_id": "event-1",
                "event_slug": "slug",
                "event_title": "title",
                "series_slug": "xrp-up-or-down-15m",
                "closed_ts": None,
                "source_snapshot_ts": "2026-03-19T09-00-00Z",
            }
        ]
    )
    write_parquet_atomic(market_table, cfg.layout.market_catalog_table_path)

    summary = run_orderbook_recorder(
        cfg,
        client=_FakeClobClient(),
        iterations=2,
        loop=True,
        sleep_sec=0.0,
    )

    assert summary["status"] == "ok"
    assert summary["completed_iterations"] == 2
    state_path = Path(summary["state_path"])
    log_path = Path(summary["log_path"])
    assert state_path.exists()
    assert log_path.exists()

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["status"] == "ok"
    assert state["completed_iterations"] == 2
    assert state["provider"] == "DirectOrderbookProvider"

    lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) >= 3
    assert any('"event": "iteration_ok"' in line for line in lines)
    assert any('"event": "run_finished"' in line for line in lines)
