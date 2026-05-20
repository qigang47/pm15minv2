from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm15min.core.orderbook_index import iter_orderbook_index_record_batches
from pm15min.data.io.parquet import write_parquet_atomic


def test_iter_orderbook_index_record_batches_filters_and_projects_columns(tmp_path: Path) -> None:
    index_path = tmp_path / "orderbook_index.parquet"
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "captured_ts_ms": 1,
                    "market_id": "m-keep",
                    "token_id": "tok-up",
                    "side": "up",
                    "best_ask": 0.41,
                    "ask_size_1": 10.0,
                    "huge_unused_payload": "x" * 10_000,
                },
                {
                    "captured_ts_ms": 2,
                    "market_id": "m-drop",
                    "token_id": "tok-other",
                    "side": "up",
                    "best_ask": 0.99,
                    "ask_size_1": 1.0,
                    "huge_unused_payload": "y" * 10_000,
                },
            ]
        ),
        index_path,
    )

    batches = list(
        iter_orderbook_index_record_batches(
            index_path=index_path,
            columns=["captured_ts_ms", "market_id", "token_id", "side", "best_ask", "ask_size_1"],
            filters=[("market_id", "in", ["m-keep"])],
            batch_size=1,
        )
    )

    assert len(batches) == 1
    assert batches[0]["market_id"].tolist() == ["m-keep"]
    assert "huge_unused_payload" not in batches[0].columns
