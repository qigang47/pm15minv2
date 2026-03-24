from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.pipelines.export_tables import export_oracle_prices_15m, export_truth_15m


def test_export_oracle_prices_and_truth(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", root=tmp_path / "v2")

    oracle_df = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1700000000,
                "cycle_end_ts": 1700000900,
                "price_to_beat": 100.0,
                "final_price": 110.0,
                "source_price_to_beat": "streams",
                "source_final_price": "streams",
                "has_price_to_beat": True,
                "has_final_price": True,
                "has_both": True,
            }
        ]
    )
    truth_df = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1700000000,
                "cycle_end_ts": 1700000900,
                "market_id": "m1",
                "condition_id": "c1",
                "winner_side": "UP",
                "label_updown": "UP",
                "resolved": True,
                "truth_source": "settlement_truth",
                "full_truth": True,
            }
        ]
    )

    write_parquet_atomic(oracle_df, cfg.layout.oracle_prices_table_path)
    write_parquet_atomic(truth_df, cfg.layout.truth_table_path)

    oracle_summary = export_oracle_prices_15m(cfg)
    truth_summary = export_truth_15m(cfg)

    assert Path(oracle_summary["export_path"]).exists()
    assert Path(truth_summary["export_path"]).exists()

    oracle_csv = pd.read_csv(oracle_summary["export_path"])
    truth_csv = pd.read_csv(truth_summary["export_path"])
    assert len(oracle_csv) == 1
    assert len(truth_csv) == 1
    assert oracle_csv.iloc[0]["asset"] == "sol"
    assert truth_csv.iloc[0]["winner_side"] == "UP"
