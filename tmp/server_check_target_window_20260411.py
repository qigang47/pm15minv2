from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(".")
ASSETS = ["btc", "eth", "sol", "xrp"]
START_TS = int(datetime(2026, 4, 7, 0, 0, tzinfo=timezone.utc).timestamp())
END_TS = int(datetime(2026, 4, 9, 23, 45, tzinfo=timezone.utc).timestamp())
STEP = 900


def main() -> None:
    expected = list(range(START_TS, END_TS + 1, STEP))
    report: dict[str, object] = {}
    for asset in ASSETS:
        oracle = pd.read_parquet(
            ROOT / f"data/backtest/tables/oracle_prices/cycle=15m/asset={asset}/data.parquet",
            columns=["cycle_start_ts", "has_both"],
        )
        truth = pd.read_parquet(
            ROOT / f"data/backtest/tables/truth/cycle=15m/asset={asset}/data.parquet",
            columns=["cycle_start_ts", "resolved", "full_truth", "winner_side"],
        )
        labels = pd.read_parquet(
            ROOT / f"research/label_frames/cycle=15m/asset={asset}/label_set=truth/data.parquet",
            columns=["cycle_start_ts", "resolved", "full_truth", "winner_side"],
        )
        oracle["cycle_start_ts"] = pd.to_numeric(oracle["cycle_start_ts"], errors="coerce").astype("Int64")
        truth["cycle_start_ts"] = pd.to_numeric(truth["cycle_start_ts"], errors="coerce").astype("Int64")
        labels["cycle_start_ts"] = pd.to_numeric(labels["cycle_start_ts"], errors="coerce").astype("Int64")

        oracle_range = oracle[oracle["cycle_start_ts"].isin(expected)].copy()
        truth_range = truth[truth["cycle_start_ts"].isin(expected)].copy()
        labels_range = labels[labels["cycle_start_ts"].isin(expected)].copy()

        report[asset] = {
            "expected_cycles": len(expected),
            "oracle_rows": int(len(oracle_range)),
            "oracle_has_both": int(pd.to_numeric(oracle_range["has_both"], errors="coerce").fillna(0).astype(int).sum()),
            "truth_rows": int(len(truth_range)),
            "truth_resolved": int(pd.to_numeric(truth_range["resolved"], errors="coerce").fillna(0).astype(int).sum()),
            "labels_rows": int(len(labels_range)),
            "labels_resolved": int(pd.to_numeric(labels_range["resolved"], errors="coerce").fillna(0).astype(int).sum()),
            "missing_truth_cycles": [
                int(ts)
                for ts in expected
                if ts not in set(truth_range["cycle_start_ts"].dropna().astype(int).tolist())
            ][:10],
        }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
