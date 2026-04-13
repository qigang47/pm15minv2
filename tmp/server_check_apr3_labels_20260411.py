from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ROOT = Path(".")
ASSETS = ["btc", "eth", "sol", "xrp"]
POINTS = [1775227500, 1775228400, 1775259000]


def main() -> None:
    out: dict[str, object] = {}
    for asset in ASSETS:
        df = pd.read_parquet(
            ROOT / f"research/label_frames/cycle=15m/asset={asset}/label_set=truth/data.parquet",
            columns=["cycle_start_ts", "cycle_end_ts", "market_id", "condition_id", "resolved", "winner_side", "label_source", "full_truth"],
        )
        rows = []
        for ts in POINTS:
            sub = df[pd.to_numeric(df["cycle_start_ts"], errors="coerce") == int(ts)].copy()
            rows.append(
                {
                    "cycle_start_ts": ts,
                    "count": int(len(sub)),
                    "resolved_true": int(pd.to_numeric(sub.get("resolved"), errors="coerce").fillna(0).astype(int).sum()) if not sub.empty else 0,
                    "full_truth_true": int(pd.to_numeric(sub.get("full_truth"), errors="coerce").fillna(0).astype(int).sum()) if not sub.empty else 0,
                    "winner_side_values": sorted(set(sub.get("winner_side", pd.Series(dtype='string')).fillna("").astype(str).tolist())),
                    "label_source_values": sorted(set(sub.get("label_source", pd.Series(dtype='string')).fillna("").astype(str).tolist())),
                    "sample_rows": sub.head(5).to_dict(orient="records"),
                }
            )
        out[asset] = rows
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
