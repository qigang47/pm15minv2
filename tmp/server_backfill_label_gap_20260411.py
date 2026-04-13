from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.sources.polymarket_oracle_api import PolymarketOracleApiClient
from pm15min.research.config import ResearchConfig
from pm15min.research.labels.frames import build_label_frame


ASSETS = ["btc", "eth", "sol", "xrp"]
START_TS = int(datetime(2026, 4, 3, 14, 45, tzinfo=timezone.utc).timestamp())
END_TS = int(datetime(2026, 4, 3, 23, 30, tzinfo=timezone.utc).timestamp())
CYCLE_SECONDS = 900


def _read_frame(path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def main() -> None:
    client = PolymarketOracleApiClient(timeout_sec=20)
    expected = list(range(START_TS, END_TS + 1, CYCLE_SECONDS))
    all_results: list[dict[str, object]] = []

    for asset in ASSETS:
        data_cfg = DataConfig.build(market=asset, cycle="15m", surface="backtest", root=".")
        oracle_path = data_cfg.layout.oracle_prices_table_path
        truth_path = data_cfg.layout.truth_table_path

        oracle = _read_frame(oracle_path)
        truth = _read_frame(truth_path)

        oracle_existing = set(
            pd.to_numeric(oracle.get("cycle_start_ts", pd.Series(dtype="int64")), errors="coerce")
            .dropna()
            .astype(int)
            .tolist()
        )
        truth_existing = set(
            pd.to_numeric(truth.get("cycle_start_ts", pd.Series(dtype="int64")), errors="coerce")
            .dropna()
            .astype(int)
            .tolist()
        )
        missing = [ts for ts in expected if ts not in oracle_existing or ts not in truth_existing]

        oracle_rows: list[dict[str, object]] = []
        truth_rows: list[dict[str, object]] = []
        failures: list[dict[str, object]] = []

        for ts in missing:
            try:
                obj = client.fetch_crypto_price(
                    symbol=asset.upper(),
                    cycle_start_ts=int(ts),
                    cycle_seconds=CYCLE_SECONDS,
                    sleep_sec=0.0,
                )
            except Exception as exc:  # pragma: no cover - operational script
                failures.append({"cycle_start_ts": int(ts), "error": str(exc)})
                continue
            open_price = obj.get("openPrice") if isinstance(obj, dict) else None
            close_price = obj.get("closePrice") if isinstance(obj, dict) else None
            if open_price is None or close_price is None:
                failures.append({"cycle_start_ts": int(ts), "error": f"empty_payload:{obj}"})
                continue
            winner = "UP" if float(close_price) >= float(open_price) else "DOWN"
            oracle_rows.append(
                {
                    "asset": asset,
                    "cycle_start_ts": int(ts),
                    "cycle_end_ts": int(ts + CYCLE_SECONDS),
                    "price_to_beat": float(open_price),
                    "final_price": float(close_price),
                    "source_price_to_beat": "polymarket_api_crypto_price",
                    "source_final_price": "polymarket_api_crypto_price",
                    "has_price_to_beat": True,
                    "has_final_price": True,
                    "has_both": True,
                }
            )
            truth_rows.append(
                {
                    "asset": asset,
                    "cycle_start_ts": int(ts),
                    "cycle_end_ts": int(ts + CYCLE_SECONDS),
                    "market_id": "",
                    "condition_id": "",
                    "winner_side": winner,
                    "label_updown": winner,
                    "resolved": True,
                    "truth_source": "oracle_prices",
                    "full_truth": True,
                }
            )

        if oracle_rows:
            incoming_oracle = pd.DataFrame(oracle_rows)
            oracle_combined = (
                pd.concat([oracle, incoming_oracle], ignore_index=True, sort=False)
                if not oracle.empty
                else incoming_oracle
            )
            oracle_combined = (
                oracle_combined.sort_values(["cycle_start_ts"])
                .drop_duplicates(subset=["asset", "cycle_start_ts"], keep="last")
                .reset_index(drop=True)
            )
            write_parquet_atomic(oracle_combined, oracle_path)

        if truth_rows:
            incoming_truth = pd.DataFrame(truth_rows)
            truth_combined = (
                pd.concat([truth, incoming_truth], ignore_index=True, sort=False)
                if not truth.empty
                else incoming_truth
            )
            truth_combined = (
                truth_combined.sort_values(["cycle_end_ts", "full_truth", "resolved"])
                .drop_duplicates(subset=["asset", "cycle_end_ts"], keep="last")
                .reset_index(drop=True)
            )
            write_parquet_atomic(truth_combined, truth_path)

        research_cfg = ResearchConfig.build(
            market=asset,
            cycle="15m",
            source_surface="backtest",
            label_set="truth",
            root=".",
        )
        truth_for_labels = pd.read_parquet(truth_path)
        oracle_for_labels = pd.read_parquet(oracle_path)
        labels = build_label_frame(
            label_set="truth",
            truth_table=truth_for_labels,
            oracle_prices_table=oracle_for_labels,
        )
        write_parquet_atomic(labels, research_cfg.layout.label_frame_path("truth"))
        label_existing = set(
            pd.to_numeric(labels["cycle_start_ts"], errors="coerce").dropna().astype(int).tolist()
        )
        remaining_missing = [ts for ts in expected if ts not in label_existing]
        all_results.append(
            {
                "asset": asset,
                "requested_cycles": len(expected),
                "fetched_cycles": len(oracle_rows),
                "failure_count": len(failures),
                "failures_head": failures[:5],
                "label_rows_written": int(len(labels)),
                "remaining_missing_count": len(remaining_missing),
                "remaining_missing_head": remaining_missing[:5],
            }
        )

    print(all_results)


if __name__ == "__main__":
    main()
