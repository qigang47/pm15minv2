from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.sources.polymarket_oracle_api import PolymarketOracleApiClient
from pm15min.research.config import ResearchConfig
from pm15min.research.labels.frames import build_label_frame


ASSETS = ["btc", "eth", "sol", "xrp"]
START_TS = int(datetime(2026, 4, 7, 0, 0, tzinfo=timezone.utc).timestamp())
END_TS = int(datetime(2026, 4, 9, 23, 45, tzinfo=timezone.utc).timestamp())
CYCLE_SECONDS = 900


def _read_frame(path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def _parse_iso_to_ts(raw: str | None) -> int | None:
    if not raw:
        return None
    dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp())


def _fetch_window_prices(*, client: PolymarketOracleApiClient, asset: str, expected: list[int]) -> dict[int, tuple[float, float]]:
    out: dict[int, tuple[float, float]] = {}
    current_ts = max(expected) + CYCLE_SECONDS
    requests_done = 0
    while requests_done < 20:
        requests_done += 1
        batch = client.fetch_past_results_batch(
            symbol=asset.upper(),
            current_event_start_time=datetime.fromtimestamp(current_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            cycle_seconds=CYCLE_SECONDS,
            count=50,
            sleep_sec=0.02,
            max_retries=6,
        )
        if not batch:
            break
        starts: list[int] = []
        for row in batch:
            ts = _parse_iso_to_ts(row.get("startTime"))
            if ts is None:
                continue
            starts.append(ts)
            if ts in expected:
                open_price = row.get("openPrice")
                close_price = row.get("closePrice")
                if open_price is None or close_price is None:
                    continue
                out[int(ts)] = (float(open_price), float(close_price))
        if not starts or min(starts) <= min(expected):
            break
        current_ts = min(starts)

    for ts in expected:
        if ts in out:
            continue
        obj = client.fetch_crypto_price(
            symbol=asset.upper(),
            cycle_start_ts=int(ts),
            cycle_seconds=CYCLE_SECONDS,
            sleep_sec=0.0,
            max_retries=4,
        )
        open_price = obj.get("openPrice") if isinstance(obj, dict) else None
        close_price = obj.get("closePrice") if isinstance(obj, dict) else None
        if open_price is None or close_price is None:
            continue
        out[int(ts)] = (float(open_price), float(close_price))
    return out


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

        fetched = _fetch_window_prices(client=client, asset=asset, expected=expected)
        oracle_rows: list[dict[str, object]] = []
        truth_rows: list[dict[str, object]] = []
        for ts in expected:
            prices = fetched.get(int(ts))
            if prices is None:
                continue
            open_price, close_price = prices
            winner = "UP" if close_price >= open_price else "DOWN"
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
            oracle_combined = pd.concat([oracle, incoming_oracle], ignore_index=True, sort=False) if not oracle.empty else incoming_oracle
            oracle_combined = (
                oracle_combined.sort_values(["cycle_start_ts"])
                .drop_duplicates(subset=["asset", "cycle_start_ts"], keep="last")
                .reset_index(drop=True)
            )
            write_parquet_atomic(oracle_combined, oracle_path)

        if truth_rows:
            incoming_truth = pd.DataFrame(truth_rows)
            truth_combined = pd.concat([truth, incoming_truth], ignore_index=True, sort=False) if not truth.empty else incoming_truth
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
        labels = build_label_frame(
            label_set="truth",
            truth_table=pd.read_parquet(truth_path),
            oracle_prices_table=pd.read_parquet(oracle_path),
        )
        write_parquet_atomic(labels, research_cfg.layout.label_frame_path("truth"))
        labels["cycle_start_ts"] = pd.to_numeric(labels["cycle_start_ts"], errors="coerce").astype("Int64")
        labels_range = labels[labels["cycle_start_ts"].isin(expected)].copy()
        truth_now = pd.read_parquet(truth_path, columns=["cycle_start_ts", "resolved", "full_truth"])
        truth_now["cycle_start_ts"] = pd.to_numeric(truth_now["cycle_start_ts"], errors="coerce").astype("Int64")
        truth_range = truth_now[truth_now["cycle_start_ts"].isin(expected)].copy()
        all_results.append(
            {
                "asset": asset,
                "expected_cycles": len(expected),
                "fetched_cycles": len(fetched),
                "truth_rows_in_range": int(len(truth_range)),
                "truth_resolved_in_range": int(pd.to_numeric(truth_range["resolved"], errors="coerce").fillna(0).astype(int).sum()),
                "truth_full_in_range": int(pd.to_numeric(truth_range["full_truth"], errors="coerce").fillna(0).astype(int).sum()),
                "labels_rows_in_range": int(len(labels_range)),
                "labels_resolved_in_range": int(pd.to_numeric(labels_range["resolved"], errors="coerce").fillna(0).astype(int).sum()),
            }
        )

    print(all_results)


if __name__ == "__main__":
    main()
