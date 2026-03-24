from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from ..config import DataConfig
from ..io.parquet import upsert_parquet, write_parquet_atomic
from ..queries.loaders import load_market_catalog
from ..sources.polymarket_oracle_api import PolymarketOracleApiClient


DIRECT_ORACLE_COLUMNS = [
    "asset",
    "cycle",
    "cycle_start_ts",
    "cycle_end_ts",
    "price_to_beat",
    "final_price",
    "has_price_to_beat",
    "has_final_price",
    "has_both",
    "completed",
    "incomplete",
    "cached",
    "api_timestamp_ms",
    "http_status",
    "source",
    "source_priority",
    "fetched_at",
]


def _utc_now_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso_to_ts(raw: str | None) -> int | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp())


def sync_polymarket_oracle_prices_direct(
    cfg: DataConfig,
    *,
    start_ts: int | None = None,
    end_ts: int | None = None,
    lookback_days: int = 35,
    timeout_sec: float = 20.0,
    count: int = 50,
    sleep_sec: float = 0.15,
    max_requests: int = 400,
    fallback_single: bool = True,
    client: PolymarketOracleApiClient | None = None,
) -> dict[str, object]:
    if cfg.cycle != "15m":
        raise ValueError("direct oracle sync currently requires cycle=15m.")

    markets = load_market_catalog(cfg)
    if markets.empty:
        raise FileNotFoundError(
            f"Missing canonical market catalog: {cfg.layout.market_catalog_table_path}. "
            "Run `pm15min data sync market-catalog` first."
        )

    client = client or PolymarketOracleApiClient(timeout_sec=timeout_sec)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    if end_ts is None:
        end_ts = now_ts
    if start_ts is None:
        start_ts = int((datetime.now(timezone.utc) - timedelta(days=int(lookback_days))).timestamp())

    mk = markets[(markets["cycle_start_ts"] >= int(start_ts)) & (markets["cycle_start_ts"] <= int(end_ts))].copy()
    if mk.empty:
        return {
            "dataset": "polymarket_direct_oracle_prices",
            "market": cfg.asset.slug,
            "surface": cfg.surface,
            "rows_imported": 0,
            "target_path": str(cfg.layout.direct_oracle_source_path),
        }

    cycle_starts = sorted(set(mk["cycle_start_ts"].astype(int).tolist()))
    symbol = cfg.asset.slug.upper()
    fetched: dict[int, dict[str, object]] = {}

    # Batch fetch via past-results for recent windows.
    current_ts = max(cycle_starts) + cfg.layout.cycle_seconds
    requests_done = 0
    while requests_done < int(max_requests):
        requests_done += 1
        batch = client.fetch_past_results_batch(
            symbol=symbol,
            current_event_start_time=datetime.fromtimestamp(current_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            count=int(count),
            sleep_sec=float(sleep_sec),
        )
        if not batch:
            break

        batch_cycle_starts: list[int] = []
        for row in batch:
            st = _parse_iso_to_ts(str(row.get("startTime") or ""))
            if st is None:
                continue
            batch_cycle_starts.append(int(st))
            if st in cycle_starts and st not in fetched:
                fetched[int(st)] = {
                    "asset": cfg.asset.slug,
                    "cycle": cfg.cycle,
                    "cycle_start_ts": int(st),
                    "cycle_end_ts": int(st + cfg.layout.cycle_seconds),
                    "price_to_beat": row.get("openPrice"),
                    "final_price": row.get("closePrice"),
                    "completed": row.get("completed"),
                    "incomplete": row.get("incomplete"),
                    "cached": row.get("cached"),
                    "api_timestamp_ms": row.get("timestamp"),
                    "http_status": 200,
                    "source": "polymarket_api_past_results",
                    "source_priority": 2,
                    "fetched_at": _utc_now_label(),
                }

        if not batch_cycle_starts or min(batch_cycle_starts) <= min(cycle_starts):
            break
        current_ts = min(batch_cycle_starts)

    # Fallback single-window fetch for missing rows, especially important for live.
    if fallback_single:
        for cycle_start_ts in cycle_starts:
            existing = fetched.get(int(cycle_start_ts))
            if existing and existing.get("price_to_beat") not in ("", None):
                continue
            obj = client.fetch_crypto_price(
                symbol=symbol,
                cycle_start_ts=int(cycle_start_ts),
                cycle_seconds=cfg.layout.cycle_seconds,
                sleep_sec=min(0.05, float(sleep_sec)),
            )
            if not obj:
                continue
            fetched[int(cycle_start_ts)] = {
                "asset": cfg.asset.slug,
                "cycle": cfg.cycle,
                "cycle_start_ts": int(cycle_start_ts),
                "cycle_end_ts": int(cycle_start_ts + cfg.layout.cycle_seconds),
                "price_to_beat": obj.get("openPrice"),
                "final_price": obj.get("closePrice"),
                "completed": obj.get("completed"),
                "incomplete": obj.get("incomplete"),
                "cached": obj.get("cached"),
                "api_timestamp_ms": obj.get("timestamp"),
                "http_status": 200,
                "source": "polymarket_api_crypto_price",
                "source_priority": 3,
                "fetched_at": _utc_now_label(),
            }

    out = pd.DataFrame(list(fetched.values()), columns=DIRECT_ORACLE_COLUMNS)
    if out.empty:
        return {
            "dataset": "polymarket_direct_oracle_prices",
            "market": cfg.asset.slug,
            "surface": cfg.surface,
            "rows_imported": 0,
            "target_path": str(cfg.layout.direct_oracle_source_path),
        }

    out["price_to_beat"] = pd.to_numeric(out["price_to_beat"], errors="coerce")
    out["final_price"] = pd.to_numeric(out["final_price"], errors="coerce")
    out["has_price_to_beat"] = out["price_to_beat"].notna()
    out["has_final_price"] = out["final_price"].notna()
    out["has_both"] = out["has_price_to_beat"] & out["has_final_price"]
    out = out.sort_values(["cycle_start_ts", "source_priority", "fetched_at"]).drop_duplicates(
        subset=["asset", "cycle_start_ts"], keep="last"
    )
    out = out.reset_index(drop=True)

    canonical = upsert_parquet(
        path=cfg.layout.direct_oracle_source_path,
        incoming=out,
        key_columns=["asset", "cycle_start_ts"],
        sort_columns=["cycle_start_ts", "source_priority", "fetched_at"],
    )
    return {
        "dataset": "polymarket_direct_oracle_prices",
        "market": cfg.asset.slug,
        "surface": cfg.surface,
        "rows_imported": int(len(out)),
        "canonical_rows": int(len(canonical)),
        "target_path": str(cfg.layout.direct_oracle_source_path),
    }
