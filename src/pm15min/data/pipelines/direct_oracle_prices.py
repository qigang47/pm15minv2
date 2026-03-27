from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from ..config import DataConfig
from ..io.parquet import read_parquet_if_exists, write_parquet_atomic
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


def _candidate_has_number(value: object) -> bool:
    try:
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    except Exception:
        return False
    return bool(pd.notna(parsed))


def _candidate_rank(row: dict[str, object]) -> tuple[int, int, int, int, str]:
    has_price = _candidate_has_number(row.get("price_to_beat"))
    has_final = _candidate_has_number(row.get("final_price"))
    return (
        int(has_price and has_final),
        int(has_final),
        int(has_price),
        int(row.get("source_priority") or 0),
        str(row.get("fetched_at") or ""),
    )


def _merge_candidate(
    fetched: dict[int, dict[str, object]],
    *,
    cycle_start_ts: int,
    candidate: dict[str, object],
) -> None:
    existing = fetched.get(int(cycle_start_ts))
    if existing is None or _candidate_rank(candidate) > _candidate_rank(existing):
        fetched[int(cycle_start_ts)] = candidate


def _candidate_rank_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["price_to_beat"] = pd.to_numeric(out.get("price_to_beat"), errors="coerce")
    out["final_price"] = pd.to_numeric(out.get("final_price"), errors="coerce")
    if "has_price_to_beat" not in out.columns:
        out["has_price_to_beat"] = out["price_to_beat"].notna()
    else:
        out["has_price_to_beat"] = out["has_price_to_beat"].fillna(out["price_to_beat"].notna()).astype(bool)
    if "has_final_price" not in out.columns:
        out["has_final_price"] = out["final_price"].notna()
    else:
        out["has_final_price"] = out["has_final_price"].fillna(out["final_price"].notna()).astype(bool)
    if "has_both" not in out.columns:
        out["has_both"] = out["has_price_to_beat"] & out["has_final_price"]
    else:
        out["has_both"] = out["has_both"].fillna(out["has_price_to_beat"] & out["has_final_price"]).astype(bool)
    out["candidate_has_both"] = out["has_both"].astype(int)
    out["candidate_has_final_price"] = out["has_final_price"].astype(int)
    out["candidate_has_price_to_beat"] = out["has_price_to_beat"].astype(int)
    out["source_priority"] = pd.to_numeric(out.get("source_priority"), errors="coerce").fillna(0).astype(int)
    out["fetched_at"] = out.get("fetched_at", "").fillna("").astype(str)
    return out


def _write_direct_oracle_canonical(*, target_path: Path, incoming: pd.DataFrame) -> pd.DataFrame:
    existing = read_parquet_if_exists(target_path)
    if existing is None or existing.empty:
        combined = incoming.copy()
    elif incoming.empty:
        combined = existing.copy()
    else:
        combined = pd.concat([existing, incoming], ignore_index=True, sort=False)
    if combined.empty:
        write_parquet_atomic(combined, target_path)
        return combined
    ranked = _candidate_rank_columns(combined)
    ranked = ranked.sort_values(
        [
            "cycle_start_ts",
            "candidate_has_both",
            "candidate_has_final_price",
            "candidate_has_price_to_beat",
            "source_priority",
            "fetched_at",
        ]
    ).drop_duplicates(subset=["asset", "cycle_start_ts"], keep="last")
    canonical = ranked.reindex(columns=DIRECT_ORACLE_COLUMNS).reset_index(drop=True)
    write_parquet_atomic(canonical, target_path)
    return canonical


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
            if st in cycle_starts:
                _merge_candidate(
                    fetched,
                    cycle_start_ts=int(st),
                    candidate={
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
                    },
                )

        if not batch_cycle_starts or min(batch_cycle_starts) <= min(cycle_starts):
            break
        current_ts = min(batch_cycle_starts)

    # Fallback single-window fetch for missing rows, especially important for live.
    if fallback_single:
        for cycle_start_ts in cycle_starts:
            existing = fetched.get(int(cycle_start_ts))
            if existing and _candidate_rank(existing)[0] > 0:
                continue
            obj = client.fetch_crypto_price(
                symbol=symbol,
                cycle_start_ts=int(cycle_start_ts),
                cycle_seconds=cfg.layout.cycle_seconds,
                sleep_sec=min(0.05, float(sleep_sec)),
            )
            if not obj:
                continue
            _merge_candidate(
                fetched,
                cycle_start_ts=int(cycle_start_ts),
                candidate={
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
                },
            )

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
    out = _candidate_rank_columns(out)
    out = out.sort_values(
        [
            "cycle_start_ts",
            "candidate_has_both",
            "candidate_has_final_price",
            "candidate_has_price_to_beat",
            "source_priority",
            "fetched_at",
        ]
    ).drop_duplicates(subset=["asset", "cycle_start_ts"], keep="last")
    out = out.reindex(columns=DIRECT_ORACLE_COLUMNS).reset_index(drop=True)

    canonical = _write_direct_oracle_canonical(
        target_path=cfg.layout.direct_oracle_source_path,
        incoming=out,
    )
    return {
        "dataset": "polymarket_direct_oracle_prices",
        "market": cfg.asset.slug,
        "surface": cfg.surface,
        "rows_imported": int(len(out)),
        "canonical_rows": int(len(canonical)),
        "target_path": str(cfg.layout.direct_oracle_source_path),
    }


def sync_polymarket_oracle_price_window(
    cfg: DataConfig,
    *,
    cycle_start_ts: int,
    timeout_sec: float = 20.0,
    sleep_sec: float = 0.0,
    max_retries: int = 1,
    client: PolymarketOracleApiClient | None = None,
) -> dict[str, object]:
    if cfg.cycle != "15m":
        raise ValueError("direct oracle window sync currently requires cycle=15m.")

    client = client or PolymarketOracleApiClient(timeout_sec=timeout_sec)
    cycle_start_ts = int(cycle_start_ts)
    obj = client.fetch_crypto_price(
        symbol=cfg.asset.slug.upper(),
        cycle_start_ts=cycle_start_ts,
        cycle_seconds=cfg.layout.cycle_seconds,
        sleep_sec=float(sleep_sec),
        max_retries=max(1, int(max_retries)),
    )
    fetched_at = _utc_now_label()
    candidate = pd.DataFrame(
        [
            {
                "asset": cfg.asset.slug,
                "cycle": cfg.cycle,
                "cycle_start_ts": cycle_start_ts,
                "cycle_end_ts": int(cycle_start_ts + cfg.layout.cycle_seconds),
                "price_to_beat": None if not isinstance(obj, dict) else obj.get("openPrice"),
                "final_price": None if not isinstance(obj, dict) else obj.get("closePrice"),
                "has_price_to_beat": False,
                "has_final_price": False,
                "has_both": False,
                "completed": None if not isinstance(obj, dict) else obj.get("completed"),
                "incomplete": None if not isinstance(obj, dict) else obj.get("incomplete"),
                "cached": None if not isinstance(obj, dict) else obj.get("cached"),
                "api_timestamp_ms": None if not isinstance(obj, dict) else obj.get("timestamp"),
                "http_status": 200 if isinstance(obj, dict) and obj else None,
                "source": "polymarket_api_crypto_price",
                "source_priority": 3,
                "fetched_at": fetched_at,
            }
        ],
        columns=DIRECT_ORACLE_COLUMNS,
    )
    candidate["price_to_beat"] = pd.to_numeric(candidate["price_to_beat"], errors="coerce")
    candidate["final_price"] = pd.to_numeric(candidate["final_price"], errors="coerce")
    candidate["has_price_to_beat"] = candidate["price_to_beat"].notna()
    candidate["has_final_price"] = candidate["final_price"].notna()
    candidate["has_both"] = candidate["has_price_to_beat"] & candidate["has_final_price"]
    rows_imported = int(candidate["has_price_to_beat"].sum())
    canonical_rows = 0
    if rows_imported > 0:
        canonical = _write_direct_oracle_canonical(
            target_path=cfg.layout.direct_oracle_source_path,
            incoming=candidate,
        )
        canonical_rows = int(len(canonical))
    return {
        "dataset": "polymarket_direct_oracle_price_window",
        "market": cfg.asset.slug,
        "surface": cfg.surface,
        "cycle_start_ts": cycle_start_ts,
        "rows_imported": rows_imported,
        "canonical_rows": canonical_rows,
        "target_path": str(cfg.layout.direct_oracle_source_path),
    }
