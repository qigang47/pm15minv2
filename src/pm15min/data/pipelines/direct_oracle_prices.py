from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
import threading
import time
import inspect
from typing import Callable

import pandas as pd

from ..config import DataConfig
from ..io.parquet import read_parquet_if_exists, write_parquet_atomic
from ..queries.loaders import load_market_catalog
from ..sources.polymarket_oracle_api import PolymarketOracleApiClient
from .oracle_prices import build_oracle_prices_table
from .truth import build_truth_table


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

LabelFrameRebuildFn = Callable[..., dict[str, object]]


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
    batch_fetch_error = ""

    # Batch fetch via past-results for recent windows.
    current_ts = max(cycle_starts) + cfg.layout.cycle_seconds
    requests_done = 0
    while requests_done < int(max_requests):
        requests_done += 1
        try:
            batch = client.fetch_past_results_batch(
                symbol=symbol,
                current_event_start_time=datetime.fromtimestamp(current_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                cycle_seconds=cfg.layout.cycle_seconds,
                count=int(count),
                sleep_sec=float(sleep_sec),
            )
        except RuntimeError as exc:
            batch_fetch_error = str(exc)
            if fallback_single:
                break
            raise
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
                    "source": str(obj.get("source") or "polymarket_api_crypto_price"),
                    "source_priority": 3 if str(obj.get("source") or "polymarket_api_crypto_price") == "polymarket_api_crypto_price" else 2,
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
        "batch_fetch_error": batch_fetch_error or None,
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


def backfill_direct_oracle_prices(
    cfg: DataConfig,
    *,
    workers: int = 1,
    flush_every: int = 200,
    timeout_sec: float = 30.0,
    max_retries: int = 6,
    sleep_sec: float = 0.0,
    skip_freshness: bool = True,
    rebuild_label_frame_fn: LabelFrameRebuildFn | None = None,
) -> dict[str, object]:
    market_table = pd.read_parquet(cfg.layout.market_catalog_table_path, columns=["cycle_start_ts"])
    cycle_starts = sorted(set(pd.to_numeric(market_table["cycle_start_ts"], errors="coerce").dropna().astype(int).tolist()))

    existing = read_parquet_if_exists(cfg.layout.direct_oracle_source_path)
    completed: set[int] = set()
    if existing is not None and not existing.empty:
        existing = existing.copy()
        existing["cycle_start_ts"] = pd.to_numeric(existing["cycle_start_ts"], errors="coerce").astype("Int64")
        mask = existing["has_both"].fillna(False).astype(bool) & existing["cycle_start_ts"].notna()
        completed = set(existing.loc[mask, "cycle_start_ts"].astype(int).tolist())

    pending = [ts for ts in cycle_starts if ts not in completed]
    if not pending:
        oracle = build_oracle_prices_table(cfg)
        truth = build_truth_table(cfg)
        label = _resolve_label_frame_rebuild_summary(
            cfg,
            rebuild_label_frame_fn=rebuild_label_frame_fn,
            skip_freshness=skip_freshness,
        )
        return {
            "dataset": "direct_oracle_backfill",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "cycle_starts": int(len(cycle_starts)),
            "completed": int(len(completed)),
            "pending": 0,
            "oracle": oracle,
            "truth": truth,
            "label": label,
        }

    thread_local = threading.local()

    def _client() -> PolymarketOracleApiClient:
        client = getattr(thread_local, "client", None)
        if client is None:
            client = PolymarketOracleApiClient(timeout_sec=float(timeout_sec))
            thread_local.client = client
        return client

    def _fetch_one(cycle_start_ts: int) -> tuple[int, dict[str, object]]:
        last_err = ""
        for attempt in range(1, max(1, int(max_retries)) + 1):
            try:
                payload = _client().fetch_crypto_price(
                    symbol=cfg.asset.slug.upper(),
                    cycle_start_ts=int(cycle_start_ts),
                    cycle_seconds=cfg.layout.cycle_seconds,
                    sleep_sec=float(sleep_sec),
                    max_retries=1,
                )
                return int(cycle_start_ts), payload
            except RuntimeError as exc:
                last_err = str(exc)
                if "Too Many Requests" in last_err:
                    time.sleep(min(60.0, 2.0 * attempt))
                    continue
                if "Timestamp too old" in last_err:
                    return int(cycle_start_ts), {}
                time.sleep(min(10.0, 0.5 * attempt))
        raise RuntimeError(f"fetch_failed:{cfg.asset.slug}:{cycle_start_ts}:{last_err}")

    def _flush_rows(rows: list[dict[str, object]]) -> int:
        if not rows:
            return 0
        frame = pd.DataFrame(rows, columns=DIRECT_ORACLE_COLUMNS)
        frame["price_to_beat"] = pd.to_numeric(frame["price_to_beat"], errors="coerce")
        frame["final_price"] = pd.to_numeric(frame["final_price"], errors="coerce")
        frame["has_price_to_beat"] = frame["price_to_beat"].notna()
        frame["has_final_price"] = frame["final_price"].notna()
        frame["has_both"] = frame["has_price_to_beat"] & frame["has_final_price"]
        canonical = _write_direct_oracle_canonical(target_path=cfg.layout.direct_oracle_source_path, incoming=frame)
        rows.clear()
        return int(len(canonical))

    buffer: list[dict[str, object]] = []
    fetched = 0
    last_canonical_rows = len(existing) if existing is not None else 0
    flush_every = max(1, int(flush_every))
    workers = max(1, int(workers))

    def _append_payload(cycle_start_ts: int, payload: dict[str, object]) -> None:
        nonlocal fetched
        if not payload:
            return
        source = str(payload.get("source") or "polymarket_api_crypto_price")
        buffer.append(
            {
                "asset": cfg.asset.slug,
                "cycle": cfg.cycle,
                "cycle_start_ts": int(cycle_start_ts),
                "cycle_end_ts": int(cycle_start_ts + cfg.layout.cycle_seconds),
                "price_to_beat": payload.get("openPrice"),
                "final_price": payload.get("closePrice"),
                "has_price_to_beat": False,
                "has_final_price": False,
                "has_both": False,
                "completed": payload.get("completed"),
                "incomplete": payload.get("incomplete"),
                "cached": payload.get("cached"),
                "api_timestamp_ms": payload.get("timestamp"),
                "http_status": 200,
                "source": source,
                "source_priority": 3 if source == "polymarket_api_crypto_price" else 2,
                "fetched_at": _utc_now_label(),
            }
        )
        fetched += 1

    if workers <= 1:
        for cycle_start_ts in pending:
            while True:
                try:
                    _, payload = _fetch_one(int(cycle_start_ts))
                    break
                except RuntimeError as exc:
                    if "Too Many Requests" in str(exc):
                        time.sleep(60.0)
                        continue
                    payload = {}
                    break
            _append_payload(int(cycle_start_ts), payload)
            if len(buffer) >= flush_every:
                last_canonical_rows = _flush_rows(buffer)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {executor.submit(_fetch_one, ts): ts for ts in pending}
            for future in as_completed(future_map):
                cycle_start_ts = int(future_map[future])
                try:
                    _, payload = future.result()
                except RuntimeError:
                    payload = {}
                _append_payload(cycle_start_ts, payload)
                if len(buffer) >= flush_every:
                    last_canonical_rows = _flush_rows(buffer)

    last_canonical_rows = _flush_rows(buffer)
    oracle = build_oracle_prices_table(cfg)
    truth = build_truth_table(cfg)
    label = _resolve_label_frame_rebuild_summary(
        cfg,
        rebuild_label_frame_fn=rebuild_label_frame_fn,
        skip_freshness=skip_freshness,
    )
    truth_table = pd.read_parquet(cfg.layout.truth_table_path, columns=["cycle_start_ts", "resolved"])
    ts = pd.to_datetime(pd.to_numeric(truth_table["cycle_start_ts"], errors="coerce"), unit="s", utc=True, errors="coerce").dropna()
    return {
        "dataset": "direct_oracle_backfill",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "cycle_starts": int(len(cycle_starts)),
        "completed": int(len(completed)),
        "pending": int(len(pending)),
        "fetched": int(fetched),
        "canonical_rows": int(last_canonical_rows),
        "oracle": oracle,
        "truth": truth,
        "label": label,
        "truth_rows": int(len(truth_table)),
        "resolved": int(truth_table["resolved"].fillna(False).sum()),
        "first": str(ts.min()) if not ts.empty else None,
        "last": str(ts.max()) if not ts.empty else None,
    }


def _resolve_label_frame_rebuild_summary(
    cfg: DataConfig,
    *,
    rebuild_label_frame_fn: LabelFrameRebuildFn | None,
    skip_freshness: bool,
) -> dict[str, object]:
    if rebuild_label_frame_fn is None:
        return {
            "status": "skipped",
            "reason": "research_label_frame_rebuild_moved_out_of_data_domain",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "surface": cfg.surface,
            "skip_freshness_requested": bool(skip_freshness),
        }
    kwargs: dict[str, object] = {}
    try:
        signature = inspect.signature(rebuild_label_frame_fn)
    except (TypeError, ValueError):
        signature = None
    if signature is not None and "skip_freshness" in signature.parameters:
        kwargs["skip_freshness"] = bool(skip_freshness)
    return rebuild_label_frame_fn(cfg, **kwargs)
