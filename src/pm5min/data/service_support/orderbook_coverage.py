from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from pmshared.io.ndjson_zst import iter_ndjson_zst

from ..config import DataConfig


def build_orderbook_coverage_report(
    cfg: DataConfig,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict[str, Any]:
    root = cfg.layout.orderbook_source_root
    existing_dates = sorted(
        path.name.split("=", 1)[1]
        for path in root.glob("date=*")
        if path.is_dir()
    )
    selected_dates = _selected_dates(existing_dates, date_from=date_from, date_to=date_to)
    expected_daily_market_count = max(1, int(24 * 60 * 60 // int(cfg.layout.cycle_seconds)))

    days: list[dict[str, Any]] = []
    for date_str in selected_dates:
        path = cfg.layout.orderbook_depth_path(date_str)
        day_summary = _summarize_orderbook_depth_partition(
            path=path,
            date_str=date_str,
            expected_daily_market_count=expected_daily_market_count,
        )
        days.append(day_summary)

    existing_day_payloads = [item for item in days if bool(item.get("exists"))]
    complete_days = [item for item in existing_day_payloads if int(item.get("unique_market_count") or 0) >= expected_daily_market_count]
    return {
        "domain": "data",
        "dataset": "orderbook_depth_coverage",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "surface": cfg.surface,
        "orderbook_source_root": str(root),
        "expected_daily_market_count": expected_daily_market_count,
        "date_from": selected_dates[0] if selected_dates else None,
        "date_to": selected_dates[-1] if selected_dates else None,
        "day_count": int(len(days)),
        "existing_day_count": int(len(existing_day_payloads)),
        "complete_day_count": int(len(complete_days)),
        "missing_dates": [str(item.get("date")) for item in days if not bool(item.get("exists"))],
        "incomplete_dates": [
            str(item.get("date"))
            for item in existing_day_payloads
            if int(item.get("unique_market_count") or 0) < expected_daily_market_count
        ],
        "days": days,
    }


def _selected_dates(existing_dates: list[str], *, date_from: str | None, date_to: str | None) -> list[str]:
    if not existing_dates and not date_from and not date_to:
        return []
    start = str(date_from or (existing_dates[0] if existing_dates else "")).strip()
    end = str(date_to or (existing_dates[-1] if existing_dates else "")).strip()
    if not start or not end:
        return []
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    out: list[str] = []
    cursor = start_date
    while cursor <= end_date:
        out.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return out


def _summarize_orderbook_depth_partition(
    *,
    path: Path,
    date_str: str,
    expected_daily_market_count: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "date": date_str,
        "path": str(path),
        "exists": path.exists(),
        "expected_market_count": int(expected_daily_market_count),
    }
    if not path.exists():
        payload.update(
            {
                "status": "missing",
                "row_count": 0,
                "unique_market_count": 0,
                "missing_market_count": int(expected_daily_market_count),
                "coverage_ratio": 0.0,
                "first_snapshot_ts": None,
                "last_snapshot_ts": None,
                "schema_counts": {},
                "provenance": "missing",
            }
        )
        return payload

    row_count = 0
    market_ids: set[str] = set()
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    schema_counts: Counter[str] = Counter()
    for raw in iter_ndjson_zst(path):
        row_count += 1
        market_id = str(raw.get("market_id") or "").strip()
        if market_id:
            market_ids.add(market_id)
        snapshot_dt = _raw_snapshot_dt(raw)
        if snapshot_dt is not None:
            if first_ts is None or snapshot_dt < first_ts:
                first_ts = snapshot_dt
            if last_ts is None or snapshot_dt > last_ts:
                last_ts = snapshot_dt
        schema_counts[_schema_tag(raw)] += 1

    unique_market_count = int(len(market_ids))
    missing_market_count = max(0, int(expected_daily_market_count) - unique_market_count)
    payload.update(
        {
            "status": "ok" if unique_market_count >= int(expected_daily_market_count) else "incomplete",
            "row_count": int(row_count),
            "unique_market_count": unique_market_count,
            "missing_market_count": int(missing_market_count),
            "coverage_ratio": round(float(unique_market_count) / float(expected_daily_market_count), 4),
            "first_snapshot_ts": None if first_ts is None else first_ts.isoformat(),
            "last_snapshot_ts": None if last_ts is None else last_ts.isoformat(),
            "schema_counts": dict(schema_counts),
            "provenance": _provenance_from_schema_counts(schema_counts),
        }
    )
    return payload


def _schema_tag(raw: dict[str, Any]) -> str:
    if "captured_ts_ms" in raw and "asset" in raw and "cycle" in raw:
        return "v2_native"
    if "decision_ts" in raw and "offset" in raw:
        return "legacy_compatible"
    return "unknown"


def _provenance_from_schema_counts(schema_counts: Counter[str]) -> str:
    tags = {tag for tag, count in schema_counts.items() if int(count) > 0}
    if not tags:
        return "empty"
    if tags == {"legacy_compatible"}:
        return "legacy_import_or_legacy_writer"
    if tags == {"v2_native"}:
        return "v2_native_recorder"
    if len(tags) == 1:
        return next(iter(tags))
    return "mixed"


def _raw_snapshot_dt(raw: dict[str, Any]) -> datetime | None:
    for key in ("captured_ts_ms", "orderbook_ts", "logged_at", "source_ts_ms", "decision_ts"):
        value = raw.get(key)
        if value is None:
            continue
        try:
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
            ts = pd.to_datetime(value, utc=True, errors="coerce")
            if ts is None or pd.isna(ts):
                continue
            return ts.to_pydatetime()
        except Exception:
            continue
    return None
