from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from pm15min.data.config import DataConfig


def build_cancel_candidates(
    *,
    open_orders_snapshot: dict[str, Any],
    market_table: pd.DataFrame,
    cancel_window_minutes: int,
    now: pd.Timestamp,
) -> list[dict[str, Any]]:
    cycle_end_by_market: dict[str, pd.Timestamp] = {}
    question_by_market: dict[str, str] = {}
    for _, row in market_table.iterrows():
        market_id = str(row.get("market_id") or "").strip()
        if not market_id:
            continue
        cycle_end_ts = parse_market_cycle_end(row.get("cycle_end_ts"))
        if cycle_end_ts is not None:
            cycle_end_by_market[market_id] = cycle_end_ts
        question_by_market[market_id] = str(row.get("question") or "")

    out: list[dict[str, Any]] = []
    for raw in open_orders_snapshot.get("orders") or []:
        if not isinstance(raw, dict):
            continue
        market_id = str(raw.get("market_id") or "").strip()
        order_id = str(raw.get("order_id") or "").strip() or None
        token_id = str(raw.get("token_id") or "").strip() or None
        cycle_end_ts = cycle_end_by_market.get(market_id)
        if cycle_end_ts is None:
            continue
        minutes_left = max(0.0, float((cycle_end_ts - now).total_seconds() / 60.0))
        if minutes_left > float(cancel_window_minutes):
            continue
        out.append(
            {
                "order_id": order_id,
                "market_id": market_id or None,
                "token_id": token_id,
                "question": question_by_market.get(market_id) or None,
                "market_cycle_end_ts": cycle_end_ts.isoformat(),
                "minutes_left_to_market_end": minutes_left,
            }
        )
    return out


def load_live_market_table(cfg) -> pd.DataFrame:
    data_cfg = DataConfig.build(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        surface="live",
        root=cfg.layout.rewrite.root,
    )
    path = data_cfg.layout.market_catalog_table_path
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    return df if not df.empty else pd.DataFrame()


def build_action_key(signature: object) -> str | None:
    if signature in (None, {}, []):
        return None
    text = json.dumps(signature, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_cancel_action_signature(*, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [
        {
            "order_id": row.get("order_id"),
            "market_id": row.get("market_id"),
            "token_id": row.get("token_id"),
            "market_cycle_end_ts": row.get("market_cycle_end_ts"),
        }
        for row in candidates
    ]
    rows.sort(
        key=lambda row: (
            str(row.get("market_id") or ""),
            str(row.get("token_id") or ""),
            str(row.get("order_id") or ""),
        )
    )
    return {"candidates": rows}


def build_redeem_action_signature(*, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [
        {
            "condition_id": row.get("condition_id"),
            "index_sets": sorted(int(v) for v in (row.get("index_sets") or [])),
        }
        for row in candidates
    ]
    rows.sort(key=lambda row: str(row.get("condition_id") or ""))
    return {"candidates": rows}


def build_order_action_signature(*, order_request: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(order_request.get("metadata") or {})
    return {
        "action": str(order_request.get("action") or "BUY").strip().upper(),
        "market_id": str(order_request.get("market_id") or "").strip(),
        "token_id": str(order_request.get("token_id") or "").strip(),
        "side": str(order_request.get("side") or "").strip().upper(),
        "order_type": str(order_request.get("order_type") or "").strip().upper(),
        "offset": int_or_none(metadata.get("offset")),
        "window_start_ts": order_request.get("window_start_ts") or metadata.get("window_start_ts"),
        "window_end_ts": order_request.get("window_end_ts") or metadata.get("window_end_ts"),
    }


def parse_market_cycle_end(value: object) -> pd.Timestamp | None:
    if value is None:
        return None
    if isinstance(value, str):
        ts = pd.to_datetime(value, utc=True, errors="coerce")
    else:
        ts = pd.to_datetime(value, utc=True, unit="s", errors="coerce")
    if ts is None or pd.isna(ts):
        return None
    return ts


def build_order_request_from_execution(*, execution: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    token_id = str(execution.get("token_id") or "").strip()
    market_id = str(execution.get("market_id") or "").strip()
    side = str(execution.get("selected_side") or "").strip().upper()
    order_type = str(execution.get("order_type") or "").strip().upper()
    limit_price = float_or_none(execution.get("entry_price"))
    requested_notional = float_or_none(execution.get("requested_notional_usd"))
    decision_ts = execution.get("decision_ts")
    window_start_ts, window_end_ts, window_duration_seconds = resolve_execution_window_bounds(execution=execution)
    if not market_id:
        return None, "market_id_missing"
    if not token_id:
        return None, "token_id_missing"
    if side not in {"UP", "DOWN"}:
        return None, "selected_side_missing"
    if not order_type:
        return None, "order_type_missing"
    if limit_price is None or limit_price <= 0.0:
        return None, "entry_price_missing"
    if requested_notional is None or requested_notional <= 0.0:
        return None, "requested_notional_missing"
    submitted_shares = resolve_submitted_shares(
        execution=execution,
        limit_price=limit_price,
        requested_notional=requested_notional,
    )
    if submitted_shares is None or submitted_shares <= 0.0:
        return None, "submitted_shares_missing"
    repriced = execution.get("repriced_metrics") or {}
    return {
        "market_id": market_id,
        "token_id": token_id,
        "side": side,
        "order_type": order_type,
        "order_kind": resolve_order_kind(order_type),
        "action": "BUY",
        "price": float(limit_price),
        "size": float(submitted_shares),
        "decision_ts": decision_ts,
        "window_start_ts": window_start_ts,
        "window_end_ts": window_end_ts,
        "window_duration_seconds": window_duration_seconds,
        "metadata": {
            "stake": float(requested_notional),
            "offset": execution.get("selected_offset"),
            "roi": float_or_none(repriced.get("repriced_roi_net")) or float_or_none(execution.get("roi_net_vs_quote")) or 0.0,
            "roi_threshold": float_or_none(repriced.get("repriced_roi_threshold_required")) or 0.0,
            "window_start_ts": window_start_ts,
            "window_end_ts": window_end_ts,
            "window_duration_seconds": window_duration_seconds,
        },
    }, None


def resolve_execution_window_bounds(*, execution: dict[str, Any]) -> tuple[str | None, str | None, float | None]:
    window_start = snapshot_label_to_timestamp(execution.get("window_start_ts"))
    window_end = snapshot_label_to_timestamp(execution.get("window_end_ts"))
    duration_seconds = float_or_none(execution.get("window_duration_seconds"))
    if window_start is None:
        cycle_start = snapshot_label_to_timestamp(execution.get("cycle_start_ts"))
        selected_offset = int_or_none(execution.get("selected_offset"))
        if cycle_start is not None and selected_offset is not None:
            window_start = cycle_start + pd.to_timedelta(int(selected_offset), unit="m")
    if window_end is None and window_start is not None and duration_seconds is not None and duration_seconds > 0.0:
        window_end = window_start + pd.to_timedelta(duration_seconds, unit="s")
    if window_end is None:
        decision_ts = snapshot_label_to_timestamp(execution.get("decision_ts"))
        if decision_ts is not None:
            if window_start is None:
                window_start = decision_ts
            if duration_seconds is None or duration_seconds <= 0.0:
                duration_seconds = 60.0
            window_end = decision_ts + pd.to_timedelta(duration_seconds, unit="s")
    if duration_seconds is None and window_start is not None and window_end is not None:
        duration_seconds = max(0.0, float((window_end - window_start).total_seconds()))
    return (
        None if window_start is None else window_start.isoformat(),
        None if window_end is None else window_end.isoformat(),
        duration_seconds,
    )


def resolve_submitted_shares(*, execution: dict[str, Any], limit_price: float, requested_notional: float) -> float | None:
    depth_plan = execution.get("depth_plan") or {}
    depth_filled_shares = float_or_none(depth_plan.get("filled_shares"))
    if depth_filled_shares is not None and depth_filled_shares > 0.0:
        return float(depth_filled_shares)
    requested_shares = float_or_none(execution.get("requested_shares"))
    if requested_shares is not None and requested_shares > 0.0:
        return float(min(requested_shares, requested_notional / max(limit_price, 1e-9)))
    return requested_notional / max(limit_price, 1e-9)


def resolve_order_kind(order_type: str) -> str:
    token = str(order_type or "").strip().upper()
    if token == "GTC":
        return "limit"
    return "market"


def normalize_now(value: object) -> pd.Timestamp:
    if value is None:
        return pd.Timestamp.now(tz="UTC")
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def int_or_none(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out


def snapshot_label_to_timestamp(value: object) -> pd.Timestamp | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.strptime(text, "%Y-%m-%dT%H-%M-%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        ts = pd.to_datetime(text, utc=True, errors="coerce")
        if ts is None or pd.isna(ts):
            return None
        return ts
    return pd.Timestamp(dt)
