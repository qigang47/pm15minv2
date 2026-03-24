from __future__ import annotations

from pathlib import Path
from typing import Any

from pm15min.data.config import DataConfig
from pm15min.data.io.ndjson_zst import iter_ndjson_zst
from pm15min.data.sources.orderbook_provider import OrderbookProvider, build_orderbook_provider_from_env
from pm15min.data.sources.polymarket_clob import normalize_book
from .utils import (
    normalize_levels,
    quote_captured_ts_ms,
    raw_snapshot_ts_ms,
)


POST_DECISION_EXECUTION_TOLERANCE_MS = 120_000
DEFAULT_LIVE_ORDERBOOK_LEVELS = 10
DEFAULT_LIVE_ORDERBOOK_TIMEOUT_SEC = 1.2


def build_depth_execution_plan(
    *,
    data_cfg: DataConfig,
    quote_row: dict[str, Any],
    side: str,
    requested_notional: float,
    price_cap: float | None,
    max_slippage_bps: float,
    min_fill_ratio: float,
    orderbook_provider: OrderbookProvider | None = None,
    prefer_live_provider: bool = False,
    provider_levels: int = DEFAULT_LIVE_ORDERBOOK_LEVELS,
    provider_timeout_sec: float = DEFAULT_LIVE_ORDERBOOK_TIMEOUT_SEC,
) -> tuple[dict[str, Any] | None, str | None]:
    import pandas as pd

    decision_ts = quote_row.get("decision_ts")
    decision_dt = None if decision_ts is None else pd.to_datetime(decision_ts, utc=True, errors="coerce")
    if decision_dt is None or pd.isna(decision_dt):
        return None, "decision_ts_missing"
    date_str = decision_dt.strftime("%Y-%m-%d")
    depth_path = data_cfg.layout.orderbook_depth_path(date_str)
    if not depth_path.exists():
        return {
            "status": "missing",
            "depth_source_path": str(depth_path),
        }, "depth_snapshot_missing"

    token_id = str(quote_row.get("token_up") if side == "UP" else quote_row.get("token_down") or "")
    market_id = str(quote_row.get("market_id") or "")
    side_slug = "up" if side == "UP" else "down"
    target_ts_ms = quote_captured_ts_ms(quote_row=quote_row, side=side)
    if prefer_live_provider:
        provider_payload = build_live_depth_execution_plan_from_provider(
            market_id=market_id,
            token_id=token_id,
            side=side_slug,
            target_ts_ms=target_ts_ms,
            requested_notional=requested_notional,
            price_cap=price_cap,
            max_slippage_bps=max_slippage_bps,
            min_fill_ratio=min_fill_ratio,
            orderbook_provider=orderbook_provider,
            provider_levels=provider_levels,
            provider_timeout_sec=provider_timeout_sec,
        )
        if provider_payload is not None:
            return provider_payload
    record = resolve_depth_snapshot(
        depth_path=depth_path,
        market_id=market_id,
        token_id=token_id,
        side=side_slug,
        target_ts_ms=target_ts_ms,
    )
    if record is None:
        return {
            "status": "missing",
            "depth_source_path": str(depth_path),
            "market_id": market_id,
            "token_id": token_id,
            "side": side_slug,
            **_snapshot_metrics(snapshot_ts_ms=None, target_ts_ms=target_ts_ms),
        }, "depth_snapshot_missing"

    snapshot_ts_ms = raw_snapshot_ts_ms(record)
    fill = compute_fill_from_depth_record(
        record=record,
        target_notional=requested_notional,
        max_slippage_bps=max_slippage_bps,
        price_cap=price_cap,
    )
    if fill is None:
        return {
            "status": "blocked",
            "depth_source_path": str(depth_path),
            "market_id": market_id,
            "token_id": token_id,
            "side": side_slug,
            "price_cap": price_cap,
            **_snapshot_metrics(snapshot_ts_ms=snapshot_ts_ms, target_ts_ms=target_ts_ms),
        }, "depth_fill_unavailable"
    if float(fill["fill_ratio"]) < float(min_fill_ratio):
        return {
            "status": "blocked",
            "depth_source_path": str(depth_path),
            "market_id": market_id,
            "token_id": token_id,
            "side": side_slug,
            "price_cap": price_cap,
            **_snapshot_metrics(snapshot_ts_ms=snapshot_ts_ms, target_ts_ms=target_ts_ms),
            **fill,
        }, "depth_fill_ratio_below_threshold"
    return {
        "status": "ok",
        "depth_source_path": str(depth_path),
        "market_id": market_id,
        "token_id": token_id,
        "side": side_slug,
        "price_cap": price_cap,
        **_snapshot_metrics(snapshot_ts_ms=snapshot_ts_ms, target_ts_ms=target_ts_ms),
        **fill,
    }, None


def build_live_depth_execution_plan_from_provider(
    *,
    market_id: str,
    token_id: str,
    side: str,
    target_ts_ms: int | None,
    requested_notional: float,
    price_cap: float | None,
    max_slippage_bps: float,
    min_fill_ratio: float,
    orderbook_provider: OrderbookProvider | None = None,
    provider_levels: int = DEFAULT_LIVE_ORDERBOOK_LEVELS,
    provider_timeout_sec: float = DEFAULT_LIVE_ORDERBOOK_TIMEOUT_SEC,
) -> tuple[dict[str, Any] | None, str | None] | None:
    if not market_id or not token_id:
        return None
    provider = orderbook_provider
    if provider is None:
        try:
            provider = build_orderbook_provider_from_env(
                source_name=f"v2-live-depth:{market_id}:{side}",
                subscribe_on_read=True,
            )
        except Exception:
            provider = None
    if provider is None:
        return None
    try:
        payload = provider.get_orderbook_summary(
            token_id,
            levels=max(0, int(provider_levels)),
            timeout=max(0.1, float(provider_timeout_sec)),
            force_refresh=False,
        )
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    asks, bids, ts_ms = normalize_book(payload)
    record = dict(payload)
    record["market_id"] = market_id
    record["token_id"] = token_id
    record["side"] = side
    record["asks"] = asks
    record["bids"] = bids
    if ts_ms is not None:
        record["orderbook_ts"] = int(ts_ms)
    snapshot_ts_ms = raw_snapshot_ts_ms(record)
    fill = compute_fill_from_depth_record(
        record=record,
        target_notional=requested_notional,
        max_slippage_bps=max_slippage_bps,
        price_cap=price_cap,
    )
    if fill is None:
        return (
            {
                "status": "blocked",
                "depth_source_kind": "provider",
                "depth_source_path": None,
                "market_id": market_id,
                "token_id": token_id,
                "side": side,
                "price_cap": price_cap,
                **_snapshot_metrics(snapshot_ts_ms=snapshot_ts_ms, target_ts_ms=target_ts_ms),
            },
            "depth_fill_unavailable",
        )
    if float(fill["fill_ratio"]) < float(min_fill_ratio):
        return (
            {
                "status": "blocked",
                "depth_source_kind": "provider",
                "depth_source_path": None,
                "market_id": market_id,
                "token_id": token_id,
                "side": side,
                "price_cap": price_cap,
                **_snapshot_metrics(snapshot_ts_ms=snapshot_ts_ms, target_ts_ms=target_ts_ms),
                **fill,
            },
            "depth_fill_ratio_below_threshold",
        )
    return (
        {
            "status": "ok",
            "depth_source_kind": "provider",
            "depth_source_path": None,
            "market_id": market_id,
            "token_id": token_id,
            "side": side,
            "price_cap": price_cap,
            **_snapshot_metrics(snapshot_ts_ms=snapshot_ts_ms, target_ts_ms=target_ts_ms),
            **fill,
        },
        None,
    )


def resolve_depth_snapshot(
    *,
    depth_path: Path,
    market_id: str,
    token_id: str,
    side: str,
    target_ts_ms: int | None,
) -> dict[str, Any] | None:
    best: dict[str, Any] | None = None
    best_dist: int | None = None
    for raw in iter_ndjson_zst(depth_path):
        if str(raw.get("market_id") or "") != str(market_id):
            continue
        if str(raw.get("token_id") or "") != str(token_id):
            continue
        if str(raw.get("side") or "").lower() != str(side).lower():
            continue
        captured_ts_ms = raw_snapshot_ts_ms(raw)
        if captured_ts_ms is None:
            continue
        if target_ts_ms is None:
            return raw
        dist = abs(int(captured_ts_ms) - int(target_ts_ms))
        if best is None or (best_dist is not None and dist < best_dist):
            best = raw
            best_dist = dist
            if dist == 0:
                break
    if best is None:
        return None
    if target_ts_ms is not None and best_dist is not None and best_dist > POST_DECISION_EXECUTION_TOLERANCE_MS:
        return None
    return best


def compute_fill_from_depth_record(
    *,
    record: dict[str, Any],
    target_notional: float,
    max_slippage_bps: float,
    price_cap: float | None,
) -> dict[str, object] | None:
    asks = normalize_levels(record.get("asks"))
    if not asks or target_notional <= 0:
        return None
    best_price = asks[0][0]
    if best_price <= 0:
        return None
    if price_cap is not None and price_cap > 0 and best_price > price_cap:
        return None
    slip_factor = max(0.0, float(max_slippage_bps)) / 10000.0
    price_limit = best_price * (1.0 + slip_factor)
    if price_cap is not None and price_cap > 0:
        price_limit = min(price_limit, float(price_cap))
    total_cost = 0.0
    total_shares = 0.0
    max_price_used = 0.0
    levels_consumed = 0
    stop_reason = "depth_exhausted"
    for price, size in asks:
        if price > price_limit:
            stop_reason = "price_limit_reached"
            break
        remaining_notional = float(target_notional) - total_cost
        if remaining_notional <= 0:
            stop_reason = "filled_target"
            break
        take_notional = min(price * size, remaining_notional)
        if take_notional <= 0:
            continue
        total_cost += take_notional
        total_shares += take_notional / price
        levels_consumed += 1
        if price > max_price_used:
            max_price_used = price
        if total_cost >= float(target_notional) - 1e-10:
            stop_reason = "filled_target"
            break
    if total_cost <= 0 or total_shares <= 0:
        return None
    remaining_notional = max(0.0, float(target_notional) - total_cost)
    return {
        "filled_shares": float(total_shares),
        "total_cost": float(total_cost),
        "max_price": float(max_price_used if max_price_used else best_price),
        "best_price": float(best_price),
        "fill_ratio": float(total_cost / float(target_notional)),
        "avg_price": float(total_cost / total_shares),
        "requested_notional": float(target_notional),
        "remaining_notional": float(remaining_notional),
        "levels_available": int(len(asks)),
        "levels_consumed": int(levels_consumed),
        "partial_fill": bool(remaining_notional > 1e-10),
        "stop_reason": str(stop_reason),
        "price_limit": float(price_limit),
    }


def _snapshot_metrics(*, snapshot_ts_ms: int | None, target_ts_ms: int | None) -> dict[str, int | None]:
    if snapshot_ts_ms is None:
        return {
            "snapshot_ts_ms": None,
            "snapshot_age_ms": None,
            "snapshot_distance_ms": None,
        }
    age_ms = None if target_ts_ms is None else int(target_ts_ms) - int(snapshot_ts_ms)
    distance_ms = None if target_ts_ms is None else abs(int(target_ts_ms) - int(snapshot_ts_ms))
    return {
        "snapshot_ts_ms": int(snapshot_ts_ms),
        "snapshot_age_ms": age_ms,
        "snapshot_distance_ms": distance_ms,
    }
