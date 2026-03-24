from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


from .fetch import (
    PERP_FALLBACK_BASES,
    SPOT_FALLBACK_BASES,
    compute_ratio as _compute_ratio_impl,
    fetch_book_ticker as _fetch_book_ticker_impl,
    fetch_klines as _fetch_klines_impl,
    fetch_open_interest as _fetch_open_interest_impl,
    normalize_now as _normalize_now_impl,
    request_json as _request_json_impl,
    spread_bps as _spread_bps_impl,
    window_sum as _window_sum_impl,
)
from .policy import (
    apply_temporal_filter as _apply_temporal_filter_impl,
    can_reuse_previous as _can_reuse_previous_impl,
    evaluate_liquidity_raw as _evaluate_liquidity_raw_impl,
    liquidity_thresholds as _liquidity_thresholds_impl,
)
from .state import (
    build_liquidity_state_snapshot as _build_liquidity_state_snapshot_impl,
    load_latest_liquidity_state_snapshot as _load_latest_liquidity_state_snapshot_impl,
    persist_liquidity_state_snapshot as _persist_liquidity_state_snapshot_impl,
    summarize_liquidity_state as _summarize_liquidity_state_impl,
)
from ...data.layout import utc_snapshot_label


def build_liquidity_state_snapshot(
    cfg,
    *,
    persist: bool = True,
    force_refresh: bool = False,
    now: pd.Timestamp | None = None,
) -> dict[str, Any]:
    return _build_liquidity_state_snapshot_impl(
        cfg,
        persist=persist,
        force_refresh=force_refresh,
        now=now,
        utc_snapshot_label_fn=utc_snapshot_label,
        normalize_now_fn=_normalize_now,
        load_latest_liquidity_state_snapshot_fn=load_latest_liquidity_state_snapshot,
        can_reuse_previous_fn=_can_reuse_previous,
        liquidity_thresholds_fn=_liquidity_thresholds,
        evaluate_liquidity_raw_fn=_evaluate_liquidity_raw,
        apply_temporal_filter_fn=_apply_temporal_filter,
        persist_liquidity_state_snapshot_fn=persist_liquidity_state_snapshot,
    )


def persist_liquidity_state_snapshot(*, rewrite_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    return _persist_liquidity_state_snapshot_impl(rewrite_root=rewrite_root, payload=payload)


def load_latest_liquidity_state_snapshot(
    *,
    rewrite_root: Path,
    market: str,
    cycle: str,
    profile: str,
) -> dict[str, Any] | None:
    return _load_latest_liquidity_state_snapshot_impl(
        rewrite_root=rewrite_root,
        market=market,
        cycle=cycle,
        profile=profile,
    )


def summarize_liquidity_state(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    return _summarize_liquidity_state_impl(payload)


def _can_reuse_previous(*, previous_payload: dict[str, Any] | None, now: pd.Timestamp, refresh_seconds: float) -> bool:
    return _can_reuse_previous_impl(
        previous_payload=previous_payload,
        now=now,
        refresh_seconds=refresh_seconds,
    )


def _liquidity_thresholds(*, spec, market: str) -> dict[str, float]:
    return _liquidity_thresholds_impl(spec=spec, market=market)


def _evaluate_liquidity_raw(
    *,
    symbol: str,
    now: pd.Timestamp,
    thresholds: dict[str, float],
    lookback_minutes: int,
    baseline_minutes: int,
    soft_fail_min_count: int,
    hard_spread_multiplier: float,
    hard_basis_multiplier: float,
    spot_base_url: str,
    perp_base_url: str,
) -> dict[str, Any]:
    return _evaluate_liquidity_raw_impl(
        symbol=symbol,
        now=now,
        thresholds=thresholds,
        lookback_minutes=lookback_minutes,
        baseline_minutes=baseline_minutes,
        soft_fail_min_count=soft_fail_min_count,
        hard_spread_multiplier=hard_spread_multiplier,
        hard_basis_multiplier=hard_basis_multiplier,
        spot_base_url=spot_base_url,
        perp_base_url=perp_base_url,
        fetch_klines_fn=_fetch_klines,
        fetch_book_ticker_fn=_fetch_book_ticker,
        fetch_open_interest_fn=_fetch_open_interest,
        compute_ratio_fn=_compute_ratio,
        window_sum_fn=_window_sum,
        spread_bps_fn=_spread_bps,
    )


def _apply_temporal_filter(
    *,
    raw_result: dict[str, Any],
    previous_payload: dict[str, Any] | None,
    min_failed_checks: int,
    min_recovered_checks: int,
    block_on_degrade: bool,
) -> dict[str, Any]:
    return _apply_temporal_filter_impl(
        raw_result=raw_result,
        previous_payload=previous_payload,
        min_failed_checks=min_failed_checks,
        min_recovered_checks=min_recovered_checks,
        block_on_degrade=block_on_degrade,
    )


def _fetch_klines(base_url: str, path: str, symbol: str, limit: int, now: pd.Timestamp) -> pd.DataFrame:
    return _fetch_klines_impl(base_url, path, symbol, limit, now)


def _fetch_book_ticker(base_url: str, path: str, symbol: str) -> tuple[float, float]:
    return _fetch_book_ticker_impl(base_url, path, symbol)


def _fetch_open_interest(base_url: str, symbol: str) -> float | None:
    return _fetch_open_interest_impl(base_url, symbol)


def _request_json(base_url: str, path: str, params: dict[str, object]) -> object:
    return _request_json_impl(base_url, path, params)


def _compute_ratio(
    series: pd.Series,
    *,
    lookback_minutes: int,
    baseline_minutes: int,
) -> tuple[float | None, float | None, float | None]:
    return _compute_ratio_impl(
        series,
        lookback_minutes=lookback_minutes,
        baseline_minutes=baseline_minutes,
    )


def _window_sum(series: pd.Series, *, lookback_minutes: int) -> float | None:
    return _window_sum_impl(series, lookback_minutes=lookback_minutes)


def _spread_bps(bid: float, ask: float) -> tuple[float | None, float | None]:
    return _spread_bps_impl(bid, ask)


def _normalize_now(value: object) -> pd.Timestamp:
    return _normalize_now_impl(value)
