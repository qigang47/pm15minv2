from __future__ import annotations

from typing import Any

import pandas as pd


def evaluate_liquidity_raw(
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
    fetch_klines_fn,
    fetch_book_ticker_fn,
    fetch_open_interest_fn,
    compute_ratio_fn,
    window_sum_fn,
    spread_bps_fn,
) -> dict[str, Any]:
    limit = max(int(baseline_minutes) + int(lookback_minutes) + 2, 80)
    spot_klines = fetch_klines_fn(spot_base_url, "/api/v3/klines", symbol, limit, now)
    perp_klines = fetch_klines_fn(perp_base_url, "/fapi/v1/klines", symbol, limit, now)

    spot_quote_ratio, spot_quote_recent, spot_quote_base = compute_ratio_fn(
        spot_klines["quote_asset_volume"],
        lookback_minutes=lookback_minutes,
        baseline_minutes=baseline_minutes,
    )
    perp_quote_ratio, perp_quote_recent, perp_quote_base = compute_ratio_fn(
        perp_klines["quote_asset_volume"],
        lookback_minutes=lookback_minutes,
        baseline_minutes=baseline_minutes,
    )
    spot_trades_ratio, spot_trades_recent, spot_trades_base = compute_ratio_fn(
        spot_klines["number_of_trades"],
        lookback_minutes=lookback_minutes,
        baseline_minutes=baseline_minutes,
    )
    perp_trades_ratio, perp_trades_recent, perp_trades_base = compute_ratio_fn(
        perp_klines["number_of_trades"],
        lookback_minutes=lookback_minutes,
        baseline_minutes=baseline_minutes,
    )
    spot_quote_window = window_sum_fn(spot_klines["quote_asset_volume"], lookback_minutes=lookback_minutes)
    perp_quote_window = window_sum_fn(perp_klines["quote_asset_volume"], lookback_minutes=lookback_minutes)
    spot_trades_window = window_sum_fn(spot_klines["number_of_trades"], lookback_minutes=lookback_minutes)
    perp_trades_window = window_sum_fn(perp_klines["number_of_trades"], lookback_minutes=lookback_minutes)

    spot_bid, spot_ask = fetch_book_ticker_fn(spot_base_url, "/api/v3/ticker/bookTicker", symbol)
    perp_bid, perp_ask = fetch_book_ticker_fn(perp_base_url, "/fapi/v1/ticker/bookTicker", symbol)
    spot_spread_bps, spot_mid = spread_bps_fn(spot_bid, spot_ask)
    perp_spread_bps, perp_mid = spread_bps_fn(perp_bid, perp_ask)
    basis_bps = None
    if spot_mid is not None and perp_mid is not None and spot_mid > 0.0:
        basis_bps = (perp_mid - spot_mid) / spot_mid * 10000.0
    open_interest_usd = None
    min_open_interest = float(thresholds.get("min_open_interest_usd") or 0.0)
    if min_open_interest > 0 and perp_mid is not None and perp_mid > 0.0:
        open_interest = fetch_open_interest_fn(perp_base_url, symbol)
        if open_interest is not None:
            open_interest_usd = open_interest * perp_mid

    metrics = {
        "spot_quote_ratio": float(spot_quote_ratio or 0.0),
        "perp_quote_ratio": float(perp_quote_ratio or 0.0),
        "spot_trades_ratio": float(spot_trades_ratio or 0.0),
        "perp_trades_ratio": float(perp_trades_ratio or 0.0),
        "spot_quote_recent": float(spot_quote_recent or 0.0),
        "spot_quote_baseline": float(spot_quote_base or 0.0),
        "perp_quote_recent": float(perp_quote_recent or 0.0),
        "perp_quote_baseline": float(perp_quote_base or 0.0),
        "spot_trades_recent": float(spot_trades_recent or 0.0),
        "spot_trades_baseline": float(spot_trades_base or 0.0),
        "perp_trades_recent": float(perp_trades_recent or 0.0),
        "perp_trades_baseline": float(perp_trades_base or 0.0),
        "spot_spread_bps": float(spot_spread_bps or 0.0),
        "perp_spread_bps": float(perp_spread_bps or 0.0),
        "basis_bps": float(basis_bps or 0.0),
        "open_interest_usd": float(open_interest_usd or 0.0),
        "spot_quote_window": float(spot_quote_window or 0.0),
        "perp_quote_window": float(perp_quote_window or 0.0),
        "spot_trades_window": float(spot_trades_window or 0.0),
        "perp_trades_window": float(perp_trades_window or 0.0),
        "min_spot_quote_window": float(thresholds.get("min_spot_quote_volume_window") or 0.0),
        "min_perp_quote_window": float(thresholds.get("min_perp_quote_volume_window") or 0.0),
        "min_spot_trades_window": float(thresholds.get("min_spot_trades_window") or 0.0),
        "min_perp_trades_window": float(thresholds.get("min_perp_trades_window") or 0.0),
    }

    soft_reasons: list[str] = []
    hard_reasons: list[str] = []
    if metrics["spot_quote_window"] < float(thresholds.get("min_spot_quote_volume_window") or 0.0):
        soft_reasons.append("spot_quote_window")
    if metrics["perp_quote_window"] < float(thresholds.get("min_perp_quote_volume_window") or 0.0):
        soft_reasons.append("perp_quote_window")
    if metrics["spot_trades_window"] < float(thresholds.get("min_spot_trades_window") or 0.0):
        soft_reasons.append("spot_trades_window")
    if metrics["perp_trades_window"] < float(thresholds.get("min_perp_trades_window") or 0.0):
        soft_reasons.append("perp_trades_window")
    if metrics["spot_quote_ratio"] < float(thresholds.get("min_spot_quote_volume_ratio") or 0.0):
        soft_reasons.append("spot_quote_ratio")
    if metrics["perp_quote_ratio"] < float(thresholds.get("min_perp_quote_volume_ratio") or 0.0):
        soft_reasons.append("perp_quote_ratio")
    if metrics["spot_trades_ratio"] < float(thresholds.get("min_spot_trades_ratio") or 0.0):
        soft_reasons.append("spot_trades_ratio")
    if metrics["perp_trades_ratio"] < float(thresholds.get("min_perp_trades_ratio") or 0.0):
        soft_reasons.append("perp_trades_ratio")
    max_spot_spread = float(thresholds.get("max_spot_spread_bps") or 0.0)
    max_perp_spread = float(thresholds.get("max_perp_spread_bps") or 0.0)
    max_basis = float(thresholds.get("max_basis_bps") or 0.0)
    if max_spot_spread > 0 and metrics["spot_spread_bps"] > max_spot_spread:
        soft_reasons.append("spot_spread")
        if metrics["spot_spread_bps"] > max_spot_spread * hard_spread_multiplier:
            hard_reasons.append("spot_spread_hard")
    if max_perp_spread > 0 and metrics["perp_spread_bps"] > max_perp_spread:
        soft_reasons.append("perp_spread")
        if metrics["perp_spread_bps"] > max_perp_spread * hard_spread_multiplier:
            hard_reasons.append("perp_spread_hard")
    if max_basis > 0 and abs(metrics["basis_bps"]) > max_basis:
        soft_reasons.append("basis")
        if abs(metrics["basis_bps"]) > max_basis * hard_basis_multiplier:
            hard_reasons.append("basis_hard")
    if min_open_interest > 0 and metrics["open_interest_usd"] < min_open_interest:
        soft_reasons.append("open_interest")

    soft_fail_count = len(soft_reasons)
    hard_fail_count = len(hard_reasons)
    metrics["soft_fail_count"] = float(soft_fail_count)
    metrics["hard_fail_count"] = float(hard_fail_count)
    metrics["soft_fail_min_count"] = float(soft_fail_min_count)
    reason_codes = soft_reasons + hard_reasons
    if hard_fail_count > 0 or soft_fail_count >= int(soft_fail_min_count):
        return {
            "ok": False,
            "blocked": False,
            "reason_codes": reason_codes,
            "metrics": metrics,
            "error": None,
        }
    return {
        "ok": True,
        "blocked": False,
        "reason_codes": ["ok"],
        "metrics": metrics,
        "error": None,
    }
