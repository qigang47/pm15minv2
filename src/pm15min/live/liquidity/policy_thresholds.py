from __future__ import annotations


def liquidity_thresholds(*, spec, market: str) -> dict[str, float]:
    return {
        "lookback_minutes": float(spec.liquidity_guard_lookback_minutes),
        "baseline_minutes": float(spec.liquidity_guard_baseline_minutes),
        "min_spot_quote_volume_ratio": spec.liquidity_min_spot_quote_volume_ratio_for(market),
        "min_perp_quote_volume_ratio": spec.liquidity_min_perp_quote_volume_ratio_for(market),
        "min_spot_trades_ratio": spec.liquidity_min_spot_trades_ratio_for(market),
        "min_perp_trades_ratio": spec.liquidity_min_perp_trades_ratio_for(market),
        "min_spot_quote_volume_window": spec.liquidity_min_spot_quote_volume_window_for(market),
        "min_perp_quote_volume_window": spec.liquidity_min_perp_quote_volume_window_for(market),
        "min_spot_trades_window": spec.liquidity_min_spot_trades_window_for(market),
        "min_perp_trades_window": spec.liquidity_min_perp_trades_window_for(market),
        "max_spot_spread_bps": spec.liquidity_max_spot_spread_bps_for(market),
        "max_perp_spread_bps": spec.liquidity_max_perp_spread_bps_for(market),
        "max_basis_bps": spec.liquidity_max_basis_bps_for(market),
        "min_open_interest_usd": spec.liquidity_min_open_interest_usd_for(market),
    }
