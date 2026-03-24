from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class LiveProfileSpec:
    profile: str
    target: str
    default_feature_set: str
    active_markets: tuple[str, ...]
    offsets: tuple[int, ...]
    entry_price_min: float | None
    entry_price_max: float | None
    min_dir_prob_default: float
    min_dir_prob_by_offset: dict[str, dict[int, float]]
    min_net_edge_default: float
    min_net_edge_by_offset: dict[int, float]
    min_net_edge_entry_price_le_0p10_bonus: float
    min_net_edge_entry_price_le_0p05_bonus: float
    roi_threshold_default: float
    roi_threshold_by_offset: dict[int, float]
    slippage_bps: float
    fee_model: str
    fee_bps: float
    fee_curve_k: float
    stake_usd: float
    stake_cash_pct: float
    stake_cash_refresh_seconds: float
    stake_cash_min_usd: float | None
    stake_cash_max_usd: float | None
    stop_trading_below_cash_usd: float
    max_trades_per_market: int
    stake_balance_step_threshold_usd: float
    stake_balance_step_usd: float
    stake_balance_base_usd: float
    stake_balance_increment_usd: float
    max_notional_usd: float
    max_open_markets: int
    max_daily_loss_usd: float | None
    default_order_type: str
    cancel_markets_when_minutes_left: int | None
    orderbook_min_fill_ratio: float
    orderbook_max_slippage_bps: float
    order_retry_interval_seconds: float
    fast_retry_interval_seconds: float
    max_order_retries: int
    orderbook_fast_retry_max: int
    orderbook_fast_retry_interval_seconds: float
    fak_immediate_retry_max: int
    repeat_same_decision_enabled: bool
    repeat_same_decision_max_trades: int
    repeat_same_decision_max_stake_usd: float | None
    repeat_same_decision_max_total_stake_usd: float | None
    repeat_same_decision_stake_multiple: float
    repeat_same_decision_lock_side: bool
    ret_30m_up_floor_by_asset: dict[str, float]
    ret_30m_down_ceiling_by_asset: dict[str, float]
    tail_space_guard_enabled: bool
    tail_space_max_move_z_default: float | None
    tail_space_max_move_z_by_offset: dict[int, float]
    feature_blacklist_by_asset: dict[str, tuple[str, ...]]
    liquidity_guard_enabled: bool
    liquidity_guard_block: bool
    liquidity_guard_fail_open: bool
    liquidity_guard_refresh_seconds: float
    liquidity_guard_lookback_minutes: int
    liquidity_guard_baseline_minutes: int
    liquidity_guard_min_failed_checks: int
    liquidity_guard_min_recovered_checks: int
    liquidity_guard_soft_fail_min_count: int
    liquidity_guard_hard_spread_multiplier: float
    liquidity_guard_hard_basis_multiplier: float
    liquidity_min_spot_quote_volume_ratio_by_asset: dict[str, float]
    liquidity_min_perp_quote_volume_ratio_by_asset: dict[str, float]
    liquidity_min_spot_trades_ratio_by_asset: dict[str, float]
    liquidity_min_perp_trades_ratio_by_asset: dict[str, float]
    liquidity_min_spot_quote_volume_window_by_asset: dict[str, float]
    liquidity_min_perp_quote_volume_window_by_asset: dict[str, float]
    liquidity_min_spot_trades_window_by_asset: dict[str, float]
    liquidity_min_perp_trades_window_by_asset: dict[str, float]
    liquidity_max_spot_spread_bps_by_asset: dict[str, float]
    liquidity_max_perp_spread_bps_by_asset: dict[str, float]
    liquidity_max_basis_bps_by_asset: dict[str, float]
    liquidity_min_open_interest_usd_by_asset: dict[str, float]
    regime_controller_enabled: bool
    regime_switch_confirmations: int
    regime_recover_confirmations: int
    regime_caution_min_liquidity_ratio: float
    regime_defense_min_liquidity_ratio: float
    regime_caution_soft_fail_count: int
    regime_defense_soft_fail_count: int
    regime_up_pressure_ret_15m: float
    regime_up_pressure_ret_30m: float
    regime_down_pressure_ret_15m: float
    regime_down_pressure_ret_30m: float
    regime_apply_stake_scale: bool
    regime_caution_stake_multiplier: float
    regime_defense_stake_multiplier: float
    regime_caution_min_dir_prob_boost: float
    regime_defense_min_dir_prob_boost: float
    regime_caution_disable_offsets: tuple[int, ...]
    regime_defense_disable_offsets: tuple[int, ...]
    regime_defense_force_with_pressure: bool
    regime_defense_max_trades_per_market: int

    def threshold_for(self, *, market: str, offset: int) -> float:
        asset_map = self.min_dir_prob_by_offset.get(str(market).lower(), {})
        return float(asset_map.get(int(offset), self.min_dir_prob_default))

    def blacklist_for(self, market: str) -> tuple[str, ...]:
        return tuple(self.feature_blacklist_by_asset.get(str(market).lower(), ()))

    def min_net_edge_for(self, *, offset: int, entry_price: float | None = None) -> float:
        value = float(self.min_net_edge_by_offset.get(int(offset), self.min_net_edge_default))
        if entry_price is not None:
            try:
                px = float(entry_price)
            except Exception:
                px = float("nan")
            if px > 0:
                if px <= 0.05:
                    value += float(self.min_net_edge_entry_price_le_0p05_bonus)
                elif px <= 0.10:
                    value += float(self.min_net_edge_entry_price_le_0p10_bonus)
        return max(0.0, value)

    def roi_threshold_for(self, *, offset: int) -> float:
        return max(0.0, float(self.roi_threshold_by_offset.get(int(offset), self.roi_threshold_default)))

    def fee_rate(self, *, price: float) -> float:
        model = str(self.fee_model or "flat_bps").strip().lower()
        if model == "polymarket_curve":
            p = max(0.0, min(float(price), 1.0))
            k = float(self.fee_curve_k)
            return max(0.0, k * (p * (1.0 - p)) ** 2)
        return max(0.0, float(self.fee_bps)) / 10000.0

    def ret_30m_up_floor_for(self, market: str) -> float | None:
        return self.ret_30m_up_floor_by_asset.get(str(market).lower())

    def ret_30m_down_ceiling_for(self, market: str) -> float | None:
        return self.ret_30m_down_ceiling_by_asset.get(str(market).lower())

    def tail_space_max_move_z_for(self, offset: int) -> float | None:
        if not self.tail_space_guard_enabled:
            return None
        return self.tail_space_max_move_z_by_offset.get(int(offset), self.tail_space_max_move_z_default)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def liquidity_min_spot_quote_volume_ratio_for(self, market: str) -> float:
        return self._asset_threshold(self.liquidity_min_spot_quote_volume_ratio_by_asset, market)

    def liquidity_min_perp_quote_volume_ratio_for(self, market: str) -> float:
        return self._asset_threshold(self.liquidity_min_perp_quote_volume_ratio_by_asset, market)

    def liquidity_min_spot_trades_ratio_for(self, market: str) -> float:
        return self._asset_threshold(self.liquidity_min_spot_trades_ratio_by_asset, market)

    def liquidity_min_perp_trades_ratio_for(self, market: str) -> float:
        return self._asset_threshold(self.liquidity_min_perp_trades_ratio_by_asset, market)

    def liquidity_min_spot_quote_volume_window_for(self, market: str) -> float:
        return self._asset_threshold(self.liquidity_min_spot_quote_volume_window_by_asset, market)

    def liquidity_min_perp_quote_volume_window_for(self, market: str) -> float:
        return self._asset_threshold(self.liquidity_min_perp_quote_volume_window_by_asset, market)

    def liquidity_min_spot_trades_window_for(self, market: str) -> float:
        return self._asset_threshold(self.liquidity_min_spot_trades_window_by_asset, market)

    def liquidity_min_perp_trades_window_for(self, market: str) -> float:
        return self._asset_threshold(self.liquidity_min_perp_trades_window_by_asset, market)

    def liquidity_max_spot_spread_bps_for(self, market: str) -> float:
        return self._asset_threshold(self.liquidity_max_spot_spread_bps_by_asset, market)

    def liquidity_max_perp_spread_bps_for(self, market: str) -> float:
        return self._asset_threshold(self.liquidity_max_perp_spread_bps_by_asset, market)

    def liquidity_max_basis_bps_for(self, market: str) -> float:
        return self._asset_threshold(self.liquidity_max_basis_bps_by_asset, market)

    def liquidity_min_open_interest_usd_for(self, market: str) -> float:
        return self._asset_threshold(self.liquidity_min_open_interest_usd_by_asset, market)

    def regime_min_dir_prob_boost_for(self, state: str) -> float:
        token = str(state or "").strip().upper()
        if token == "DEFENSE":
            return max(0.0, float(self.regime_defense_min_dir_prob_boost))
        if token == "CAUTION":
            return max(0.0, float(self.regime_caution_min_dir_prob_boost))
        return 0.0

    def regime_disabled_offsets_for(self, state: str) -> tuple[int, ...]:
        token = str(state or "").strip().upper()
        if token == "DEFENSE":
            values = self.regime_defense_disable_offsets
        elif token == "CAUTION":
            values = self.regime_caution_disable_offsets
        else:
            values = ()
        return tuple(sorted({int(value) for value in values}))

    @staticmethod
    def _asset_threshold(mapping: dict[str, float], market: str) -> float:
        token = str(market or "").strip().lower()
        return float(mapping.get(token, mapping.get("default", 0.0)))
