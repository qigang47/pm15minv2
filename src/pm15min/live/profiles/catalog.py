from __future__ import annotations

from .spec import LiveProfileSpec


_DEFAULT_PROFILE_VALUES = {
    "profile": "default",
    "target": "direction",
    "default_feature_set": "v6_user_core",
    "active_markets": (),
    "offsets": (),
    "entry_price_min": None,
    "entry_price_max": None,
    "min_dir_prob_default": 0.60,
    "min_dir_prob_by_offset": {},
    "min_net_edge_default": 0.0,
    "min_net_edge_by_offset": {},
    "min_net_edge_entry_price_le_0p10_bonus": 0.0,
    "min_net_edge_entry_price_le_0p05_bonus": 0.0,
    "roi_threshold_default": 0.0,
    "roi_threshold_by_offset": {},
    "slippage_bps": 0.0,
    "fee_model": "polymarket_curve",
    "fee_bps": 1.0,
    "fee_curve_k": 0.25,
    "stake_usd": 1.0,
    "stake_cash_pct": 0.0,
    "stake_cash_refresh_seconds": 30.0,
    "stake_cash_min_usd": 1.0,
    "stake_cash_max_usd": None,
    "stop_trading_below_cash_usd": 0.0,
    "max_trades_per_market": 0,
    "stake_balance_step_threshold_usd": 0.0,
    "stake_balance_step_usd": 0.0,
    "stake_balance_base_usd": 0.0,
    "stake_balance_increment_usd": 0.0,
    "max_notional_usd": 2000.0,
    "max_open_markets": 0,
    "max_daily_loss_usd": None,
    "default_order_type": "FAK",
    "cancel_markets_when_minutes_left": 2,
    "orderbook_min_fill_ratio": 0.5,
    "orderbook_max_slippage_bps": 50.0,
    "order_retry_interval_seconds": 1.0,
    "fast_retry_interval_seconds": 1.0,
    "max_order_retries": 3,
    "orderbook_fast_retry_max": 5,
    "orderbook_fast_retry_interval_seconds": 0.2,
    "fak_immediate_retry_max": 3,
    "repeat_same_decision_enabled": False,
    "repeat_same_decision_max_trades": 1,
    "repeat_same_decision_max_stake_usd": None,
    "repeat_same_decision_max_total_stake_usd": None,
    "repeat_same_decision_stake_multiple": 1.0,
    "repeat_same_decision_lock_side": True,
    "ret_30m_up_floor_by_asset": {},
    "ret_30m_down_ceiling_by_asset": {},
    "tail_space_guard_enabled": False,
    "tail_space_max_move_z_default": None,
    "tail_space_max_move_z_by_offset": {},
    "feature_blacklist_by_asset": {},
    "liquidity_guard_enabled": False,
    "liquidity_guard_block": True,
    "liquidity_guard_fail_open": True,
    "liquidity_guard_refresh_seconds": 30.0,
    "liquidity_guard_lookback_minutes": 10,
    "liquidity_guard_baseline_minutes": 180,
    "liquidity_guard_min_failed_checks": 2,
    "liquidity_guard_min_recovered_checks": 2,
    "liquidity_guard_soft_fail_min_count": 2,
    "liquidity_guard_hard_spread_multiplier": 1.8,
    "liquidity_guard_hard_basis_multiplier": 1.8,
    "liquidity_min_spot_quote_volume_ratio_by_asset": {},
    "liquidity_min_perp_quote_volume_ratio_by_asset": {},
    "liquidity_min_spot_trades_ratio_by_asset": {},
    "liquidity_min_perp_trades_ratio_by_asset": {},
    "liquidity_min_spot_quote_volume_window_by_asset": {},
    "liquidity_min_perp_quote_volume_window_by_asset": {},
    "liquidity_min_spot_trades_window_by_asset": {},
    "liquidity_min_perp_trades_window_by_asset": {},
    "liquidity_max_spot_spread_bps_by_asset": {"default": 12.0},
    "liquidity_max_perp_spread_bps_by_asset": {"default": 12.0},
    "liquidity_max_basis_bps_by_asset": {"default": 20.0},
    "liquidity_min_open_interest_usd_by_asset": {},
    "regime_controller_enabled": False,
    "regime_switch_confirmations": 2,
    "regime_recover_confirmations": 2,
    "regime_caution_min_liquidity_ratio": 0.60,
    "regime_defense_min_liquidity_ratio": 0.40,
    "regime_caution_soft_fail_count": 2,
    "regime_defense_soft_fail_count": 3,
    "regime_up_pressure_ret_15m": 0.0010,
    "regime_up_pressure_ret_30m": 0.0015,
    "regime_down_pressure_ret_15m": -0.0010,
    "regime_down_pressure_ret_30m": -0.0015,
    "regime_apply_stake_scale": False,
    "regime_caution_stake_multiplier": 0.80,
    "regime_defense_stake_multiplier": 0.50,
    "regime_caution_min_dir_prob_boost": 0.03,
    "regime_defense_min_dir_prob_boost": 0.05,
    "regime_caution_disable_offsets": (),
    "regime_defense_disable_offsets": (),
    "regime_defense_force_with_pressure": True,
    "regime_defense_max_trades_per_market": 1,
}

DEFAULT_LIVE_PROFILE_SPEC = LiveProfileSpec(**_DEFAULT_PROFILE_VALUES)

DEEP_OTM_LIVE_PROFILE_SPEC = LiveProfileSpec(
    **(
        _DEFAULT_PROFILE_VALUES
        | {
            "profile": "deep_otm",
            "active_markets": ("btc", "eth", "sol", "xrp"),
            "offsets": (7, 8, 9),
            "entry_price_min": 0.01,
            "entry_price_max": 0.30,
            "min_dir_prob_by_offset": {
                "sol": {7: 0.62},
                "xrp": {},
            },
            "min_net_edge_default": 0.010,
            "min_net_edge_by_offset": {7: 0.012, 8: 0.015, 9: 0.018},
            "min_net_edge_entry_price_le_0p10_bonus": 0.002,
            "min_net_edge_entry_price_le_0p05_bonus": 0.005,
            "stake_cash_pct": 0.0,
            "max_trades_per_market": 0,
            "stake_balance_step_threshold_usd": 150.0,
            "stake_balance_step_usd": 50.0,
            "stake_balance_base_usd": 1.0,
            "stake_balance_increment_usd": 0.5,
            "repeat_same_decision_enabled": True,
            "repeat_same_decision_max_trades": 1,
            "repeat_same_decision_max_stake_usd": 3.0,
            "repeat_same_decision_stake_multiple": 3.0,
            "repeat_same_decision_lock_side": False,
            "ret_30m_up_floor_by_asset": {
                "btc": 0.0,
                "eth": 0.0,
                "sol": 0.0,
                "xrp": 0.0,
            },
            "ret_30m_down_ceiling_by_asset": {
                "btc": 0.002,
                "eth": 0.002,
                "sol": 0.002,
                "xrp": 0.009,
            },
            "tail_space_guard_enabled": True,
            "tail_space_max_move_z_default": 2.0,
            "tail_space_max_move_z_by_offset": {7: 1.85, 8: 2.05, 9: 2.30},
            "feature_blacklist_by_asset": {},
            "liquidity_guard_enabled": True,
            "liquidity_guard_block": False,
            "liquidity_min_spot_quote_volume_ratio_by_asset": {"default": 0.0},
            "liquidity_min_perp_quote_volume_ratio_by_asset": {"default": 0.0},
            "liquidity_min_spot_trades_ratio_by_asset": {"default": 0.0},
            "liquidity_min_perp_trades_ratio_by_asset": {"default": 0.0},
            "liquidity_min_spot_quote_volume_window_by_asset": {
                "btc": 3.0e7,
                "eth": 1.5e7,
                "sol": 3.0e6,
                "xrp": 1.54e6,
                "default": 0.0,
            },
            "liquidity_min_perp_quote_volume_window_by_asset": {
                "btc": 2.5e7,
                "eth": 1.5e7,
                "sol": 3.0e6,
                "xrp": 1.40e6,
                "default": 0.0,
            },
            "liquidity_min_spot_trades_window_by_asset": {
                "btc": 8000,
                "eth": 5000,
                "sol": 2500,
                "xrp": 1600,
                "default": 0.0,
            },
            "liquidity_min_perp_trades_window_by_asset": {
                "btc": 8000,
                "eth": 5000,
                "sol": 2500,
                "xrp": 2500,
                "default": 0.0,
            },
            "liquidity_max_spot_spread_bps_by_asset": {
                "btc": 6.0,
                "eth": 8.0,
                "sol": 14.0,
                "xrp": 14.0,
                "default": 12.0,
            },
            "liquidity_max_perp_spread_bps_by_asset": {
                "btc": 6.0,
                "eth": 8.0,
                "sol": 14.0,
                "xrp": 14.0,
                "default": 12.0,
            },
            "liquidity_max_basis_bps_by_asset": {
                "btc": 12.0,
                "eth": 15.0,
                "sol": 20.0,
                "xrp": 20.0,
                "default": 20.0,
            },
            "liquidity_min_open_interest_usd_by_asset": {
                "btc": 4.0e8,
                "eth": 2.0e8,
                "sol": 4.0e7,
                "xrp": 4.0e7,
                "default": 0.0,
            },
            "regime_controller_enabled": True,
            "regime_caution_stake_multiplier": 1.0,
            "regime_defense_stake_multiplier": 1.0,
            "regime_caution_min_dir_prob_boost": 0.0,
            "regime_defense_min_dir_prob_boost": 0.0,
        }
    )
)

DEEP_OTM_BASELINE_LIVE_PROFILE_SPEC = LiveProfileSpec(
    **(
        DEEP_OTM_LIVE_PROFILE_SPEC.to_dict()
        | {
            "profile": "deep_otm_baseline",
            "min_net_edge_default": 0.0,
            "min_net_edge_by_offset": {7: 0.0, 8: 0.0, 9: 0.0},
            "min_net_edge_entry_price_le_0p10_bonus": 0.0,
            "min_net_edge_entry_price_le_0p05_bonus": 0.0,
            "tail_space_guard_enabled": False,
            "ret_30m_up_floor_by_asset": {
                "btc": 0.0020,
                "eth": 0.0015,
                "sol": -0.04,
                "xrp": -0.04,
            },
            "ret_30m_down_ceiling_by_asset": {
                "btc": 0.0,
                "eth": 0.0,
                "sol": 0.002,
                "xrp": 0.009,
            },
        }
    )
)

DEEP_OTM_5M_LIVE_PROFILE_SPEC = LiveProfileSpec(
    **(
        DEEP_OTM_LIVE_PROFILE_SPEC.to_dict()
        | {
            "profile": "deep_otm_5m",
            "offsets": (2, 3, 4),
            "min_dir_prob_by_offset": {
                "sol": {2: 0.62},
                "xrp": {},
            },
            "min_net_edge_by_offset": {2: 0.012, 3: 0.015, 4: 0.018},
            "tail_space_max_move_z_by_offset": {2: 1.85, 3: 2.05, 4: 2.30},
        }
    )
)

DEEP_OTM_5M_BASELINE_LIVE_PROFILE_SPEC = LiveProfileSpec(
    **(
        DEEP_OTM_5M_LIVE_PROFILE_SPEC.to_dict()
        | {
            "profile": "deep_otm_5m_baseline",
            "min_net_edge_default": 0.0,
            "min_net_edge_by_offset": {2: 0.0, 3: 0.0, 4: 0.0},
            "min_net_edge_entry_price_le_0p10_bonus": 0.0,
            "min_net_edge_entry_price_le_0p05_bonus": 0.0,
            "tail_space_guard_enabled": False,
            "ret_30m_up_floor_by_asset": {
                "btc": 0.0020,
                "eth": 0.0015,
                "sol": -0.04,
                "xrp": -0.04,
            },
            "ret_30m_down_ceiling_by_asset": {
                "btc": 0.0,
                "eth": 0.0,
                "sol": 0.002,
                "xrp": 0.009,
            },
        }
    )
)

LIVE_PROFILE_SPECS = {
    "default": DEFAULT_LIVE_PROFILE_SPEC,
    "deep_otm": DEEP_OTM_LIVE_PROFILE_SPEC,
    "deep_otm_baseline": DEEP_OTM_BASELINE_LIVE_PROFILE_SPEC,
    "deep_otm_5m": DEEP_OTM_5M_LIVE_PROFILE_SPEC,
    "deep_otm_5m_baseline": DEEP_OTM_5M_BASELINE_LIVE_PROFILE_SPEC,
}
