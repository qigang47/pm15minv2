from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class FeatureDefinition:
    name: str
    group: str
    description: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


_FEATURES = (
    FeatureDefinition("ret_1m", "price", "1-minute log return"),
    FeatureDefinition("ret_3m", "price", "3-minute log return"),
    FeatureDefinition("ret_5m", "price", "5-minute log return"),
    FeatureDefinition("ret_15m", "price", "15-minute log return"),
    FeatureDefinition("ret_30m", "price", "30-minute log return"),
    FeatureDefinition("rv_30", "price", "30-minute realized volatility"),
    FeatureDefinition("rv_30_lag1", "price", "Lagged 30-minute realized volatility"),
    FeatureDefinition("ma_gap_5", "price", "Gap versus 5-period moving average"),
    FeatureDefinition("bb_pos_20", "price", "Position inside 20-period Bollinger band"),
    FeatureDefinition("atr_14", "price", "14-period average true range"),
    FeatureDefinition("price_pos_iqr_20", "price", "Position inside 20-period interquartile range"),
    FeatureDefinition("adx_14", "price", "14-period ADX trend strength"),
    FeatureDefinition("hour_sin", "calendar", "Hour-of-day sine encoding"),
    FeatureDefinition("hour_cos", "calendar", "Hour-of-day cosine encoding"),
    FeatureDefinition("bias_60", "price", "Gap versus 60-period moving average"),
    FeatureDefinition("z_ret_30m", "price", "Z-scored 30-minute return"),
    FeatureDefinition("ret_1m_lag1", "price", "Lagged 1-minute return"),
    FeatureDefinition("ret_5m_lag1", "price", "Lagged 5-minute return"),
    FeatureDefinition("delta_rsi", "price", "1-step RSI delta"),
    FeatureDefinition("delta_rsi_5", "price", "5-step RSI delta"),
    FeatureDefinition("macd_z", "price", "Z-scored MACD histogram"),
    FeatureDefinition("rsi_divergence", "price", "Short-horizon RSI divergence"),
    FeatureDefinition("regime_high_vol", "price", "High-volatility regime flag"),
    FeatureDefinition("taker_buy_ratio", "volume", "Taker-buy quote share"),
    FeatureDefinition("taker_buy_ratio_z", "volume", "Z-scored taker-buy ratio"),
    FeatureDefinition("volume_z", "volume", "Z-scored base volume"),
    FeatureDefinition("volume_z_3", "volume", "Short-horizon rolling volume z-score"),
    FeatureDefinition("vwap_gap_20", "volume", "Gap versus 20-period VWAP"),
    FeatureDefinition("vol_ratio_5_60", "volume", "5/60 rolling volume ratio"),
    FeatureDefinition("vol_price_corr_15", "volume", "15-period rolling price/volume correlation"),
    FeatureDefinition("ret_from_cycle_open", "cycle", "Return from cycle open"),
    FeatureDefinition("pullback_from_cycle_high", "cycle", "Pullback from cycle high"),
    FeatureDefinition("rebound_from_cycle_low", "cycle", "Rebound from cycle low"),
    FeatureDefinition("cycle_range_pos", "cycle", "Position inside current cycle range"),
    FeatureDefinition("move_z", "cycle", "Cycle move normalized by rv_30"),
    FeatureDefinition("ret_from_strike", "strike", "Return vs oracle strike"),
    FeatureDefinition("basis_bp", "strike", "Binance/oracle basis in bps"),
    FeatureDefinition("has_oracle_strike", "strike", "Whether strike price was available"),
    FeatureDefinition("has_cl_strike", "strike", "Legacy-compatible alias for strike availability"),
    FeatureDefinition("move_z_strike", "strike", "Strike return normalized by rv_30"),
    FeatureDefinition("q_bs_up_strike", "strike", "Raw BS-style strike probability proxy"),
    FeatureDefinition("q_bs_up_strike_centered", "strike", "Centered BS-style strike probability proxy"),
    FeatureDefinition("btc_ret_5m", "cross_asset", "BTC 5-minute return"),
    FeatureDefinition("btc_vol_30m", "cross_asset", "BTC 30-minute volatility"),
    FeatureDefinition("rel_strength_15m", "cross_asset", "Asset minus BTC 15-minute return"),
)

_FEATURE_SET_COLUMNS = {
    "v6_user_core": (
        "ret_1m",
        "ret_3m",
        "ret_15m",
        "rv_30",
        "atr_14",
        "adx_14",
        "regime_high_vol",
        "taker_buy_ratio",
        "taker_buy_ratio_z",
        "volume_z",
        "vwap_gap_20",
        "bias_60",
        "vol_price_corr_15",
        "volume_z_3",
        "vol_ratio_5_60",
        "ret_from_cycle_open",
        "move_z",
        "pullback_from_cycle_high",
        "rebound_from_cycle_low",
        "cycle_range_pos",
        "ret_1m_lag1",
        "ret_5m_lag1",
        "delta_rsi",
        "delta_rsi_5",
        "macd_z",
        "btc_ret_5m",
        "btc_vol_30m",
        "rel_strength_15m",
        "ret_from_strike",
        "basis_bp",
        "has_cl_strike",
    ),
    "deep_otm_v1": (
        "ret_1m",
        "ret_3m",
        "ret_5m",
        "ret_15m",
        "rv_30",
        "taker_buy_ratio",
        "taker_buy_ratio_z",
        "volume_z",
        "ret_from_cycle_open",
        "pullback_from_cycle_high",
        "rebound_from_cycle_low",
        "cycle_range_pos",
        "move_z",
        "ret_from_strike",
        "basis_bp",
        "has_oracle_strike",
        "move_z_strike",
        "q_bs_up_strike_centered",
        "btc_ret_5m",
        "btc_vol_30m",
        "rel_strength_15m",
    ),
    "alpha_search_direction_live": (
        "ret_1m",
        "ret_3m",
        "ret_5m",
        "ret_15m",
        "ret_30m",
        "ma_gap_5",
        "bb_pos_20",
        "rv_30",
        "rv_30_lag1",
        "atr_14",
        "price_pos_iqr_20",
        "adx_14",
        "regime_high_vol",
        "taker_buy_ratio",
        "taker_buy_ratio_z",
        "volume_z",
        "hour_sin",
        "hour_cos",
        "bias_60",
        "vol_price_corr_15",
        "vol_ratio_5_60",
        "z_ret_30m",
        "ret_from_cycle_open",
        "pullback_from_cycle_high",
        "rebound_from_cycle_low",
        "cycle_range_pos",
        "move_z",
        "ret_1m_lag1",
        "ret_5m_lag1",
        "delta_rsi",
        "delta_rsi_5",
        "macd_z",
        "rsi_divergence",
        "ret_from_strike",
        "basis_bp",
        "has_cl_strike",
        "q_bs_up_strike",
        "q_bs_up_strike_centered",
        "btc_ret_5m",
        "btc_vol_30m",
        "rel_strength_15m",
    ),
}

_FEATURE_SET_DROP_COLUMNS = {
    "deep_otm_v1": (
        "has_cl_strike",
    ),
    "v6_user_core": (
        "has_oracle_strike",
    ),
    "alpha_search_direction_live": (
        "has_oracle_strike",
    ),
}


def feature_registry() -> dict[str, FeatureDefinition]:
    return {feature.name: feature for feature in _FEATURES}


def feature_set_columns(feature_set: str) -> tuple[str, ...]:
    key = str(feature_set).strip().lower()
    try:
        return _FEATURE_SET_COLUMNS[key]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported feature_set {feature_set!r}. Expected one of: {', '.join(sorted(_FEATURE_SET_COLUMNS))}"
        ) from exc


def feature_schema(feature_set: str) -> list[dict[str, str]]:
    registry = feature_registry()
    return [registry[name].to_dict() for name in feature_set_columns(feature_set) if name in registry]


def feature_set_drop_columns(feature_set: str) -> tuple[str, ...]:
    key = str(feature_set).strip().lower()
    feature_set_columns(key)
    return _FEATURE_SET_DROP_COLUMNS.get(key, ())
