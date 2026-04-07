from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path

from pm15min.core.layout import rewrite_root
from pm15min.research.layout import slug_token


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
    FeatureDefinition("ret_60m", "price", "60-minute log return"),
    FeatureDefinition("rv_30", "price", "30-minute realized volatility"),
    FeatureDefinition("rv_30_lag1", "price", "Lagged 30-minute realized volatility"),
    FeatureDefinition("rv_30_change", "price", "1-step change in 30-minute realized volatility"),
    FeatureDefinition("ma_gap_5", "price", "Gap versus 5-period moving average"),
    FeatureDefinition("ma_gap_15", "price", "Gap versus 15-period moving average"),
    FeatureDefinition("ema_gap_12", "price", "Gap versus 12-period EMA"),
    FeatureDefinition("ma_15_slope", "price", "Slope of the 15-period moving average"),
    FeatureDefinition("bb_pos_20", "price", "Position inside 20-period Bollinger band"),
    FeatureDefinition("atr_14", "price", "14-period average true range"),
    FeatureDefinition("gk_vol_30", "price", "30-minute Garman-Klass volatility"),
    FeatureDefinition("rs_vol_30", "price", "30-minute Rogers-Satchell volatility"),
    FeatureDefinition("rr_30", "price", "30-minute realized range"),
    FeatureDefinition("macd_hist", "price", "MACD histogram"),
    FeatureDefinition("rsi_14", "price", "14-period RSI"),
    FeatureDefinition("rsi_14_lag1", "price", "Lagged 14-period RSI"),
    FeatureDefinition("median_gap_20", "price", "Gap versus 20-period rolling median"),
    FeatureDefinition("price_pos_iqr_20", "price", "Position inside 20-period interquartile range"),
    FeatureDefinition("adx_14", "price", "14-period ADX trend strength"),
    FeatureDefinition("regime_trend", "price", "Trend regime flag"),
    FeatureDefinition("hour_sin", "calendar", "Hour-of-day sine encoding"),
    FeatureDefinition("hour_cos", "calendar", "Hour-of-day cosine encoding"),
    FeatureDefinition("dow_sin", "calendar", "Day-of-week sine encoding"),
    FeatureDefinition("dow_cos", "calendar", "Day-of-week cosine encoding"),
    FeatureDefinition("bias_60", "price", "Gap versus 60-period moving average"),
    FeatureDefinition("z_ret_30m", "price", "Z-scored 30-minute return"),
    FeatureDefinition("z_ret_60m", "price", "Z-scored 60-minute return"),
    FeatureDefinition("ret_1m_lag1", "price", "Lagged 1-minute return"),
    FeatureDefinition("ret_1m_lag2", "price", "Second lag of 1-minute return"),
    FeatureDefinition("ret_5m_lag1", "price", "Lagged 5-minute return"),
    FeatureDefinition("ret_15m_lag1", "price", "Lagged 15-minute return"),
    FeatureDefinition("delta_rsi", "price", "1-step RSI delta"),
    FeatureDefinition("delta_rsi_5", "price", "5-step RSI delta"),
    FeatureDefinition("macd_z", "price", "Z-scored MACD histogram"),
    FeatureDefinition("macd_extreme", "price", "Whether MACD z-score is extreme"),
    FeatureDefinition("rsi_divergence", "price", "Short-horizon RSI divergence"),
    FeatureDefinition("momentum_agree", "price", "Whether short-horizon returns agree in sign"),
    FeatureDefinition("regime_high_vol", "price", "High-volatility regime flag"),
    FeatureDefinition("taker_buy_ratio", "volume", "Taker-buy quote share"),
    FeatureDefinition("taker_buy_ratio_z", "volume", "Z-scored taker-buy ratio"),
    FeatureDefinition("taker_buy_ratio_lag1", "volume", "Lagged taker-buy ratio"),
    FeatureDefinition("taker_buy_ratio_change", "volume", "1-step change in taker-buy ratio"),
    FeatureDefinition("trade_intensity", "volume", "Change in trade count intensity"),
    FeatureDefinition("volume_z", "volume", "Z-scored base volume"),
    FeatureDefinition("volume_z_3", "volume", "Short-horizon rolling volume z-score"),
    FeatureDefinition("vwap_gap_20", "volume", "Gap versus 20-period VWAP"),
    FeatureDefinition("vwap_gap_60", "volume", "Gap versus 60-period VWAP"),
    FeatureDefinition("obv_z", "volume", "Z-scored on-balance volume"),
    FeatureDefinition("donch_pos_20", "volume", "Position inside 20-period Donchian channel"),
    FeatureDefinition("vol_ratio_5_60", "volume", "5/60 rolling volume ratio"),
    FeatureDefinition("vol_price_corr_15", "volume", "15-period rolling price/volume correlation"),
    FeatureDefinition("ret_from_cycle_open", "cycle", "Return from cycle open"),
    FeatureDefinition("pullback_from_cycle_high", "cycle", "Pullback from cycle high"),
    FeatureDefinition("rebound_from_cycle_low", "cycle", "Rebound from cycle low"),
    FeatureDefinition("cycle_range_pos", "cycle", "Position inside current cycle range"),
    FeatureDefinition("cycle_range_vs_rv", "cycle", "Current cycle high-low range normalized by rv_30"),
    FeatureDefinition("move_z", "cycle", "Cycle move normalized by rv_30"),
    FeatureDefinition("first_half_ret", "cycle", "Return from cycle open to the first-half anchor"),
    FeatureDefinition("second_half_ret_proxy", "cycle", "Return from the first-half anchor to now"),
    FeatureDefinition("ret_from_strike", "strike", "Return vs oracle strike"),
    FeatureDefinition("basis_bp", "strike", "Binance/oracle basis in bps"),
    FeatureDefinition("has_oracle_strike", "strike", "Whether strike price was available"),
    FeatureDefinition("has_cl_strike", "strike", "Legacy-compatible alias for strike availability"),
    FeatureDefinition("move_z_strike", "strike", "Strike return normalized by rv_30"),
    FeatureDefinition("strike_abs_z", "strike", "Absolute strike distance normalized by rv_30"),
    FeatureDefinition("strike_flip_count_cycle", "strike", "Count of strike-side flips within the current cycle"),
    FeatureDefinition("q_bs_up_strike", "strike", "Raw BS-style strike probability proxy"),
    FeatureDefinition("q_bs_up_strike_centered", "strike", "Centered BS-style strike probability proxy"),
    FeatureDefinition("btc_ret_5m", "cross_asset", "BTC 5-minute return"),
    FeatureDefinition("btc_vol_30m", "cross_asset", "BTC 30-minute volatility"),
    FeatureDefinition("rel_strength_15m", "cross_asset", "Asset minus BTC 15-minute return"),
)

_FEATURE_GROUP_BY_NAME = {
    str(feature.name): str(feature.group)
    for feature in _FEATURES
}

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
    "bs_q_replace_direction": (
        "ret_1m",
        "ret_3m",
        "ret_5m",
        "ret_15m",
        "ret_30m",
        "ret_60m",
        "ma_gap_5",
        "ma_gap_15",
        "ema_gap_12",
        "ma_15_slope",
        "bb_pos_20",
        "rv_30",
        "rv_30_lag1",
        "atr_14",
        "gk_vol_30",
        "rs_vol_30",
        "rr_30",
        "macd_hist",
        "rsi_14",
        "rsi_14_lag1",
        "median_gap_20",
        "price_pos_iqr_20",
        "vwap_gap_60",
        "adx_14",
        "regime_trend",
        "regime_high_vol",
        "taker_buy_ratio",
        "taker_buy_ratio_z",
        "taker_buy_ratio_lag1",
        "trade_intensity",
        "volume_z",
        "obv_z",
        "vwap_gap_20",
        "donch_pos_20",
        "hour_sin",
        "hour_cos",
        "dow_sin",
        "dow_cos",
        "bias_60",
        "vol_price_corr_15",
        "volume_z_3",
        "vol_ratio_5_60",
        "z_ret_30m",
        "z_ret_60m",
        "ret_from_cycle_open",
        "pullback_from_cycle_high",
        "rebound_from_cycle_low",
        "cycle_range_pos",
        "first_half_ret",
        "second_half_ret_proxy",
        "ret_1m_lag1",
        "ret_1m_lag2",
        "ret_5m_lag1",
        "ret_15m_lag1",
        "delta_rsi",
        "delta_rsi_5",
        "macd_z",
        "macd_extreme",
        "rsi_divergence",
        "momentum_agree",
        "ret_from_strike",
        "basis_bp",
        "has_cl_strike",
        "q_bs_up_strike",
    ),
    "baseline_trim30_v1": (
        "q_bs_up_strike",
        "ret_from_strike",
        "basis_bp",
        "ret_from_cycle_open",
        "first_half_ret",
        "second_half_ret_proxy",
        "cycle_range_pos",
        "pullback_from_cycle_high",
        "rebound_from_cycle_low",
        "rv_30",
        "rv_30_lag1",
        "gk_vol_30",
        "regime_high_vol",
        "adx_14",
        "bb_pos_20",
        "ema_gap_12",
        "bias_60",
        "vwap_gap_60",
        "price_pos_iqr_20",
        "macd_z",
        "delta_rsi",
        "rsi_14",
        "momentum_agree",
        "ret_3m",
        "ret_15m",
        "volume_z",
        "obv_z",
        "vol_ratio_5_60",
        "taker_buy_ratio_z",
        "trade_intensity",
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

_CUSTOM_FEATURE_SET_FILE_NAME = "custom_feature_sets.json"


def feature_group(name: str) -> str | None:
    return _FEATURE_GROUP_BY_NAME.get(str(name))


def feature_registry() -> dict[str, FeatureDefinition]:
    return {feature.name: feature for feature in _FEATURES}


def feature_set_columns(feature_set: str, *, root: Path | str | None = None) -> tuple[str, ...]:
    key = str(feature_set).strip().lower()
    try:
        return _FEATURE_SET_COLUMNS[key]
    except KeyError as exc:
        custom_sets = _custom_feature_sets(root=root)
        if key in custom_sets:
            return custom_sets[key]["columns"]
        available = sorted(set(_FEATURE_SET_COLUMNS) | set(custom_sets))
        raise ValueError(
            f"Unsupported feature_set {feature_set!r}. Expected one of: {', '.join(available)}"
        ) from exc


def feature_schema(feature_set: str, *, root: Path | str | None = None) -> list[dict[str, str]]:
    registry = feature_registry()
    return [registry[name].to_dict() for name in feature_set_columns(feature_set, root=root) if name in registry]


def feature_set_drop_columns(feature_set: str, *, root: Path | str | None = None) -> tuple[str, ...]:
    key = str(feature_set).strip().lower()
    feature_set_columns(key, root=root)
    if key in _FEATURE_SET_DROP_COLUMNS:
        return _FEATURE_SET_DROP_COLUMNS[key]
    custom_sets = _custom_feature_sets(root=root)
    if key in custom_sets:
        return custom_sets[key]["drop_columns"]
    return ()


def custom_feature_sets_path(*, root: Path | str | None = None) -> Path:
    base_root = Path(root) if root is not None else Path(rewrite_root())
    return base_root / "research" / "experiments" / _CUSTOM_FEATURE_SET_FILE_NAME


def _custom_feature_sets(*, root: Path | str | None = None) -> dict[str, dict[str, tuple[str, ...]]]:
    path = custom_feature_sets_path(root=root)
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise TypeError(f"Expected custom feature set registry mapping at {path}, got: {payload!r}")
    known_features = set(feature_registry())
    out: dict[str, dict[str, tuple[str, ...]]] = {}
    for raw_name, raw_item in payload.items():
        name = slug_token(str(raw_name), default="")
        if not name:
            continue
        if isinstance(raw_item, Mapping):
            columns_raw = raw_item.get("columns") or raw_item.get("feature_columns") or ()
            drop_columns_raw = raw_item.get("drop_columns") or ()
        else:
            columns_raw = raw_item
            drop_columns_raw = ()
        columns = _normalize_feature_column_list(
            columns_raw,
            known_features=known_features,
            context=f"{path}:{raw_name}:columns",
        )
        drop_columns = _normalize_feature_column_list(
            drop_columns_raw,
            known_features=known_features,
            context=f"{path}:{raw_name}:drop_columns",
        )
        if not columns:
            raise ValueError(f"Custom feature set {raw_name!r} at {path} must define at least one feature column")
        out[name] = {
            "columns": columns,
            "drop_columns": drop_columns,
        }
    return out


def _normalize_feature_column_list(
    raw: object,
    *,
    known_features: set[str],
    context: str,
) -> tuple[str, ...]:
    if raw is None or raw == "":
        return ()
    if not isinstance(raw, (list, tuple)):
        raise TypeError(f"Expected a list of feature columns at {context}, got: {raw!r}")
    out: list[str] = []
    for item in raw:
        name = str(item).strip()
        if not name:
            continue
        if name not in known_features:
            raise ValueError(f"Unknown feature column {name!r} referenced at {context}")
        if name not in out:
            out.append(name)
    return tuple(out)
