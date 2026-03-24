from __future__ import annotations

import pandas as pd

from pm15min.research.features.base import normalize_feature_frame, prepare_klines
from pm15min.research.features.cross_asset import append_cross_asset_features
from pm15min.research.features.cycle import append_cycle_features
from pm15min.research.features.price import append_price_features
from pm15min.research.features.registry import feature_set_columns
from pm15min.research.features.strike import append_strike_features
from pm15min.research.features.volume import append_volume_features


_META_COLUMNS = [
    "open_time",
    "decision_ts",
    "cycle_start_ts",
    "cycle_end_ts",
    "offset",
    "close",
]


def build_feature_frame(
    raw_klines: pd.DataFrame,
    *,
    feature_set: str,
    oracle_prices: pd.DataFrame,
    btc_klines: pd.DataFrame | None = None,
    cycle: str = "15m",
) -> pd.DataFrame:
    base = prepare_klines(raw_klines)
    frame = append_price_features(base)
    frame = append_volume_features(frame)
    frame = append_cycle_features(frame, cycle=cycle)
    frame["decision_ts"] = pd.to_datetime(frame["open_time"], utc=True, errors="coerce") + pd.Timedelta(minutes=1)
    frame = append_strike_features(frame, oracle_prices=oracle_prices, cycle=cycle)
    frame = append_cross_asset_features(frame, btc_klines=btc_klines)

    requested_columns = set(feature_set_columns(feature_set))
    computed_columns = {
        str(name)
        for name in frame.columns
        if str(name)
        and str(name) not in set(_META_COLUMNS)
        and str(name) not in {"open", "high", "low", "volume", "quote_asset_volume", "taker_buy_quote_volume", "number_of_trades"}
    }
    columns = list(_META_COLUMNS) + sorted(requested_columns | computed_columns)
    out = normalize_feature_frame(frame, columns=columns)
    out = out.sort_values("decision_ts").reset_index(drop=True)
    return out
