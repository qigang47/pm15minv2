from __future__ import annotations

import numpy as np
import pandas as pd

from pm15min.research.features.base import rolling_zscore


def append_volume_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    quote_volume = pd.to_numeric(out["quote_asset_volume"], errors="coerce")
    taker_quote = pd.to_numeric(out["taker_buy_quote_volume"], errors="coerce")
    volume = pd.to_numeric(out["volume"], errors="coerce")
    close = pd.to_numeric(out["close"], errors="coerce")
    high = pd.to_numeric(out["high"], errors="coerce")
    low = pd.to_numeric(out["low"], errors="coerce")
    trades = pd.to_numeric(out.get("number_of_trades"), errors="coerce")

    out["taker_buy_ratio"] = (taker_quote / quote_volume.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
    out["taker_buy_ratio_z"] = rolling_zscore(out["taker_buy_ratio"], 100)
    out["taker_buy_ratio_lag1"] = out["taker_buy_ratio"].shift(1)
    out["trade_intensity"] = trades.pct_change().replace([np.inf, -np.inf], np.nan)
    out["volume_z"] = rolling_zscore(volume, 100)
    out["volume_z_3"] = (volume - volume.rolling(30).mean()) / volume.rolling(30).std(ddof=0).replace(0.0, np.nan)
    out["vol_ratio_5_60"] = close.pct_change().rolling(5).std(ddof=0) / close.pct_change().rolling(60).std(ddof=0).replace(0.0, np.nan)
    out["vol_price_corr_15"] = close.pct_change().rolling(15).corr(volume.pct_change())
    vwap_20 = (close * volume).rolling(20).sum() / volume.rolling(20).sum().replace(0.0, np.nan)
    out["vwap_gap_20"] = close / vwap_20.replace(0.0, np.nan) - 1.0
    vwap_60 = (close * volume).rolling(60).sum() / volume.rolling(60).sum().replace(0.0, np.nan)
    out["vwap_gap_60"] = close / vwap_60.replace(0.0, np.nan) - 1.0
    obv = (np.sign(close.diff()).fillna(0.0) * volume).cumsum()
    out["obv_z"] = rolling_zscore(obv, 200)
    channel_high = high.rolling(20).max()
    channel_low = low.rolling(20).min()
    out["donch_pos_20"] = (close - ((channel_high + channel_low) / 2.0)) / (channel_high - channel_low).replace(0.0, np.nan)
    return out
