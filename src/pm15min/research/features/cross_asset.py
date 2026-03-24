from __future__ import annotations

import pandas as pd


def append_cross_asset_features(
    frame: pd.DataFrame,
    *,
    btc_klines: pd.DataFrame | None,
) -> pd.DataFrame:
    out = frame.copy()
    if btc_klines is None or btc_klines.empty:
        return out

    btc = btc_klines.copy()
    btc["open_time"] = pd.to_datetime(btc["open_time"], utc=True, errors="coerce")
    btc = btc.dropna(subset=["open_time"]).sort_values("open_time")
    btc = btc.drop_duplicates(subset=["open_time"], keep="last").set_index("open_time")
    btc_close = pd.to_numeric(btc["close"], errors="coerce")

    features = pd.DataFrame(index=btc_close.index)
    features["btc_ret_5m"] = btc_close.pct_change(5)
    features["btc_vol_30m"] = btc_close.pct_change().rolling(30).std(ddof=0)

    target_close = pd.to_numeric(out["close"], errors="coerce")
    target_ts = pd.DatetimeIndex(out["open_time"])
    target_returns = pd.Series(target_close.values, index=target_ts).pct_change(15)
    features["rel_strength_15m"] = target_returns.reindex(features.index) - btc_close.pct_change(15)

    aligned = features.reindex(target_ts, method="ffill")
    aligned.index = out.index
    for column in aligned.columns:
        out[column] = aligned[column].astype(float)
    return out
