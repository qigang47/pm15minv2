from __future__ import annotations

import numpy as np
import pandas as pd

from pm15min.research.features.base import (
    compute_log_returns,
    decision_reference_ts,
    ema,
    realized_volatility,
    relative_strength_index,
    rolling_zscore,
)


def _garman_klass_vol(open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, window: int = 30) -> pd.Series:
    log_hl = np.log(high / low)
    log_co = np.log(close / open_)
    var = 0.5 * (log_hl**2) - (2.0 * np.log(2.0) - 1.0) * (log_co**2)
    var = var.clip(lower=0.0)
    return var.rolling(int(window)).mean().pow(0.5)


def _rogers_satchell_vol(open_: pd.Series, high: pd.Series, low: pd.Series, close: pd.Series, window: int = 30) -> pd.Series:
    term1 = np.log(high / close) * np.log(high / open_)
    term2 = np.log(low / close) * np.log(low / open_)
    rs = (term1 + term2).clip(lower=0.0)
    return rs.rolling(int(window)).mean().pow(0.5)


def _realized_range(high: pd.Series, low: pd.Series, window: int = 30) -> pd.Series:
    rr = np.log(high / low).abs()
    return rr.rolling(int(window)).mean()


def append_price_features(
    frame: pd.DataFrame,
    *,
    requested_columns: set[str] | None = None,
) -> pd.DataFrame:
    out = frame.copy()
    requested = None if requested_columns is None else {str(column) for column in requested_columns}

    def needs(*columns: str) -> bool:
        if requested is None:
            return True
        return any(str(column) in requested for column in columns)

    close = pd.to_numeric(out["close"], errors="coerce")
    high = pd.to_numeric(out["high"], errors="coerce")
    low = pd.to_numeric(out["low"], errors="coerce")
    returns_needed = {
        1: needs("ret_1m", "ret_1m_lag1", "ret_1m_lag2", "rv_30", "trade_intensity"),
        3: needs("ret_3m", "momentum_agree"),
        5: needs("ret_5m", "ret_5m_lag1"),
        15: needs("ret_15m", "ret_15m_lag1"),
        30: needs("ret_30m", "z_ret_30m"),
        60: needs("ret_60m", "z_ret_60m"),
    }
    if returns_needed[1]:
        out["ret_1m"] = compute_log_returns(close, 1)
    if returns_needed[3]:
        out["ret_3m"] = compute_log_returns(close, 3)
    if returns_needed[5]:
        out["ret_5m"] = compute_log_returns(close, 5)
    if returns_needed[15]:
        out["ret_15m"] = compute_log_returns(close, 15)
    if returns_needed[30]:
        out["ret_30m"] = compute_log_returns(close, 30)
    if returns_needed[60]:
        out["ret_60m"] = compute_log_returns(close, 60)
    if needs("rv_30", "rv_30_lag1", "regime_high_vol"):
        out["rv_30"] = realized_volatility(close, 30)
    if needs("rv_30_lag1"):
        out["rv_30_lag1"] = out["rv_30"].shift(1)

    if needs("ma_gap_5"):
        ma_5 = close.rolling(5).mean()
        out["ma_gap_5"] = close / ma_5.replace(0.0, np.nan) - 1.0
    if needs("ma_gap_15", "ma_15_slope"):
        ma_15 = close.rolling(15).mean()
        if needs("ma_gap_15"):
            out["ma_gap_15"] = close / ma_15.replace(0.0, np.nan) - 1.0
        if needs("ma_15_slope"):
            out["ma_15_slope"] = ma_15.diff()
    if needs("ema_gap_12"):
        out["ema_gap_12"] = close / ema(close, 12).replace(0.0, np.nan) - 1.0
    if needs("bb_pos_20"):
        ma_20 = close.rolling(20).mean()
        std_20 = close.rolling(20).std(ddof=0)
        out["bb_pos_20"] = (close - ma_20) / ((ma_20 + 2.0 * std_20) - (ma_20 - 2.0 * std_20)).replace(0.0, np.nan)
    if needs("price_pos_iqr_20"):
        q25_20 = close.rolling(20).quantile(0.25)
        q75_20 = close.rolling(20).quantile(0.75)
        out["price_pos_iqr_20"] = (close - ((q25_20 + q75_20) / 2.0)) / (q75_20 - q25_20).replace(0.0, np.nan)
    if needs("median_gap_20"):
        median_20 = close.rolling(20).median()
        out["median_gap_20"] = close / median_20.replace(0.0, np.nan) - 1.0
    if needs("bias_60"):
        ma_60 = close.rolling(60).mean()
        out["bias_60"] = close / ma_60.replace(0.0, np.nan) - 1.0
    if needs("z_ret_30m"):
        out["z_ret_30m"] = rolling_zscore(out["ret_30m"], 200)
    if needs("z_ret_60m"):
        out["z_ret_60m"] = rolling_zscore(out["ret_60m"], 200)

    if needs("atr_14", "adx_14", "regime_trend"):
        prev_close = close.shift(1)
        true_range = pd.concat(
            [
                (high - low).abs(),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        out["atr_14"] = true_range.rolling(14).mean()
    if needs("gk_vol_30"):
        out["gk_vol_30"] = _garman_klass_vol(pd.to_numeric(out["open"], errors="coerce"), high, low, close, 30)
    if needs("rs_vol_30"):
        out["rs_vol_30"] = _rogers_satchell_vol(pd.to_numeric(out["open"], errors="coerce"), high, low, close, 30)
    if needs("rr_30"):
        out["rr_30"] = _realized_range(high, low, 30)

    if needs("adx_14", "regime_trend"):
        up_move = high.diff()
        down_move = -low.diff()
        plus_dm = up_move.where((up_move > down_move) & (up_move > 0.0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0.0), 0.0)
        atr_14 = out["atr_14"].replace(0.0, np.nan)
        plus_di = 100.0 * plus_dm.rolling(14).mean() / atr_14
        minus_di = 100.0 * minus_dm.rolling(14).mean() / atr_14
        dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
        out["adx_14"] = dx.rolling(14).mean()
        if needs("regime_trend"):
            out["regime_trend"] = (out["adx_14"] > 25.0).astype(float)

    if needs("rsi_14", "delta_rsi", "delta_rsi_5", "rsi_14_lag1", "rsi_divergence"):
        rsi_14 = relative_strength_index(close, 14)
        if needs("rsi_14"):
            out["rsi_14"] = rsi_14
        if needs("delta_rsi"):
            out["delta_rsi"] = rsi_14.diff(3)
        if needs("delta_rsi_5", "rsi_divergence"):
            rsi_diff_5 = rsi_14.diff(5)
            if needs("delta_rsi_5"):
                out["delta_rsi_5"] = rsi_diff_5
            if needs("rsi_divergence"):
                price_up = (close.diff(5) > 0.0).astype(float)
                rsi_up = (rsi_diff_5 > 0.0).astype(float)
                out["rsi_divergence"] = price_up - rsi_up
        if needs("rsi_14_lag1"):
            out["rsi_14_lag1"] = rsi_14.shift(1)

    if needs("macd_hist", "macd_z", "macd_extreme"):
        macd = ema(close, 12) - ema(close, 26)
        macd_signal = ema(macd, 9)
        macd_hist = macd - macd_signal
        if needs("macd_hist"):
            out["macd_hist"] = macd_hist
        if needs("macd_z", "macd_extreme"):
            out["macd_z"] = (macd_hist - macd_hist.rolling(60).mean()) / macd_hist.rolling(60).std(ddof=0).replace(0.0, np.nan)
            if needs("macd_extreme"):
                out["macd_extreme"] = (out["macd_z"].abs() > 2.0).astype(float)
    if needs("momentum_agree"):
        out["momentum_agree"] = (np.sign(out["ret_3m"]) * np.sign(out["ret_1m"])).fillna(0.0)

    if needs("ret_1m_lag1"):
        out["ret_1m_lag1"] = out["ret_1m"].shift(1)
    if needs("ret_1m_lag2"):
        out["ret_1m_lag2"] = out["ret_1m"].shift(2)
    if needs("ret_5m_lag1"):
        out["ret_5m_lag1"] = out["ret_5m"].shift(1)
    if needs("ret_15m_lag1"):
        out["ret_15m_lag1"] = out["ret_15m"].shift(1)
    if needs("regime_high_vol"):
        out["regime_high_vol"] = (
            out["rv_30"] > out["rv_30"].rolling(200).median()
        ).astype(float)

    if needs("hour_sin", "hour_cos", "dow_sin", "dow_cos"):
        ref_ts = decision_reference_ts(out)
        if needs("hour_sin", "hour_cos"):
            hours = ref_ts.dt.hour.fillna(0.0)
            angle = 2.0 * np.pi * hours / 24.0
            if needs("hour_sin"):
                out["hour_sin"] = np.sin(angle)
            if needs("hour_cos"):
                out["hour_cos"] = np.cos(angle)
        if needs("dow_sin", "dow_cos"):
            dow = ref_ts.dt.dayofweek.fillna(0.0)
            dow_angle = 2.0 * np.pi * dow / 7.0
            if needs("dow_sin"):
                out["dow_sin"] = np.sin(dow_angle)
            if needs("dow_cos"):
                out["dow_cos"] = np.cos(dow_angle)
    return out
