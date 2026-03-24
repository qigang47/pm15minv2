from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import math

import pandas as pd


@dataclass(frozen=True)
class BacktestLiquidityThresholds:
    min_spot_quote_volume_ratio: float = 0.0
    min_perp_quote_volume_ratio: float = 0.0
    min_spot_trades_ratio: float = 0.0
    min_perp_trades_ratio: float = 0.0
    min_spot_quote_volume_window: float = 0.0
    min_perp_quote_volume_window: float = 0.0
    min_spot_trades_window: float = 0.0
    min_perp_trades_window: float = 0.0

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object] | None) -> "BacktestLiquidityThresholds":
        payload = dict(raw or {})
        return cls(
            min_spot_quote_volume_ratio=_float_or_zero(payload.get("min_spot_quote_volume_ratio")),
            min_perp_quote_volume_ratio=_float_or_zero(payload.get("min_perp_quote_volume_ratio")),
            min_spot_trades_ratio=_float_or_zero(payload.get("min_spot_trades_ratio")),
            min_perp_trades_ratio=_float_or_zero(payload.get("min_perp_trades_ratio")),
            min_spot_quote_volume_window=_float_or_zero(payload.get("min_spot_quote_volume_window")),
            min_perp_quote_volume_window=_float_or_zero(payload.get("min_perp_quote_volume_window")),
            min_spot_trades_window=_float_or_zero(payload.get("min_spot_trades_window")),
            min_perp_trades_window=_float_or_zero(payload.get("min_perp_trades_window")),
        )


@dataclass(frozen=True)
class BacktestLiquidityProxyConfig:
    mode: str = "off"
    lookback_minutes: int = 10
    baseline_minutes: int = 180
    soft_fail_min_count: int = 2
    thresholds: BacktestLiquidityThresholds = BacktestLiquidityThresholds()

    def normalized_mode(self) -> str:
        return str(self.mode or "off").strip().lower()

    @property
    def enabled(self) -> bool:
        return self.normalized_mode() not in {"", "off", "none", "false", "0"}


@dataclass(frozen=True)
class BacktestLiquidityStatus:
    checked_at: pd.Timestamp
    blocked: bool
    degraded: bool
    reason_codes: tuple[str, ...]
    metrics: dict[str, float]

    def to_dict(self) -> dict[str, object]:
        return {
            "checked_at": self.checked_at.isoformat(),
            "blocked": bool(self.blocked),
            "degraded": bool(self.degraded),
            "reason_codes": list(self.reason_codes),
            "metrics": dict(self.metrics),
        }


class SpotKlineMirrorLiquidityProxy:
    def __init__(
        self,
        *,
        raw_klines: pd.DataFrame,
        config: BacktestLiquidityProxyConfig,
    ) -> None:
        self.config = config
        self.enabled = bool(config.enabled)
        self.mode = config.normalized_mode()
        self._metrics_df = pd.DataFrame()
        if not self.enabled:
            return
        if self.mode != "spot_kline_mirror":
            raise ValueError(f"Unsupported backtest liquidity proxy mode: {self.mode}")
        self._metrics_df = build_spot_kline_mirror_metrics(
            raw_klines,
            lookback_minutes=int(config.lookback_minutes),
            baseline_minutes=int(config.baseline_minutes),
        )

    def get_status(self, now: pd.Timestamp) -> BacktestLiquidityStatus | None:
        if not self.enabled or self._metrics_df.empty:
            return None
        ts = _normalize_ts(now)
        pos = int(self._metrics_df["open_time"].searchsorted(ts, side="right") - 1)
        if pos < 0:
            return None
        row = self._metrics_df.iloc[pos]
        metrics = _status_metrics(row, self.config)
        reason_codes = _soft_reason_codes(metrics, self.config.thresholds)
        degraded = len(reason_codes) >= int(max(1, self.config.soft_fail_min_count))
        return BacktestLiquidityStatus(
            checked_at=ts,
            blocked=False,
            degraded=bool(degraded),
            reason_codes=tuple(reason_codes) if reason_codes else ("ok",),
            metrics=metrics,
        )


def build_spot_kline_mirror_metrics(
    raw_klines: pd.DataFrame,
    *,
    lookback_minutes: int,
    baseline_minutes: int,
) -> pd.DataFrame:
    if raw_klines is None or raw_klines.empty or "open_time" not in raw_klines.columns:
        return pd.DataFrame()

    lookback = max(1, int(lookback_minutes))
    baseline = max(1, int(baseline_minutes))
    df = raw_klines.copy()
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True, errors="coerce")
    df = df.dropna(subset=["open_time"]).sort_values("open_time").drop_duplicates(subset=["open_time"])
    if df.empty:
        return pd.DataFrame()

    for column in ("quote_asset_volume", "number_of_trades"):
        if column not in df.columns:
            df[column] = 0.0
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)

    quote_recent_avg = df["quote_asset_volume"].rolling(lookback, min_periods=lookback).mean()
    trades_recent_avg = df["number_of_trades"].rolling(lookback, min_periods=lookback).mean()
    quote_recent_sum = df["quote_asset_volume"].rolling(lookback, min_periods=lookback).sum()
    trades_recent_sum = df["number_of_trades"].rolling(lookback, min_periods=lookback).sum()

    quote_baseline_med = df["quote_asset_volume"].shift(lookback).rolling(baseline, min_periods=1).median()
    trades_baseline_med = df["number_of_trades"].shift(lookback).rolling(baseline, min_periods=1).median()

    out = pd.DataFrame(
        {
            "open_time": df["open_time"],
            "spot_quote_ratio": _safe_ratio(quote_recent_avg, quote_baseline_med),
            "spot_trades_ratio": _safe_ratio(trades_recent_avg, trades_baseline_med),
            "spot_quote_window": quote_recent_sum,
            "spot_trades_window": trades_recent_sum,
        }
    )
    out["perp_quote_ratio"] = out["spot_quote_ratio"]
    out["perp_trades_ratio"] = out["spot_trades_ratio"]
    out["perp_quote_window"] = out["spot_quote_window"]
    out["perp_trades_window"] = out["spot_trades_window"]
    return out.replace([math.inf, -math.inf], pd.NA).reset_index(drop=True)


def build_backtest_liquidity_proxy(
    *,
    raw_klines: pd.DataFrame,
    config: BacktestLiquidityProxyConfig,
) -> SpotKlineMirrorLiquidityProxy:
    return SpotKlineMirrorLiquidityProxy(raw_klines=raw_klines, config=config)


def _status_metrics(
    row: pd.Series,
    config: BacktestLiquidityProxyConfig,
) -> dict[str, float]:
    metrics: dict[str, float] = {}
    for name in (
        "spot_quote_ratio",
        "perp_quote_ratio",
        "spot_trades_ratio",
        "perp_trades_ratio",
        "spot_quote_window",
        "perp_quote_window",
        "spot_trades_window",
        "perp_trades_window",
    ):
        value = _float_or_none(row.get(name))
        if value is not None:
            metrics[name] = value
    metrics["min_spot_quote_window"] = float(config.thresholds.min_spot_quote_volume_window)
    metrics["min_perp_quote_window"] = float(config.thresholds.min_perp_quote_volume_window)
    metrics["min_spot_trades_window"] = float(config.thresholds.min_spot_trades_window)
    metrics["min_perp_trades_window"] = float(config.thresholds.min_perp_trades_window)
    reason_codes = _soft_reason_codes(metrics, config.thresholds)
    metrics["soft_fail_count"] = float(len(reason_codes))
    metrics["hard_fail_count"] = 0.0
    metrics["soft_fail_min_count"] = float(max(1, int(config.soft_fail_min_count)))
    return metrics


def _soft_reason_codes(
    metrics: Mapping[str, float],
    thresholds: BacktestLiquidityThresholds,
) -> list[str]:
    reasons: list[str] = []
    if thresholds.min_spot_quote_volume_ratio > 0 and metrics.get("spot_quote_ratio", math.inf) < thresholds.min_spot_quote_volume_ratio:
        reasons.append("spot_quote_ratio")
    if thresholds.min_perp_quote_volume_ratio > 0 and metrics.get("perp_quote_ratio", math.inf) < thresholds.min_perp_quote_volume_ratio:
        reasons.append("perp_quote_ratio")
    if thresholds.min_spot_trades_ratio > 0 and metrics.get("spot_trades_ratio", math.inf) < thresholds.min_spot_trades_ratio:
        reasons.append("spot_trades_ratio")
    if thresholds.min_perp_trades_ratio > 0 and metrics.get("perp_trades_ratio", math.inf) < thresholds.min_perp_trades_ratio:
        reasons.append("perp_trades_ratio")
    if thresholds.min_spot_quote_volume_window > 0 and metrics.get("spot_quote_window", math.inf) < thresholds.min_spot_quote_volume_window:
        reasons.append("spot_quote_window")
    if thresholds.min_perp_quote_volume_window > 0 and metrics.get("perp_quote_window", math.inf) < thresholds.min_perp_quote_volume_window:
        reasons.append("perp_quote_window")
    if thresholds.min_spot_trades_window > 0 and metrics.get("spot_trades_window", math.inf) < thresholds.min_spot_trades_window:
        reasons.append("spot_trades_window")
    if thresholds.min_perp_trades_window > 0 and metrics.get("perp_trades_window", math.inf) < thresholds.min_perp_trades_window:
        reasons.append("perp_trades_window")
    return reasons


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denom = denominator.where(pd.to_numeric(denominator, errors="coerce").gt(0))
    return pd.to_numeric(numerator, errors="coerce") / denom


def _normalize_ts(value: pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def _float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _float_or_zero(value: object) -> float:
    out = _float_or_none(value)
    return 0.0 if out is None else float(out)
