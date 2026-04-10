from __future__ import annotations

from typing import Any

import pandas as pd

from pm15min.core.cycle_contracts import resolve_cycle_contract


PRESSURE_NEUTRAL = "neutral"
REGIME_NORMAL = "NORMAL"
REGIME_CAUTION = "CAUTION"
REGIME_DEFENSE = "DEFENSE"

_RANK = {
    REGIME_NORMAL: 0,
    REGIME_CAUTION: 1,
    REGIME_DEFENSE: 2,
}


class RegimeController:
    def __init__(
        self,
        *,
        caution_min_liquidity_ratio: float,
        defense_min_liquidity_ratio: float,
        caution_soft_fail_count: int,
        defense_soft_fail_count: int,
        switch_confirmations: int,
        recover_confirmations: int,
        up_pressure_ret_15m: float,
        up_pressure_ret_30m: float,
        down_pressure_ret_15m: float,
        down_pressure_ret_30m: float,
    ) -> None:
        self.caution_min_liquidity_ratio = max(0.0, float(caution_min_liquidity_ratio))
        self.defense_min_liquidity_ratio = max(0.0, float(defense_min_liquidity_ratio))
        self.caution_soft_fail_count = max(1, int(caution_soft_fail_count))
        self.defense_soft_fail_count = max(self.caution_soft_fail_count, int(defense_soft_fail_count))
        self.switch_confirmations = max(1, int(switch_confirmations))
        self.recover_confirmations = max(1, int(recover_confirmations))
        self.up_pressure_ret_15m = float(up_pressure_ret_15m)
        self.up_pressure_ret_30m = float(up_pressure_ret_30m)
        self.down_pressure_ret_15m = float(down_pressure_ret_15m)
        self.down_pressure_ret_30m = float(down_pressure_ret_30m)
        self._state = REGIME_NORMAL
        self._pending_target = REGIME_NORMAL
        self._pending_count = 0

    def evaluate(
        self,
        *,
        now: pd.Timestamp,
        liquidity_metrics: dict[str, Any] | None,
        liquidity_blocked: bool,
        ret_15m: float | None,
        ret_30m: float | None,
    ) -> dict[str, Any]:
        target_state, reasons, min_ratio, soft_fail_count, hard_fail_count = self._classify_liquidity(
            liquidity_metrics=liquidity_metrics,
            liquidity_blocked=liquidity_blocked,
        )
        self._advance_state(target_state)
        pressure = self._classify_pressure(ret_15m=ret_15m, ret_30m=ret_30m)
        return {
            "state": self._state,
            "target_state": target_state,
            "pressure": pressure,
            "checked_at": now,
            "reason_codes": tuple(reasons),
            "min_liquidity_ratio": float(min_ratio),
            "soft_fail_count": int(soft_fail_count),
            "hard_fail_count": int(hard_fail_count),
            "ret_15m": ret_15m,
            "ret_30m": ret_30m,
            "pending_target": self._pending_target,
            "pending_count": int(self._pending_count),
        }

    def _classify_liquidity(
        self,
        *,
        liquidity_metrics: dict[str, Any] | None,
        liquidity_blocked: bool,
    ) -> tuple[str, list[str], float, int, int]:
        reasons: list[str] = []
        if not liquidity_metrics:
            return REGIME_NORMAL, ["liquidity_metrics_missing"], 1.0, 0, 0
        soft_fail_count = int(liquidity_metrics.get("soft_fail_count", 0) or 0)
        hard_fail_count = int(liquidity_metrics.get("hard_fail_count", 0) or 0)
        min_ratio = self._compute_min_liquidity_ratio(liquidity_metrics)
        if liquidity_blocked:
            reasons.append("liquidity_blocked")
        if hard_fail_count > 0:
            reasons.append("liquidity_hard_fail")
        if soft_fail_count >= self.defense_soft_fail_count:
            reasons.append("liquidity_soft_fail_defense")
        elif soft_fail_count >= self.caution_soft_fail_count:
            reasons.append("liquidity_soft_fail_caution")
        if min_ratio < self.defense_min_liquidity_ratio:
            reasons.append("liquidity_ratio_defense")
        elif min_ratio < self.caution_min_liquidity_ratio:
            reasons.append("liquidity_ratio_caution")
        if (
            liquidity_blocked
            or hard_fail_count > 0
            or soft_fail_count >= self.defense_soft_fail_count
            or min_ratio < self.defense_min_liquidity_ratio
        ):
            return REGIME_DEFENSE, reasons, min_ratio, soft_fail_count, hard_fail_count
        if soft_fail_count >= self.caution_soft_fail_count or min_ratio < self.caution_min_liquidity_ratio:
            return REGIME_CAUTION, reasons, min_ratio, soft_fail_count, hard_fail_count
        return REGIME_NORMAL, (reasons or ["liquidity_ok"]), min_ratio, soft_fail_count, hard_fail_count

    @staticmethod
    def _compute_min_liquidity_ratio(metrics: dict[str, Any]) -> float:
        ratios: list[float] = []
        for key in ("spot_quote_ratio", "perp_quote_ratio", "spot_trades_ratio", "perp_trades_ratio"):
            value = float_or_none(metrics.get(key))
            if value is not None and value >= 0.0:
                ratios.append(value)
        return 1.0 if not ratios else float(min(ratios))

    def _classify_pressure(self, *, ret_15m: float | None, ret_30m: float | None) -> str:
        if ret_15m is None or ret_30m is None:
            return PRESSURE_NEUTRAL
        if ret_15m >= self.up_pressure_ret_15m and ret_30m >= self.up_pressure_ret_30m:
            return "up"
        if ret_15m <= self.down_pressure_ret_15m and ret_30m <= self.down_pressure_ret_30m:
            return "down"
        return PRESSURE_NEUTRAL

    def _advance_state(self, target_state: str) -> None:
        target_rank = _RANK.get(target_state, _RANK[REGIME_NORMAL])
        current_rank = _RANK.get(self._state, _RANK[REGIME_NORMAL])
        if target_state == self._state:
            self._pending_target = target_state
            self._pending_count = 0
            return
        if target_state != self._pending_target:
            self._pending_target = target_state
            self._pending_count = 1
        else:
            self._pending_count += 1
        needed = self.switch_confirmations if target_rank > current_rank else self.recover_confirmations
        if self._pending_count >= needed:
            self._state = target_state
            self._pending_target = target_state
            self._pending_count = 0


def seed_regime_controller(*, controller: RegimeController, previous_payload: dict[str, Any] | None) -> None:
    if not isinstance(previous_payload, dict):
        return
    try:
        controller._state = str(previous_payload.get("state") or REGIME_NORMAL).upper()
        controller._pending_target = str(previous_payload.get("pending_target") or controller._state).upper()
        controller._pending_count = max(0, int(previous_payload.get("pending_count") or 0))
    except Exception:
        controller._state = REGIME_NORMAL
        controller._pending_target = REGIME_NORMAL
        controller._pending_count = 0


def resolve_checked_at(*, features: pd.DataFrame | None, now: pd.Timestamp | None) -> pd.Timestamp:
    if now is not None:
        ts = pd.Timestamp(now)
        return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
    if isinstance(features, pd.DataFrame) and not features.empty and "decision_ts" in features.columns:
        series = pd.to_datetime(features["decision_ts"], utc=True, errors="coerce").dropna()
        if not series.empty:
            return pd.Timestamp(series.max())
    return pd.Timestamp.now(tz="UTC")


def infer_regime_cycle(
    *,
    cycle: str | int | None = None,
    features: pd.DataFrame | None = None,
    offsets: tuple[int, ...] | None = None,
) -> str:
    if cycle is not None:
        return resolve_cycle_contract(cycle).cycle
    if isinstance(features, pd.DataFrame) and "ret_5m" in features.columns:
        return "5m"
    if offsets:
        try:
            if max(int(value) for value in offsets) <= 4:
                return "5m"
        except ValueError:
            pass
    return "15m"


def latest_regime_returns(features: pd.DataFrame | None, *, cycle: str | int) -> tuple[float | None, float | None]:
    if not isinstance(features, pd.DataFrame) or features.empty:
        return None, None
    short_col, long_col = resolve_cycle_contract(cycle).regime_return_columns
    rows = features.copy()
    if "decision_ts" in rows.columns:
        rows = rows.sort_values("decision_ts")
    row = rows.tail(1)
    if row.empty:
        return None, None
    return float_or_none(row.get(short_col).iloc[-1] if short_col in row.columns else None), float_or_none(
        row.get(long_col).iloc[-1] if long_col in row.columns else None
    )


def float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out
