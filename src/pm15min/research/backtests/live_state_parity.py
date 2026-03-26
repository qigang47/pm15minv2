from __future__ import annotations

from collections.abc import Callable
from collections import Counter
from dataclasses import dataclass
import math
from typing import Any

import pandas as pd

from pm15min.live.liquidity.policy import apply_temporal_filter
from pm15min.live.profiles.spec import LiveProfileSpec
from pm15min.live.profiles import resolve_live_profile_spec
from pm15min.research.backtests.regime_parity import (
    build_backtest_regime_liquidity_proxy,
    build_backtest_regime_state,
)


LIVE_STATE_PARITY_COLUMNS = [
    "liquidity_status",
    "liquidity_blocked",
    "liquidity_degraded",
    "liquidity_primary_reason",
    "liquidity_reason_codes",
    "liquidity_metrics",
    "liquidity_checked_at",
    "regime_status",
    "regime_reason",
    "regime_state",
    "regime_target_state",
    "regime_pressure",
    "regime_primary_reason",
    "regime_reason_codes",
    "regime_min_liquidity_ratio",
    "regime_soft_fail_count",
    "regime_hard_fail_count",
    "regime_pending_target",
    "regime_pending_count",
    "regime_guard_hints",
    "regime_checked_at",
]

LIVE_STATE_HEARTBEAT_INTERVAL_ROWS = 1_000


@dataclass(frozen=True)
class LiveStateParitySummary:
    evaluated_rows: int
    liquidity_proxy_enabled: bool
    liquidity_proxy_mode: str
    liquidity_available_rows: int
    liquidity_degraded_rows: int
    liquidity_blocked_rows: int
    regime_enabled: bool
    regime_caution_rows: int
    regime_defense_rows: int
    liquidity_status_counts: dict[str, int]
    liquidity_reason_counts: dict[str, int]
    regime_state_counts: dict[str, int]
    regime_pressure_counts: dict[str, int]
    regime_reason_counts: dict[str, int]

    def to_dict(self) -> dict[str, object]:
        return {
            "evaluated_rows": int(self.evaluated_rows),
            "liquidity_proxy_enabled": bool(self.liquidity_proxy_enabled),
            "liquidity_proxy_mode": str(self.liquidity_proxy_mode),
            "liquidity_available_rows": int(self.liquidity_available_rows),
            "liquidity_missing_rows": int(max(0, self.evaluated_rows - self.liquidity_available_rows)),
            "liquidity_degraded_rows": int(self.liquidity_degraded_rows),
            "liquidity_blocked_rows": int(self.liquidity_blocked_rows),
            "regime_enabled": bool(self.regime_enabled),
            "regime_caution_rows": int(self.regime_caution_rows),
            "regime_defense_rows": int(self.regime_defense_rows),
            "liquidity_status_counts": dict(self.liquidity_status_counts),
            "liquidity_reason_counts": dict(self.liquidity_reason_counts),
            "regime_state_counts": dict(self.regime_state_counts),
            "regime_pressure_counts": dict(self.regime_pressure_counts),
            "regime_reason_counts": dict(self.regime_reason_counts),
        }


def attach_live_state_parity(
    *,
    market: str,
    profile: str | LiveProfileSpec,
    replay: pd.DataFrame,
    raw_klines: pd.DataFrame,
    liquidity_proxy_mode: str = "spot_kline_mirror",
    heartbeat: Callable[[str], None] | None = None,
) -> tuple[pd.DataFrame, LiveStateParitySummary]:
    spec = _resolve_profile_spec(profile)
    proxy_mode = _normalize_proxy_mode(liquidity_proxy_mode)
    if replay.empty:
        frame = replay.copy()
        for column in LIVE_STATE_PARITY_COLUMNS:
            if column not in frame.columns:
                frame[column] = pd.Series(dtype="object")
        return frame, summarize_live_state_parity(
            frame,
            liquidity_proxy_mode=proxy_mode,
            regime_enabled=bool(spec.regime_controller_enabled),
        )

    proxy = build_backtest_regime_liquidity_proxy(
        market=market,
        profile=spec,
        raw_klines=raw_klines,
        mode=proxy_mode,
    )
    close_series = _prepare_close_series(raw_klines)
    frame = replay.copy()
    frame["_live_state_order"] = pd.RangeIndex(len(frame))
    frame["_live_state_ts"] = _resolve_row_timestamps(frame)
    ordered = frame.sort_values(["_live_state_ts", "_live_state_order"], na_position="last")

    previous_liquidity_state: dict[str, Any] | None = None
    previous_regime_state: dict[str, Any] | None = None
    rows: list[dict[str, object]] = []
    total_rows = len(ordered)
    if heartbeat is not None:
        heartbeat(f"Attaching live state parity: 0/{total_rows:,} rows")

    for row_index, row in enumerate(ordered.itertuples(index=False, name="LiveStateParityRow"), start=1):
        checked_at = _checked_at_from_row(row)
        liquidity_state = _build_liquidity_state_payload(
            spec=spec,
            raw_status=None if checked_at is None else proxy.get_status(checked_at),
            previous_payload=previous_liquidity_state,
        )
        if liquidity_state is not None:
            previous_liquidity_state = liquidity_state

        features = _single_row_features(row, checked_at=checked_at, close_series=close_series)
        regime_state = build_backtest_regime_state(
            market=market,
            profile=spec,
            features=features,
            liquidity_state=liquidity_state,
            previous_state=previous_regime_state,
            now=checked_at,
        )
        if bool(regime_state.get("enabled", False)):
            previous_regime_state = regime_state
        rows.append(_flatten_state_row(liquidity_state=liquidity_state, regime_state=regime_state))
        if heartbeat is not None and (
            row_index == total_rows
            or row_index % LIVE_STATE_HEARTBEAT_INTERVAL_ROWS == 0
        ):
            heartbeat(f"Attaching live state parity: {row_index:,}/{total_rows:,} rows")

    attached = pd.DataFrame(rows, index=ordered.index)
    attached = attached.reindex(columns=LIVE_STATE_PARITY_COLUMNS)
    out = pd.concat([frame.drop(columns=["_live_state_order", "_live_state_ts"]), attached], axis=1)
    return out, summarize_live_state_parity(
        out,
        liquidity_proxy_mode=proxy_mode,
        regime_enabled=bool(spec.regime_controller_enabled),
    )


def summarize_live_state_parity(
    frame: pd.DataFrame,
    *,
    liquidity_proxy_mode: str = "off",
    regime_enabled: bool = False,
) -> LiveStateParitySummary:
    proxy_mode = _normalize_proxy_mode(liquidity_proxy_mode)
    if frame.empty:
        return LiveStateParitySummary(
            evaluated_rows=0,
            liquidity_proxy_enabled=_proxy_enabled(proxy_mode),
            liquidity_proxy_mode=proxy_mode,
            liquidity_available_rows=0,
            liquidity_degraded_rows=0,
            liquidity_blocked_rows=0,
            regime_enabled=bool(regime_enabled),
            regime_caution_rows=0,
            regime_defense_rows=0,
            liquidity_status_counts={},
            liquidity_reason_counts={},
            regime_state_counts={},
            regime_pressure_counts={},
            regime_reason_counts={},
        )

    liquidity_statuses = _string_series(frame, "liquidity_status")
    regime_states = _string_series(frame, "regime_state").replace("", "NORMAL")
    regime_pressures = _string_series(frame, "regime_pressure").replace("", "neutral")
    return LiveStateParitySummary(
        evaluated_rows=int(len(frame)),
        liquidity_proxy_enabled=_proxy_enabled(proxy_mode),
        liquidity_proxy_mode=proxy_mode,
        liquidity_available_rows=int(liquidity_statuses.ne("").sum()),
        liquidity_degraded_rows=int(_bool_series(frame, "liquidity_degraded").sum()),
        liquidity_blocked_rows=int(_bool_series(frame, "liquidity_blocked").sum()),
        regime_enabled=bool(regime_enabled),
        regime_caution_rows=int(regime_states.eq("CAUTION").sum()),
        regime_defense_rows=int(regime_states.eq("DEFENSE").sum()),
        liquidity_status_counts=_value_counts(liquidity_statuses),
        liquidity_reason_counts=_explode_reason_counts(frame.get("liquidity_reason_codes")),
        regime_state_counts=_value_counts(regime_states),
        regime_pressure_counts=_value_counts(regime_pressures),
        regime_reason_counts=_explode_reason_counts(frame.get("regime_reason_codes")),
    )


def _resolve_profile_spec(profile: str | LiveProfileSpec) -> LiveProfileSpec:
    return profile if isinstance(profile, LiveProfileSpec) else resolve_live_profile_spec(profile)


def _resolve_row_timestamps(frame: pd.DataFrame) -> pd.Series:
    out = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
    for column in ("decision_ts", "open_time", "cycle_start_ts"):
        if column not in frame.columns:
            continue
        series = pd.to_datetime(frame[column], utc=True, errors="coerce")
        out = out.where(out.notna(), series)
    return out


def _checked_at_from_row(row: object) -> pd.Timestamp | None:
    for column in ("decision_ts", "open_time", "cycle_start_ts"):
        value = pd.to_datetime(_row_value(row, column), utc=True, errors="coerce")
        if value is not None and not pd.isna(value):
            return pd.Timestamp(value)
    return None


def _build_liquidity_state_payload(
    *,
    spec: LiveProfileSpec,
    raw_status,
    previous_payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if raw_status is None:
        return None

    raw_result = {
        "ok": not bool(raw_status.blocked or raw_status.degraded),
        "blocked": bool(raw_status.blocked),
        "reason_codes": list(raw_status.reason_codes),
        "metrics": dict(raw_status.metrics),
        "error": None,
    }
    filtered = apply_temporal_filter(
        raw_result=raw_result,
        previous_payload=previous_payload,
        min_failed_checks=int(spec.liquidity_guard_min_failed_checks),
        min_recovered_checks=int(spec.liquidity_guard_min_recovered_checks),
        block_on_degrade=bool(spec.liquidity_guard_block),
    )
    reason_codes = [str(code) for code in filtered.get("reason_codes") or [] if str(code)]
    degraded = bool(raw_status.degraded) or any(code in {"filtered_pending", "recovering_pending"} for code in reason_codes)
    return {
        "status": _liquidity_status(
            raw_blocked=bool(raw_status.blocked),
            filtered_blocked=bool(filtered.get("blocked", False)),
            degraded=degraded,
            reason_codes=reason_codes,
        ),
        "reason": reason_codes[0] if reason_codes else None,
        "blocked": bool(filtered.get("blocked", False)),
        "degraded": bool(degraded),
        "reason_codes": reason_codes,
        "metrics": dict(filtered.get("metrics") or {}),
        "error": filtered.get("error"),
        "checked_at": raw_status.checked_at.isoformat(),
        "temporal_state": dict(filtered.get("temporal_state") or {}),
    }


def _liquidity_status(
    *,
    raw_blocked: bool,
    filtered_blocked: bool,
    degraded: bool,
    reason_codes: list[str],
) -> str:
    if raw_blocked or filtered_blocked:
        return "blocked"
    if "recovering_pending" in reason_codes:
        return "recovering_pending"
    if "filtered_pending" in reason_codes:
        return "filtered_pending"
    if degraded:
        return "degraded"
    return "ok"


def _single_row_features(
    row: object,
    *,
    checked_at: pd.Timestamp | None,
    close_series: pd.Series | None,
) -> pd.DataFrame:
    payload = {
        "decision_ts": checked_at,
        "ret_15m": _float_or_none(_row_value(row, "ret_15m")),
        "ret_30m": _float_or_none(_row_value(row, "ret_30m")),
    }
    if checked_at is not None:
        payload["decision_ts"] = checked_at
    if _float_or_none(payload.get("ret_15m")) is None:
        payload["ret_15m"] = _log_return_over_minutes(close_series, checked_at, 15)
    if _float_or_none(payload.get("ret_30m")) is None:
        payload["ret_30m"] = _log_return_over_minutes(close_series, checked_at, 30)
    return pd.DataFrame([payload])


def _log_return_over_minutes(
    close_series: pd.Series | None,
    checked_at: pd.Timestamp | None,
    minutes: int,
) -> float | None:
    if checked_at is None or minutes <= 0 or close_series is None or close_series.empty:
        return None

    ts = pd.Timestamp(checked_at).floor("min")
    price_now = _price_at(close_series, ts)
    price_prev = _price_at(close_series, ts - pd.Timedelta(minutes=minutes))
    if price_now is None or price_prev is None or price_now <= 0.0 or price_prev <= 0.0:
        return None
    return float(math.log(price_now / price_prev))


def _prepare_close_series(raw_klines: pd.DataFrame) -> pd.Series | None:
    if raw_klines is None or raw_klines.empty:
        return None
    if "open_time" not in raw_klines.columns or "close" not in raw_klines.columns:
        return None
    frame = raw_klines.loc[:, ["open_time", "close"]].copy()
    frame["open_time"] = pd.to_datetime(frame["open_time"], utc=True, errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["open_time", "close"]).drop_duplicates(subset=["open_time"], keep="last")
    if frame.empty:
        return None
    return frame.set_index("open_time")["close"].sort_index()


def _price_at(series: pd.Series, ts: pd.Timestamp) -> float | None:
    try:
        value = series.loc[ts]
    except KeyError:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _flatten_state_row(
    *,
    liquidity_state: dict[str, Any] | None,
    regime_state: dict[str, Any],
) -> dict[str, object]:
    liquidity_reason_codes = [] if liquidity_state is None else list(liquidity_state.get("reason_codes") or [])
    regime_reason_codes = list(regime_state.get("reason_codes") or [])
    return {
        "liquidity_status": "" if liquidity_state is None else str(liquidity_state.get("status") or ""),
        "liquidity_blocked": False if liquidity_state is None else bool(liquidity_state.get("blocked", False)),
        "liquidity_degraded": False if liquidity_state is None else bool(liquidity_state.get("degraded", False)),
        "liquidity_primary_reason": "" if not liquidity_reason_codes else str(liquidity_reason_codes[0]),
        "liquidity_reason_codes": liquidity_reason_codes,
        "liquidity_metrics": {} if liquidity_state is None else dict(liquidity_state.get("metrics") or {}),
        "liquidity_checked_at": None if liquidity_state is None else liquidity_state.get("checked_at"),
        "regime_status": str(regime_state.get("status") or ""),
        "regime_reason": str(regime_state.get("reason") or ""),
        "regime_state": str(regime_state.get("state") or "NORMAL"),
        "regime_target_state": str(regime_state.get("target_state") or "NORMAL"),
        "regime_pressure": str(regime_state.get("pressure") or "neutral"),
        "regime_primary_reason": "" if not regime_reason_codes else str(regime_reason_codes[0]),
        "regime_reason_codes": regime_reason_codes,
        "regime_min_liquidity_ratio": _float_or_none(regime_state.get("min_liquidity_ratio")),
        "regime_soft_fail_count": int(regime_state.get("soft_fail_count") or 0),
        "regime_hard_fail_count": int(regime_state.get("hard_fail_count") or 0),
        "regime_pending_target": str(regime_state.get("pending_target") or "NORMAL"),
        "regime_pending_count": int(regime_state.get("pending_count") or 0),
        "regime_guard_hints": dict(regime_state.get("guard_hints") or {}),
        "regime_checked_at": regime_state.get("checked_at"),
    }


def _value_counts(series: pd.Series) -> dict[str, int]:
    clean = series.astype("string").fillna("").astype(str)
    clean = clean[clean != ""]
    counts = clean.value_counts().sort_index()
    return {str(index): int(value) for index, value in counts.items()}


def _explode_reason_counts(series: pd.Series | None) -> dict[str, int]:
    if series is None or len(series) == 0:
        return {}
    counts: Counter[str] = Counter()
    for value in series.tolist():
        if isinstance(value, str):
            tokens = [item.strip() for item in value.split(",") if item.strip()]
        elif isinstance(value, (list, tuple)):
            tokens = [str(item).strip() for item in value if str(item).strip()]
        else:
            tokens = []
        for token in tokens:
            if token not in {"", "ok"}:
                counts[token] += 1
    return dict(sorted(counts.items()))


def _string_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series("", index=frame.index, dtype="string")
    return values.astype("string").fillna("").astype(str)


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series(False, index=frame.index, dtype="boolean")
    return values.astype("boolean").fillna(False).astype(bool)


def _float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out


def _normalize_proxy_mode(value: object) -> str:
    token = str(value or "off").strip().lower()
    return token or "off"


def _proxy_enabled(mode: str) -> bool:
    return str(mode or "off").strip().lower() not in {"", "off", "none", "false", "0"}


def _row_value(row: object, key: str, default: object = None) -> object:
    if isinstance(row, pd.Series):
        return row.get(key, default)
    return getattr(row, key, default)
