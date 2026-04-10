from __future__ import annotations

from dataclasses import dataclass, replace
from collections.abc import Mapping
from typing import Any

import pandas as pd

from pm15min.core.cycle_contracts import resolve_cycle_contract
from pm15min.live.profiles.spec import LiveProfileSpec
from pm15min.live.profiles import resolve_live_profile_spec
from pm15min.live.regime.controller import (
    PRESSURE_NEUTRAL,
    REGIME_NORMAL,
    RegimeController,
    infer_regime_cycle,
    latest_regime_returns,
    resolve_checked_at,
    seed_regime_controller,
)
from pm15min.research._contracts_runs import BacktestParitySpec
from pm15min.research.backtests.liquidity_proxy import (
    BacktestLiquidityProxyConfig,
    BacktestLiquidityStatus,
    BacktestLiquidityThresholds,
    SpotKlineMirrorLiquidityProxy,
    build_backtest_liquidity_proxy,
)


@dataclass(frozen=True)
class RegimeParitySummary:
    evaluated_rows: int
    liquidity_proxy_enabled: bool
    liquidity_proxy_mode: str
    liquidity_available_rows: int
    liquidity_missing_rows: int
    liquidity_degraded_rows: int
    regime_enabled: bool
    regime_state_counts: dict[str, int]
    regime_pressure_counts: dict[str, int]

    def to_dict(self) -> dict[str, object]:
        return {
            "evaluated_rows": int(self.evaluated_rows),
            "liquidity_proxy_enabled": bool(self.liquidity_proxy_enabled),
            "liquidity_proxy_mode": str(self.liquidity_proxy_mode),
            "liquidity_available_rows": int(self.liquidity_available_rows),
            "liquidity_missing_rows": int(self.liquidity_missing_rows),
            "liquidity_degraded_rows": int(self.liquidity_degraded_rows),
            "regime_enabled": bool(self.regime_enabled),
            "regime_state_counts": dict(self.regime_state_counts),
            "regime_pressure_counts": dict(self.regime_pressure_counts),
        }


def resolve_backtest_profile_spec(
    *,
    market: str | None = None,
    profile: str | LiveProfileSpec,
    parity: BacktestParitySpec | Mapping[str, Any] | None = None,
) -> LiveProfileSpec:
    _, spec = _resolve_profile_spec(profile)
    resolved = _coerce_parity(parity)
    market_token = str(market or "").strip().lower() or "default"
    updates: dict[str, object] = {}
    if market_token != "default":
        updates["active_markets"] = tuple(dict.fromkeys((*spec.active_markets, market_token)))
    if resolved.regime_enabled is not None:
        updates["regime_controller_enabled"] = bool(resolved.regime_enabled)
    if resolved.regime_apply_stake_scale is not None:
        updates["regime_apply_stake_scale"] = bool(resolved.regime_apply_stake_scale)
    if resolved.regime_defense_force_with_pressure is not None:
        updates["regime_defense_force_with_pressure"] = bool(resolved.regime_defense_force_with_pressure)
    if resolved.regime_caution_stake_multiplier is not None:
        updates["regime_caution_stake_multiplier"] = float(resolved.regime_caution_stake_multiplier)
    if resolved.regime_defense_stake_multiplier is not None:
        updates["regime_defense_stake_multiplier"] = float(resolved.regime_defense_stake_multiplier)
    if resolved.regime_caution_min_dir_prob_boost is not None:
        updates["regime_caution_min_dir_prob_boost"] = float(resolved.regime_caution_min_dir_prob_boost)
    if resolved.regime_defense_min_dir_prob_boost is not None:
        updates["regime_defense_min_dir_prob_boost"] = float(resolved.regime_defense_min_dir_prob_boost)
    if resolved.regime_defense_max_trades_per_market is not None:
        updates["regime_defense_max_trades_per_market"] = int(resolved.regime_defense_max_trades_per_market)
    if resolved.regime_caution_min_liquidity_ratio is not None:
        updates["regime_caution_min_liquidity_ratio"] = float(resolved.regime_caution_min_liquidity_ratio)
    if resolved.regime_defense_min_liquidity_ratio is not None:
        updates["regime_defense_min_liquidity_ratio"] = float(resolved.regime_defense_min_liquidity_ratio)
    if resolved.regime_caution_soft_fail_count is not None:
        updates["regime_caution_soft_fail_count"] = int(resolved.regime_caution_soft_fail_count)
    if resolved.regime_defense_soft_fail_count is not None:
        updates["regime_defense_soft_fail_count"] = int(resolved.regime_defense_soft_fail_count)
    if resolved.regime_caution_disable_offsets is not None:
        updates["regime_caution_disable_offsets"] = tuple(int(value) for value in resolved.regime_caution_disable_offsets)
    if resolved.regime_defense_disable_offsets is not None:
        updates["regime_defense_disable_offsets"] = tuple(int(value) for value in resolved.regime_defense_disable_offsets)
    if resolved.liquidity_lookback_minutes is not None:
        updates["liquidity_guard_lookback_minutes"] = int(resolved.liquidity_lookback_minutes)
    if resolved.liquidity_baseline_minutes is not None:
        updates["liquidity_guard_baseline_minutes"] = int(resolved.liquidity_baseline_minutes)
    if resolved.liquidity_soft_fail_min_count is not None:
        updates["liquidity_guard_soft_fail_min_count"] = int(resolved.liquidity_soft_fail_min_count)
    if resolved.disable_ret_30m_direction_guard:
        updates["ret_30m_up_floor_by_asset"] = _with_market_threshold(
            getattr(spec, "ret_30m_up_floor_by_asset"),
            market=market_token,
            value=-1.0e9,
        )
        updates["ret_30m_down_ceiling_by_asset"] = _with_market_threshold(
            getattr(spec, "ret_30m_down_ceiling_by_asset"),
            market=market_token,
            value=1.0e9,
        )
    for field_name, attr_name in (
        ("liquidity_min_spot_quote_volume_ratio", "liquidity_min_spot_quote_volume_ratio_by_asset"),
        ("liquidity_min_perp_quote_volume_ratio", "liquidity_min_perp_quote_volume_ratio_by_asset"),
        ("liquidity_min_spot_trades_ratio", "liquidity_min_spot_trades_ratio_by_asset"),
        ("liquidity_min_perp_trades_ratio", "liquidity_min_perp_trades_ratio_by_asset"),
        ("liquidity_min_spot_quote_volume_window", "liquidity_min_spot_quote_volume_window_by_asset"),
        ("liquidity_min_perp_quote_volume_window", "liquidity_min_perp_quote_volume_window_by_asset"),
        ("liquidity_min_spot_trades_window", "liquidity_min_spot_trades_window_by_asset"),
        ("liquidity_min_perp_trades_window", "liquidity_min_perp_trades_window_by_asset"),
    ):
        value = getattr(resolved, field_name)
        if value is None:
            continue
        updates[attr_name] = _with_market_threshold(
            getattr(spec, attr_name),
            market=market_token,
            value=float(value),
        )
    return spec if not updates else replace(spec, **updates)


def build_backtest_regime_liquidity_config(
    *,
    market: str,
    profile: str | LiveProfileSpec,
    mode: str = "spot_kline_mirror",
) -> BacktestLiquidityProxyConfig:
    market_token = str(market or "").strip().lower()
    _, spec = _resolve_profile_spec(profile)
    return BacktestLiquidityProxyConfig(
        mode=str(mode or "off"),
        lookback_minutes=int(spec.liquidity_guard_lookback_minutes),
        baseline_minutes=int(spec.liquidity_guard_baseline_minutes),
        soft_fail_min_count=int(spec.liquidity_guard_soft_fail_min_count),
        thresholds=BacktestLiquidityThresholds(
            min_spot_quote_volume_ratio=spec.liquidity_min_spot_quote_volume_ratio_for(market_token),
            min_perp_quote_volume_ratio=spec.liquidity_min_perp_quote_volume_ratio_for(market_token),
            min_spot_trades_ratio=spec.liquidity_min_spot_trades_ratio_for(market_token),
            min_perp_trades_ratio=spec.liquidity_min_perp_trades_ratio_for(market_token),
            min_spot_quote_volume_window=spec.liquidity_min_spot_quote_volume_window_for(market_token),
            min_perp_quote_volume_window=spec.liquidity_min_perp_quote_volume_window_for(market_token),
            min_spot_trades_window=spec.liquidity_min_spot_trades_window_for(market_token),
            min_perp_trades_window=spec.liquidity_min_perp_trades_window_for(market_token),
        ),
    )


def build_backtest_regime_liquidity_proxy(
    *,
    market: str,
    profile: str | LiveProfileSpec,
    raw_klines: pd.DataFrame,
    mode: str = "spot_kline_mirror",
) -> SpotKlineMirrorLiquidityProxy:
    return build_backtest_liquidity_proxy(
        raw_klines=raw_klines,
        config=build_backtest_regime_liquidity_config(
            market=market,
            profile=profile,
            mode=mode,
        ),
    )


def attach_backtest_regime_parity(
    *,
    market: str,
    profile: str | LiveProfileSpec,
    decisions: pd.DataFrame,
    raw_klines: pd.DataFrame,
    parity: BacktestParitySpec | Mapping[str, Any] | None = None,
) -> tuple[pd.DataFrame, RegimeParitySummary, LiveProfileSpec]:
    resolved_parity = _coerce_parity(parity)
    profile_spec = resolve_backtest_profile_spec(market=market, profile=profile, parity=resolved_parity)
    proxy_mode = _normalize_mode(resolved_parity.liquidity_proxy_mode)
    if decisions.empty:
        return (
            decisions.copy(),
            RegimeParitySummary(
                evaluated_rows=0,
                liquidity_proxy_enabled=bool(proxy_mode != "off"),
                liquidity_proxy_mode=proxy_mode,
                liquidity_available_rows=0,
                liquidity_missing_rows=0,
                liquidity_degraded_rows=0,
                regime_enabled=bool(profile_spec.regime_controller_enabled),
                regime_state_counts={},
                regime_pressure_counts={},
            ),
            profile_spec,
        )

    proxy = build_backtest_regime_liquidity_proxy(
        market=market,
        profile=profile_spec,
        raw_klines=raw_klines,
        mode=proxy_mode,
    )
    cycle = infer_regime_cycle(features=decisions, offsets=profile_spec.offsets)
    out = decisions.copy()
    previous_state: dict[str, Any] | None = None

    liquidity_payloads: list[dict[str, Any] | None] = []
    regime_payloads: list[dict[str, Any]] = []
    for idx in range(len(out)):
        row = out.iloc[idx]
        checked_at = _row_checked_at(row)
        liquidity_status = proxy.get_status(checked_at)
        liquidity_payload = summarize_backtest_liquidity_state(liquidity_status)
        regime_payload = build_backtest_regime_state(
            market=market,
            profile=profile_spec,
            cycle=cycle,
            features=_row_feature_frame(row=row, checked_at=checked_at, cycle=cycle),
            liquidity_state=liquidity_payload,
            previous_state=previous_state,
            now=checked_at,
        )
        previous_state = regime_payload
        liquidity_payloads.append(liquidity_payload)
        regime_payloads.append(regime_payload)

    out["liquidity_status"] = [
        "" if payload is None else str(payload.get("status") or "")
        for payload in liquidity_payloads
    ]
    out["liquidity_reason"] = [
        "" if payload is None else str(payload.get("reason") or "")
        for payload in liquidity_payloads
    ]
    out["liquidity_blocked"] = [
        False if payload is None else bool(payload.get("blocked", False))
        for payload in liquidity_payloads
    ]
    out["liquidity_degraded"] = [
        False if payload is None else bool(payload.get("degraded", False))
        for payload in liquidity_payloads
    ]
    out["liquidity_reason_codes"] = [
        [] if payload is None else list(payload.get("reason_codes") or [])
        for payload in liquidity_payloads
    ]
    out["liquidity_metrics"] = [
        {} if payload is None else dict(payload.get("metrics") or {})
        for payload in liquidity_payloads
    ]
    out["regime_enabled"] = [bool(payload.get("enabled", False)) for payload in regime_payloads]
    out["regime_status"] = [str(payload.get("status") or "") for payload in regime_payloads]
    out["regime_reason"] = [str(payload.get("reason") or "") for payload in regime_payloads]
    out["regime_state"] = [str(payload.get("state") or REGIME_NORMAL) for payload in regime_payloads]
    out["regime_target_state"] = [str(payload.get("target_state") or REGIME_NORMAL) for payload in regime_payloads]
    out["regime_pressure"] = [str(payload.get("pressure") or PRESSURE_NEUTRAL) for payload in regime_payloads]
    out["regime_reason_codes"] = [list(payload.get("reason_codes") or []) for payload in regime_payloads]
    out["regime_min_liquidity_ratio"] = [float(payload.get("min_liquidity_ratio") or 0.0) for payload in regime_payloads]
    out["regime_soft_fail_count"] = [int(payload.get("soft_fail_count") or 0) for payload in regime_payloads]
    out["regime_hard_fail_count"] = [int(payload.get("hard_fail_count") or 0) for payload in regime_payloads]
    out["regime_ret_15m"] = [payload.get("ret_15m") for payload in regime_payloads]
    out["regime_ret_30m"] = [payload.get("ret_30m") for payload in regime_payloads]
    out["regime_pending_target"] = [str(payload.get("pending_target") or REGIME_NORMAL) for payload in regime_payloads]
    out["regime_pending_count"] = [int(payload.get("pending_count") or 0) for payload in regime_payloads]
    out["regime_guard_hints"] = [dict(payload.get("guard_hints") or {}) for payload in regime_payloads]
    out["regime_source_of_truth"] = [dict(payload.get("source_of_truth") or {}) for payload in regime_payloads]
    return out, _build_regime_parity_summary(out=out, proxy_mode=proxy_mode), profile_spec


def summarize_backtest_liquidity_state(
    liquidity_state: BacktestLiquidityStatus | Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if liquidity_state is None:
        return None
    if isinstance(liquidity_state, BacktestLiquidityStatus):
        reason_codes = list(liquidity_state.reason_codes)
        return {
            "status": "ok",
            "reason": reason_codes[0] if reason_codes else None,
            "blocked": bool(liquidity_state.blocked),
            "degraded": bool(liquidity_state.degraded),
            "reason_codes": reason_codes,
            "metrics": dict(liquidity_state.metrics),
            "checked_at": liquidity_state.checked_at.isoformat(),
        }
    if not isinstance(liquidity_state, Mapping):
        return None
    reason_codes = _reason_codes_list(liquidity_state.get("reason_codes"))
    metrics = liquidity_state.get("metrics")
    return {
        "status": str(liquidity_state.get("status") or "ok"),
        "reason": liquidity_state.get("reason") or (reason_codes[0] if reason_codes else None),
        "blocked": bool(liquidity_state.get("blocked", False)),
        "degraded": bool(liquidity_state.get("degraded", False)),
        "reason_codes": reason_codes,
        "metrics": dict(metrics) if isinstance(metrics, Mapping) else {},
        "checked_at": liquidity_state.get("checked_at"),
    }


def build_backtest_regime_state(
    *,
    market: str,
    profile: str | LiveProfileSpec,
    cycle: str | int | None = None,
    features: pd.DataFrame | None = None,
    liquidity_state: BacktestLiquidityStatus | Mapping[str, Any] | None = None,
    previous_state: Mapping[str, Any] | None = None,
    now: pd.Timestamp | None = None,
) -> dict[str, Any]:
    market_token = str(market or "").strip().lower()
    profile_name, spec = _resolve_profile_spec(profile)
    resolved_cycle = infer_regime_cycle(cycle=cycle, features=features, offsets=spec.offsets)
    checked_at = resolve_checked_at(features=features, now=now)
    liquidity_summary = summarize_backtest_liquidity_state(liquidity_state)
    guard_hints = _guard_hints(spec=spec, regime_state=REGIME_NORMAL)
    payload = {
        "domain": "research_backtest",
        "dataset": "backtest_regime_state",
        "market": market_token,
        "profile": profile_name,
        "enabled": bool(spec.regime_controller_enabled),
        "status": "ok",
        "reason": None,
        "checked_at": checked_at.isoformat(),
        "state": REGIME_NORMAL,
        "target_state": REGIME_NORMAL,
        "pressure": PRESSURE_NEUTRAL,
        "reason_codes": [],
        "min_liquidity_ratio": 1.0,
        "soft_fail_count": 0,
        "hard_fail_count": 0,
        "ret_15m": None,
        "ret_30m": None,
        "pending_target": REGIME_NORMAL,
        "pending_count": 0,
        "liquidity_state_status": None if liquidity_summary is None else liquidity_summary.get("status"),
        "liquidity_state_reason": None if liquidity_summary is None else liquidity_summary.get("reason"),
        "liquidity_state_blocked": False if liquidity_summary is None else bool(liquidity_summary.get("blocked", False)),
        "liquidity_reason_codes": [] if liquidity_summary is None else list(liquidity_summary.get("reason_codes") or []),
        "guard_hints": guard_hints,
        "source_of_truth": {
            "liquidity_state_available": bool(liquidity_summary is not None),
            "liquidity_metrics_available": bool(liquidity_summary and liquidity_summary.get("metrics")),
            "feature_returns_available": False,
        },
    }
    if not spec.regime_controller_enabled:
        payload["reason"] = "regime_controller_disabled"
        payload["reason_codes"] = ["disabled"]
        return payload

    ret_15m, ret_30m = latest_regime_returns(features, cycle=resolved_cycle)
    controller = _build_regime_controller(spec)
    seed_regime_controller(
        controller=controller,
        previous_payload=dict(previous_state) if isinstance(previous_state, Mapping) else None,
    )
    snapshot = controller.evaluate(
        now=checked_at,
        liquidity_metrics=None if liquidity_summary is None else dict(liquidity_summary.get("metrics") or {}),
        liquidity_blocked=False if liquidity_summary is None else bool(liquidity_summary.get("blocked", False)),
        ret_15m=ret_15m,
        ret_30m=ret_30m,
    )
    payload.update(
        {
            "reason": "regime_state_built",
            "state": snapshot["state"],
            "target_state": snapshot["target_state"],
            "pressure": snapshot["pressure"],
            "reason_codes": list(snapshot["reason_codes"]),
            "min_liquidity_ratio": float(snapshot["min_liquidity_ratio"]),
            "soft_fail_count": int(snapshot["soft_fail_count"]),
            "hard_fail_count": int(snapshot["hard_fail_count"]),
            "ret_15m": snapshot["ret_15m"],
            "ret_30m": snapshot["ret_30m"],
            "pending_target": snapshot["pending_target"],
            "pending_count": int(snapshot["pending_count"]),
            "guard_hints": _guard_hints(spec=spec, regime_state=snapshot["state"]),
            "source_of_truth": {
                "liquidity_state_available": bool(liquidity_summary is not None),
                "liquidity_metrics_available": bool(liquidity_summary and liquidity_summary.get("metrics")),
                "feature_returns_available": ret_15m is not None or ret_30m is not None,
            },
        }
    )
    return payload


def _resolve_profile_spec(profile: str | LiveProfileSpec) -> tuple[str, LiveProfileSpec]:
    if isinstance(profile, LiveProfileSpec):
        token = str(profile.profile or "default").strip().lower() or "default"
        return token, profile
    token = str(profile or "default").strip().lower() or "default"
    return token, resolve_live_profile_spec(token)


def _coerce_parity(parity: BacktestParitySpec | Mapping[str, Any] | None) -> BacktestParitySpec:
    if isinstance(parity, BacktestParitySpec):
        return parity
    if isinstance(parity, Mapping):
        return BacktestParitySpec.from_mapping(dict(parity))
    return BacktestParitySpec()


def _build_regime_controller(spec: LiveProfileSpec) -> RegimeController:
    return RegimeController(
        caution_min_liquidity_ratio=float(spec.regime_caution_min_liquidity_ratio),
        defense_min_liquidity_ratio=float(spec.regime_defense_min_liquidity_ratio),
        caution_soft_fail_count=int(spec.regime_caution_soft_fail_count),
        defense_soft_fail_count=int(spec.regime_defense_soft_fail_count),
        switch_confirmations=int(spec.regime_switch_confirmations),
        recover_confirmations=int(spec.regime_recover_confirmations),
        up_pressure_ret_15m=float(spec.regime_up_pressure_ret_15m),
        up_pressure_ret_30m=float(spec.regime_up_pressure_ret_30m),
        down_pressure_ret_15m=float(spec.regime_down_pressure_ret_15m),
        down_pressure_ret_30m=float(spec.regime_down_pressure_ret_30m),
    )


def _guard_hints(*, spec: LiveProfileSpec, regime_state: str) -> dict[str, object]:
    return {
        "min_dir_prob_boost": float(spec.regime_min_dir_prob_boost_for(regime_state)),
        "disabled_offsets": list(spec.regime_disabled_offsets_for(regime_state)),
        "defense_force_with_pressure": bool(spec.regime_defense_force_with_pressure),
        "defense_max_trades_per_market": int(spec.regime_defense_max_trades_per_market),
    }


def _build_regime_parity_summary(*, out: pd.DataFrame, proxy_mode: str) -> RegimeParitySummary:
    liquidity_available = out["liquidity_status"].astype(str).ne("").sum() if "liquidity_status" in out.columns else 0
    liquidity_degraded = out["liquidity_degraded"].astype(bool).sum() if "liquidity_degraded" in out.columns else 0
    regime_rows = out
    if "regime_enabled" in out.columns:
        regime_rows = out.loc[out["regime_enabled"].astype(bool)].copy()
    state_counts = _value_counts(regime_rows, "regime_state")
    pressure_counts = _value_counts(regime_rows, "regime_pressure")
    return RegimeParitySummary(
        evaluated_rows=int(len(out)),
        liquidity_proxy_enabled=bool(proxy_mode != "off"),
        liquidity_proxy_mode=proxy_mode,
        liquidity_available_rows=int(liquidity_available),
        liquidity_missing_rows=int(len(out) - int(liquidity_available)),
        liquidity_degraded_rows=int(liquidity_degraded),
        regime_enabled=bool("regime_enabled" in out.columns and out["regime_enabled"].astype(bool).any()),
        regime_state_counts=state_counts,
        regime_pressure_counts=pressure_counts,
    )


def _row_checked_at(row: pd.Series) -> pd.Timestamp:
    for column in ("decision_ts", "cycle_start_ts", "cycle_end_ts"):
        ts = pd.to_datetime(row.get(column), utc=True, errors="coerce")
        if not pd.isna(ts):
            return pd.Timestamp(ts)
    return pd.Timestamp.now(tz="UTC")


def _row_feature_frame(*, row: pd.Series, checked_at: pd.Timestamp, cycle: str | int) -> pd.DataFrame:
    short_col, long_col = resolve_cycle_contract(cycle).regime_return_columns
    return pd.DataFrame(
        [
            {
                "decision_ts": checked_at,
                short_col: row.get(short_col),
                long_col: row.get(long_col),
            }
        ]
    )


def _normalize_mode(value: object) -> str:
    token = str(value or "off").strip().lower()
    return token or "off"


def _reason_codes_list(value: object) -> list[str]:
    if isinstance(value, str):
        return [token for token in (item.strip() for item in value.split(",")) if token]
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _value_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if column not in frame.columns:
        return {}
    counts = frame[column].astype(str).value_counts().sort_index()
    counts = counts[counts.index != ""]
    return {str(index): int(value) for index, value in counts.items()}


def _with_market_threshold(
    mapping: Mapping[str, float] | None,
    *,
    market: str,
    value: float,
) -> dict[str, float]:
    out = dict(mapping or {})
    out[str(market or "default").strip().lower() or "default"] = float(value)
    return out
