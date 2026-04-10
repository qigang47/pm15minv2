from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pandas as pd

from pm15min.live.guards import evaluate_signal_guard_reasons
from pm15min.live.regime.controller import infer_regime_cycle
from pm15min.live.profiles.spec import LiveProfileSpec
from pm15min.live.profiles import resolve_live_profile_spec


@dataclass(frozen=True)
class GuardParitySummary:
    evaluated_rows: int
    blocked_rows: int

    def to_dict(self) -> dict[str, int]:
        return {
            "evaluated_rows": self.evaluated_rows,
            "blocked_rows": self.blocked_rows,
        }


_FEATURE_SNAPSHOT_COLUMNS = (
    "ret_5m",
    "ret_15m",
    "ret_30m",
    "ret_from_strike",
    "ret_from_cycle_open",
    "move_z_strike",
    "move_z",
)

GUARD_PARITY_HEARTBEAT_INTERVAL_ROWS = 1_000


def apply_live_guard_parity(
    *,
    market: str,
    cycle: str | None = None,
    profile: str,
    decisions: pd.DataFrame,
    profile_spec: LiveProfileSpec | None = None,
    heartbeat: Callable[[str], None] | None = None,
) -> tuple[pd.DataFrame, GuardParitySummary]:
    if decisions.empty:
        return decisions.copy(), GuardParitySummary(evaluated_rows=0, blocked_rows=0)

    spec = profile_spec or resolve_live_profile_spec(profile)
    resolved_cycle = infer_regime_cycle(cycle=cycle, features=decisions, offsets=spec.offsets)
    out = decisions.copy()
    guard_reasons_list: list[list[str]] = []
    quote_metrics_list: list[dict[str, Any]] = []
    account_context_list: list[dict[str, Any]] = []
    blocked_count = 0
    total_rows = len(out)
    if heartbeat is not None:
        heartbeat(f"Applying live guard parity: 0/{total_rows:,} rows")

    for row_index, row in enumerate(out.itertuples(index=False, name="GuardParityRow"), start=1):
        quote_row = _build_quote_row(row)
        signal_row = _build_signal_row(row)
        row_cycle = str(_row_value(row, "cycle") or resolved_cycle)
        if quote_row is None:
            reasons: list[str] = []
            quote_metrics: dict[str, Any] = {}
            account_context: dict[str, Any] = {}
        else:
            reasons, quote_metrics, account_context = evaluate_signal_guard_reasons(
                cycle=row_cycle,
                market=market,
                profile_spec=spec,
                signal_row=signal_row,
                quote_row=quote_row,
                liquidity_state=_build_liquidity_state(row),
                regime_state=_build_regime_state(row),
                account_state=_build_account_state(row),
            )
        guard_reasons_list.append(reasons)
        quote_metrics_list.append(quote_metrics)
        account_context_list.append(account_context)
        if str(_row_value(row, "policy_action") or "reject") == "trade" and reasons:
            blocked_count += 1
        if heartbeat is not None and (
            row_index == total_rows
            or row_index % GUARD_PARITY_HEARTBEAT_INTERVAL_ROWS == 0
        ):
            heartbeat(f"Applying live guard parity: {row_index:,}/{total_rows:,} rows")

    out["guard_reasons"] = guard_reasons_list
    out["guard_primary_reason"] = [reasons[0] if reasons else "" for reasons in guard_reasons_list]
    out["guard_blocked"] = [bool(reasons) for reasons in guard_reasons_list]
    out["quote_metrics"] = quote_metrics_list
    out["account_context"] = account_context_list

    trade_mask = out["policy_action"].astype(str).eq("trade") & out["guard_blocked"].astype(bool)
    out.loc[trade_mask, "trade_decision"] = False
    out.loc[trade_mask, "policy_action"] = "reject"
    out.loc[trade_mask, "policy_reason"] = out.loc[trade_mask, "guard_primary_reason"]
    out.loc[trade_mask, "reject_reason"] = out.loc[trade_mask, "guard_primary_reason"]
    return out, GuardParitySummary(
        evaluated_rows=int(len(out)),
        blocked_rows=int(blocked_count),
    )


def _build_signal_row(row: object) -> dict[str, Any]:
    p_up = _float_or_none(_row_value(row, "p_up"))
    p_down = _float_or_none(_row_value(row, "p_down"))
    recommended_side = "UP" if (p_up or -1.0) >= (p_down or -1.0) else "DOWN"
    confidence = max(value for value in (p_up, p_down) if value is not None) if (p_up is not None or p_down is not None) else 0.0
    offset_value = pd.to_numeric(_row_value(row, "offset"), errors="coerce")
    coverage = _row_value(row, "coverage")
    return {
        "offset": int(offset_value) if pd.notna(offset_value) else 0,
        "recommended_side": recommended_side,
        "confidence": float(confidence),
        "p_up": p_up,
        "p_down": p_down,
        "score_valid": bool(_row_value(row, "score_valid", True)),
        "score_reason": str(_row_value(row, "score_reason") or ""),
        "status": str(_row_value(row, "status") or ""),
        "coverage": coverage if isinstance(coverage, dict) else {},
        "feature_snapshot": _build_feature_snapshot(row),
    }


def _build_quote_row(row: object) -> dict[str, Any] | None:
    quote_status = str(_row_value(row, "quote_status") or "")
    has_quote_fields = any(_row_has_value(row, name) for name in ("quote_up_ask", "quote_down_ask", "quote_prob_up", "quote_prob_down"))
    if not quote_status and not has_quote_fields:
        return None

    def _fallback(name: str, alt: str) -> float | None:
        direct = _float_or_none(_row_value(row, name))
        return direct if direct is not None else _float_or_none(_row_value(row, alt))

    reasons = [reason for reason in str(_row_value(row, "quote_reason") or "").split(",") if reason]
    if quote_status == "missing_quote_inputs" and set(reasons).issubset({"market_or_decision_missing", "orderbook_index_missing"}):
        return None
    if not quote_status:
        quote_status = "ok" if has_quote_fields else "missing_quote_inputs"
    return {
        "status": quote_status,
        "reasons": reasons,
        "market_id": _row_value(row, "market_id"),
        "quote_up_ask": _fallback("quote_up_ask", "quote_prob_up"),
        "quote_down_ask": _fallback("quote_down_ask", "quote_prob_down"),
        "quote_up_bid": _float_or_none(_row_value(row, "quote_up_bid")),
        "quote_down_bid": _float_or_none(_row_value(row, "quote_down_bid")),
        "quote_up_ask_size_1": _float_or_none(_row_value(row, "quote_up_ask_size_1")),
        "quote_down_ask_size_1": _float_or_none(_row_value(row, "quote_down_ask_size_1")),
        "quote_captured_ts_ms_up": _row_value(row, "quote_captured_ts_ms_up"),
        "quote_captured_ts_ms_down": _row_value(row, "quote_captured_ts_ms_down"),
    }


def _build_liquidity_state(row: object) -> dict[str, Any]:
    blocked = bool(_row_value(row, "liquidity_blocked", False))
    degraded = bool(_row_value(row, "liquidity_degraded", False))
    reason_codes = _row_value(row, "liquidity_reason_codes")
    if isinstance(reason_codes, str):
        codes = [item for item in reason_codes.split(",") if item]
    elif isinstance(reason_codes, (list, tuple)):
        codes = [str(item) for item in reason_codes if str(item)]
    else:
        codes = []
    metrics = _row_value(row, "liquidity_metrics")
    return {
        "blocked": blocked,
        "degraded": degraded,
        "reason_codes": codes,
        "metrics": metrics if isinstance(metrics, dict) else {},
    }


def _build_regime_state(row: object) -> dict[str, Any]:
    status = str(_row_value(row, "regime_status") or "ok").lower()
    state = str(_row_value(row, "regime_state") or "NORMAL").upper()
    pressure = str(_row_value(row, "regime_pressure") or "neutral").lower()
    reason_codes = _row_value(row, "regime_reason_codes")
    if isinstance(reason_codes, str):
        codes = [item for item in reason_codes.split(",") if item]
    elif isinstance(reason_codes, (list, tuple)):
        codes = [str(item) for item in reason_codes if str(item)]
    else:
        codes = []
    return {
        "status": status,
        "state": state,
        "pressure": pressure,
        "reason_codes": codes,
    }


def _build_account_state(row: object) -> dict[str, Any]:
    account_state = _row_value(row, "account_state")
    return account_state if isinstance(account_state, dict) else {}


def _build_feature_snapshot(row: object) -> dict[str, Any]:
    existing = _row_value(row, "feature_snapshot")
    if isinstance(existing, dict):
        return existing
    snapshot: dict[str, Any] = {}
    for column in _FEATURE_SNAPSHOT_COLUMNS:
        value = _row_value(row, column)
        if _is_missing_scalar(value):
            continue
        snapshot[column] = value
    return snapshot


def _row_value(row: object, key: str, default: object = None) -> object:
    if isinstance(row, pd.Series):
        return row.get(key, default)
    return getattr(row, key, default)


def _row_has_value(row: object, key: str) -> bool:
    return not _is_missing_scalar(_row_value(row, key))


def _is_missing_scalar(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


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
