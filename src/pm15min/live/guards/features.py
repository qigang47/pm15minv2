from __future__ import annotations

from typing import Any

from ..profiles import LiveProfileSpec


def directional_return_guard_reasons(
    *,
    market: str,
    profile_spec: LiveProfileSpec,
    signal_row: dict[str, Any],
    feature_snapshot: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    side = str(signal_row.get("recommended_side") or "").upper()
    ret_30m = float_or_none(feature_snapshot.get("ret_30m"))
    if ret_30m is None:
        return reasons

    up_floor = profile_spec.ret_30m_up_floor_for(market)
    if side == "UP" and up_floor is not None and ret_30m < float(up_floor):
        reasons.append("ret30m_up_floor")

    down_ceiling = profile_spec.ret_30m_down_ceiling_for(market)
    if side == "DOWN" and down_ceiling is not None and ret_30m > float(down_ceiling):
        reasons.append("ret30m_down_ceiling")
    return reasons


def tail_space_guard_reasons(
    *,
    profile_spec: LiveProfileSpec,
    signal_row: dict[str, Any],
    feature_snapshot: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    max_move_z = profile_spec.tail_space_max_move_z_for(int(signal_row["offset"]))
    if max_move_z is None:
        return reasons

    side = str(signal_row.get("recommended_side") or "").upper()
    ret_anchor = float_or_none(feature_snapshot.get("ret_from_strike"))
    if ret_anchor is None:
        ret_anchor = float_or_none(feature_snapshot.get("ret_from_cycle_open"))
    if ret_anchor is None or ret_anchor == 0.0:
        return reasons

    needs_tail = (side == "UP" and ret_anchor < 0.0) or (side == "DOWN" and ret_anchor > 0.0)
    if not needs_tail:
        return reasons

    move_z = float_or_none(feature_snapshot.get("move_z_strike"))
    if move_z is None:
        move_z = float_or_none(feature_snapshot.get("move_z"))
    if move_z is None:
        return reasons

    if abs(move_z) > float(max_move_z):
        reasons.append("tail_space_too_far")
    return reasons


def float_or_none(value) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out
