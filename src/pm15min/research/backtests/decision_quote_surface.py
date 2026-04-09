from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from pm15min.live.execution.depth import compute_fill_from_depth_record
from pm15min.live.execution.utils import normalize_levels
from pm15min.live.profiles.spec import LiveProfileSpec
from pm15min.research.backtests.decision_engine_parity import (
    DecisionEngineParityConfig,
    apply_decision_engine_parity,
)
from pm15min.research.backtests.fills import BacktestFillConfig, build_depth_candidate_lookup
from pm15min.research.backtests.replay_loader import REPLAY_KEY_COLUMNS


DECISION_QUOTE_SURFACE_COLUMNS = (
    "decision_quote_up_ask",
    "decision_quote_down_ask",
    "decision_quote_up_liquidity",
    "decision_quote_down_liquidity",
    "decision_quote_up_status",
    "decision_quote_down_status",
    "decision_quote_has_raw_depth",
    "decision_quote_any_repriced",
    "decision_quote_limit_reject",
    "decision_quote_orderbook_missing",
    "decision_quote_candidate_total_rows",
    "decision_quote_candidate_examined_rows",
    "decision_quote_up_candidate_examined_rows",
    "decision_quote_up_candidate_orderbook_missing_rows",
    "decision_quote_up_candidate_price_reject_rows",
    "decision_quote_up_candidate_depth_reject_rows",
    "decision_quote_up_candidate_fillable_rows",
    "decision_quote_down_candidate_examined_rows",
    "decision_quote_down_candidate_orderbook_missing_rows",
    "decision_quote_down_candidate_price_reject_rows",
    "decision_quote_down_candidate_depth_reject_rows",
    "decision_quote_down_candidate_fillable_rows",
)


@dataclass(frozen=True)
class InitialSnapshotDecisionSummary:
    raw_depth_rows: int
    repriced_rows: int
    limit_reject_rows: int
    orderbook_missing_rows: int
    candidate_total_rows: int
    candidate_examined_rows: int
    signal_rows: int
    signal_candidate_total_rows: int
    signal_candidate_examined_rows: int
    signal_candidate_orderbook_missing_rows: int
    signal_candidate_price_reject_rows: int
    signal_candidate_depth_reject_rows: int
    signal_candidate_fillable_rows: int

    def to_dict(self) -> dict[str, int]:
        return {
            "raw_depth_rows": int(self.raw_depth_rows),
            "repriced_rows": int(self.repriced_rows),
            "limit_reject_rows": int(self.limit_reject_rows),
            "orderbook_missing_rows": int(self.orderbook_missing_rows),
            "candidate_total_rows": int(self.candidate_total_rows),
            "candidate_examined_rows": int(self.candidate_examined_rows),
            "signal_rows": int(self.signal_rows),
            "signal_candidate_total_rows": int(self.signal_candidate_total_rows),
            "signal_candidate_examined_rows": int(self.signal_candidate_examined_rows),
            "signal_candidate_orderbook_missing_rows": int(self.signal_candidate_orderbook_missing_rows),
            "signal_candidate_price_reject_rows": int(self.signal_candidate_price_reject_rows),
            "signal_candidate_depth_reject_rows": int(self.signal_candidate_depth_reject_rows),
            "signal_candidate_fillable_rows": int(self.signal_candidate_fillable_rows),
        }


def apply_initial_snapshot_decision_parity(
    replay: pd.DataFrame,
    *,
    depth_replay: pd.DataFrame | None,
    profile_spec: LiveProfileSpec,
    fill_config: BacktestFillConfig,
    decision_config: DecisionEngineParityConfig,
    min_dir_prob_boost_column: str | None = None,
) -> tuple[pd.DataFrame, InitialSnapshotDecisionSummary]:
    frame = attach_initial_snapshot_decision_surface(
        replay,
        depth_replay=depth_replay,
        profile_spec=profile_spec,
        fill_config=fill_config,
    )
    out = apply_decision_engine_parity(
        frame,
        config=decision_config,
        up_price_columns=("decision_quote_up_ask", "quote_up_ask", "quote_prob_up", "p_up"),
        down_price_columns=("decision_quote_down_ask", "quote_down_ask", "quote_prob_down", "p_down"),
        min_dir_prob_boost_column=min_dir_prob_boost_column,
    )
    out["decision_engine_signal_side"] = out.get(
        "decision_engine_side",
        pd.Series(pd.NA, index=out.index, dtype="string"),
    ).astype("string")
    out["decision_engine_signal_triggered"] = out["decision_engine_signal_side"].notna()
    if not out.empty:
        missing_mask = _bool_series(out, "decision_quote_orderbook_missing", default=False)
        limit_reject_mask = _bool_series(out, "decision_quote_limit_reject", default=False)
        if missing_mask.any():
            _apply_decision_engine_reject(
                out,
                mask=missing_mask,
                reason="orderbook_missing",
            )
        if limit_reject_mask.any():
            _apply_decision_engine_reject(
                out,
                mask=limit_reject_mask,
                reason="orderbook_limit_reject",
            )

    summary = summarize_initial_snapshot_decision_surface(out)
    return out, summary


def attach_initial_snapshot_decision_surface(
    replay: pd.DataFrame,
    *,
    depth_replay: pd.DataFrame | None,
    profile_spec: LiveProfileSpec,
    fill_config: BacktestFillConfig,
) -> pd.DataFrame:
    frame = replay.copy()
    for column in DECISION_QUOTE_SURFACE_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA
    if depth_replay is None or depth_replay.empty:
        frame["decision_quote_has_raw_depth"] = False
        frame["decision_quote_any_repriced"] = False
        frame["decision_quote_limit_reject"] = False
        frame["decision_quote_orderbook_missing"] = False
        frame["decision_quote_candidate_total_rows"] = 0
        frame["decision_quote_candidate_examined_rows"] = 0
        return frame

    lookup = build_depth_candidate_lookup(depth_replay)
    replay_keys = _build_replay_key_rows(frame)
    probability_up = frame["p_up"].tolist() if "p_up" in frame.columns else [None] * len(frame)
    probability_down = frame["p_down"].tolist() if "p_down" in frame.columns else [None] * len(frame)
    offsets = frame["offset"].tolist() if "offset" in frame.columns else [None] * len(frame)
    surface_values = {column: frame[column].tolist() for column in DECISION_QUOTE_SURFACE_COLUMNS}

    for row_idx, replay_key in enumerate(replay_keys):
        candidates = lookup.get(replay_key)
        if not candidates:
            surface_values["decision_quote_has_raw_depth"][row_idx] = False
            surface_values["decision_quote_any_repriced"][row_idx] = False
            surface_values["decision_quote_limit_reject"][row_idx] = False
            surface_values["decision_quote_orderbook_missing"][row_idx] = False
            surface_values["decision_quote_candidate_total_rows"][row_idx] = 0
            surface_values["decision_quote_candidate_examined_rows"][row_idx] = 0
            continue
        surface_values["decision_quote_has_raw_depth"][row_idx] = True
        row_scan = _resolve_window_depth_quotes(
            probability_up=_float_or_none(probability_up[row_idx]),
            probability_down=_float_or_none(probability_down[row_idx]),
            offset=_int_or_none(offsets[row_idx]) or 0,
            raw_depth_candidates=candidates,
            profile_spec=profile_spec,
            fill_config=fill_config,
        )
        up_payload = row_scan["up"]
        down_payload = row_scan["down"]
        surface_values["decision_quote_up_ask"][row_idx] = up_payload["price"]
        surface_values["decision_quote_down_ask"][row_idx] = down_payload["price"]
        surface_values["decision_quote_up_liquidity"][row_idx] = up_payload["liquidity"]
        surface_values["decision_quote_down_liquidity"][row_idx] = down_payload["liquidity"]
        surface_values["decision_quote_up_status"][row_idx] = up_payload["status"]
        surface_values["decision_quote_down_status"][row_idx] = down_payload["status"]
        surface_values["decision_quote_candidate_total_rows"][row_idx] = row_scan["candidate_total_rows"]
        surface_values["decision_quote_candidate_examined_rows"][row_idx] = row_scan["candidate_examined_rows"]
        surface_values["decision_quote_up_candidate_examined_rows"][row_idx] = row_scan["up_stats"]["candidate_examined_rows"]
        surface_values["decision_quote_up_candidate_orderbook_missing_rows"][row_idx] = row_scan["up_stats"]["candidate_orderbook_missing_rows"]
        surface_values["decision_quote_up_candidate_price_reject_rows"][row_idx] = row_scan["up_stats"]["candidate_price_reject_rows"]
        surface_values["decision_quote_up_candidate_depth_reject_rows"][row_idx] = row_scan["up_stats"]["candidate_depth_reject_rows"]
        surface_values["decision_quote_up_candidate_fillable_rows"][row_idx] = row_scan["up_stats"]["candidate_fillable_rows"]
        surface_values["decision_quote_down_candidate_examined_rows"][row_idx] = row_scan["down_stats"]["candidate_examined_rows"]
        surface_values["decision_quote_down_candidate_orderbook_missing_rows"][row_idx] = row_scan["down_stats"]["candidate_orderbook_missing_rows"]
        surface_values["decision_quote_down_candidate_price_reject_rows"][row_idx] = row_scan["down_stats"]["candidate_price_reject_rows"]
        surface_values["decision_quote_down_candidate_depth_reject_rows"][row_idx] = row_scan["down_stats"]["candidate_depth_reject_rows"]
        surface_values["decision_quote_down_candidate_fillable_rows"][row_idx] = row_scan["down_stats"]["candidate_fillable_rows"]
        any_repriced = bool(up_payload["repriced"]) or bool(down_payload["repriced"])
        orderbook_missing = (
            str(up_payload["status"]) == "orderbook_missing"
            or str(down_payload["status"]) == "orderbook_missing"
        )
        limit_reject = (
            not orderbook_missing
            and not any_repriced
            and str(up_payload["status"]) == "orderbook_limit_reject"
            and str(down_payload["status"]) == "orderbook_limit_reject"
        )
        surface_values["decision_quote_any_repriced"][row_idx] = any_repriced
        surface_values["decision_quote_limit_reject"][row_idx] = limit_reject
        surface_values["decision_quote_orderbook_missing"][row_idx] = orderbook_missing
    for column, values in surface_values.items():
        frame[column] = values
    return frame


def _resolve_window_depth_quotes(
    *,
    probability_up: float | None,
    probability_down: float | None,
    offset: int,
    raw_depth_candidates: list[dict[str, object]],
    profile_spec: LiveProfileSpec,
    fill_config: BacktestFillConfig,
) -> dict[str, object]:
    if not raw_depth_candidates:
        return {
            "up": {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "orderbook_missing"},
            "down": {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "orderbook_missing"},
            "candidate_total_rows": 0,
            "candidate_examined_rows": 0,
        }
    up_state = _build_side_scan_state(
        probability=probability_up,
        offset=offset,
        side="UP",
        profile_spec=profile_spec,
        fill_config=fill_config,
    )
    down_state = _build_side_scan_state(
        probability=probability_down,
        offset=offset,
        side="DOWN",
        profile_spec=profile_spec,
        fill_config=fill_config,
    )
    candidate_examined_rows = _scan_window_depth_candidates(
        raw_depth_candidates=raw_depth_candidates,
        up_state=up_state,
        down_state=down_state,
    )
    return {
        "up": up_state["payload"],
        "down": down_state["payload"],
        "up_stats": _side_candidate_scan_stats(up_state),
        "down_stats": _side_candidate_scan_stats(down_state),
        "candidate_total_rows": _resolve_candidate_total_rows(raw_depth_candidates),
        "candidate_examined_rows": candidate_examined_rows,
    }


def _build_side_scan_state(
    *,
    probability: float | None,
    offset: int,
    side: str,
    profile_spec: LiveProfileSpec,
    fill_config: BacktestFillConfig,
) -> dict[str, object]:
    if probability is None:
        return {
            "side": side,
            "should_scan": False,
            "payload": {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "prob_missing"},
        }
    requested_notional = _resolve_side_target_notional(
        probability=probability,
        fill_config=fill_config,
    )
    if requested_notional <= 0.0:
        return {
            "side": side,
            "should_scan": False,
            "payload": {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "target_notional_nonpositive"},
        }
    max_slippage_bps = (
        float(fill_config.depth_max_slippage_bps)
        if fill_config.depth_max_slippage_bps is not None
        else float(profile_spec.orderbook_max_slippage_bps)
    )
    return {
        "side": side,
        "should_scan": True,
        "probability": float(probability),
        "requested_notional": float(requested_notional),
        "roi_threshold": float(profile_spec.roi_threshold_for(offset=offset)),
        "max_slippage_bps": float(max_slippage_bps),
        "profile_spec": profile_spec,
        "saw_orderbook": False,
        "payload": None,
        "candidate_examined_rows": 0,
        "candidate_orderbook_missing_rows": 0,
        "candidate_price_reject_rows": 0,
        "candidate_depth_reject_rows": 0,
        "candidate_fillable_rows": 0,
    }


def _scan_window_depth_candidates(
    *,
    raw_depth_candidates: list[dict[str, object]],
    up_state: dict[str, object],
    down_state: dict[str, object],
) -> int:
    active_states = [state for state in (up_state, down_state) if bool(state.get("should_scan"))]
    if not active_states:
        return 0
    candidate_examined_rows = 0
    for candidate in raw_depth_candidates:
        candidate_examined_rows += 1
        for state in active_states:
            state["candidate_examined_rows"] = int(state.get("candidate_examined_rows") or 0) + 1
            payload = _resolve_side_payload_from_candidate(
                candidate=candidate,
                state=state,
            )
            if payload is not None and _prefer_side_payload(current=state.get("payload"), candidate=payload):
                state["payload"] = payload
    for state in active_states:
        if state.get("payload") is not None:
            continue
        state["payload"] = _finalize_unfilled_side_payload(state=state)
    return int(candidate_examined_rows)


def _resolve_side_payload_from_candidate(
    *,
    candidate: dict[str, object],
    state: dict[str, object],
) -> dict[str, object] | None:
    side = str(state.get("side") or "").upper()
    side_key = "depth_up_record" if side == "UP" else "depth_down_record"
    record = candidate.get(side_key)
    if not isinstance(record, dict):
        state["candidate_orderbook_missing_rows"] = int(state.get("candidate_orderbook_missing_rows") or 0) + 1
        return None
    asks = normalize_levels(record.get("asks"))
    if not asks:
        state["candidate_orderbook_missing_rows"] = int(state.get("candidate_orderbook_missing_rows") or 0) + 1
        return None
    state["saw_orderbook"] = True
    profile_spec = state["profile_spec"]
    fill = compute_fill_from_depth_record(
        record=record,
        target_notional=float(state["requested_notional"]),
        max_slippage_bps=float(state["max_slippage_bps"]),
        price_cap=None,
    )
    if fill is None:
        state["candidate_depth_reject_rows"] = int(state.get("candidate_depth_reject_rows") or 0) + 1
        return None
    stop_reason = str(fill.get("stop_reason") or "")
    if stop_reason != "filled_target":
        state["candidate_depth_reject_rows"] = int(state.get("candidate_depth_reject_rows") or 0) + 1
        return None
    state["candidate_fillable_rows"] = int(state.get("candidate_fillable_rows") or 0) + 1
    max_price = _float_or_none(fill.get("max_price"))
    total_cost = _float_or_none(fill.get("total_cost")) or 0.0
    if max_price is not None:
        entry_price_min = _float_or_none(getattr(profile_spec, "entry_price_min", None))
        entry_price_max = _float_or_none(getattr(profile_spec, "entry_price_max", None))
        if (
            (entry_price_min is not None and max_price < entry_price_min)
            or (entry_price_max is not None and max_price > entry_price_max)
        ):
            state["candidate_price_reject_rows"] = int(state.get("candidate_price_reject_rows") or 0) + 1
    liquidity = 0.0 if not max_price or max_price <= 0 else float(total_cost / max_price)
    return {
        "price": float(max_price or 1.0),
        "liquidity": float(liquidity),
        "repriced": True,
        "status": stop_reason or "ok",
    }


def _prefer_side_payload(*, current: object, candidate: dict[str, object], tol: float = 1e-12) -> bool:
    if not isinstance(current, dict):
        return True
    current_price = _float_or_none(current.get("price"))
    candidate_price = _float_or_none(candidate.get("price"))
    if current_price is None:
        return candidate_price is not None
    if candidate_price is None:
        return False
    if float(candidate_price) < float(current_price) - float(tol):
        return True
    if float(candidate_price) > float(current_price) + float(tol):
        return False
    current_liquidity = _float_or_none(current.get("liquidity")) or 0.0
    candidate_liquidity = _float_or_none(candidate.get("liquidity")) or 0.0
    return float(candidate_liquidity) > float(current_liquidity) + float(tol)


def _finalize_unfilled_side_payload(*, state: dict[str, object]) -> dict[str, object]:
    status = "orderbook_limit_reject" if bool(state.get("saw_orderbook")) else "orderbook_missing"
    return {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": status}


def _resolve_candidate_total_rows(raw_depth_candidates: list[dict[str, object]]) -> int:
    if not raw_depth_candidates:
        return 0
    first_candidate = raw_depth_candidates[0]
    if isinstance(first_candidate, dict):
        total = _int_or_none(first_candidate.get("depth_candidate_total_count"))
        if total is not None and total > 0:
            return int(total)
    return int(len(raw_depth_candidates))


def _side_candidate_scan_stats(state: dict[str, object]) -> dict[str, int]:
    return {
        "candidate_examined_rows": int(state.get("candidate_examined_rows") or 0),
        "candidate_orderbook_missing_rows": int(state.get("candidate_orderbook_missing_rows") or 0),
        "candidate_price_reject_rows": int(state.get("candidate_price_reject_rows") or 0),
        "candidate_depth_reject_rows": int(state.get("candidate_depth_reject_rows") or 0),
        "candidate_fillable_rows": int(state.get("candidate_fillable_rows") or 0),
    }


def build_empty_initial_snapshot_decision_summary() -> InitialSnapshotDecisionSummary:
    return InitialSnapshotDecisionSummary(
        raw_depth_rows=0,
        repriced_rows=0,
        limit_reject_rows=0,
        orderbook_missing_rows=0,
        candidate_total_rows=0,
        candidate_examined_rows=0,
        signal_rows=0,
        signal_candidate_total_rows=0,
        signal_candidate_examined_rows=0,
        signal_candidate_orderbook_missing_rows=0,
        signal_candidate_price_reject_rows=0,
        signal_candidate_depth_reject_rows=0,
        signal_candidate_fillable_rows=0,
    )


def summarize_initial_snapshot_decision_surface(frame: pd.DataFrame) -> InitialSnapshotDecisionSummary:
    if frame.empty:
        return build_empty_initial_snapshot_decision_summary()
    signal_side = _signal_side_series(frame)
    signal_mask = signal_side.notna()
    return InitialSnapshotDecisionSummary(
        raw_depth_rows=int(_bool_series(frame, "decision_quote_has_raw_depth", default=False).sum()),
        repriced_rows=int(_bool_series(frame, "decision_quote_any_repriced", default=False).sum()),
        limit_reject_rows=int(_bool_series(frame, "decision_quote_limit_reject", default=False).sum()),
        orderbook_missing_rows=int(_bool_series(frame, "decision_quote_orderbook_missing", default=False).sum()),
        candidate_total_rows=int(pd.to_numeric(frame.get("decision_quote_candidate_total_rows", 0), errors="coerce").fillna(0).sum()),
        candidate_examined_rows=int(pd.to_numeric(frame.get("decision_quote_candidate_examined_rows", 0), errors="coerce").fillna(0).sum()),
        signal_rows=int(signal_mask.sum()),
        signal_candidate_total_rows=int(pd.to_numeric(frame.loc[signal_mask, "decision_quote_candidate_total_rows"], errors="coerce").fillna(0).sum()),
        signal_candidate_examined_rows=_selected_side_counter_sum(frame, signal_side=signal_side, suffix="candidate_examined_rows"),
        signal_candidate_orderbook_missing_rows=_selected_side_counter_sum(frame, signal_side=signal_side, suffix="candidate_orderbook_missing_rows"),
        signal_candidate_price_reject_rows=_selected_side_counter_sum(frame, signal_side=signal_side, suffix="candidate_price_reject_rows"),
        signal_candidate_depth_reject_rows=_selected_side_counter_sum(frame, signal_side=signal_side, suffix="candidate_depth_reject_rows"),
        signal_candidate_fillable_rows=_selected_side_counter_sum(frame, signal_side=signal_side, suffix="candidate_fillable_rows"),
    )


def _resolve_side_target_notional(
    *,
    probability: float,
    fill_config: BacktestFillConfig,
) -> float:
    base = max(0.0, float(fill_config.base_stake))
    cap = float(fill_config.max_stake if fill_config.max_stake is not None else base)
    threshold = float(fill_config.high_conf_threshold)
    multiplier = max(1.0, float(fill_config.high_conf_multiplier))
    target = base * multiplier if float(probability) > threshold else base
    return min(target, cap)


def _resolve_probability_price_cap(
    *,
    probability: float,
    roi_threshold: float,
    fee_rate: float,
    slippage_bps: float,
) -> float:
    prob = max(1e-6, min(float(probability), 1.0))
    slip = max(0.0, float(slippage_bps)) / 10_000.0
    denom = (1.0 + float(roi_threshold) + float(fee_rate)) * (1.0 + slip)
    return max(1e-6, min(prob / max(denom, 1e-9), 1.0))


def _build_replay_key_rows(frame: pd.DataFrame) -> list[tuple[object, ...]]:
    columns: dict[str, list[object]] = {}
    for column in REPLAY_KEY_COLUMNS:
        if column == "offset":
            raw_values = frame[column].tolist() if column in frame.columns else [None] * len(frame)
            columns[column] = [_int_or_none(value) for value in raw_values]
            continue
        raw_series = frame[column] if column in frame.columns else pd.Series(index=frame.index, dtype="object")
        normalized = pd.to_datetime(raw_series, utc=True, errors="coerce")
        columns[column] = [None if pd.isna(value) else pd.Timestamp(value).isoformat() for value in normalized.tolist()]
    return [tuple(columns[column][idx] for column in REPLAY_KEY_COLUMNS) for idx in range(len(frame))]


def _apply_decision_engine_reject(
    frame: pd.DataFrame,
    *,
    mask: pd.Series,
    reason: str,
) -> None:
    frame.loc[mask, "decision_engine_action"] = "reject"
    frame.loc[mask, "decision_engine_reason"] = str(reason)
    frame.loc[mask, "decision_engine_side"] = pd.NA
    frame.loc[mask, "decision_engine_rationale"] = ""
    for column in (
        "decision_engine_entry_price",
        "decision_engine_prob",
        "decision_engine_probability_gap",
        "decision_engine_edge",
        "decision_engine_roi",
        "decision_engine_roi_net",
    ):
        frame.loc[mask, column] = pd.NA


def _signal_side_series(frame: pd.DataFrame) -> pd.Series:
    values = frame.get(
        "decision_engine_signal_side",
        frame.get("decision_engine_side", pd.Series(pd.NA, index=frame.index, dtype="string")),
    )
    normalized = values.astype("string").str.upper()
    normalized.loc[~normalized.isin(["UP", "DOWN"])] = pd.NA
    return normalized


def _selected_side_counter_sum(
    frame: pd.DataFrame,
    *,
    signal_side: pd.Series,
    suffix: str,
) -> int:
    total = 0
    for side in ("UP", "DOWN"):
        mask = signal_side.eq(side)
        if not bool(mask.any()):
            continue
        column = f"decision_quote_{side.lower()}_{suffix}"
        total += int(pd.to_numeric(frame.loc[mask, column], errors="coerce").fillna(0).sum())
    return int(total)


def _bool_series(frame: pd.DataFrame, column: str, *, default: bool) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series(default, index=frame.index, dtype="boolean")
    return values.astype("boolean").fillna(default).astype(bool)


def _int_or_none(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        parsed = float(value)
    except Exception:
        return None
    if parsed != parsed:
        return None
    return parsed
