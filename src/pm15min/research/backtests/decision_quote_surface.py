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
from pm15min.research.backtests.fills import BacktestFillConfig
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
)


@dataclass(frozen=True)
class InitialSnapshotDecisionSummary:
    raw_depth_rows: int
    repriced_rows: int
    limit_reject_rows: int
    orderbook_missing_rows: int

    def to_dict(self) -> dict[str, int]:
        return {
            "raw_depth_rows": int(self.raw_depth_rows),
            "repriced_rows": int(self.repriced_rows),
            "limit_reject_rows": int(self.limit_reject_rows),
            "orderbook_missing_rows": int(self.orderbook_missing_rows),
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

    summary = InitialSnapshotDecisionSummary(
        raw_depth_rows=int(_bool_series(out, "decision_quote_has_raw_depth", default=False).sum()) if not out.empty else 0,
        repriced_rows=int(_bool_series(out, "decision_quote_any_repriced", default=False).sum()) if not out.empty else 0,
        limit_reject_rows=int(_bool_series(out, "decision_quote_limit_reject", default=False).sum()) if not out.empty else 0,
        orderbook_missing_rows=int(_bool_series(out, "decision_quote_orderbook_missing", default=False).sum()) if not out.empty else 0,
    )
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
        return frame

    lookup = _build_depth_candidate_lookup(depth_replay)
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
            continue
        surface_values["decision_quote_has_raw_depth"][row_idx] = True
        up_payload = _resolve_window_side_depth_quote(
            probability=_float_or_none(probability_up[row_idx]),
            offset=_int_or_none(offsets[row_idx]) or 0,
            raw_depth_candidates=candidates,
            side="UP",
            profile_spec=profile_spec,
            fill_config=fill_config,
        )
        down_payload = _resolve_window_side_depth_quote(
            probability=_float_or_none(probability_down[row_idx]),
            offset=_int_or_none(offsets[row_idx]) or 0,
            raw_depth_candidates=candidates,
            side="DOWN",
            profile_spec=profile_spec,
            fill_config=fill_config,
        )
        surface_values["decision_quote_up_ask"][row_idx] = up_payload["price"]
        surface_values["decision_quote_down_ask"][row_idx] = down_payload["price"]
        surface_values["decision_quote_up_liquidity"][row_idx] = up_payload["liquidity"]
        surface_values["decision_quote_down_liquidity"][row_idx] = down_payload["liquidity"]
        surface_values["decision_quote_up_status"][row_idx] = up_payload["status"]
        surface_values["decision_quote_down_status"][row_idx] = down_payload["status"]
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


def _resolve_window_side_depth_quote(
    *,
    probability: float | None,
    offset: int,
    raw_depth_candidates: list[dict[str, object]],
    side: str,
    profile_spec: LiveProfileSpec,
    fill_config: BacktestFillConfig,
) -> dict[str, object]:
    side_key = "depth_up_record" if side == "UP" else "depth_down_record"
    if probability is None:
        return {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "prob_missing"}
    if not raw_depth_candidates:
        return {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "orderbook_missing"}
    requested_notional = _resolve_side_target_notional(
        probability=probability,
        fill_config=fill_config,
    )
    if requested_notional <= 0.0:
        return {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "target_notional_nonpositive"}
    roi_threshold = profile_spec.roi_threshold_for(offset=offset)
    max_slippage_bps = (
        float(fill_config.depth_max_slippage_bps)
        if fill_config.depth_max_slippage_bps is not None
        else float(profile_spec.orderbook_max_slippage_bps)
    )
    first_candidate = raw_depth_candidates[0]
    record = first_candidate.get(side_key)
    if not isinstance(record, dict):
        return {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "orderbook_missing"}
    asks = normalize_levels(record.get("asks"))
    if not asks:
        return {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "orderbook_missing"}
    best_price = float(asks[0][0])
    slip = max(0.0, float(profile_spec.slippage_bps)) / 10_000.0
    fee_rate = float(profile_spec.fee_rate(price=best_price * (1.0 + slip)))
    price_cap = _resolve_probability_price_cap(
        probability=probability,
        roi_threshold=roi_threshold,
        fee_rate=fee_rate,
        slippage_bps=float(profile_spec.slippage_bps),
    )
    fill = compute_fill_from_depth_record(
        record=record,
        target_notional=requested_notional,
        max_slippage_bps=max_slippage_bps,
        price_cap=price_cap,
    )
    if fill is None:
        return {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "orderbook_limit_reject"}
    max_price = _float_or_none(fill.get("max_price"))
    total_cost = _float_or_none(fill.get("total_cost")) or 0.0
    liquidity = 0.0 if not max_price or max_price <= 0 else float(total_cost / max_price)
    return {
        "price": float(max_price or 1.0),
        "liquidity": float(liquidity),
        "repriced": True,
        "status": str(fill.get("stop_reason") or "ok"),
    }


def build_empty_initial_snapshot_decision_summary() -> InitialSnapshotDecisionSummary:
    return InitialSnapshotDecisionSummary(
        raw_depth_rows=0,
        repriced_rows=0,
        limit_reject_rows=0,
        orderbook_missing_rows=0,
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


def _build_depth_candidate_lookup(depth_replay: pd.DataFrame) -> dict[tuple[object, ...], list[dict[str, object]]]:
    frame = depth_replay.copy()
    if "depth_snapshot_rank" in frame.columns:
        frame["depth_snapshot_rank"] = pd.to_numeric(frame["depth_snapshot_rank"], errors="coerce")
        frame = frame.sort_values([*REPLAY_KEY_COLUMNS, "depth_snapshot_rank"], kind="stable", na_position="last")
    lookup: dict[tuple[object, ...], list[dict[str, object]]] = {}
    for record in frame.to_dict(orient="records"):
        lookup.setdefault(_replay_key_tuple(record), []).append(record)
    return lookup


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


def _replay_key_tuple(row: pd.Series | dict[str, object]) -> tuple[object, ...]:
    out: list[object] = []
    for column in REPLAY_KEY_COLUMNS:
        value = row.get(column) if isinstance(row, dict) else row.get(column)
        if column == "offset":
            out.append(_int_or_none(value))
        else:
            ts = pd.to_datetime(value, utc=True, errors="coerce")
            out.append(None if pd.isna(ts) else pd.Timestamp(ts).isoformat())
    return tuple(out)


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
