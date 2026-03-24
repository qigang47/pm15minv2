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
    for column in (
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
    ):
        if column not in frame.columns:
            frame[column] = pd.NA
    if depth_replay is None or depth_replay.empty:
        frame["decision_quote_has_raw_depth"] = False
        frame["decision_quote_any_repriced"] = False
        frame["decision_quote_limit_reject"] = False
        frame["decision_quote_orderbook_missing"] = False
        return frame

    lookup = _build_depth_candidate_lookup(depth_replay)
    for idx, row in frame.iterrows():
        candidates = lookup.get(_replay_key_tuple(row))
        if not candidates:
            frame.at[idx, "decision_quote_has_raw_depth"] = False
            frame.at[idx, "decision_quote_any_repriced"] = False
            frame.at[idx, "decision_quote_limit_reject"] = False
            frame.at[idx, "decision_quote_orderbook_missing"] = False
            continue
        frame.at[idx, "decision_quote_has_raw_depth"] = True
        initial_snapshot = candidates[0]
        up_payload = _resolve_initial_side_depth_quote(
            row=row,
            raw_depth_candidate=initial_snapshot,
            side="UP",
            profile_spec=profile_spec,
            fill_config=fill_config,
        )
        down_payload = _resolve_initial_side_depth_quote(
            row=row,
            raw_depth_candidate=initial_snapshot,
            side="DOWN",
            profile_spec=profile_spec,
            fill_config=fill_config,
        )
        frame.at[idx, "decision_quote_up_ask"] = up_payload["price"]
        frame.at[idx, "decision_quote_down_ask"] = down_payload["price"]
        frame.at[idx, "decision_quote_up_liquidity"] = up_payload["liquidity"]
        frame.at[idx, "decision_quote_down_liquidity"] = down_payload["liquidity"]
        frame.at[idx, "decision_quote_up_status"] = up_payload["status"]
        frame.at[idx, "decision_quote_down_status"] = down_payload["status"]
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
        frame.at[idx, "decision_quote_any_repriced"] = any_repriced
        frame.at[idx, "decision_quote_limit_reject"] = limit_reject
        frame.at[idx, "decision_quote_orderbook_missing"] = orderbook_missing
    return frame


def _resolve_initial_side_depth_quote(
    *,
    row: pd.Series,
    raw_depth_candidate: dict[str, object],
    side: str,
    profile_spec: LiveProfileSpec,
    fill_config: BacktestFillConfig,
) -> dict[str, object]:
    side_key = "depth_up_record" if side == "UP" else "depth_down_record"
    probability = _float_or_none(row.get("p_up")) if side == "UP" else _float_or_none(row.get("p_down"))
    if probability is None:
        return {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "prob_missing"}
    record = raw_depth_candidate.get(side_key)
    if not isinstance(record, dict):
        return {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "orderbook_missing"}
    asks = normalize_levels(record.get("asks"))
    if not asks:
        return {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "orderbook_missing"}
    requested_notional = _resolve_side_target_notional(
        probability=probability,
        fill_config=fill_config,
    )
    if requested_notional <= 0.0:
        return {"price": 1.0, "liquidity": 0.0, "repriced": False, "status": "target_notional_nonpositive"}
    offset = _int_or_none(row.get("offset")) or 0
    roi_threshold = profile_spec.roi_threshold_for(offset=offset)
    best_price = float(asks[0][0])
    slip = max(0.0, float(profile_spec.slippage_bps)) / 10_000.0
    fee_rate = float(profile_spec.fee_rate(price=best_price * (1.0 + slip)))
    price_cap = _resolve_probability_price_cap(
        probability=probability,
        roi_threshold=roi_threshold,
        fee_rate=fee_rate,
        slippage_bps=float(profile_spec.slippage_bps),
    )
    max_slippage_bps = float(fill_config.depth_max_slippage_bps) if fill_config.depth_max_slippage_bps is not None else float(profile_spec.orderbook_max_slippage_bps)
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
