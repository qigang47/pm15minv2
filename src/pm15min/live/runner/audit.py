from __future__ import annotations

import hashlib
import json
from typing import Any

from pm15min.research.inference.scorer import load_offset_model_context


def build_runner_decision_audit_event(
    *,
    iteration_payload: dict[str, object],
    iteration: int,
    emitted_at: str,
) -> tuple[str, dict[str, object]] | None:
    decision_payload = _as_dict(iteration_payload.get("decision_payload"))
    execution_payload = _as_dict(iteration_payload.get("execution_payload"))
    decision = _as_dict(decision_payload.get("decision"))
    accepted_offsets = _as_dict_list(decision_payload.get("accepted_offsets"))
    rejected_offsets = _as_dict_list(decision_payload.get("rejected_offsets"))

    selected_row = _find_offset_row(accepted_offsets, decision.get("selected_offset"))
    best_rejected_row = _best_offset_row(rejected_offsets)
    signal_bundle = _signal_bundle_context(decision_payload)
    execution_context = _compact_execution_payload(execution_payload)
    selected_model_context = _offset_model_context(signal_bundle=signal_bundle, row=selected_row)
    best_rejected_model_context = _offset_model_context(signal_bundle=signal_bundle, row=best_rejected_row)
    signature = _signature(
        {
            "market": iteration_payload.get("market"),
            "profile": iteration_payload.get("profile"),
            "cycle": iteration_payload.get("cycle"),
            "target": iteration_payload.get("target"),
            "decision": _decision_signature(decision),
            "accepted_offsets": [_offset_signature(row) for row in accepted_offsets],
            "rejected_offsets": [_offset_signature(row) for row in rejected_offsets],
            "execution": _execution_signature(execution_context),
        }
    )
    return (
        signature,
        {
            "ts": emitted_at,
            "event": "decision_state",
            "iteration": int(iteration),
            "snapshot_ts": iteration_payload.get("snapshot_ts"),
            "market": iteration_payload.get("market"),
            "profile": iteration_payload.get("profile"),
            "cycle": iteration_payload.get("cycle"),
            "target": iteration_payload.get("target"),
            "signal_bundle": signal_bundle,
            "decision": _compact_decision(decision),
            "selected_offset_context": None if selected_row is None else _compact_offset_row(selected_row),
            "best_rejected_offset_context": None if best_rejected_row is None else _compact_offset_row(best_rejected_row),
            "selected_model_context": selected_model_context,
            "best_rejected_model_context": best_rejected_model_context,
            "selected_factor_snapshot": _factor_snapshot(row=selected_row, model_context=selected_model_context),
            "best_rejected_factor_snapshot": _factor_snapshot(row=best_rejected_row, model_context=best_rejected_model_context),
            "accepted_offset_summaries": [_compact_offset_summary(row) for row in accepted_offsets],
            "rejected_offset_summaries": [_compact_offset_summary(row) for row in rejected_offsets],
            "execution": execution_context,
            "risk_alert_summary": _as_dict(iteration_payload.get("risk_alert_summary")),
        },
    )


def build_runner_order_audit_event(
    *,
    iteration_payload: dict[str, object],
    iteration: int,
    emitted_at: str,
) -> tuple[str, dict[str, object]] | None:
    order_action_payload = _as_dict(iteration_payload.get("order_action_payload"))
    if not order_action_payload:
        return None
    if not _order_action_is_relevant(order_action_payload):
        return None

    decision_payload = _as_dict(iteration_payload.get("decision_payload"))
    execution_payload = _as_dict(iteration_payload.get("execution_payload"))
    decision = _as_dict(decision_payload.get("decision"))
    signal_bundle = _signal_bundle_context(decision_payload)
    accepted_offsets = _as_dict_list(decision_payload.get("accepted_offsets"))
    selected_row = _find_offset_row(accepted_offsets, decision.get("selected_offset"))
    selected_model_context = _offset_model_context(signal_bundle=signal_bundle, row=selected_row)
    execution_context = _compact_execution_payload(execution_payload)
    order_context = _compact_order_action_payload(order_action_payload)
    signature = _signature(
        {
            "market": iteration_payload.get("market"),
            "profile": iteration_payload.get("profile"),
            "cycle": iteration_payload.get("cycle"),
            "target": iteration_payload.get("target"),
            "decision": _decision_signature(decision),
            "execution": _execution_signature(execution_context),
            "order_action": _order_action_signature(order_context),
        }
    )
    return (
        signature,
        {
            "ts": emitted_at,
            "event": "order_action",
            "iteration": int(iteration),
            "snapshot_ts": iteration_payload.get("snapshot_ts"),
            "market": iteration_payload.get("market"),
            "profile": iteration_payload.get("profile"),
            "cycle": iteration_payload.get("cycle"),
            "target": iteration_payload.get("target"),
            "signal_bundle": signal_bundle,
            "decision": _compact_decision(decision),
            "selected_offset_context": None if selected_row is None else _compact_offset_row(selected_row),
            "selected_model_context": selected_model_context,
            "selected_factor_snapshot": _factor_snapshot(row=selected_row, model_context=selected_model_context),
            "execution": execution_context,
            "order_action": order_context,
            "risk_alert_summary": _as_dict(iteration_payload.get("risk_alert_summary")),
        },
    )


def _signal_bundle_context(decision_payload: dict[str, object]) -> dict[str, object]:
    return {
        "bundle_dir": decision_payload.get("bundle_dir"),
        "bundle_label": decision_payload.get("bundle_label"),
        "builder_feature_set": decision_payload.get("builder_feature_set"),
        "bundle_feature_set": decision_payload.get("bundle_feature_set"),
        "active_bundle_selection_path": decision_payload.get("active_bundle_selection_path"),
    }


def _compact_decision(decision: dict[str, object]) -> dict[str, object]:
    return {
        "status": decision.get("status"),
        "selected_offset": _int_or_none(decision.get("selected_offset")),
        "selected_side": decision.get("selected_side"),
        "selected_confidence": _float_or_none(decision.get("selected_confidence")),
        "selected_edge": _float_or_none(decision.get("selected_edge")),
        "selected_decision_ts": decision.get("selected_decision_ts"),
        "selected_window_start_ts": decision.get("selected_window_start_ts"),
        "selected_window_end_ts": decision.get("selected_window_end_ts"),
        "selected_window_duration_seconds": _float_or_none(decision.get("selected_window_duration_seconds")),
        "selected_quote_market_id": decision.get("selected_quote_market_id"),
        "selected_entry_price": _float_or_none(decision.get("selected_entry_price")),
        "selected_roi_net_vs_quote": _float_or_none(decision.get("selected_roi_net_vs_quote")),
        "selected_trigger_metric": decision.get("selected_trigger_metric"),
        "selected_trigger_probability": _float_or_none(decision.get("selected_trigger_probability")),
        "selected_p_up_raw": _float_or_none(decision.get("selected_p_up_raw")),
        "selected_p_up_lcb": _float_or_none(decision.get("selected_p_up_lcb")),
        "selected_p_up_ucb": _float_or_none(decision.get("selected_p_up_ucb")),
        "selected_p_lgb": _float_or_none(decision.get("selected_p_lgb")),
        "selected_p_lr": _float_or_none(decision.get("selected_p_lr")),
        "selected_p_signal": _float_or_none(decision.get("selected_p_signal")),
        "selected_w_lgb": _float_or_none(decision.get("selected_w_lgb")),
        "selected_w_lr": _float_or_none(decision.get("selected_w_lr")),
        "selected_probability_mode": decision.get("selected_probability_mode"),
    }


def _compact_offset_row(row: dict[str, object]) -> dict[str, object]:
    quote_row = _as_dict(row.get("quote_row"))
    return {
        "offset": _int_or_none(row.get("offset")),
        "decision_ts": row.get("decision_ts"),
        "window_start_ts": row.get("window_start_ts"),
        "window_end_ts": row.get("window_end_ts"),
        "window_duration_seconds": _float_or_none(row.get("window_duration_seconds")),
        "signal_target": row.get("signal_target"),
        "recommended_side": row.get("recommended_side"),
        "model_recommended_side": row.get("model_recommended_side"),
        "trigger_side": row.get("trigger_side"),
        "trigger_metric": row.get("trigger_metric"),
        "threshold": _float_or_none(row.get("threshold")),
        "score_valid": bool(row.get("score_valid")),
        "score_reason": row.get("score_reason"),
        "confidence": _float_or_none(row.get("confidence")),
        "edge": _float_or_none(row.get("edge")),
        "model_confidence": _float_or_none(row.get("model_confidence")),
        "model_edge": _float_or_none(row.get("model_edge")),
        "candidate_probability": _float_or_none(row.get("candidate_probability")),
        "opposite_probability": _float_or_none(row.get("opposite_probability")),
        "trigger_probability": _float_or_none(row.get("trigger_probability")),
        "p_up_raw": _float_or_none(row.get("p_up_raw")),
        "p_up_lcb": _float_or_none(row.get("p_up_lcb")),
        "p_up_ucb": _float_or_none(row.get("p_up_ucb")),
        "p_lgb": _float_or_none(row.get("p_lgb")),
        "p_lr": _float_or_none(row.get("p_lr")),
        "p_signal": _float_or_none(row.get("p_signal")),
        "p_up": _float_or_none(row.get("p_up")),
        "p_down": _float_or_none(row.get("p_down")),
        "w_lgb": _float_or_none(row.get("w_lgb")),
        "w_lr": _float_or_none(row.get("w_lr")),
        "probability_mode": row.get("probability_mode"),
        "guard_reasons": [str(item) for item in list(row.get("guard_reasons") or []) if str(item)],
        "coverage": _coverage_summary(row.get("coverage")),
        "quote": {
            "status": quote_row.get("status"),
            "reasons": list(quote_row.get("reasons") or []),
            "market_id": quote_row.get("market_id"),
        },
        "quote_metrics": _compact_quote_metrics(row.get("quote_metrics")),
        "feature_snapshot": dict(row.get("feature_snapshot") or {}),
    }


def _compact_execution_payload(execution_payload: dict[str, object]) -> dict[str, object]:
    execution = _as_dict(execution_payload.get("execution"))
    depth_plan = _as_dict(execution.get("depth_plan"))
    return {
        "status": execution.get("status"),
        "reason": execution.get("reason"),
        "execution_reasons": [str(item) for item in list(execution.get("execution_reasons") or []) if str(item)],
        "selected_offset": _int_or_none(execution.get("selected_offset")),
        "selected_side": execution.get("selected_side"),
        "decision_ts": execution.get("decision_ts"),
        "window_start_ts": execution.get("window_start_ts"),
        "window_end_ts": execution.get("window_end_ts"),
        "window_duration_seconds": _float_or_none(execution.get("window_duration_seconds")),
        "market_id": execution.get("market_id"),
        "order_type": execution.get("order_type"),
        "entry_price": _float_or_none(execution.get("entry_price")),
        "stake_base_usd": _float_or_none(execution.get("stake_base_usd")),
        "stake_multiplier": _float_or_none(execution.get("stake_multiplier")),
        "stake_regime_state": execution.get("stake_regime_state"),
        "stake_source": execution.get("stake_source"),
        "requested_notional_usd": _float_or_none(execution.get("requested_notional_usd")),
        "l1_fill_ratio": _float_or_none(execution.get("l1_fill_ratio")),
        "fee_rate": _float_or_none(execution.get("fee_rate")),
        "slippage_bps": _float_or_none(execution.get("slippage_bps")),
        "roi_net_vs_quote": _float_or_none(execution.get("roi_net_vs_quote")),
        "price_cap": _float_or_none(execution.get("price_cap")),
        "depth": {
            "status": depth_plan.get("status"),
            "stop_reason": depth_plan.get("stop_reason"),
            "max_price": _float_or_none(depth_plan.get("max_price")),
            "avg_price": _float_or_none(depth_plan.get("avg_price")),
            "fill_ratio": _float_or_none(depth_plan.get("fill_ratio")),
        },
    }


def _compact_order_action_payload(order_action_payload: dict[str, object]) -> dict[str, object]:
    gate = _as_dict(order_action_payload.get("gate"))
    order_request = _as_dict(order_action_payload.get("order_request"))
    order_response = _as_dict(order_action_payload.get("order_response"))
    trading_gateway = _as_dict(order_action_payload.get("trading_gateway"))
    return {
        "status": order_action_payload.get("status"),
        "reason": order_action_payload.get("reason"),
        "dry_run": bool(order_action_payload.get("dry_run")),
        "attempt": _int_or_none(order_action_payload.get("attempt")),
        "attempted": bool(order_action_payload.get("attempted")),
        "action_key": order_action_payload.get("action_key"),
        "decision_window_start_ts": order_action_payload.get("decision_window_start_ts"),
        "decision_window_end_ts": order_action_payload.get("decision_window_end_ts"),
        "decision_age_seconds": _float_or_none(order_action_payload.get("decision_age_seconds")),
        "decision_window_remaining_seconds": _float_or_none(order_action_payload.get("decision_window_remaining_seconds")),
        "gate": {
            "decision": gate.get("decision"),
            "reason": gate.get("reason"),
            "current_attempt": gate.get("current_attempt"),
        },
        "order_request": {
            "token_id": order_request.get("token_id"),
            "side": order_request.get("side"),
            "price": _float_or_none(order_request.get("price")),
            "size": _float_or_none(order_request.get("size")),
            "window_start_ts": order_request.get("window_start_ts"),
            "window_end_ts": order_request.get("window_end_ts"),
        },
        "order_response": {
            "success": bool(order_response.get("success")),
            "status": order_response.get("status"),
            "order_id": order_response.get("order_id"),
            "message": order_response.get("message"),
        },
        "trading_gateway": {
            "adapter": trading_gateway.get("adapter"),
        },
    }


def _order_action_is_relevant(order_action_payload: dict[str, object]) -> bool:
    reason = str(order_action_payload.get("reason") or "")
    status = str(order_action_payload.get("status") or "").strip().lower()
    return bool(order_action_payload.get("attempted")) or status == "error" or (bool(reason) and not reason.startswith("execution_not_plan:"))


def _decision_signature(decision: dict[str, object]) -> dict[str, object]:
    return {
        "status": decision.get("status"),
        "selected_offset": _int_or_none(decision.get("selected_offset")),
        "selected_side": decision.get("selected_side"),
        "selected_decision_ts": decision.get("selected_decision_ts"),
        "selected_window_start_ts": decision.get("selected_window_start_ts"),
        "selected_window_end_ts": decision.get("selected_window_end_ts"),
        "selected_quote_market_id": decision.get("selected_quote_market_id"),
        "selected_p_signal": _round_float(decision.get("selected_p_signal")),
        "selected_confidence": _round_float(decision.get("selected_confidence")),
        "selected_edge": _round_float(decision.get("selected_edge")),
    }


def _offset_signature(row: dict[str, object]) -> dict[str, object]:
    coverage = _as_dict(row.get("coverage"))
    return {
        "offset": _int_or_none(row.get("offset")),
        "decision_ts": row.get("decision_ts"),
        "window_start_ts": row.get("window_start_ts"),
        "window_end_ts": row.get("window_end_ts"),
        "recommended_side": row.get("recommended_side"),
        "score_valid": bool(row.get("score_valid")),
        "score_reason": row.get("score_reason"),
        "threshold": _round_float(row.get("threshold")),
        "confidence": _round_float(row.get("confidence")),
        "edge": _round_float(row.get("edge")),
        "p_signal": _round_float(row.get("p_signal")),
        "p_up": _round_float(row.get("p_up")),
        "p_down": _round_float(row.get("p_down")),
        "guard_reasons": [str(item) for item in list(row.get("guard_reasons") or []) if str(item)],
        "effective_missing_feature_count": _int_or_none(coverage.get("effective_missing_feature_count")),
        "not_allowed_blacklist_count": _int_or_none(coverage.get("not_allowed_blacklist_count")),
        "nan_feature_count": _int_or_none(coverage.get("nan_feature_count")),
    }


def _execution_signature(execution: dict[str, object]) -> dict[str, object]:
    return {
        "status": execution.get("status"),
        "reason": execution.get("reason"),
        "execution_reasons": list(execution.get("execution_reasons") or []),
        "selected_offset": execution.get("selected_offset"),
        "selected_side": execution.get("selected_side"),
        "decision_ts": execution.get("decision_ts"),
        "window_start_ts": execution.get("window_start_ts"),
        "window_end_ts": execution.get("window_end_ts"),
        "market_id": execution.get("market_id"),
        "entry_price": _round_float(execution.get("entry_price")),
        "requested_notional_usd": _round_float(execution.get("requested_notional_usd")),
        "price_cap": _round_float(execution.get("price_cap")),
    }


def _order_action_signature(order_action: dict[str, object]) -> dict[str, object]:
    order_response = _as_dict(order_action.get("order_response"))
    order_request = _as_dict(order_action.get("order_request"))
    gate = _as_dict(order_action.get("gate"))
    return {
        "status": order_action.get("status"),
        "reason": order_action.get("reason"),
        "dry_run": bool(order_action.get("dry_run")),
        "attempt": _int_or_none(order_action.get("attempt")),
        "attempted": bool(order_action.get("attempted")),
        "action_key": order_action.get("action_key"),
        "gate_decision": gate.get("decision"),
        "gate_reason": gate.get("reason"),
        "order_request": {
            "price": _round_float(order_request.get("price")),
            "size": _round_float(order_request.get("size")),
            "side": order_request.get("side"),
            "token_id": order_request.get("token_id"),
        },
        "order_response": {
            "success": bool(order_response.get("success")),
            "status": order_response.get("status"),
            "order_id": order_response.get("order_id"),
            "message": order_response.get("message"),
        },
    }


def _offset_model_context(*, signal_bundle: dict[str, object], row: dict[str, object] | None) -> dict[str, object] | None:
    if row is None:
        return None
    offset = _int_or_none(row.get("offset"))
    bundle_dir = signal_bundle.get("bundle_dir")
    if offset is None or not isinstance(bundle_dir, str) or not bundle_dir:
        return None
    return load_offset_model_context_from_bundle(bundle_dir=bundle_dir, offset=offset)


def load_offset_model_context_from_bundle(*, bundle_dir: str, offset: int) -> dict[str, object] | None:
    try:
        from pathlib import Path

        return load_offset_model_context(Path(bundle_dir) / "offsets" / f"offset={int(offset)}")
    except Exception:
        return None


def _factor_snapshot(
    *,
    row: dict[str, object] | None,
    model_context: dict[str, object] | None,
) -> dict[str, object] | None:
    if row is None or not isinstance(model_context, dict):
        return None
    feature_snapshot = _as_dict(row.get("feature_snapshot"))
    if not feature_snapshot:
        return None
    return {
        "top_positive_factors": _annotate_factor_rows(
            factors=_as_dict_list(model_context.get("top_positive_factors")),
            feature_snapshot=feature_snapshot,
        ),
        "top_negative_factors": _annotate_factor_rows(
            factors=_as_dict_list(model_context.get("top_negative_factors")),
            feature_snapshot=feature_snapshot,
        ),
    }


def _annotate_factor_rows(
    *,
    factors: list[dict[str, object]],
    feature_snapshot: dict[str, object],
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for row in factors:
        feature = str(row.get("feature") or "")
        if not feature:
            continue
        out.append(
            {
                **row,
                "live_value": feature_snapshot.get(feature),
            }
        )
    return out


def _compact_offset_summary(row: dict[str, object]) -> dict[str, object]:
    return {
        "offset": _int_or_none(row.get("offset")),
        "decision_ts": row.get("decision_ts"),
        "window_start_ts": row.get("window_start_ts"),
        "window_end_ts": row.get("window_end_ts"),
        "recommended_side": row.get("recommended_side"),
        "model_recommended_side": row.get("model_recommended_side"),
        "trigger_side": row.get("trigger_side"),
        "trigger_metric": row.get("trigger_metric"),
        "score_valid": bool(row.get("score_valid")),
        "score_reason": row.get("score_reason"),
        "confidence": _float_or_none(row.get("confidence")),
        "edge": _float_or_none(row.get("edge")),
        "model_confidence": _float_or_none(row.get("model_confidence")),
        "model_edge": _float_or_none(row.get("model_edge")),
        "candidate_probability": _float_or_none(row.get("candidate_probability")),
        "opposite_probability": _float_or_none(row.get("opposite_probability")),
        "trigger_probability": _float_or_none(row.get("trigger_probability")),
        "p_up_raw": _float_or_none(row.get("p_up_raw")),
        "p_up_lcb": _float_or_none(row.get("p_up_lcb")),
        "p_up_ucb": _float_or_none(row.get("p_up_ucb")),
        "p_signal": _float_or_none(row.get("p_signal")),
        "w_lgb": _float_or_none(row.get("w_lgb")),
        "w_lr": _float_or_none(row.get("w_lr")),
        "guard_reasons": [str(item) for item in list(row.get("guard_reasons") or []) if str(item)],
    }


def _coverage_summary(value: object) -> dict[str, object]:
    coverage = _as_dict(value)
    return {
        "coverage_ratio": _float_or_none(coverage.get("coverage_ratio")),
        "effective_missing_feature_count": _int_or_none(coverage.get("effective_missing_feature_count")),
        "not_allowed_blacklist_count": _int_or_none(coverage.get("not_allowed_blacklist_count")),
        "nan_feature_count": _int_or_none(coverage.get("nan_feature_count")),
    }


def _compact_quote_metrics(value: object) -> dict[str, object]:
    metrics = _as_dict(value)
    depth_plan = _as_dict(metrics.get("depth_plan"))
    return {
        "quote_status": metrics.get("quote_status"),
        "entry_side": metrics.get("entry_side"),
        "entry_price": _float_or_none(metrics.get("entry_price")),
        "effective_entry_price": _float_or_none(metrics.get("effective_entry_price")),
        "edge_vs_quote": _float_or_none(metrics.get("edge_vs_quote")),
        "roi_net_vs_quote": _float_or_none(metrics.get("roi_net_vs_quote")),
        "price_cap": _float_or_none(metrics.get("price_cap")),
        "requested_notional_usd": _float_or_none(metrics.get("requested_notional_usd")),
        "stake_source": metrics.get("stake_source"),
        "depth": {
            "status": depth_plan.get("status"),
            "stop_reason": depth_plan.get("stop_reason"),
            "max_price": _float_or_none(depth_plan.get("max_price")),
            "avg_price": _float_or_none(depth_plan.get("avg_price")),
            "fill_ratio": _float_or_none(depth_plan.get("fill_ratio")),
        },
        "depth_reason": metrics.get("depth_reason"),
    }


def _find_offset_row(rows: list[dict[str, object]], offset: object) -> dict[str, object] | None:
    wanted = _int_or_none(offset)
    if wanted is None:
        return None
    for row in rows:
        if _int_or_none(row.get("offset")) == wanted:
            return row
    return None


def _best_offset_row(rows: list[dict[str, object]]) -> dict[str, object] | None:
    if not rows:
        return None
    return max(
        rows,
        key=lambda row: (
            _float_or_none(row.get("confidence")) or 0.0,
            _float_or_none(row.get("edge")) or 0.0,
            -(_int_or_none(row.get("offset")) or 0),
        ),
    )


def _signature(payload: dict[str, object]) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _as_dict(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


def _as_dict_list(value: object) -> list[dict[str, object]]:
    return [dict(item) for item in list(value or []) if isinstance(item, dict)]


def _float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _int_or_none(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _round_float(value: object, digits: int = 8) -> float | None:
    out = _float_or_none(value)
    return None if out is None else round(out, digits)
