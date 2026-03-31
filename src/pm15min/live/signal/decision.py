from __future__ import annotations
import os
from pathlib import Path
from typing import Any

from pm15min.data.config import DataConfig
from pm15min.data.layout import utc_snapshot_label
from ..guards import evaluate_signal_guard_reasons
from ..layout import LiveStateLayout
from ..persistence import write_live_payload_pair
from ..profiles import resolve_live_profile_spec
from ..execution.depth import build_depth_execution_plan
from ..execution.policy_helpers import (
    repriced_order_guard,
    resolve_dynamic_stake_base,
    resolve_regime_stake_multiplier,
)
from ..execution.utils import float_or_none, resolve_probability_interval_view, resolve_side_probability


def build_decision_snapshot(
    signal_payload: dict[str, Any],
    quote_payload: dict[str, Any] | None = None,
    account_state_payload: dict[str, Any] | None = None,
    session_state: dict[str, Any] | None = None,
    rewrite_root=None,
    orderbook_provider=None,
) -> dict[str, Any]:
    market = str(signal_payload["market"])
    profile = str(signal_payload["profile"])
    cycle = str(signal_payload["cycle"])
    target = str(signal_payload["target"])
    spec = resolve_live_profile_spec(profile)
    liquidity_state = signal_payload.get("liquidity_state") or {}
    regime_state = signal_payload.get("regime_state") or {}
    account_state = account_state_payload or signal_payload.get("account_state") or {}
    account_summary = account_state.get("summary") if isinstance(account_state, dict) else None
    quote_by_offset = {
        int(row["offset"]): row
        for row in (quote_payload.get("quote_rows") or [])
        if isinstance(row, dict) and row.get("offset") is not None
    } if quote_payload else {}

    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for item in signal_payload.get("offset_signals") or []:
        offset = int(item["offset"])
        quote_row = quote_by_offset.get(offset)
        if quote_payload is not None and quote_row is None:
            quote_row = {
                "offset": offset,
                "status": "missing_quote_inputs",
                "reasons": ["quote_row_missing"],
            }
        accepted_candidates: list[dict[str, Any]] = []
        for candidate in _build_trade_candidates(signal_row=item, profile_spec=spec):
            quote_metrics_override = _build_decision_quote_metrics(
                market=market,
                cycle=cycle,
                rewrite_root=rewrite_root,
                profile_spec=spec,
                signal_row=candidate,
                quote_row=quote_row,
                regime_state=regime_state,
                account_state=account_state,
                orderbook_provider=orderbook_provider,
            )
            reasons, quote_metrics, account_context = evaluate_signal_guard_reasons(
                market=market,
                profile_spec=spec,
                signal_row=candidate,
                quote_row=quote_row,
                liquidity_state=liquidity_state,
                regime_state=regime_state,
                account_state=account_state,
                session_state=session_state,
                quote_metrics_override=quote_metrics_override,
            )
            threshold = spec.threshold_for(market=market, offset=offset)

            enriched = {
                **candidate,
                "threshold": threshold,
                "guard_reasons": reasons,
                "quote_row": quote_row,
                "quote_metrics": quote_metrics,
                "account_context": account_context,
            }
            if reasons:
                rejected.append(enriched)
            else:
                accepted_candidates.append(enriched)

        best_offset_candidate = _best_trade_candidate(accepted_candidates)
        if best_offset_candidate is not None:
            accepted.append(best_offset_candidate)

    best = _best_trade_candidate(accepted)
    snapshot_ts = utc_snapshot_label()
    payload = {
        "domain": "live",
        "dataset": "live_decision_snapshot",
        "snapshot_ts": snapshot_ts,
        "market": market,
        "profile": profile,
        "cycle": cycle,
        "target": target,
        "bundle_dir": signal_payload.get("bundle_dir"),
        "bundle_label": signal_payload.get("bundle_label"),
        "builder_feature_set": signal_payload.get("builder_feature_set"),
        "bundle_feature_set": signal_payload.get("bundle_feature_set"),
        "active_bundle": signal_payload.get("active_bundle"),
        "signal_snapshot_ts": signal_payload.get("snapshot_ts"),
        "signal_snapshot_path": signal_payload.get("snapshot_path"),
        "latest_feature_decision_ts": signal_payload.get("latest_feature_decision_ts"),
        "feature_rows": signal_payload.get("feature_rows"),
        "quote_snapshot_ts": quote_payload.get("snapshot_ts") if quote_payload else None,
        "quote_snapshot_path": quote_payload.get("quote_snapshot_path") if quote_payload else None,
        "latest_quote_path": quote_payload.get("latest_quote_path") if quote_payload else None,
        "active_bundle_selection_path": signal_payload.get("active_bundle_selection_path"),
        "profile_spec": spec.to_dict(),
        "applied_guard_layers": [
            "active_market",
            "offset_enabled",
            "trade_side_filter",
            "signal_valid",
            "feature_coverage",
            "bundle_blacklist_compatibility",
            "nan_feature_guard",
            "probability_interval_threshold",
            "liquidity_guard",
            "regime_controller",
            "ret_30m_direction_guard",
            "tail_space_guard",
            "entry_price_band",
            "trade_count_cap",
            "cash_balance_guard",
            "max_open_markets_guard",
        ],
        "pending_guard_layers": [
            "repeat_same_decision_guard",
        ],
        "liquidity_state_snapshot_ts": signal_payload.get("liquidity_state_snapshot_ts"),
        "latest_liquidity_path": signal_payload.get("latest_liquidity_path"),
        "liquidity_snapshot_path": signal_payload.get("liquidity_snapshot_path"),
        "liquidity_state": liquidity_state,
        "regime_state_snapshot_ts": signal_payload.get("regime_state_snapshot_ts"),
        "latest_regime_path": signal_payload.get("latest_regime_path"),
        "regime_snapshot_path": signal_payload.get("regime_snapshot_path"),
        "regime_state": regime_state,
        "account_state": {
            "snapshot_ts": account_state.get("snapshot_ts"),
            "open_orders_status": (account_state.get("open_orders") or {}).get("status") if isinstance(account_state, dict) else None,
            "positions_status": (account_state.get("positions") or {}).get("status") if isinstance(account_state, dict) else None,
            "summary": account_summary if isinstance(account_summary, dict) else None,
        },
        "account_summary": account_summary if isinstance(account_summary, dict) else None,
        "decision": {
            "status": "accept" if best else "reject",
            "selected_offset": int(best["offset"]) if best else None,
            "selected_side": str(best["recommended_side"]) if best else None,
            "selected_confidence": float(best["confidence"]) if best else None,
            "selected_edge": float(best["edge"]) if best else None,
            "selected_decision_ts": best.get("decision_ts") if best else None,
            "selected_window_start_ts": best.get("window_start_ts") if best else None,
            "selected_window_end_ts": best.get("window_end_ts") if best else None,
            "selected_window_duration_seconds": float_or_none(best.get("window_duration_seconds")) if best else None,
            "selected_entry_price": (best.get("quote_metrics") or {}).get("entry_price") if best else None,
            "selected_roi_net_vs_quote": (best.get("quote_metrics") or {}).get("roi_net_vs_quote") if best else None,
            "selected_quote_market_id": (best.get("quote_row") or {}).get("market_id") if best else None,
            "selected_trigger_metric": best.get("trigger_metric") if best else None,
            "selected_trigger_probability": float_or_none(best.get("trigger_probability")) if best else None,
            "selected_p_up_raw": float_or_none(best.get("p_up_raw")) if best else None,
            "selected_p_up_lcb": float_or_none(best.get("p_up_lcb")) if best else None,
            "selected_p_up_ucb": float_or_none(best.get("p_up_ucb")) if best else None,
            "selected_p_lgb": float_or_none(best.get("p_lgb")) if best else None,
            "selected_p_lr": float_or_none(best.get("p_lr")) if best else None,
            "selected_p_signal": float_or_none(best.get("p_signal")) if best else None,
            "selected_w_lgb": float_or_none(best.get("w_lgb")) if best else None,
            "selected_w_lr": float_or_none(best.get("w_lr")) if best else None,
            "selected_probability_mode": best.get("probability_mode") if best else None,
        },
        "accepted_offsets": accepted,
        "rejected_offsets": rejected,
    }
    return payload


def _build_trade_candidates(*, signal_row: dict[str, Any], profile_spec) -> list[dict[str, Any]]:
    del profile_spec
    model_side = str(signal_row.get("recommended_side") or "").upper() or None
    model_confidence = float_or_none(signal_row.get("confidence"))
    model_edge = float_or_none(signal_row.get("edge"))
    probability_view = resolve_probability_interval_view(selected_row=signal_row)
    if probability_view is None:
        candidate = dict(signal_row)
        candidate["model_recommended_side"] = model_side
        candidate["model_confidence"] = model_confidence
        candidate["model_edge"] = model_edge
        candidate["candidate_probability"] = resolve_side_probability(selected_row=signal_row, side=str(candidate.get("recommended_side") or ""))
        candidate["opposite_probability"] = None
        candidate["trigger_side"] = model_side
        candidate["trigger_metric"] = "confidence"
        candidate["trigger_probability"] = candidate["candidate_probability"]
        return [candidate]

    return [
        _build_interval_trade_candidate(
            signal_row=signal_row,
            model_side=model_side,
            model_confidence=model_confidence,
            model_edge=model_edge,
            probability_view=probability_view,
        )
    ]


def _build_interval_trade_candidate(
    *,
    signal_row: dict[str, Any],
    model_side: str | None,
    model_confidence: float | None,
    model_edge: float | None,
    probability_view: dict[str, float],
) -> dict[str, Any]:
    raw = float(probability_view["p_up_raw"])
    eff_up = float(probability_view["p_eff_up"])
    eff_down = float(probability_view["p_eff_down"])
    up_lcb = float(probability_view["p_up_lcb"])
    up_ucb = float(probability_view["p_up_ucb"])
    if raw > 0.5:
        side = "UP"
        confidence = up_lcb
        candidate_probability = eff_up
        opposite_probability = eff_down
        trigger_metric = "p_up_lcb"
        trigger_probability = up_lcb
    elif raw < 0.5:
        side = "DOWN"
        confidence = eff_down
        candidate_probability = eff_down
        opposite_probability = eff_up
        trigger_metric = "p_up_ucb"
        trigger_probability = up_ucb
    else:
        side = model_side if model_side in {"UP", "DOWN"} else "UP"
        confidence = up_lcb if side == "UP" else eff_down
        candidate_probability = eff_up if side == "UP" else eff_down
        opposite_probability = eff_down if side == "UP" else eff_up
        trigger_metric = "p_up_raw"
        trigger_probability = raw
    candidate = dict(signal_row)
    candidate["recommended_side"] = side
    candidate["confidence"] = float(confidence)
    candidate["edge"] = float(candidate_probability) - float(opposite_probability)
    candidate["model_recommended_side"] = model_side
    candidate["model_confidence"] = model_confidence
    candidate["model_edge"] = model_edge
    candidate["candidate_probability"] = float(candidate_probability)
    candidate["opposite_probability"] = float(opposite_probability)
    candidate["trigger_side"] = side
    candidate["trigger_metric"] = trigger_metric
    candidate["trigger_probability"] = float(trigger_probability)
    candidate["p_up_raw"] = raw
    candidate["p_down_raw"] = float(probability_view["p_down_raw"])
    candidate["p_eff_up"] = eff_up
    candidate["p_eff_down"] = eff_down
    candidate["p_up_lcb"] = up_lcb
    candidate["p_up_ucb"] = up_ucb
    return candidate


def _best_trade_candidate(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates = [dict(row) for row in rows if isinstance(row, dict)]
    if not candidates:
        return None
    return max(candidates, key=_trade_candidate_sort_key)


def _trade_candidate_sort_key(row: dict[str, Any]) -> tuple[float, float, int]:
    quote_metrics = row.get("quote_metrics") or {}
    confidence = float_or_none(row.get("confidence"))
    entry_price = float_or_none(quote_metrics.get("entry_price"))
    offset = int(row.get("offset") or 0)
    return (
        float("-inf") if confidence is None else float(confidence),
        float("-inf") if entry_price is None else -float(entry_price),
        -offset,
    )


def _build_decision_quote_metrics(
    *,
    market: str,
    cycle: str,
    rewrite_root,
    profile_spec,
    signal_row: dict[str, Any],
    quote_row: dict[str, Any] | None,
    regime_state: dict[str, Any] | None,
    account_state: dict[str, Any] | None,
    orderbook_provider=None,
) -> dict[str, Any] | None:
    if not isinstance(quote_row, dict):
        return None
    metrics: dict[str, Any] = {
        "quote_status": str(quote_row.get("status") or ""),
        "quote_reasons": list(quote_row.get("reasons") or []),
        "quote_market_id": quote_row.get("market_id"),
    }
    if str(quote_row.get("status") or "") != "ok":
        return metrics

    side = str(signal_row.get("recommended_side") or "").upper()
    if side not in {"UP", "DOWN"}:
        return metrics

    entry_price_l1 = float_or_none(quote_row.get("quote_up_ask") if side == "UP" else quote_row.get("quote_down_ask"))
    metrics["entry_side"] = side
    metrics["entry_price_l1"] = entry_price_l1
    metrics["entry_price"] = entry_price_l1
    metrics["entry_price_min"] = profile_spec.entry_price_min
    metrics["entry_price_max"] = profile_spec.entry_price_max
    metrics["slippage_bps"] = float(profile_spec.slippage_bps)

    p_side = resolve_side_probability(selected_row=signal_row, side=side)
    metrics["p_side"] = p_side

    account_summary = None
    if isinstance(account_state, dict) and isinstance(account_state.get("summary"), dict):
        account_summary = account_state.get("summary")
    stake_multiplier = resolve_regime_stake_multiplier(spec=profile_spec, regime_state=regime_state)
    requested_notional_base, stake_context = resolve_dynamic_stake_base(
        spec=profile_spec,
        account_summary=account_summary,
    )
    requested_notional = min(
        float(requested_notional_base) * float(stake_multiplier),
        float(profile_spec.max_notional_usd),
    )
    metrics["stake_base_usd"] = float(requested_notional_base)
    metrics["stake_multiplier"] = float(stake_multiplier)
    metrics["requested_notional_usd"] = float(requested_notional)
    metrics["stake_source"] = stake_context.get("stake_source")
    metrics["cash_balance_usd"] = stake_context.get("cash_balance_usd")
    metrics["cash_balance_available"] = stake_context.get("cash_balance_available")
    metrics["depth_enforced"] = False

    if entry_price_l1 is not None and entry_price_l1 > 0.0:
        slip = max(0.0, float(profile_spec.slippage_bps)) / 10000.0
        effective_price = float(entry_price_l1) * (1.0 + slip)
        fee_rate = profile_spec.fee_rate(price=effective_price)
        raw_edge = None if p_side is None else float(p_side) - float(entry_price_l1)
        min_net_edge = profile_spec.min_net_edge_for(offset=int(signal_row["offset"]), entry_price=entry_price_l1)
        roi_net = None if p_side is None else float(p_side) / max(effective_price, 1e-9) - 1.0 - fee_rate
        roi_threshold = profile_spec.roi_threshold_for(offset=int(signal_row["offset"]))
        metrics["fee_rate"] = fee_rate
        metrics["effective_entry_price"] = effective_price
        metrics["edge_vs_quote"] = raw_edge
        metrics["min_net_edge_required"] = float(min_net_edge)
        metrics["roi_net_vs_quote"] = roi_net
        metrics["roi_threshold_required"] = float(roi_threshold)

    if requested_notional <= 0.0 or p_side is None:
        return metrics

    quote_row_for_depth = dict(quote_row)
    quote_row_for_depth.setdefault("decision_ts", signal_row.get("decision_ts"))

    try:
        data_cfg = DataConfig.build(
            market=market,
            cycle=cycle,
            surface="live",
            root=rewrite_root,
        )
    except Exception:
        data_cfg = DataConfig.build(
            market=market,
            cycle=cycle,
            surface="live",
        )

    fee_reference_price = entry_price_l1
    if fee_reference_price is None or fee_reference_price <= 0.0:
        fee_reference_price = 0.5
    fee_rate = profile_spec.fee_rate(price=float(fee_reference_price))
    slip = max(0.0, float(profile_spec.slippage_bps)) / 10000.0
    roi_threshold = profile_spec.roi_threshold_for(offset=int(signal_row["offset"]))
    denom = max((1.0 + float(roi_threshold) + float(fee_rate)) * (1.0 + slip), 1e-9)
    p_cap = max(1e-6, min(float(p_side) / denom, 1.0))
    metrics["price_cap"] = p_cap
    if not _decision_depth_enforced():
        return metrics

    has_persisted_depth = False
    if rewrite_root is not None:
        try:
            decision_ts = signal_row.get("decision_ts")
            if decision_ts:
                import pandas as pd

                dt = pd.to_datetime(decision_ts, utc=True, errors="coerce")
                if dt is not None and not pd.isna(dt):
                    has_persisted_depth = data_cfg.layout.orderbook_depth_path(dt.strftime("%Y-%m-%d")).exists()
        except Exception:
            has_persisted_depth = False

    use_depth = bool(orderbook_provider is not None or (rewrite_root is not None and has_persisted_depth))
    if not use_depth:
        return metrics

    metrics["depth_enforced"] = True
    depth_plan, depth_reason = build_depth_execution_plan(
        data_cfg=data_cfg,
        quote_row=quote_row_for_depth,
        side=side,
        requested_notional=requested_notional,
        price_cap=p_cap,
        max_slippage_bps=float(profile_spec.orderbook_max_slippage_bps),
        min_fill_ratio=float(profile_spec.orderbook_min_fill_ratio),
        orderbook_provider=orderbook_provider,
        prefer_live_provider=orderbook_provider is not None,
    )
    metrics["depth_plan"] = depth_plan
    metrics["depth_reason"] = depth_reason
    if depth_plan is None or str(depth_plan.get("status") or "") != "ok":
        return metrics

    repriced_metrics, _ = repriced_order_guard(
        spec=profile_spec,
        selected_row=signal_row,
        repriced_entry_price=float(depth_plan["max_price"]),
    )
    metrics["repriced_metrics"] = repriced_metrics
    metrics["entry_price"] = repriced_metrics.get("repriced_entry_price")
    metrics["effective_entry_price"] = repriced_metrics.get("repriced_effective_price")
    metrics["fee_rate"] = repriced_metrics.get("repriced_fee_rate")
    metrics["edge_vs_quote"] = repriced_metrics.get("repriced_raw_edge")
    metrics["min_net_edge_required"] = repriced_metrics.get("repriced_min_net_edge_required")
    metrics["roi_net_vs_quote"] = repriced_metrics.get("repriced_roi_net")
    metrics["roi_threshold_required"] = repriced_metrics.get("repriced_roi_threshold_required")
    return metrics


def _decision_depth_enforced() -> bool:
    raw = os.getenv("PM15MIN_LIVE_DECISION_DEPTH_ENFORCED")
    if raw in (None, ""):
        return False
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def persist_decision_snapshot(*, rewrite_root: Path, payload: dict[str, Any]) -> dict[str, Path]:
    layout = LiveStateLayout.discover(root=rewrite_root)
    latest_path = layout.latest_decision_path(
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        profile=str(payload["profile"]),
        target=str(payload["target"]),
    )
    history_path = layout.decision_snapshot_path(
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        profile=str(payload["profile"]),
        target=str(payload["target"]),
        snapshot_ts=str(payload["snapshot_ts"]),
    )
    return write_live_payload_pair(
        payload=payload,
        latest_path=latest_path,
        snapshot_path=history_path,
        write_snapshot_history=False,
    )
