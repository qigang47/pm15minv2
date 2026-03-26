from __future__ import annotations

from typing import Any

from pm15min.data.config import DataConfig


def build_execution_snapshot(
    cfg,
    decision_payload: dict[str, Any],
    *,
    orderbook_provider=None,
    prefer_live_depth: bool = False,
    resolve_live_profile_spec_fn,
    utc_snapshot_label_fn,
    load_policy_state_fn,
    build_policy_context_fn,
    build_execution_record_fn,
    resolve_regime_stake_multiplier_fn,
    resolve_execution_account_summary_fn,
    resolve_dynamic_stake_base_fn,
    resolve_side_probability_fn,
    float_or_none_fn,
    build_depth_execution_plan_fn,
    repriced_order_guard_fn,
) -> dict[str, Any]:
    market = str(decision_payload["market"])
    profile = str(decision_payload["profile"])
    cycle = str(decision_payload["cycle"])
    target = str(decision_payload["target"])
    spec = resolve_live_profile_spec_fn(profile)
    snapshot_ts = utc_snapshot_label_fn()

    decision = decision_payload.get("decision") or {}
    selected_offset = decision.get("selected_offset")
    selected_row = None
    for row in decision_payload.get("accepted_offsets") or []:
        if int(row.get("offset", -1)) == int(selected_offset or -1):
            selected_row = row
            break

    payload = {
        "domain": "live",
        "dataset": "live_execution_snapshot",
        "snapshot_ts": snapshot_ts,
        "market": market,
        "profile": profile,
        "cycle": cycle,
        "target": target,
        "decision_snapshot_ts": decision_payload.get("snapshot_ts"),
        "decision_snapshot_path": decision_payload.get("decision_snapshot_path"),
        "latest_decision_path": decision_payload.get("latest_decision_path"),
        "applied_execution_layers": [
            "decision_selected",
            "quote_selected",
            "dynamic_cash_stake",
            "regime_stake_scale",
            "order_type_selection",
            "l1_fill_proxy",
            "full_orderbook_depth",
            "orderbook_limit_reprice",
            "retry_policy",
            "cancel_policy",
            "redeem_policy",
        ],
        "pending_execution_layers": [
            "live_order_submission",
            "cancel_side_effect",
            "redeem_side_effect",
        ],
    }
    policy_state = load_policy_state_fn(rewrite_root=cfg.layout.rewrite.root, market=market)
    policy_context = build_policy_context_fn()
    if not selected_row:
        payload["execution"] = build_execution_record_fn(
            spec=spec,
            execution_status="no_action",
            execution_reason="decision_reject",
            order_type=spec.default_order_type,
            policy_context=policy_context,
            policy_state=policy_state,
        )
        return payload

    quote_row = selected_row.get("quote_row") or {}
    quote_metrics = selected_row.get("quote_metrics") or {}
    policy_context = build_policy_context_fn(selected_row=selected_row, quote_row=quote_row)
    account_summary = resolve_execution_account_summary_fn(decision_payload=decision_payload, policy_state=policy_state)
    side = str(selected_row.get("recommended_side") or decision.get("selected_side") or "").upper()
    if side not in {"UP", "DOWN"}:
        payload["execution"] = build_execution_record_fn(
            spec=spec,
            execution_status="no_action",
            execution_reason="selected_side_missing",
            order_type=spec.default_order_type,
            policy_context=policy_context,
            policy_state=policy_state,
        )
        return payload

    entry_price = float_or_none_fn(quote_metrics.get("entry_price"))
    if entry_price is None or entry_price <= 0.0:
        payload["execution"] = build_execution_record_fn(
            spec=spec,
            execution_status="no_action",
            execution_reason="entry_price_missing",
            order_type=spec.default_order_type,
            policy_context=policy_context,
            policy_state=policy_state,
        )
        return payload

    token_id = str(quote_row.get("token_up") if side == "UP" else quote_row.get("token_down") or "")
    policy_context = {
        **policy_context,
        "token_id": token_id,
    }
    ask_size_1 = float_or_none_fn(quote_row.get("quote_up_ask_size_1") if side == "UP" else quote_row.get("quote_down_ask_size_1"))
    bid_price = float_or_none_fn(quote_row.get("quote_up_bid") if side == "UP" else quote_row.get("quote_down_bid"))
    regime_state = decision_payload.get("regime_state") or {}
    stake_multiplier = resolve_regime_stake_multiplier_fn(spec=spec, regime_state=regime_state)
    requested_notional_base, stake_context = resolve_dynamic_stake_base_fn(spec=spec, account_summary=account_summary)
    requested_notional = min(float(requested_notional_base) * float(stake_multiplier), float(spec.max_notional_usd))
    if requested_notional <= 0.0:
        payload["execution"] = build_execution_record_fn(
            spec=spec,
            execution_status="no_action",
            execution_reason="regime_stake_nonpositive",
            order_type=spec.default_order_type,
            policy_context=policy_context,
            policy_state=policy_state,
            extra={
                "stake_base_usd": requested_notional_base,
                "stake_multiplier": float(stake_multiplier),
                "stake_regime_state": str(regime_state.get("state") or "NORMAL"),
                "stake_source": stake_context.get("stake_source"),
                "cash_balance_usd": stake_context.get("cash_balance_usd"),
                "cash_balance_available": stake_context.get("cash_balance_available"),
                "requested_notional_usd": requested_notional,
            },
        )
        return payload
    requested_shares = requested_notional / max(entry_price, 1e-9)
    l1_fill_ratio = None
    l1_fillable_shares = None
    l1_fillable_notional = None
    execution_reasons: list[str] = []
    if ask_size_1 is not None:
        l1_fillable_shares = max(0.0, float(ask_size_1))
        l1_fillable_notional = l1_fillable_shares * entry_price
        l1_fill_ratio = min(1.0, l1_fillable_shares / max(requested_shares, 1e-9))
    else:
        execution_reasons.append("l1_ask_size_missing")

    p_side = resolve_side_probability_fn(selected_row=selected_row, side=side)
    fee_rate = float_or_none_fn(quote_metrics.get("fee_rate")) or spec.fee_rate(price=entry_price)
    slippage_bps = float_or_none_fn(quote_metrics.get("slippage_bps"))
    if slippage_bps is None:
        slippage_bps = float(spec.slippage_bps)
    roi_threshold = spec.roi_threshold_for(offset=int(selected_row["offset"]))
    slip = max(0.0, float(slippage_bps)) / 10000.0
    p_cap = None
    if p_side is not None:
        denom = max((1.0 + float(roi_threshold) + float(fee_rate)) * (1.0 + slip), 1e-9)
        p_cap = max(1e-6, min(float(p_side) / denom, 1.0))

    data_cfg = DataConfig.build(
        market=market,
        cycle=cycle,
        surface="live",
        root=cfg.layout.rewrite.root,
    )
    depth_plan, depth_reason = build_depth_execution_plan_fn(
        data_cfg=data_cfg,
        quote_row=quote_row,
        side=side,
        requested_notional=requested_notional,
        price_cap=p_cap,
        max_slippage_bps=float(spec.orderbook_max_slippage_bps),
        min_fill_ratio=float(spec.orderbook_min_fill_ratio),
        orderbook_provider=orderbook_provider,
        prefer_live_provider=prefer_live_depth,
    )
    if depth_reason:
        execution_reasons.append(depth_reason)
    repriced_metrics = None
    if depth_plan is not None and str(depth_plan.get("status") or "") == "ok":
        repriced_metrics, repriced_reasons = repriced_order_guard_fn(
            spec=spec,
            selected_row=selected_row,
            repriced_entry_price=float(depth_plan["max_price"]),
        )
        execution_reasons.extend(repriced_reasons)

    execution_status = "plan" if not execution_reasons else "blocked"
    order_type = str(spec.default_order_type)
    payload["execution"] = build_execution_record_fn(
        spec=spec,
        execution_status=execution_status,
        execution_reason=None if not execution_reasons else execution_reasons[0],
        execution_reasons=execution_reasons,
        order_type=order_type,
        policy_context=policy_context,
        policy_state=policy_state,
        extra={
            "selected_offset": int(selected_row["offset"]),
            "selected_side": side,
            "decision_ts": selected_row.get("decision_ts"),
            "cycle_start_ts": selected_row.get("cycle_start_ts"),
            "cycle_end_ts": selected_row.get("cycle_end_ts"),
            "window_start_ts": selected_row.get("window_start_ts") or decision.get("selected_window_start_ts"),
            "window_end_ts": selected_row.get("window_end_ts") or decision.get("selected_window_end_ts"),
            "window_duration_seconds": (
                float_or_none_fn(selected_row.get("window_duration_seconds"))
                or float_or_none_fn(decision.get("selected_window_duration_seconds"))
            ),
            "market_id": quote_row.get("market_id"),
            "condition_id": quote_row.get("condition_id"),
            "question": quote_row.get("question"),
            "token_id": token_id,
            "order_type": order_type,
            "entry_price": entry_price,
            "best_bid": bid_price,
            "stake_base_usd": requested_notional_base,
            "stake_multiplier": float(stake_multiplier),
            "stake_regime_state": str(regime_state.get("state") or "NORMAL"),
            "stake_source": stake_context.get("stake_source"),
            "cash_balance_usd": stake_context.get("cash_balance_usd"),
            "cash_balance_available": stake_context.get("cash_balance_available"),
            "stake_step_levels": stake_context.get("stake_step_levels"),
            "requested_notional_usd": requested_notional,
            "requested_shares": requested_shares,
            "l1_ask_size_1": ask_size_1,
            "l1_fillable_shares": l1_fillable_shares,
            "l1_fillable_notional_usd": l1_fillable_notional,
            "l1_fill_ratio": l1_fill_ratio,
            "min_fill_ratio_required": float(spec.orderbook_min_fill_ratio),
            "fee_rate": fee_rate,
            "slippage_bps": slippage_bps,
            "roi_net_vs_quote": float_or_none_fn(quote_metrics.get("roi_net_vs_quote")),
            "price_cap": p_cap,
            "market_cycle_end_ts": policy_context.get("cycle_end_ts"),
            "minutes_left_to_market_end": policy_context.get("minutes_left_to_market_end"),
            "depth_plan": depth_plan,
            "repriced_metrics": repriced_metrics,
        },
    )
    return payload
