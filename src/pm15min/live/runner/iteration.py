from __future__ import annotations

from typing import Any

from pm15min.data.config import DataConfig


def build_runner_iteration(
    cfg,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist_decision: bool = True,
    persist_execution: bool = True,
    run_foundation: bool = True,
    foundation_include_direct_oracle: bool = True,
    foundation_include_orderbooks: bool = True,
    apply_side_effects: bool = True,
    side_effect_dry_run: bool = False,
    gateway=None,
    session_state: dict[str, Any] | None = None,
    orderbook_provider=None,
    run_live_data_foundation_fn,
    build_liquidity_state_snapshot_fn,
    decide_live_latest_fn,
    build_execution_snapshot_fn,
    persist_execution_snapshot_fn,
    submit_execution_payload_fn,
    build_account_state_snapshot_fn,
    apply_cancel_policy_fn,
    apply_redeem_policy_fn,
    utc_snapshot_label_fn,
    build_runner_risk_summary_fn,
    build_runner_health_summary_fn,
    build_runner_risk_alerts_fn,
    summarize_runner_risk_alerts_fn,
    build_side_effect_error_payload_fn,
    build_account_state_error_payload_fn,
) -> dict[str, object]:
    cycle = f"{int(cfg.cycle_minutes)}m"
    foundation_summary: dict[str, object] | None = None
    if run_foundation:
        data_cfg = DataConfig.build(
            market=cfg.asset.slug,
            cycle=cycle,
            surface="live",
            root=cfg.layout.rewrite.root,
        )
        foundation_summary = run_live_data_foundation_fn(
            data_cfg,
            iterations=1,
            loop=False,
            include_direct_oracle=foundation_include_direct_oracle,
            include_orderbooks=foundation_include_orderbooks,
        )
    try:
        liquidity_state_payload = build_liquidity_state_snapshot_fn(
            cfg,
            persist=persist_decision,
        )
    except Exception as exc:
        liquidity_state_payload = build_side_effect_error_payload_fn(stage="build_liquidity_state_snapshot", exc=exc)

    decision_payload = decide_live_latest_fn(
        cfg,
        target=target,
        feature_set=feature_set,
        persist=persist_decision,
        session_state=session_state,
        orderbook_provider=orderbook_provider,
    )
    execution_payload = build_execution_snapshot_fn(
        cfg,
        decision_payload,
        orderbook_provider=orderbook_provider,
        prefer_live_depth=orderbook_provider is not None,
    )
    if persist_execution:
        execution_paths = persist_execution_snapshot_fn(rewrite_root=cfg.layout.rewrite.root, payload=execution_payload)
        execution_payload["latest_execution_path"] = str(execution_paths["latest"])
        execution_payload["execution_snapshot_path"] = str(execution_paths["snapshot"])
    order_action_payload: dict[str, object] | None = None
    account_state_payload: dict[str, object] | None = None
    cancel_action_payload: dict[str, object] | None = None
    redeem_action_payload: dict[str, object] | None = None
    if apply_side_effects:
        try:
            order_action_payload = submit_execution_payload_fn(
                cfg,
                execution_payload=execution_payload,
                persist=persist_execution,
                refresh_account_state=False,
                dry_run=side_effect_dry_run,
                gateway=gateway,
            )
        except Exception as exc:
            order_action_payload = build_side_effect_error_payload_fn(stage="submit_execution_payload", exc=exc)
        _record_session_trade_count(
            session_state=session_state,
            decision_payload=decision_payload,
            execution_payload=execution_payload,
            order_action_payload=order_action_payload,
        )
        try:
            account_state_payload = build_account_state_snapshot_fn(
                cfg,
                persist=persist_execution,
                gateway=gateway,
            )
        except Exception as exc:
            account_state_payload = build_account_state_error_payload_fn(stage="build_account_state_snapshot", exc=exc)
        try:
            cancel_action_payload = apply_cancel_policy_fn(
                cfg,
                persist=persist_execution,
                refresh_account_state=False,
                dry_run=side_effect_dry_run,
                gateway=gateway,
            )
        except Exception as exc:
            cancel_action_payload = build_side_effect_error_payload_fn(stage="apply_cancel_policy", exc=exc)
        try:
            redeem_action_payload = apply_redeem_policy_fn(
                cfg,
                persist=persist_execution,
                refresh_account_state=False,
                dry_run=side_effect_dry_run,
                gateway=gateway,
            )
        except Exception as exc:
            redeem_action_payload = build_side_effect_error_payload_fn(stage="apply_redeem_policy", exc=exc)
    snapshot_ts = utc_snapshot_label_fn()
    risk_summary = build_runner_risk_summary_fn(
        foundation_summary=foundation_summary,
        liquidity_state_payload=liquidity_state_payload,
        decision_payload=decision_payload,
        execution_payload=execution_payload,
        order_action_payload=order_action_payload,
        account_state_payload=account_state_payload,
        cancel_action_payload=cancel_action_payload,
        redeem_action_payload=redeem_action_payload,
        apply_side_effects=apply_side_effects,
        side_effect_dry_run=side_effect_dry_run,
    )
    runner_health = build_runner_health_summary_fn(
        foundation_summary=foundation_summary,
        liquidity_state_payload=liquidity_state_payload,
        decision_payload=decision_payload,
        execution_payload=execution_payload,
        order_action_payload=order_action_payload,
        account_state_payload=account_state_payload,
        cancel_action_payload=cancel_action_payload,
        redeem_action_payload=redeem_action_payload,
        apply_side_effects=apply_side_effects,
        side_effect_dry_run=side_effect_dry_run,
    )
    risk_alerts = build_runner_risk_alerts_fn(risk_summary=risk_summary, runner_health=runner_health)
    risk_alert_summary = summarize_runner_risk_alerts_fn(alerts=risk_alerts)
    return {
        "domain": "live",
        "dataset": "live_runner_iteration",
        "snapshot_ts": snapshot_ts,
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": cycle,
        "target": target,
        "run_foundation": bool(run_foundation),
        "apply_side_effects": bool(apply_side_effects),
        "side_effect_dry_run": bool(side_effect_dry_run),
        "foundation_summary": foundation_summary,
        "liquidity_state": None if liquidity_state_payload is None else {
            "snapshot_ts": liquidity_state_payload.get("snapshot_ts"),
            "status": liquidity_state_payload.get("status"),
            "reason": liquidity_state_payload.get("reason"),
            "blocked": liquidity_state_payload.get("blocked"),
        },
        "liquidity_state_payload": liquidity_state_payload,
        "regime_state": decision_payload.get("regime_state"),
        "decision_snapshot_ts": decision_payload.get("snapshot_ts"),
        "decision_snapshot_path": decision_payload.get("decision_snapshot_path"),
        "latest_decision_path": decision_payload.get("latest_decision_path"),
        "decision": decision_payload.get("decision"),
        "decision_payload": decision_payload,
        "execution_snapshot_ts": execution_payload.get("snapshot_ts"),
        "execution_snapshot_path": execution_payload.get("execution_snapshot_path"),
        "latest_execution_path": execution_payload.get("latest_execution_path"),
        "execution": execution_payload.get("execution"),
        "execution_payload": execution_payload,
        "risk_summary": risk_summary,
        "runner_health": runner_health,
        "risk_alerts": risk_alerts,
        "risk_alert_summary": risk_alert_summary,
        "order_action": None if order_action_payload is None else {
            "status": order_action_payload.get("status"),
            "reason": order_action_payload.get("reason"),
            "order_response": order_action_payload.get("order_response"),
        },
        "order_action_payload": order_action_payload,
        "account_state": None if account_state_payload is None else {
            "snapshot_ts": account_state_payload.get("snapshot_ts"),
            "open_orders_status": (account_state_payload.get("open_orders") or {}).get("status"),
            "positions_status": (account_state_payload.get("positions") or {}).get("status"),
        },
        "account_state_payload": account_state_payload,
        "cancel_action": None if cancel_action_payload is None else {
            "status": cancel_action_payload.get("status"),
            "reason": cancel_action_payload.get("reason"),
            "summary": cancel_action_payload.get("summary"),
        },
        "cancel_action_payload": cancel_action_payload,
        "redeem_action": None if redeem_action_payload is None else {
            "status": redeem_action_payload.get("status"),
            "reason": redeem_action_payload.get("reason"),
            "summary": redeem_action_payload.get("summary"),
        },
        "redeem_action_payload": redeem_action_payload,
        "session_state": _build_session_state_payload(session_state),
    }


def _record_session_trade_count(
    *,
    session_state: dict[str, Any] | None,
    decision_payload: dict[str, object],
    execution_payload: dict[str, object],
    order_action_payload: dict[str, object] | None,
) -> None:
    if not isinstance(session_state, dict) or not isinstance(order_action_payload, dict):
        return
    if str(order_action_payload.get("status") or "").strip().lower() != "ok":
        return
    reason = str(order_action_payload.get("reason") or "").strip().lower()
    if reason not in {"dry_run", "order_submitted"}:
        return
    execution = execution_payload.get("execution") if isinstance(execution_payload, dict) else {}
    decision = decision_payload.get("decision") if isinstance(decision_payload, dict) else {}
    market_id = str(
        (execution or {}).get("market_id")
        or (decision or {}).get("selected_quote_market_id")
        or ""
    ).strip()
    offset = int_or_none((execution or {}).get("selected_offset"))
    if offset is None:
        offset = int_or_none((decision or {}).get("selected_offset"))
    if not market_id or offset is None:
        return
    counts = _session_trade_count_map(session_state)
    key = f"{market_id}_{offset}"
    counts[key] = int_or_none(counts.get(key)) or 0
    counts[key] = int(counts[key]) + 1


def _build_session_state_payload(session_state: dict[str, Any] | None) -> dict[str, object]:
    counts = _session_trade_count_map(session_state)
    return {
        "market_offset_trade_count": {
            str(key): int(int_or_none(value) or 0)
            for key, value in sorted(counts.items(), key=lambda item: str(item[0]))
        },
        "tracked_market_offset_count": int(len(counts)),
    }


def _session_trade_count_map(session_state: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(session_state, dict):
        return {}
    counts = session_state.get("market_offset_trade_count")
    if isinstance(counts, dict):
        return counts
    counts = {}
    session_state["market_offset_trade_count"] = counts
    return counts


def int_or_none(value) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None
