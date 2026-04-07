from __future__ import annotations

from datetime import datetime, timezone
import os
import time
from typing import Any

from pm15min.data.config import DataConfig
from ..signal.utils import LiveClosedBarNotReadyError
from ..session_state import (
    build_market_offset_side_trade_count_key,
    build_market_offset_trade_count_key,
)


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
    prewarm_live_signal_inputs_fn,
    prewarm_live_signal_cache_fn,
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
    pipeline_persist_enabled = bool(
        persist_decision and _env_bool("PM15MIN_RUNNER_PERSIST_PIPELINE", default=True)
    )
    side_effect_persist_enabled = bool(
        persist_execution and _env_bool("PM15MIN_RUNNER_PERSIST_SIDE_EFFECTS", default=True)
    )
    phase_timings_ms: dict[str, object] = {}
    foundation_summary: dict[str, object] | None = None
    if run_foundation:
        foundation_started = time.perf_counter()
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
        phase_timings_ms["foundation_stage_ms"] = _elapsed_ms(foundation_started)
    liquidity_control = _reserve_liquidity_slot(
        session_state=session_state,
        interval_seconds=_env_float("PM15MIN_RUNNER_LIQUIDITY_SYNC_INTERVAL_SEC", default=0.0),
    )
    liquidity_started = time.perf_counter()
    if liquidity_control["should_run"]:
        try:
            liquidity_state_payload = build_liquidity_state_snapshot_fn(
                cfg,
                persist=pipeline_persist_enabled,
            )
        except Exception as exc:
            liquidity_state_payload = build_side_effect_error_payload_fn(stage="build_liquidity_state_snapshot", exc=exc)
        _store_liquidity_payload(session_state=session_state, payload=liquidity_state_payload)
        _clear_live_signal_cache(session_state=session_state)
        phase_timings_ms["liquidity_cache_hit"] = False
    else:
        liquidity_state_payload = _load_cached_liquidity_payload(session_state=session_state)
        if liquidity_state_payload is None:
            try:
                liquidity_state_payload = build_liquidity_state_snapshot_fn(
                    cfg,
                    persist=pipeline_persist_enabled,
                )
            except Exception as exc:
                liquidity_state_payload = build_side_effect_error_payload_fn(stage="build_liquidity_state_snapshot", exc=exc)
            _store_liquidity_payload(session_state=session_state, payload=liquidity_state_payload)
            _clear_live_signal_cache(session_state=session_state)
            phase_timings_ms["liquidity_cache_hit"] = False
        else:
            phase_timings_ms["liquidity_cache_hit"] = True
    phase_timings_ms["liquidity_stage_ms"] = _elapsed_ms(liquidity_started)
    signal_prewarm_payload = _maybe_prewarm_signal_cache(
        cfg,
        target=target,
        feature_set=feature_set,
        session_state=session_state,
        prewarm_live_signal_inputs_fn=prewarm_live_signal_inputs_fn,
        prewarm_live_signal_cache_fn=prewarm_live_signal_cache_fn,
    )
    phase_timings_ms["signal_prewarm_triggered"] = bool(signal_prewarm_payload.get("triggered"))
    phase_timings_ms["signal_prewarm_cache_hit"] = bool(signal_prewarm_payload.get("cache_hit"))
    phase_timings_ms["signal_prewarm_stage_ms"] = _float_or_none(signal_prewarm_payload.get("elapsed_ms"))
    phase_timings_ms["signal_prewarm_boundary_offset_ms"] = _float_or_none(signal_prewarm_payload.get("boundary_offset_ms"))

    decision_started = time.perf_counter()
    decision_payload = decide_live_latest_fn(
        cfg,
        target=target,
        feature_set=feature_set,
        persist=pipeline_persist_enabled,
        session_state=session_state,
        orderbook_provider=orderbook_provider,
    )
    phase_timings_ms["decision_total_stage_ms"] = _elapsed_ms(decision_started)
    execution_started = time.perf_counter()
    execution_payload = build_execution_snapshot_fn(
        cfg,
        decision_payload,
        orderbook_provider=orderbook_provider,
        prefer_live_depth=orderbook_provider is not None,
    )
    phase_timings_ms["execution_total_stage_ms"] = _elapsed_ms(execution_started)
    if persist_execution and pipeline_persist_enabled:
        execution_persist_started = time.perf_counter()
        execution_paths = persist_execution_snapshot_fn(rewrite_root=cfg.layout.rewrite.root, payload=execution_payload)
        execution_payload["latest_execution_path"] = str(execution_paths["latest"])
        execution_payload["execution_snapshot_path"] = str(execution_paths["snapshot"])
        phase_timings_ms["execution_persist_stage_ms"] = _elapsed_ms(execution_persist_started)
    order_action_payload: dict[str, object] | None = None
    account_state_payload: dict[str, object] | None = None
    cancel_action_payload: dict[str, object] | None = None
    redeem_action_payload: dict[str, object] | None = None
    if apply_side_effects:
        try:
            submit_started = time.perf_counter()
            order_action_payload = submit_execution_payload_fn(
                cfg,
                execution_payload=execution_payload,
                persist=bool(side_effect_persist_enabled or str((execution_payload.get("execution") or {}).get("status") or "") == "plan"),
                refresh_account_state=False,
                dry_run=side_effect_dry_run,
                session_state=session_state,
                gateway=gateway,
            )
            phase_timings_ms["submit_stage_ms"] = _elapsed_ms(submit_started)
        except Exception as exc:
            order_action_payload = build_side_effect_error_payload_fn(stage="submit_execution_payload", exc=exc)
            phase_timings_ms["submit_stage_ms"] = _elapsed_ms(submit_started)
        _record_session_trade_count(
            session_state=session_state,
            decision_payload=decision_payload,
            execution_payload=execution_payload,
            order_action_payload=order_action_payload,
        )
        account_sync_control = _reserve_side_effect_slot(
            session_state=session_state,
            name="account_sync",
            enabled=_env_bool("PM15MIN_RUNNER_ENABLE_ACCOUNT_SYNC", default=True),
            interval_seconds=_env_float("PM15MIN_RUNNER_ACCOUNT_SYNC_INTERVAL_SEC", default=0.0),
        )
        if account_sync_control["should_run"]:
            try:
                account_started = time.perf_counter()
                account_state_payload = build_account_state_snapshot_fn(
                    cfg,
                    persist=side_effect_persist_enabled,
                    gateway=gateway,
                )
                phase_timings_ms["account_sync_stage_ms"] = _elapsed_ms(account_started)
            except Exception as exc:
                account_state_payload = build_account_state_error_payload_fn(stage="build_account_state_snapshot", exc=exc)
                phase_timings_ms["account_sync_stage_ms"] = _elapsed_ms(account_started)
        else:
            account_state_payload = _build_account_state_skip_payload(
                cfg,
                cycle=cycle,
                snapshot_ts=utc_snapshot_label_fn(),
                reason=str(account_sync_control["reason"]),
                interval_seconds=account_sync_control.get("interval_seconds"),
                remaining_seconds=account_sync_control.get("remaining_seconds"),
            )
        _record_side_effect_outcome(session_state=session_state, name="account_sync", payload=account_state_payload)
        cancel_control = _reserve_side_effect_slot(
            session_state=session_state,
            name="cancel_policy",
            enabled=_env_bool("PM15MIN_RUNNER_ENABLE_CANCEL_POLICY", default=True),
            interval_seconds=_env_float("PM15MIN_RUNNER_CANCEL_INTERVAL_SEC", default=0.0),
        )
        if cancel_control["should_run"]:
            try:
                cancel_started = time.perf_counter()
                cancel_action_payload = apply_cancel_policy_fn(
                    cfg,
                    persist=side_effect_persist_enabled,
                    refresh_account_state=False,
                    dry_run=side_effect_dry_run,
                    gateway=gateway,
                )
                phase_timings_ms["cancel_stage_ms"] = _elapsed_ms(cancel_started)
            except Exception as exc:
                cancel_action_payload = build_side_effect_error_payload_fn(stage="apply_cancel_policy", exc=exc)
                phase_timings_ms["cancel_stage_ms"] = _elapsed_ms(cancel_started)
        else:
            cancel_action_payload = _build_side_effect_skip_payload(
                cfg,
                cycle=cycle,
                snapshot_ts=utc_snapshot_label_fn(),
                dataset="live_cancel_policy_action",
                reason=str(cancel_control["reason"]),
                interval_seconds=cancel_control.get("interval_seconds"),
                remaining_seconds=cancel_control.get("remaining_seconds"),
                summary={"candidate_orders": 0, "submitted_orders": 0, "cancelled_orders": 0, "error_orders": 0},
            )
        _record_side_effect_outcome(session_state=session_state, name="cancel_policy", payload=cancel_action_payload)
        redeem_control = _reserve_side_effect_slot(
            session_state=session_state,
            name="redeem_policy",
            enabled=_env_bool("PM15MIN_RUNNER_ENABLE_REDEEM_POLICY", default=True),
            interval_seconds=_env_float("PM15MIN_RUNNER_REDEEM_INTERVAL_SEC", default=0.0),
        )
        if redeem_control["should_run"]:
            try:
                redeem_started = time.perf_counter()
                redeem_action_payload = apply_redeem_policy_fn(
                    cfg,
                    persist=side_effect_persist_enabled,
                    refresh_account_state=False,
                    dry_run=side_effect_dry_run,
                    gateway=gateway,
                )
                phase_timings_ms["redeem_stage_ms"] = _elapsed_ms(redeem_started)
            except Exception as exc:
                redeem_action_payload = build_side_effect_error_payload_fn(stage="apply_redeem_policy", exc=exc)
                phase_timings_ms["redeem_stage_ms"] = _elapsed_ms(redeem_started)
        else:
            redeem_action_payload = _build_side_effect_skip_payload(
                cfg,
                cycle=cycle,
                snapshot_ts=utc_snapshot_label_fn(),
                dataset="live_redeem_policy_action",
                reason=str(redeem_control["reason"]),
                interval_seconds=redeem_control.get("interval_seconds"),
                remaining_seconds=redeem_control.get("remaining_seconds"),
                summary={"candidate_conditions": 0, "submitted_conditions": 0, "redeemed_conditions": 0, "error_conditions": 0},
            )
        _record_side_effect_outcome(session_state=session_state, name="redeem_policy", payload=redeem_action_payload)
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
    phase_timings_ms["decision_signal_stage_ms"] = _float_or_none((decision_payload.get("timings_ms") or {}).get("signal_stage_ms"))
    phase_timings_ms["decision_signal_bundle_resolution_stage_ms"] = _float_or_none((decision_payload.get("timings_ms") or {}).get("signal_bundle_resolution_stage_ms"))
    phase_timings_ms["decision_signal_feature_prepare_stage_ms"] = _float_or_none((decision_payload.get("timings_ms") or {}).get("signal_feature_prepare_stage_ms"))
    phase_timings_ms["decision_signal_feature_frame_stage_ms"] = _float_or_none((decision_payload.get("timings_ms") or {}).get("signal_feature_frame_stage_ms"))
    phase_timings_ms["decision_signal_liquidity_state_stage_ms"] = _float_or_none((decision_payload.get("timings_ms") or {}).get("signal_liquidity_state_stage_ms"))
    phase_timings_ms["decision_signal_regime_state_stage_ms"] = _float_or_none((decision_payload.get("timings_ms") or {}).get("signal_regime_state_stage_ms"))
    phase_timings_ms["decision_signal_offset_scoring_stage_ms"] = _float_or_none((decision_payload.get("timings_ms") or {}).get("signal_offset_scoring_stage_ms"))
    phase_timings_ms["decision_quote_stage_ms"] = _float_or_none((decision_payload.get("timings_ms") or {}).get("quote_stage_ms"))
    phase_timings_ms["decision_account_context_stage_ms"] = _float_or_none((decision_payload.get("timings_ms") or {}).get("account_context_stage_ms"))
    phase_timings_ms["decision_build_stage_ms"] = _float_or_none((decision_payload.get("timings_ms") or {}).get("decision_build_stage_ms"))
    phase_timings_ms["decision_signal_cache_hit"] = bool((decision_payload.get("timings_ms") or {}).get("signal_cache_hit"))
    for key, value in dict(decision_payload.get("timings_ms") or {}).items():
        prefixed_key = f"decision_{key}"
        if prefixed_key in phase_timings_ms:
            continue
        phase_timings_ms[prefixed_key] = value
    phase_timings_ms["execution_depth_stage_ms"] = _float_or_none((execution_payload.get("timings_ms") or {}).get("depth_stage_ms"))
    phase_timings_ms["execution_depth_plan_reused"] = bool((execution_payload.get("timings_ms") or {}).get("depth_plan_reused"))
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
        "timings_ms": phase_timings_ms,
        "persist_pipeline_enabled": pipeline_persist_enabled,
        "persist_side_effects_enabled": side_effect_persist_enabled,
        "signal_prewarm_payload": signal_prewarm_payload,
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
    if reason != "order_submitted":
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
    key = build_market_offset_trade_count_key(market_id=market_id, offset=offset)
    if key is not None:
        counts[key] = int(int_or_none(counts.get(key)) or 0) + 1
    side_counts = _session_trade_count_map(session_state, side_aware=True)
    side_key = build_market_offset_side_trade_count_key(
        market_id=market_id,
        offset=offset,
        side=(execution or {}).get("selected_side") or (decision or {}).get("selected_side"),
    )
    if side_key is not None:
        side_counts[side_key] = int(int_or_none(side_counts.get(side_key)) or 0) + 1


def _build_session_state_payload(session_state: dict[str, Any] | None) -> dict[str, object]:
    counts = _session_trade_count_map(session_state)
    side_counts = _session_trade_count_map(session_state, side_aware=True)
    return {
        "market_offset_trade_count": {
            str(key): int(int_or_none(value) or 0)
            for key, value in sorted(counts.items(), key=lambda item: str(item[0]))
        },
        "market_offset_side_trade_count": {
            str(key): int(int_or_none(value) or 0)
            for key, value in sorted(side_counts.items(), key=lambda item: str(item[0]))
        },
        "tracked_market_offset_count": int(len(counts)),
        "tracked_market_offset_side_count": int(len(side_counts)),
        "action_gate_state": _action_gate_state_payload(session_state),
        "side_effect_state": _side_effect_state_payload(session_state),
    }


def _session_trade_count_map(session_state: dict[str, Any] | None, *, side_aware: bool = False) -> dict[str, Any]:
    if not isinstance(session_state, dict):
        return {}
    state_key = "market_offset_side_trade_count" if side_aware else "market_offset_trade_count"
    counts = session_state.get(state_key)
    if isinstance(counts, dict):
        return counts
    counts = {}
    session_state[state_key] = counts
    return counts


def _action_gate_state_payload(session_state: dict[str, Any] | None) -> dict[str, object]:
    if not isinstance(session_state, dict):
        return {}
    raw = session_state.get("action_gate_state")
    if not isinstance(raw, dict):
        return {}
    payload: dict[str, object] = {}
    for action_type, action_map in sorted(raw.items(), key=lambda item: str(item[0])):
        if not isinstance(action_map, dict) or not action_map:
            continue
        rows: dict[str, object] = {}
        for action_key, state in sorted(action_map.items(), key=lambda item: str(item[0])):
            if not isinstance(state, dict):
                continue
            rows[str(action_key)] = {
                "action_key": str(state.get("action_key") or action_key),
                "snapshot_ts": state.get("snapshot_ts"),
                "status": state.get("status"),
                "reason": state.get("reason"),
                "attempt": int(int_or_none(state.get("attempt")) or 0),
                "last_attempt_snapshot_ts": state.get("last_attempt_snapshot_ts"),
                "last_attempt_status": state.get("last_attempt_status"),
                "last_attempt_reason": state.get("last_attempt_reason"),
                "dry_run": bool(state.get("dry_run")),
            }
        if rows:
            payload[str(action_type)] = rows
    return payload


def _session_side_effect_state(session_state: dict[str, Any] | None, *, create: bool = True) -> dict[str, object]:
    if not isinstance(session_state, dict):
        return {}
    raw = session_state.get("side_effect_state")
    if isinstance(raw, dict):
        return raw
    if not create:
        return {}
    raw = {}
    session_state["side_effect_state"] = raw
    return raw


def _load_cached_liquidity_payload(session_state: dict[str, Any] | None) -> dict[str, object] | None:
    if not isinstance(session_state, dict):
        return None
    raw = session_state.get("liquidity_state_cache")
    if not isinstance(raw, dict):
        return None
    if _liquidity_payload_is_stale(raw):
        session_state.pop("liquidity_state_cache", None)
        return None
    return dict(raw)


def _store_liquidity_payload(
    *,
    session_state: dict[str, Any] | None,
    payload: dict[str, object] | None,
) -> None:
    if not isinstance(session_state, dict) or not isinstance(payload, dict):
        return
    session_state["liquidity_state_cache"] = dict(payload)


def _clear_live_signal_cache(*, session_state: dict[str, Any] | None) -> None:
    if not isinstance(session_state, dict):
        return
    session_state.pop("live_signal_cache", None)


def _side_effect_state_payload(session_state: dict[str, Any] | None) -> dict[str, object]:
    raw = _session_side_effect_state(session_state, create=False)
    payload: dict[str, object] = {}
    for name, state in sorted(raw.items(), key=lambda item: str(item[0])):
        if not isinstance(state, dict):
            continue
        payload[str(name)] = {
            "last_started_at_epoch": _float_or_none(state.get("last_started_at_epoch")),
            "last_completed_at_epoch": _float_or_none(state.get("last_completed_at_epoch")),
            "last_snapshot_ts": state.get("last_snapshot_ts"),
            "last_status": state.get("last_status"),
            "last_reason": state.get("last_reason"),
        }
    return payload


def _reserve_side_effect_slot(
    *,
    session_state: dict[str, Any] | None,
    name: str,
    enabled: bool,
    interval_seconds: float,
) -> dict[str, object]:
    interval_seconds = max(0.0, float(interval_seconds))
    if not enabled:
        return {
            "should_run": False,
            "reason": f"{name}_disabled",
            "interval_seconds": interval_seconds,
            "remaining_seconds": None,
        }
    now_ts = time.time()
    if interval_seconds <= 0.0:
        _mark_side_effect_started(session_state=session_state, name=name, started_at_epoch=now_ts)
        return {
            "should_run": True,
            "reason": "enabled",
            "interval_seconds": interval_seconds,
            "remaining_seconds": 0.0,
        }
    state = _session_side_effect_state(session_state)
    previous = state.get(name) if isinstance(state.get(name), dict) else {}
    last_started_at = _float_or_none(previous.get("last_started_at_epoch"))
    if last_started_at is None or (now_ts - last_started_at) >= interval_seconds:
        _mark_side_effect_started(session_state=session_state, name=name, started_at_epoch=now_ts)
        return {
            "should_run": True,
            "reason": "enabled",
            "interval_seconds": interval_seconds,
            "remaining_seconds": 0.0,
        }
    return {
        "should_run": False,
        "reason": f"{name}_interval_not_elapsed",
        "interval_seconds": interval_seconds,
        "remaining_seconds": max(0.0, interval_seconds - max(0.0, now_ts - last_started_at)),
    }


def _reserve_liquidity_slot(
    *,
    session_state: dict[str, Any] | None,
    interval_seconds: float,
) -> dict[str, object]:
    return _reserve_side_effect_slot(
        session_state=session_state,
        name="liquidity_sync",
        enabled=True,
        interval_seconds=interval_seconds,
    )


def _mark_side_effect_started(
    *,
    session_state: dict[str, Any] | None,
    name: str,
    started_at_epoch: float,
) -> None:
    state = _session_side_effect_state(session_state)
    item = state.get(name) if isinstance(state.get(name), dict) else {}
    item["last_started_at_epoch"] = float(started_at_epoch)
    state[name] = item


def _record_side_effect_outcome(
    *,
    session_state: dict[str, Any] | None,
    name: str,
    payload: dict[str, object] | None,
) -> None:
    state = _session_side_effect_state(session_state)
    item = state.get(name) if isinstance(state.get(name), dict) else {}
    item["last_completed_at_epoch"] = float(time.time())
    item["last_snapshot_ts"] = None if not isinstance(payload, dict) else payload.get("snapshot_ts")
    item["last_status"] = None if not isinstance(payload, dict) else payload.get("status")
    item["last_reason"] = None if not isinstance(payload, dict) else payload.get("reason")
    state[name] = item


def _session_signal_prewarm_state(session_state: dict[str, Any] | None, *, create: bool = True) -> dict[str, object]:
    if not isinstance(session_state, dict):
        return {}
    raw = session_state.get("signal_prewarm_state")
    if isinstance(raw, dict):
        return raw
    if not create:
        return {}
    raw = {}
    session_state["signal_prewarm_state"] = raw
    return raw


def _maybe_prewarm_signal_cache(
    cfg,
    *,
    target: str,
    feature_set: str | None,
    session_state: dict[str, Any] | None,
    prewarm_live_signal_inputs_fn,
    prewarm_live_signal_cache_fn,
) -> dict[str, object]:
    if not _env_bool("PM15MIN_RUNNER_ENABLE_SIGNAL_PREWARM", default=True):
        return {"status": "skipped", "reason": "signal_prewarm_disabled", "triggered": False}
    now_epoch = time.time()
    boundary_offset_sec = now_epoch % 60.0
    boundary_offset_ms = round(boundary_offset_sec * 1000.0, 3)
    state = _session_signal_prewarm_state(session_state)
    finalize_min_delay_sec = max(0.0, _env_float("PM15MIN_RUNNER_SIGNAL_PREWARM_MIN_DELAY_SEC", default=0.0))
    finalize_max_delay_sec = max(finalize_min_delay_sec, _env_float("PM15MIN_RUNNER_SIGNAL_PREWARM_MAX_DELAY_SEC", default=3.0))
    finalize_closed_bar_wait_sec = max(
        0.0,
        _env_float("PM15MIN_RUNNER_SIGNAL_PREWARM_FINALIZE_CLOSED_BAR_WAIT_SEC", default=1.5),
    )
    finalize_retry_interval_sec = max(
        0.0,
        _env_float("PM15MIN_RUNNER_SIGNAL_PREWARM_FINALIZE_RETRY_INTERVAL_SEC", default=0.05),
    )
    prepare_min_delay_sec = max(0.0, _env_float("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_MIN_DELAY_SEC", default=57.0))
    prepare_max_delay_sec = max(prepare_min_delay_sec, _env_float("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_MAX_DELAY_SEC", default=59.9))
    prepare_trigger_sec = _signal_prewarm_prepare_trigger_sec(
        market=str(cfg.asset.slug),
        default=prepare_min_delay_sec,
        upper_bound=prepare_max_delay_sec,
    )
    prepare_finalize_bridge_sec = max(
        0.0,
        _env_float("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_FINALIZE_BRIDGE_SEC", default=4.0),
    )
    bucket_epoch = int(now_epoch // 60)
    finalize_closed_bar_budget_sec = min(
        max(0.0, float(finalize_max_delay_sec - boundary_offset_sec)),
        finalize_closed_bar_wait_sec,
    )

    if prepare_trigger_sec <= boundary_offset_sec <= prepare_max_delay_sec:
        prepare_payload = _run_signal_prewarm_stage(
            cfg,
            target=target,
            feature_set=feature_set,
            session_state=session_state,
            prewarm_stage_fn=prewarm_live_signal_inputs_fn,
            state=state,
            bucket_epoch=bucket_epoch,
            boundary_offset_ms=boundary_offset_ms,
            stage="prepare",
            state_bucket_key="last_prepare_bucket_epoch",
        )
        if (
            str(prepare_payload.get("status") or "").strip().lower() == "ok"
            and str(prepare_payload.get("stage") or "") == "prepare"
        ):
            now_epoch = time.time()
            boundary_offset_sec = now_epoch % 60.0
            boundary_offset_ms = round(boundary_offset_sec * 1000.0, 3)
            next_bucket_epoch = int(now_epoch // 60)
            crossed_boundary = int(next_bucket_epoch) > int(bucket_epoch)
            if (not crossed_boundary) and max(0.0, 60.0 - boundary_offset_sec) <= prepare_finalize_bridge_sec:
                seconds_to_boundary = max(0.0, 60.0 - boundary_offset_sec)
                if seconds_to_boundary > 0.0:
                    time.sleep(seconds_to_boundary)
                now_epoch = time.time()
                boundary_offset_sec = now_epoch % 60.0
                boundary_offset_ms = round(boundary_offset_sec * 1000.0, 3)
                next_bucket_epoch = int(now_epoch // 60)
                crossed_boundary = int(next_bucket_epoch) > int(bucket_epoch)
            bucket_epoch = next_bucket_epoch
            finalize_closed_bar_budget_sec = min(
                max(0.0, float(finalize_max_delay_sec - boundary_offset_sec)),
                finalize_closed_bar_wait_sec,
            )
            if crossed_boundary and finalize_min_delay_sec <= boundary_offset_sec <= finalize_max_delay_sec:
                return _run_signal_prewarm_stage(
                    cfg,
                    target=target,
                    feature_set=feature_set,
                    session_state=session_state,
                    prewarm_stage_fn=prewarm_live_signal_cache_fn,
                    state=state,
                    bucket_epoch=bucket_epoch,
                    boundary_offset_ms=boundary_offset_ms,
                    stage="finalize",
                    state_bucket_key="last_finalize_bucket_epoch",
                    closed_bar_wait_sec=finalize_closed_bar_budget_sec,
                    retry_interval_sec=finalize_retry_interval_sec if finalize_closed_bar_budget_sec > 0.0 else 0.0,
                )
        return prepare_payload
    if finalize_min_delay_sec <= boundary_offset_sec <= finalize_max_delay_sec:
        return _run_signal_prewarm_stage(
            cfg,
            target=target,
            feature_set=feature_set,
            session_state=session_state,
            prewarm_stage_fn=prewarm_live_signal_cache_fn,
            state=state,
            bucket_epoch=bucket_epoch,
            boundary_offset_ms=boundary_offset_ms,
            stage="finalize",
            state_bucket_key="last_finalize_bucket_epoch",
            closed_bar_wait_sec=finalize_closed_bar_budget_sec,
            retry_interval_sec=finalize_retry_interval_sec if finalize_closed_bar_budget_sec > 0.0 else 0.0,
        )
    return {
        "status": "skipped",
        "reason": "outside_signal_prewarm_window",
        "triggered": False,
        "boundary_offset_ms": boundary_offset_ms,
    }


def _signal_prewarm_prepare_trigger_sec(
    *,
    market: str,
    default: float,
    upper_bound: float,
) -> float:
    token = str(market or "").strip().lower()
    stagger_defaults = {
        "sol": 56.6,
        "xrp": 57.2,
        "eth": 57.8,
        "btc": 58.4,
    }
    trigger_sec = float(stagger_defaults.get(token, default))
    trigger_sec = max(0.0, trigger_sec)
    trigger_sec = min(float(upper_bound), trigger_sec)
    return trigger_sec


def _run_signal_prewarm_stage(
    cfg,
    *,
    target: str,
    feature_set: str | None,
    session_state: dict[str, Any] | None,
    prewarm_stage_fn,
    state: dict[str, object],
    bucket_epoch: int,
    boundary_offset_ms: float,
    stage: str,
    state_bucket_key: str,
    closed_bar_wait_sec: float = 0.0,
    retry_interval_sec: float = 0.0,
) -> dict[str, object]:
    if int_or_none(state.get(state_bucket_key)) == int(bucket_epoch):
        return {
            "status": "skipped",
            "reason": f"signal_prewarm_{stage}_already_attempted_for_bucket",
            "triggered": False,
            "boundary_offset_ms": boundary_offset_ms,
            "stage": stage,
        }
    started = time.perf_counter()
    closed_bar_deadline = time.monotonic() + max(0.0, float(closed_bar_wait_sec))
    while True:
        try:
            if stage == "prepare":
                payload = prewarm_stage_fn(
                    cfg,
                    target=target,
                    feature_set=feature_set,
                )
            else:
                payload = prewarm_stage_fn(
                    cfg,
                    target=target,
                    feature_set=feature_set,
                    persist=False,
                    session_state=session_state,
                    marker_source="prewarm_finalize",
                )
        except LiveClosedBarNotReadyError as exc:
            if stage == "finalize":
                deferred = _maybe_retry_signal_prewarm_finalize(
                    reason_payload={
                        "status": "deferred",
                        "reason": "signal_prewarm_waiting_for_closed_bar",
                        "triggered": False,
                        "boundary_offset_ms": boundary_offset_ms,
                        "elapsed_ms": _elapsed_ms(started),
                        "stage": stage,
                        "error": str(exc),
                    },
                    closed_bar_deadline=closed_bar_deadline,
                    retry_interval_sec=retry_interval_sec,
                )
                if deferred is None:
                    continue
                return deferred
            return {
                "status": "deferred",
                "reason": "signal_prewarm_waiting_for_closed_bar",
                "triggered": False,
                "boundary_offset_ms": boundary_offset_ms,
                "elapsed_ms": _elapsed_ms(started),
                "stage": stage,
                "error": str(exc),
            }
        if str(payload.get("status") or "").strip().lower() == "deferred":
            deferred_payload = {
                **payload,
                "triggered": False,
                "boundary_offset_ms": boundary_offset_ms,
                "elapsed_ms": _elapsed_ms(started),
                "stage": stage,
            }
            if stage == "finalize":
                deferred = _maybe_retry_signal_prewarm_finalize(
                    reason_payload=deferred_payload,
                    closed_bar_deadline=closed_bar_deadline,
                    retry_interval_sec=retry_interval_sec,
                )
                if deferred is None:
                    continue
                return deferred
            return deferred_payload
        break
    elapsed_ms = _elapsed_ms(started)
    state[state_bucket_key] = int(bucket_epoch)
    state[f"last_{stage}_boundary_offset_ms"] = boundary_offset_ms
    state[f"last_{stage}_elapsed_ms"] = elapsed_ms
    state[f"last_{stage}_snapshot_ts"] = payload.get("snapshot_ts")
    state[f"last_{stage}_cache_hit"] = bool(payload.get("cache_hit"))
    return {
        **payload,
        "status": "ok",
        "triggered": True,
        "elapsed_ms": elapsed_ms,
        "boundary_offset_ms": boundary_offset_ms,
        "stage": stage,
    }


def _maybe_retry_signal_prewarm_finalize(
    *,
    reason_payload: dict[str, object],
    closed_bar_deadline: float,
    retry_interval_sec: float,
) -> dict[str, object] | None:
    if max(0.0, float(retry_interval_sec)) <= 0.0:
        return reason_payload
    remaining = max(0.0, float(closed_bar_deadline) - time.monotonic())
    if remaining <= 0.0:
        return reason_payload
    sleep_sec = min(max(0.0, float(retry_interval_sec)), remaining)
    if sleep_sec <= 0.0:
        return reason_payload
    time.sleep(sleep_sec)
    return None


def _build_account_state_skip_payload(
    cfg,
    *,
    cycle: str,
    snapshot_ts: str,
    reason: str,
    interval_seconds: object,
    remaining_seconds: object,
) -> dict[str, object]:
    return {
        "domain": "live",
        "dataset": "live_account_state_sync",
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "snapshot_ts": snapshot_ts,
        "status": "skipped",
        "reason": reason,
        "open_orders": {"status": "skipped", "reason": reason},
        "positions": {"status": "skipped", "reason": reason},
        "summary": {
            "interval_seconds": _float_or_none(interval_seconds),
            "remaining_seconds": _float_or_none(remaining_seconds),
        },
        "cycle": cycle,
    }


def _build_side_effect_skip_payload(
    cfg,
    *,
    cycle: str,
    snapshot_ts: str,
    dataset: str,
    reason: str,
    interval_seconds: object,
    remaining_seconds: object,
    summary: dict[str, object],
) -> dict[str, object]:
    return {
        "domain": "live",
        "dataset": dataset,
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": cycle,
        "snapshot_ts": snapshot_ts,
        "status": "skipped",
        "reason": reason,
        "interval_seconds": _float_or_none(interval_seconds),
        "remaining_seconds": _float_or_none(remaining_seconds),
        "summary": dict(summary),
    }


def _env_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


def _env_float(name: str, *, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return float(default)
    value = _float_or_none(raw)
    if value is None:
        return float(default)
    return float(value)


def _float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _elapsed_ms(started_at: float) -> float:
    return round(max(0.0, (time.perf_counter() - float(started_at)) * 1000.0), 3)


def int_or_none(value) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _liquidity_payload_is_stale(payload: dict[str, Any]) -> bool:
    refresh_seconds = _float_or_none(payload.get("refresh_seconds"))
    if refresh_seconds is None or refresh_seconds <= 0.0:
        return False
    checked_at_epoch = _iso_to_epoch_seconds(payload.get("checked_at"))
    if checked_at_epoch is None:
        return False
    return max(0.0, time.time() - checked_at_epoch) >= float(refresh_seconds)


def _iso_to_epoch_seconds(value: object) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        parsed = datetime.fromisoformat(normalized)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return float(parsed.timestamp())
