from __future__ import annotations

import os
import time
import json

from ..layout import LiveStateLayout
from ..session_state import (
    build_market_offset_side_trade_count_key,
    build_market_offset_trade_count_key,
)
from .audit import (
    build_runner_decision_audit_event,
    build_runner_order_audit_event,
)


def _resolve_iteration_limit(*, iterations: int, loop: bool) -> int | None:
    raw_iterations = int(iterations)
    if loop and raw_iterations <= 0:
        return None
    return max(1, raw_iterations)


def run_live_runner(
    cfg,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    iterations: int = 1,
    loop: bool = False,
    sleep_sec: float = 1.0,
    persist: bool = True,
    run_foundation: bool = True,
    foundation_include_direct_oracle: bool = True,
    foundation_include_orderbooks: bool = True,
    apply_side_effects: bool = True,
    side_effect_dry_run: bool = False,
    gateway=None,
    build_runner_iteration_fn,
    persist_runner_iteration_fn,
    append_jsonl_fn,
    write_json_atomic_fn,
    utc_now_iso_fn,
) -> dict[str, object]:
    iteration_limit = _resolve_iteration_limit(iterations=iterations, loop=loop)
    sleep_sec = max(0.0, float(sleep_sec))
    cycle = f"{int(cfg.cycle_minutes)}m"
    layout = LiveStateLayout.discover(root=cfg.layout.rewrite.root)
    run_started_at = utc_now_iso_fn()
    completed = 0
    attempted = 0
    errors = 0
    last_iteration: dict[str, object] | None = None
    session_state: dict[str, object] = _load_persisted_session_state(
        layout=layout,
        market=cfg.asset.slug,
        cycle=cycle,
        profile=cfg.profile,
        target=target,
    )
    tracked_offsets = _resolve_tracked_runner_offsets()
    tracked_log_interval_sec = _env_float(
        "PM15MIN_RUNNER_TRACKED_LOG_INTERVAL_SEC",
        default=float(os.getenv("PM15MIN_RUNNER_OK_LOG_INTERVAL_SEC", "5")),
    )
    untracked_log_interval_sec = _env_float("PM15MIN_RUNNER_UNTRACKED_LOG_INTERVAL_SEC", default=0.0)
    next_tracked_log_at = 0.0
    next_untracked_log_at = 0.0

    runner_log_path = layout.runner_log_path(
        market=cfg.asset.slug,
        cycle=cycle,
        profile=cfg.profile,
        target=target,
    )
    runner_audit_log_path = layout.runner_audit_log_path(
        market=cfg.asset.slug,
        cycle=cycle,
        profile=cfg.profile,
        target=target,
    )

    while True:
        if iteration_limit is not None and attempted >= iteration_limit:
            break
        attempted += 1
        try:
            iteration_payload = build_runner_iteration_fn(
                cfg,
                target=target,
                feature_set=feature_set,
                persist_decision=persist,
                persist_execution=persist,
                run_foundation=run_foundation,
                foundation_include_direct_oracle=foundation_include_direct_oracle,
                foundation_include_orderbooks=foundation_include_orderbooks,
                apply_side_effects=apply_side_effects,
                side_effect_dry_run=side_effect_dry_run,
                gateway=gateway,
                session_state=session_state,
            )
            completed += 1
            last_iteration = iteration_payload
            if persist:
                paths = persist_runner_iteration_fn(rewrite_root=cfg.layout.rewrite.root, payload=iteration_payload)
                iteration_payload["latest_runner_path"] = str(paths["latest"])
                iteration_payload["runner_snapshot_path"] = str(paths["snapshot"])
            now_ts = time.time()
            should_log, tracked_iteration = _should_log_runner_iteration(
                iteration_payload=iteration_payload,
                iteration=attempted,
                now_ts=now_ts,
                tracked_offsets=tracked_offsets,
                tracked_log_interval_sec=tracked_log_interval_sec,
                untracked_log_interval_sec=untracked_log_interval_sec,
                next_tracked_log_at=next_tracked_log_at,
                next_untracked_log_at=next_untracked_log_at,
            )
            if should_log:
                if tracked_iteration:
                    next_tracked_log_at = now_ts + tracked_log_interval_sec
                else:
                    next_untracked_log_at = now_ts + untracked_log_interval_sec
                decision = iteration_payload.get("decision") or {}
                execution = iteration_payload.get("execution") or {}
                runner_health = iteration_payload.get("runner_health") or {}
                risk_alert_summary = iteration_payload.get("risk_alert_summary") or {}
                order_action = iteration_payload.get("order_action") or {}
                cancel_action = iteration_payload.get("cancel_action") or {}
                redeem_action = iteration_payload.get("redeem_action") or {}
                focal_offset = _runner_log_focal_offset(iteration_payload)
                append_jsonl_fn(
                    runner_log_path,
                    {
                        "ts": utc_now_iso_fn(),
                        "event": "runner_iteration",
                        "iteration": attempted,
                        "market": cfg.asset.slug,
                        "profile": cfg.profile,
                        "cycle": cycle,
                        "target": target,
                        "snapshot_ts": iteration_payload.get("snapshot_ts"),
                        "decision_status": decision.get("status"),
                        "decision_market_id": decision.get("selected_quote_market_id"),
                        "decision_offset": decision.get("selected_offset"),
                        "runner_log_focal_offset": focal_offset,
                        "runner_log_tracked": tracked_iteration,
                        "execution_status": execution.get("status"),
                        "execution_market_id": execution.get("market_id"),
                        "runner_health_status": runner_health.get("overall_status"),
                        "has_critical_alert": risk_alert_summary.get("has_critical"),
                        "order_action_status": order_action.get("status"),
                        "cancel_action_status": cancel_action.get("status"),
                        "redeem_action_status": redeem_action.get("status"),
                        "trade_counts": (iteration_payload.get("session_state") or {}).get("market_offset_trade_count"),
                    },
                )
            _append_runner_audit_events(
                iteration_payload=iteration_payload,
                iteration=attempted,
                session_state=session_state,
                runner_audit_log_path=runner_audit_log_path,
                append_jsonl_fn=append_jsonl_fn,
                utc_now_iso_fn=utc_now_iso_fn,
            )
        except Exception as exc:
            errors += 1
            append_jsonl_fn(
                runner_log_path,
                {
                    "ts": utc_now_iso_fn(),
                    "event": "runner_error",
                    "iteration": attempted,
                    "market": cfg.asset.slug,
                    "profile": cfg.profile,
                    "cycle": cycle,
                    "target": target,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            )
            if not loop:
                raise

        if not loop:
            break
        if iteration_limit is not None and attempted >= iteration_limit:
            break
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    last_iteration_alerts = {} if last_iteration is None else (last_iteration.get("risk_alert_summary") or {})
    has_iteration_critical = bool(last_iteration_alerts.get("has_critical"))
    summary = {
        "domain": "live",
        "dataset": "live_runner_summary",
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": cycle,
        "target": target,
        "run_started_at": run_started_at,
        "completed_iterations": completed,
        "errors": errors,
        "status": (
            "error"
            if errors > 0 and completed == 0
            else ("ok_with_errors" if errors > 0 or has_iteration_critical else "ok")
        ),
        "last_iteration": last_iteration,
        "session_state": None if last_iteration is None else last_iteration.get("session_state"),
        "runner_log_path": str(runner_log_path),
        "runner_audit_log_path": str(runner_audit_log_path),
    }
    if persist and last_iteration is not None:
        write_json_atomic_fn(summary, layout.latest_runner_path(market=cfg.asset.slug, cycle=cycle, profile=cfg.profile, target=target))
    return summary


def _resolve_tracked_runner_offsets() -> set[int]:
    raw = str(os.getenv("PM15MIN_RUNNER_LOG_OFFSETS", "7,8,9") or "")
    offsets: set[int] = set()
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            offsets.add(int(item))
        except Exception:
            continue
    return offsets


def _env_float(name: str, *, default: float) -> float:
    try:
        return max(0.0, float(os.getenv(name, str(default))))
    except Exception:
        return max(0.0, float(default))


def _should_log_runner_iteration(
    *,
    iteration_payload: dict[str, object],
    iteration: int,
    now_ts: float,
    tracked_offsets: set[int],
    tracked_log_interval_sec: float,
    untracked_log_interval_sec: float,
    next_tracked_log_at: float,
    next_untracked_log_at: float,
) -> tuple[bool, bool]:
    focal_offset = _runner_log_focal_offset(iteration_payload)
    tracked_iteration = focal_offset is not None and int(focal_offset) in tracked_offsets
    if tracked_iteration:
        if tracked_log_interval_sec <= 0.0:
            return True, True
        return iteration == 1 or now_ts >= next_tracked_log_at, True
    if untracked_log_interval_sec <= 0.0:
        return False, False
    return iteration == 1 or now_ts >= next_untracked_log_at, False


def _runner_log_focal_offset(iteration_payload: dict[str, object]) -> int | None:
    decision = iteration_payload.get("decision") if isinstance(iteration_payload, dict) else {}
    execution = iteration_payload.get("execution") if isinstance(iteration_payload, dict) else {}
    offset = _int_or_none((decision or {}).get("selected_offset"))
    if offset is not None:
        return offset
    offset = _int_or_none((execution or {}).get("selected_offset"))
    if offset is not None:
        return offset
    decision_payload = iteration_payload.get("decision_payload") if isinstance(iteration_payload, dict) else {}
    rejected_offsets = (decision_payload or {}).get("rejected_offsets") or []
    best_rejected = _best_offset_row(rejected_offsets)
    if best_rejected is not None:
        return _int_or_none(best_rejected.get("offset"))
    return None


def _best_offset_row(rows: object) -> dict[str, object] | None:
    candidates = [dict(row) for row in list(rows or []) if isinstance(row, dict)]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (
            _float_or_zero(row.get("confidence")),
            _float_or_zero(row.get("edge")),
            -(_int_or_none(row.get("offset")) or 0),
        ),
    )


def _float_or_zero(value: object) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _int_or_none(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _append_runner_audit_events(
    *,
    iteration_payload: dict[str, object],
    iteration: int,
    session_state: dict[str, object] | None,
    runner_audit_log_path,
    append_jsonl_fn,
    utc_now_iso_fn,
) -> None:
    audit_state = _session_audit_state(session_state)
    for key, builder in (
        ("decision_signature", build_runner_decision_audit_event),
        ("order_signature", build_runner_order_audit_event),
    ):
        built = builder(
            iteration_payload=iteration_payload,
            iteration=iteration,
            emitted_at=utc_now_iso_fn(),
        )
        if built is None:
            continue
        signature, payload = built
        if signature == audit_state.get(key):
            continue
        append_jsonl_fn(runner_audit_log_path, payload)
        audit_state[key] = signature


def _session_audit_state(session_state: dict[str, object] | None) -> dict[str, str]:
    if not isinstance(session_state, dict):
        return {}
    audit_state = session_state.get("audit_state")
    if isinstance(audit_state, dict):
        return audit_state
    audit_state = {}
    session_state["audit_state"] = audit_state
    return audit_state


def _load_persisted_session_state(
    *,
    layout: LiveStateLayout,
    market: str,
    cycle: str,
    profile: str,
    target: str,
) -> dict[str, object]:
    state = {
        "market_offset_trade_count": {},
        "market_offset_side_trade_count": {},
        "action_gate_state": {},
    }
    path = layout.latest_runner_path(
        market=market,
        cycle=cycle,
        profile=profile,
        target=target,
    )
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        raw = payload.get("session_state")
        if isinstance(raw, dict):
            counts_raw = raw.get("market_offset_trade_count")
            state["market_offset_trade_count"] = {
                str(key): int(_int_or_none(value) or 0)
                for key, value in (counts_raw.items() if isinstance(counts_raw, dict) else [])
            }
            side_counts_raw = raw.get("market_offset_side_trade_count")
            state["market_offset_side_trade_count"] = {
                str(key): int(_int_or_none(value) or 0)
                for key, value in (side_counts_raw.items() if isinstance(side_counts_raw, dict) else [])
            }
            gate_raw = raw.get("action_gate_state")
            if isinstance(gate_raw, dict):
                gate_state: dict[str, dict[str, object]] = {}
                for action_type, action_map in gate_raw.items():
                    if not isinstance(action_map, dict):
                        continue
                    normalized: dict[str, object] = {}
                    for action_key, item in action_map.items():
                        normalized_item = _normalize_action_gate_state_item(action_key=action_key, state=item)
                        if normalized_item is not None:
                            normalized[str(action_key)] = normalized_item
                    if normalized:
                        gate_state[str(action_type)] = normalized
                state["action_gate_state"] = gate_state

    audit_path = layout.runner_audit_log_path(
        market=market,
        cycle=cycle,
        profile=profile,
        target=target,
    )
    _merge_session_state_from_audit_log(session_state=state, audit_path=audit_path)
    return state


def _merge_session_state_from_audit_log(*, session_state: dict[str, object], audit_path) -> None:
    if not audit_path.exists():
        return
    counts: dict[str, object] = {}
    side_counts: dict[str, object] = {}
    session_state["market_offset_trade_count"] = counts
    session_state["market_offset_side_trade_count"] = side_counts
    gate_state = session_state.get("action_gate_state")
    if not isinstance(gate_state, dict):
        gate_state = {}
        session_state["action_gate_state"] = gate_state
    order_gate: dict[str, object] = {}
    gate_state["order"] = order_gate

    try:
        with audit_path.open(encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                if payload.get("event") != "order_action":
                    continue
                execution = payload.get("execution")
                execution = execution if isinstance(execution, dict) else {}
                order_action = payload.get("order_action")
                order_action = order_action if isinstance(order_action, dict) else {}
                order_status = str(order_action.get("status") or "").strip().lower()
                order_reason = str(order_action.get("reason") or "").strip().lower()
                is_dry_run = bool(order_action.get("dry_run")) or order_reason == "dry_run"
                if order_status == "ok" and order_reason == "order_submitted":
                    market_id = str(execution.get("market_id") or "").strip()
                    offset = _int_or_none(execution.get("selected_offset"))
                    trade_count_key = build_market_offset_trade_count_key(market_id=market_id, offset=offset)
                    if trade_count_key is not None:
                        counts[trade_count_key] = int(_int_or_none(counts.get(trade_count_key)) or 0) + 1
                    trade_count_side_key = build_market_offset_side_trade_count_key(
                        market_id=market_id,
                        offset=offset,
                        side=execution.get("selected_side"),
                    )
                    if trade_count_side_key is not None:
                        side_counts[trade_count_side_key] = int(_int_or_none(side_counts.get(trade_count_side_key)) or 0) + 1
                action_key = str(order_action.get("action_key") or "").strip()
                if action_key and not is_dry_run:
                    normalized_item = _normalize_action_gate_state_item(action_key=action_key, state=order_action)
                    if normalized_item is not None:
                        snapshot_ts = payload.get("snapshot_ts")
                        if normalized_item.get("snapshot_ts") in (None, ""):
                            normalized_item["snapshot_ts"] = snapshot_ts
                        if normalized_item.get("last_attempt_snapshot_ts") in (None, ""):
                            normalized_item["last_attempt_snapshot_ts"] = snapshot_ts
                        order_gate[action_key] = normalized_item
    except Exception:
        return


def _normalize_action_gate_state_item(*, action_key: object, state: object) -> dict[str, object] | None:
    if not isinstance(state, dict):
        return None
    key = str(state.get("action_key") or action_key or "").strip()
    if not key:
        return None
    return {
        "action_key": key,
        "snapshot_ts": state.get("snapshot_ts"),
        "status": state.get("status"),
        "reason": state.get("reason"),
        "attempt": int(_int_or_none(state.get("attempt")) or 0),
        "last_attempt_snapshot_ts": state.get("last_attempt_snapshot_ts") or state.get("snapshot_ts"),
        "last_attempt_status": state.get("last_attempt_status") or state.get("status"),
        "last_attempt_reason": state.get("last_attempt_reason") or state.get("reason"),
        "dry_run": bool(state.get("dry_run")),
    }
