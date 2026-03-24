from __future__ import annotations

import time

from ..layout import LiveStateLayout


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
    session_state: dict[str, object] = {"market_offset_trade_count": {}}

    runner_log_path = layout.runner_log_path(
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
                    "liquidity_state": iteration_payload.get("liquidity_state"),
                    "regime_state": iteration_payload.get("regime_state"),
                    "decision": iteration_payload.get("decision"),
                    "execution": iteration_payload.get("execution"),
                    "risk_summary": iteration_payload.get("risk_summary"),
                    "runner_health": iteration_payload.get("runner_health"),
                    "risk_alerts": iteration_payload.get("risk_alerts"),
                    "risk_alert_summary": iteration_payload.get("risk_alert_summary"),
                    "order_action": iteration_payload.get("order_action"),
                    "cancel_action": iteration_payload.get("cancel_action"),
                    "redeem_action": iteration_payload.get("redeem_action"),
                    "session_state": iteration_payload.get("session_state"),
                },
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
    }
    if persist and last_iteration is not None:
        write_json_atomic_fn(summary, layout.latest_runner_path(market=cfg.asset.slug, cycle=cycle, profile=cfg.profile, target=target))
    return summary
