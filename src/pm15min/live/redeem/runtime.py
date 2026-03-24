from __future__ import annotations

import time

from ..layout import LiveStateLayout


def _resolve_iteration_limit(*, iterations: int, loop: bool) -> int | None:
    raw_iterations = int(iterations)
    if loop and raw_iterations <= 0:
        return None
    return max(1, raw_iterations)


def run_live_redeem_loop(
    cfg,
    *,
    iterations: int = 1,
    loop: bool = False,
    sleep_sec: float = 60.0,
    persist: bool = True,
    refresh_account_state: bool = True,
    dry_run: bool = False,
    max_conditions: int | None = None,
    gateway=None,
    apply_redeem_policy_fn,
    append_jsonl_fn,
    write_live_payload_pair_fn,
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
    had_action_warning = False
    last_iteration: dict[str, object] | None = None

    log_path = layout.redeem_runner_log_path(
        market=cfg.asset.slug,
        cycle=cycle,
        profile=cfg.profile,
    )

    while True:
        if iteration_limit is not None and attempted >= iteration_limit:
            break
        attempted += 1
        try:
            redeem_payload = apply_redeem_policy_fn(
                cfg,
                persist=persist,
                refresh_account_state=refresh_account_state,
                dry_run=dry_run,
                max_conditions=max_conditions,
                gateway=gateway,
            )
            completed += 1
            action_status = str(redeem_payload.get("status") or "")
            if action_status in {"error", "ok_with_errors"}:
                had_action_warning = True
            last_iteration = {
                "snapshot_ts": redeem_payload.get("snapshot_ts"),
                "status": action_status,
                "reason": redeem_payload.get("reason"),
                "summary": redeem_payload.get("summary"),
                "trading_gateway": redeem_payload.get("trading_gateway"),
                "candidate_conditions": (redeem_payload.get("summary") or {}).get("candidate_conditions"),
                "submitted_conditions": (redeem_payload.get("summary") or {}).get("submitted_conditions"),
                "redeemed_conditions": (redeem_payload.get("summary") or {}).get("redeemed_conditions"),
                "error_conditions": (redeem_payload.get("summary") or {}).get("error_conditions"),
            }
            append_jsonl_fn(
                log_path,
                {
                    "ts": utc_now_iso_fn(),
                    "event": "redeem_iteration",
                    "iteration": attempted,
                    "market": cfg.asset.slug,
                    "profile": cfg.profile,
                    "cycle": cycle,
                    "dry_run": bool(dry_run),
                    "refresh_account_state": bool(refresh_account_state),
                    "max_conditions": None if max_conditions is None else int(max_conditions),
                    "redeem_action": last_iteration,
                },
            )
        except Exception as exc:
            errors += 1
            append_jsonl_fn(
                log_path,
                {
                    "ts": utc_now_iso_fn(),
                    "event": "redeem_error",
                    "iteration": attempted,
                    "market": cfg.asset.slug,
                    "profile": cfg.profile,
                    "cycle": cycle,
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

    status = "ok"
    if errors > 0 and completed == 0:
        status = "error"
    elif errors > 0 or had_action_warning:
        status = "ok_with_errors"

    summary = {
        "domain": "live",
        "dataset": "live_redeem_runner_summary",
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": cycle,
        "run_started_at": run_started_at,
        "completed_iterations": completed,
        "errors": errors,
        "status": status,
        "dry_run": bool(dry_run),
        "refresh_account_state": bool(refresh_account_state),
        "max_conditions": None if max_conditions is None else int(max_conditions),
        "last_iteration": last_iteration,
        "redeem_runner_log_path": str(log_path),
    }
    if persist and last_iteration is not None and last_iteration.get("snapshot_ts"):
        paths = write_live_payload_pair_fn(
            payload=summary,
            latest_path=layout.latest_redeem_runner_path(
                market=cfg.asset.slug,
                cycle=cycle,
                profile=cfg.profile,
            ),
            snapshot_path=layout.redeem_runner_snapshot_path(
                market=cfg.asset.slug,
                cycle=cycle,
                profile=cfg.profile,
                snapshot_ts=str(last_iteration["snapshot_ts"]),
            ),
        )
        summary["latest_redeem_runner_path"] = str(paths["latest"])
        summary["redeem_runner_snapshot_path"] = str(paths["snapshot"])
    return summary
