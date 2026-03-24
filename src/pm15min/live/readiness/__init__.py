from __future__ import annotations

import json

from pm15min.core.config import LiveConfig
from pm15min.data.config import DataConfig as LiveDataConfig
from pm15min.research.labels.runtime import build_truth_runtime_summary
from ..gateway.checks import check_live_trading_gateway
from ..layout import LiveStateLayout
from ..operator.followups import recommend_live_operator_actions
from ..operator.summary import build_live_operator_summary, ready_context_actions
from ..operator.smoke import build_live_operator_smoke_summary
from .state import live_latest_state_paths, live_latest_state_summary
from ..operator.utils import summarize_live_risk_alerts
from ..runtime import canonical_live_scope


def show_live_latest_runner(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    risk_only: bool = False,
) -> dict[str, object]:
    cycle = f"{int(cfg.cycle_minutes)}m"
    layout = LiveStateLayout.discover(root=cfg.layout.rewrite.root)
    latest_runner_path = layout.latest_runner_path(
        market=cfg.asset.slug,
        cycle=cycle,
        profile=cfg.profile,
        target=target,
    )
    latest_state_paths = live_latest_state_paths(cfg=cfg, target=target)
    latest_state_summary = live_latest_state_summary(cfg=cfg, target=target)
    scope = canonical_live_scope(cfg=cfg, target=target)
    truth_runtime_summary = _load_live_truth_runtime_summary(cfg=cfg, cycle=cycle)
    operator_summary = build_live_operator_summary(
        canonical_scope=scope,
        latest_state_summary=latest_state_summary,
        runner_payload=None,
        truth_runtime_summary=truth_runtime_summary,
    )
    payload: dict[str, object] = {
        "domain": "live",
        "dataset": "live_runner_status",
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": cycle,
        "target": target,
        "latest_runner_path": str(latest_runner_path),
        "latest_state_paths": latest_state_paths,
        "latest_state_summary": latest_state_summary,
        "canonical_live_scope": scope,
        "truth_runtime_summary": truth_runtime_summary,
    }
    if not latest_runner_path.exists():
        return {
            **payload,
            "status": "missing",
            "reason": "latest_runner_missing",
            "operator_summary": operator_summary,
            "next_actions": recommend_live_operator_actions(operator_summary=operator_summary),
        }
    try:
        latest = json.loads(latest_runner_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            **payload,
            "status": "error",
            "reason": f"{type(exc).__name__}: {exc}",
            "operator_summary": operator_summary,
            "next_actions": recommend_live_operator_actions(operator_summary=operator_summary),
        }
    last_iteration = latest.get("last_iteration") or {}
    risk_alerts = list(last_iteration.get("risk_alerts") or [])
    risk_alert_summary = summarize_live_risk_alerts(alerts=risk_alerts)
    runner_health = last_iteration.get("runner_health") or {}
    operator_summary = build_live_operator_summary(
        canonical_scope=scope,
        latest_state_summary=latest_state_summary,
        runner_payload=latest,
        truth_runtime_summary=truth_runtime_summary,
    )
    base = {
        **payload,
        "status": latest.get("status"),
        "reason": None,
        "run_started_at": latest.get("run_started_at"),
        "completed_iterations": latest.get("completed_iterations"),
        "errors": latest.get("errors"),
        "runner_log_path": latest.get("runner_log_path"),
        "last_iteration_snapshot_ts": last_iteration.get("snapshot_ts"),
        "risk_summary": last_iteration.get("risk_summary"),
        "runner_health": runner_health,
        "risk_alerts": risk_alerts,
        "risk_alert_summary": risk_alert_summary,
        "decision": last_iteration.get("decision"),
        "execution": last_iteration.get("execution"),
        "operator_summary": operator_summary,
        "next_actions": recommend_live_operator_actions(operator_summary=operator_summary),
    }
    if risk_only:
        return base
    return {
        **base,
        "order_action": last_iteration.get("order_action"),
        "cancel_action": last_iteration.get("cancel_action"),
        "redeem_action": last_iteration.get("redeem_action"),
        "last_iteration": last_iteration,
    }


def _load_live_truth_runtime_summary(*, cfg: LiveConfig, cycle: str) -> dict[str, object]:
    data_cfg = LiveDataConfig.build(
        market=cfg.asset.slug,
        cycle=cycle,
        surface="live",
        root=cfg.layout.rewrite.root,
    )
    try:
        return build_truth_runtime_summary(data_cfg)
    except Exception as exc:
        return {
            "truth_runtime_recent_refresh_status": "unknown",
            "truth_runtime_recent_refresh_interpretation": f"truth_runtime_summary_error:{type(exc).__name__}",
            "truth_runtime_foundation_reason": f"truth_runtime_summary_error:{type(exc).__name__}: {exc}",
        }


def show_live_ready(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    adapter: str | None = None,
    check_live_trading_gateway_fn=check_live_trading_gateway,
    show_live_latest_runner_fn=show_live_latest_runner,
) -> dict[str, object]:
    gateway_payload = check_live_trading_gateway_fn(cfg, adapter=adapter, probe_open_orders=True, probe_positions=True)
    runner_payload = show_live_latest_runner_fn(cfg, target=target, risk_only=True)
    scope = runner_payload.get("canonical_live_scope") or canonical_live_scope(cfg=cfg, target=target)
    operator_summary = runner_payload.get("operator_summary") or {}
    failed_gateway_checks = [
        str(item.get("name") or "")
        for item in (gateway_payload.get("checks") or [])
        if isinstance(item, dict) and not bool(item.get("ok", False))
    ]
    failed_gateway_probes = [
        str(name)
        for name, row in (gateway_payload.get("probes") or {}).items()
        if isinstance(row, dict) and str(row.get("status") or "").lower() not in {"", "not_run"} and not bool(row.get("ok", False))
    ]
    operator_smoke_summary = build_live_operator_smoke_summary(
        canonical_scope=scope,
        gateway_payload=gateway_payload,
        runner_payload=runner_payload,
        operator_summary=operator_summary,
    )
    ready = bool(
        scope.get("ok")
        and gateway_payload.get("ok")
        and operator_summary.get("can_run_side_effects")
    )
    primary_blocker = None
    if not bool(scope.get("ok")):
        primary_blocker = "outside_canonical_live_scope"
    elif failed_gateway_checks:
        primary_blocker = f"gateway:{failed_gateway_checks[0]}"
    elif failed_gateway_probes:
        primary_blocker = f"gateway_probe:{failed_gateway_probes[0]}"
    elif operator_summary.get("primary_blocker"):
        primary_blocker = str(operator_summary.get("primary_blocker"))
    next_actions = []
    if failed_gateway_checks:
        next_actions.append("resolve failed gateway checks before enabling side effects")
        if gateway_payload.get("recommended_smoke_runs"):
            next_actions.append("rerun the recommended smoke sequence from check-trading-gateway")
    if failed_gateway_probes:
        next_actions.append("inspect failed gateway probes before enabling side effects")
        if gateway_payload.get("recommended_smoke_runs"):
            next_actions.append("rerun the recommended smoke sequence from check-trading-gateway")
    next_actions.extend(list(runner_payload.get("next_actions") or []))
    next_actions.extend(ready_context_actions(operator_summary=operator_summary))
    next_actions = list(dict.fromkeys(next_actions))
    return {
        "domain": "live",
        "dataset": "live_ready_status",
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": f"{int(cfg.cycle_minutes)}m",
        "target": target,
        "adapter_override": adapter,
        "canonical_live_scope": scope,
        "gateway_ok": bool(gateway_payload.get("ok")),
        "gateway_failed_checks": failed_gateway_checks,
        "gateway_failed_probes": failed_gateway_probes,
        "runner_status": runner_payload.get("status"),
        "operator_summary": operator_summary,
        "operator_smoke_summary": operator_smoke_summary,
        "ready_for_side_effects": ready,
        "status": "ready" if ready else "not_ready",
        "primary_blocker": primary_blocker,
        "next_actions": next_actions,
        "gateway": gateway_payload,
        "runner": runner_payload,
    }
