from __future__ import annotations

from pm15min.data.config import DataConfig as LiveDataConfig
from pm15min.core.config import LiveConfig
from ..account import (
    load_latest_open_orders_snapshot,
    load_latest_positions_snapshot,
    summarize_open_orders_snapshot,
    summarize_positions_snapshot,
)
from ..layout import LiveStateLayout
from ..liquidity import load_latest_liquidity_state_snapshot, summarize_liquidity_state
from ..quotes.hot_cache import summarize_orderbook_hot_cache
from ..operator.utils import read_json_path
from ..regime import summarize_regime_state


def live_latest_state_paths(*, cfg: LiveConfig, target: str) -> dict[str, str]:
    cycle = f"{int(cfg.cycle_minutes)}m"
    layout = LiveStateLayout.discover(root=cfg.layout.rewrite.root)
    data_cfg = LiveDataConfig.build(
        market=cfg.asset.slug,
        cycle=cycle,
        surface="live",
        root=cfg.layout.rewrite.root,
    )
    return {
        "runner": str(layout.latest_runner_path(market=cfg.asset.slug, cycle=cycle, profile=cfg.profile, target=target)),
        "decision": str(layout.latest_decision_path(market=cfg.asset.slug, cycle=cycle, profile=cfg.profile, target=target)),
        "execution": str(layout.latest_execution_path(market=cfg.asset.slug, cycle=cycle, profile=cfg.profile, target=target)),
        "liquidity": str(layout.latest_liquidity_path(market=cfg.asset.slug, cycle=cycle, profile=cfg.profile)),
        "regime": str(layout.latest_regime_path(market=cfg.asset.slug, cycle=cycle, profile=cfg.profile)),
        "foundation_state": str(data_cfg.layout.foundation_state_path),
        "open_orders": str(layout.latest_open_orders_path(market=cfg.asset.slug)),
        "positions": str(layout.latest_positions_path(market=cfg.asset.slug)),
        "orderbook_recent": str(data_cfg.layout.orderbook_recent_path),
        "orderbook_recorder_state": str(data_cfg.layout.orderbook_state_path),
        "order_action": str(layout.latest_order_action_path(market=cfg.asset.slug, cycle=cycle, profile=cfg.profile, target=target)),
        "cancel_action": str(layout.latest_cancel_action_path(market=cfg.asset.slug, cycle=cycle, profile=cfg.profile)),
        "redeem_action": str(layout.latest_redeem_action_path(market=cfg.asset.slug, cycle=cycle, profile=cfg.profile)),
    }


def live_latest_state_summary(*, cfg: LiveConfig, target: str) -> dict[str, object]:
    cycle = f"{int(cfg.cycle_minutes)}m"
    layout = LiveStateLayout.discover(root=cfg.layout.rewrite.root)
    data_cfg = LiveDataConfig.build(
        market=cfg.asset.slug,
        cycle=cycle,
        surface="live",
        root=cfg.layout.rewrite.root,
    )
    decision_payload = read_json_path(
        layout.latest_decision_path(market=cfg.asset.slug, cycle=cycle, profile=cfg.profile, target=target)
    )
    execution_payload = read_json_path(
        layout.latest_execution_path(market=cfg.asset.slug, cycle=cycle, profile=cfg.profile, target=target)
    )
    liquidity_payload = load_latest_liquidity_state_snapshot(
        rewrite_root=cfg.layout.rewrite.root,
        market=cfg.asset.slug,
        cycle=cycle,
        profile=cfg.profile,
    )
    regime_payload = read_json_path(
        layout.latest_regime_path(market=cfg.asset.slug, cycle=cycle, profile=cfg.profile)
    )
    foundation_payload = read_json_path(data_cfg.layout.foundation_state_path)
    open_orders_payload = load_latest_open_orders_snapshot(
        rewrite_root=cfg.layout.rewrite.root,
        market=cfg.asset.slug,
    )
    positions_payload = load_latest_positions_snapshot(
        rewrite_root=cfg.layout.rewrite.root,
        market=cfg.asset.slug,
    )
    orderbook_hot_cache_summary = summarize_orderbook_hot_cache(
        recent_path=data_cfg.layout.orderbook_recent_path,
        state_path=data_cfg.layout.orderbook_state_path,
    )
    return {
        "decision": {
            "exists": decision_payload is not None,
            "status": None if decision_payload is None else (decision_payload.get("decision") or {}).get("status"),
            "snapshot_ts": None if decision_payload is None else decision_payload.get("snapshot_ts"),
        },
        "execution": {
            "exists": execution_payload is not None,
            "status": None if execution_payload is None else (execution_payload.get("execution") or {}).get("status"),
            "reason": None if execution_payload is None else (execution_payload.get("execution") or {}).get("reason"),
            "snapshot_ts": None if execution_payload is None else execution_payload.get("snapshot_ts"),
        },
        "liquidity": {
            "exists": liquidity_payload is not None,
            "summary": summarize_liquidity_state(liquidity_payload),
        },
        "regime": {
            "exists": regime_payload is not None,
            "summary": summarize_regime_state(regime_payload),
        },
        "foundation": {
            "exists": foundation_payload is not None,
            "summary": _summarize_foundation_state(foundation_payload),
        },
        "open_orders": {
            "exists": open_orders_payload is not None,
            "status": None if open_orders_payload is None else open_orders_payload.get("status"),
            "snapshot_ts": None if open_orders_payload is None else open_orders_payload.get("snapshot_ts"),
            "summary": summarize_open_orders_snapshot(open_orders_payload),
        },
        "positions": {
            "exists": positions_payload is not None,
            "status": None if positions_payload is None else positions_payload.get("status"),
            "snapshot_ts": None if positions_payload is None else positions_payload.get("snapshot_ts"),
            "summary": summarize_positions_snapshot(positions_payload),
        },
        "orderbook_hot_cache": {
            "exists": bool(orderbook_hot_cache_summary.get("exists")),
            "summary": orderbook_hot_cache_summary,
        },
    }


def _summarize_foundation_state(payload: dict[str, object] | None) -> dict[str, object]:
    summary = {} if payload is None else dict(payload)
    return {
        "status": summary.get("status"),
        "reason": summary.get("reason"),
        "issue_codes": list(summary.get("issue_codes") or []),
        "run_started_at": summary.get("run_started_at"),
        "last_completed_at": summary.get("last_completed_at"),
        "finished_at": summary.get("finished_at"),
        "completed_iterations": summary.get("completed_iterations"),
    }
