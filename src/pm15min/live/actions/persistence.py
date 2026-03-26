from __future__ import annotations

from pathlib import Path
from typing import Any

from ..layout import LiveStateLayout
from ..persistence import write_live_payload_pair


def persist_cancel_payload(*, cfg, payload: dict[str, Any], persist: bool) -> dict[str, Any]:
    if not persist:
        return payload
    layout = LiveStateLayout.discover(root=cfg.layout.rewrite.root)
    latest_path = layout.latest_cancel_action_path(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        profile=cfg.profile,
    )
    snapshot_path = layout.cancel_action_snapshot_path(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        profile=cfg.profile,
        snapshot_ts=str(payload["snapshot_ts"]),
    )
    payload["latest_cancel_action_path"] = str(latest_path)
    payload["cancel_action_snapshot_path"] = str(snapshot_path)
    write_payload(payload=payload, latest_path=latest_path, snapshot_path=snapshot_path)
    return payload


def persist_redeem_payload(*, cfg, payload: dict[str, Any], persist: bool) -> dict[str, Any]:
    if not persist:
        return payload
    layout = LiveStateLayout.discover(root=cfg.layout.rewrite.root)
    latest_path = layout.latest_redeem_action_path(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        profile=cfg.profile,
    )
    snapshot_path = layout.redeem_action_snapshot_path(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        profile=cfg.profile,
        snapshot_ts=str(payload["snapshot_ts"]),
    )
    payload["latest_redeem_action_path"] = str(latest_path)
    payload["redeem_action_snapshot_path"] = str(snapshot_path)
    write_payload(payload=payload, latest_path=latest_path, snapshot_path=snapshot_path)
    return payload


def persist_order_payload(*, cfg, payload: dict[str, Any], persist: bool) -> dict[str, Any]:
    if not persist:
        return payload
    layout = LiveStateLayout.discover(root=cfg.layout.rewrite.root)
    latest_path = layout.latest_order_action_path(
        market=cfg.asset.slug,
        cycle=str(payload["cycle"]),
        profile=cfg.profile,
        target=str(payload["target"]),
    )
    snapshot_path = layout.order_action_snapshot_path(
        market=cfg.asset.slug,
        cycle=str(payload["cycle"]),
        profile=cfg.profile,
        target=str(payload["target"]),
        snapshot_ts=str(payload["snapshot_ts"]),
    )
    payload["latest_order_action_path"] = str(latest_path)
    payload["order_action_snapshot_path"] = str(snapshot_path)
    # Keep the latest order-action pointer focused on gate-relevant actions.
    # Otherwise frequent "execution_not_plan" or dry-run payloads erase the last actionable state
    # and allow the same order key to be retried later in the same market.
    if not str(payload.get("action_key") or "").strip() or bool(payload.get("dry_run")):
        return payload
    write_payload(payload=payload, latest_path=latest_path, snapshot_path=snapshot_path)
    return payload


def write_payload(*, payload: dict[str, Any], latest_path: Path, snapshot_path: Path) -> None:
    write_live_payload_pair(
        payload=payload,
        latest_path=latest_path,
        snapshot_path=snapshot_path,
        write_snapshot_history=False,
    )
