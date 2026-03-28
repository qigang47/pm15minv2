from __future__ import annotations

from pathlib import Path
from typing import Any

from pm15min.data.layout import utc_snapshot_label
from .depth import build_depth_execution_plan
from .policy import (
    build_execution_record,
    build_policy_context,
    load_policy_state,
    resolve_dynamic_stake_base,
    resolve_execution_account_summary,
    resolve_regime_stake_multiplier,
)
from .service import build_execution_snapshot as _build_execution_snapshot_impl
from .utils import float_or_none, resolve_side_probability
from ..layout import LiveStateLayout
from ..persistence import write_live_payload_pair
from ..profiles import resolve_live_profile_spec


def build_execution_snapshot(
    cfg,
    decision_payload: dict[str, Any],
    *,
    orderbook_provider=None,
    prefer_live_depth: bool = False,
) -> dict[str, Any]:
    return _build_execution_snapshot_impl(
        cfg,
        decision_payload,
        orderbook_provider=orderbook_provider,
        prefer_live_depth=prefer_live_depth,
        resolve_live_profile_spec_fn=resolve_live_profile_spec,
        utc_snapshot_label_fn=utc_snapshot_label,
        load_policy_state_fn=load_policy_state,
        build_policy_context_fn=build_policy_context,
        build_execution_record_fn=build_execution_record,
        resolve_regime_stake_multiplier_fn=resolve_regime_stake_multiplier,
        resolve_execution_account_summary_fn=resolve_execution_account_summary,
        resolve_dynamic_stake_base_fn=resolve_dynamic_stake_base,
        resolve_side_probability_fn=resolve_side_probability,
        float_or_none_fn=float_or_none,
        build_depth_execution_plan_fn=build_depth_execution_plan,
    )


def persist_execution_snapshot(*, rewrite_root: Path, payload: dict[str, Any]) -> dict[str, Path]:
    layout = LiveStateLayout.discover(root=rewrite_root)
    latest_path = layout.latest_execution_path(
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        profile=str(payload["profile"]),
        target=str(payload["target"]),
    )
    snapshot_path = layout.execution_snapshot_path(
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        profile=str(payload["profile"]),
        target=str(payload["target"]),
        snapshot_ts=str(payload["snapshot_ts"]),
    )
    return write_live_payload_pair(
        payload=payload,
        latest_path=latest_path,
        snapshot_path=snapshot_path,
        write_snapshot_history=False,
    )
