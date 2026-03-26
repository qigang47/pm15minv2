from __future__ import annotations

from typing import Any

import pandas as pd

from .cancel import apply_cancel_policy as _apply_cancel_policy_impl
from .order_submit import submit_execution_payload as _submit_execution_payload_impl
from .redeem import apply_redeem_policy as _apply_redeem_policy_impl
from ..trading.gateway import LiveTradingGateway


def apply_cancel_policy(
    cfg,
    *,
    persist: bool = True,
    refresh_account_state: bool = True,
    dry_run: bool = False,
    now: pd.Timestamp | None = None,
    gateway: LiveTradingGateway | None = None,
    utc_snapshot_label_fn,
) -> dict[str, Any]:
    return _apply_cancel_policy_impl(
        cfg,
        persist=persist,
        refresh_account_state=refresh_account_state,
        dry_run=dry_run,
        now=now,
        gateway=gateway,
        utc_snapshot_label_fn=utc_snapshot_label_fn,
    )


def apply_redeem_policy(
    cfg,
    *,
    persist: bool = True,
    refresh_account_state: bool = True,
    dry_run: bool = False,
    max_conditions: int | None = None,
    gateway: LiveTradingGateway | None = None,
    utc_snapshot_label_fn,
) -> dict[str, Any]:
    return _apply_redeem_policy_impl(
        cfg,
        persist=persist,
        refresh_account_state=refresh_account_state,
        dry_run=dry_run,
        max_conditions=max_conditions,
        gateway=gateway,
        utc_snapshot_label_fn=utc_snapshot_label_fn,
    )


def submit_execution_payload(
    cfg,
    *,
    execution_payload: dict[str, Any],
    persist: bool = True,
    refresh_account_state: bool = True,
    dry_run: bool = False,
    session_state: dict[str, Any] | None = None,
    gateway: LiveTradingGateway | None = None,
    utc_snapshot_label_fn,
) -> dict[str, Any]:
    return _submit_execution_payload_impl(
        cfg,
        execution_payload=execution_payload,
        persist=persist,
        refresh_account_state=refresh_account_state,
        dry_run=dry_run,
        session_state=session_state,
        gateway=gateway,
        utc_snapshot_label_fn=utc_snapshot_label_fn,
    )
