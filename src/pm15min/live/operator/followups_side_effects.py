from __future__ import annotations


def append_account_state_sync_actions(
    *,
    actions: list[str],
    order_action_status: str,
    account_state_status: str,
    account_open_orders_status: str,
    account_positions_status: str,
) -> None:
    actions.append("inspect latest runner account_state payload and rerun live sync-account-state")
    if account_open_orders_status == "error":
        actions.append("rerun live check-trading-gateway --probe-open-orders to isolate the account open-orders read path")
    if account_positions_status == "error":
        actions.append("rerun live check-trading-gateway --probe-positions to isolate the account positions/data-api read path")
    if account_state_status == "error" and order_action_status == "ok":
        actions.append("treat this as a post-submit account refresh/read-path issue before changing decision or execution logic")


def append_cancel_action_followups(
    *,
    actions: list[str],
    cancel_action_status: str,
    cancel_action_reason: str,
    account_open_orders_status: str,
) -> None:
    actions.append("inspect latest cancel_action payload and latest open_orders snapshot before retrying")
    if cancel_action_reason:
        actions.append("use operator_summary.cancel_action_reason together with latest open_orders snapshot to determine whether the failure came from candidate selection or gateway cancel submit")
    if cancel_action_status == "ok_with_errors":
        actions.append("treat cancel ok_with_errors as follow-up reconciliation work; confirm which order_ids remain open before retrying cancel side effects")
    if account_open_orders_status == "error":
        actions.append("rerun live sync-account-state and live check-trading-gateway --probe-open-orders before retrying cancel side effects")


def append_redeem_action_followups(
    *,
    actions: list[str],
    redeem_action_status: str,
    redeem_action_reason: str,
    account_positions_status: str,
) -> None:
    actions.append("inspect latest redeem_action payload and latest positions snapshot before retrying")
    if redeem_action_reason:
        actions.append("use operator_summary.redeem_action_reason together with latest positions snapshot and redeemable conditions to determine whether the failure came from candidate selection or redeem relay submit")
    if redeem_action_status == "ok_with_errors":
        actions.append("treat redeem ok_with_errors as follow-up reconciliation work; confirm remaining redeemable conditions before retrying redeem side effects")
    if account_positions_status == "error":
        actions.append("rerun live sync-account-state and live check-trading-gateway --probe-positions before retrying redeem side effects")
