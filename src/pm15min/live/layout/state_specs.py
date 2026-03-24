from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StateGroupSpec:
    group: str
    snapshot_filename: str


SIGNAL_STATE_GROUP = StateGroupSpec(group="signals", snapshot_filename="signal.json")
DECISION_STATE_GROUP = StateGroupSpec(group="decisions", snapshot_filename="decision.json")
QUOTE_STATE_GROUP = StateGroupSpec(group="quotes", snapshot_filename="quote.json")
LIQUIDITY_STATE_GROUP = StateGroupSpec(group="liquidity", snapshot_filename="liquidity.json")
REGIME_STATE_GROUP = StateGroupSpec(group="regime", snapshot_filename="regime.json")
OPEN_ORDERS_STATE_GROUP = StateGroupSpec(group="open_orders", snapshot_filename="orders.json")
POSITIONS_STATE_GROUP = StateGroupSpec(group="positions", snapshot_filename="positions.json")
CANCEL_ACTION_STATE_GROUP = StateGroupSpec(group="cancel", snapshot_filename="cancel.json")
REDEEM_ACTION_STATE_GROUP = StateGroupSpec(group="redeem", snapshot_filename="redeem.json")
REDEEM_RUNNER_STATE_GROUP = StateGroupSpec(group="redeem_runner", snapshot_filename="run.json")
ORDER_ACTION_STATE_GROUP = StateGroupSpec(group="orders", snapshot_filename="order.json")
RUNNER_STATE_GROUP = StateGroupSpec(group="runner", snapshot_filename="run.json")
EXECUTION_STATE_GROUP = StateGroupSpec(group="execution", snapshot_filename="execution.json")
