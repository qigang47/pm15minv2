from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pm15min.core.layout import rewrite_root
from .paths import (
    latest_payload_path,
    profile_log_scope,
    runner_log_scope,
    snapshot_payload_path,
    state_scope_asset_only,
    state_scope_with_profile,
    state_scope_with_target,
)
from .state_specs import (
    CANCEL_ACTION_STATE_GROUP,
    DECISION_STATE_GROUP,
    EXECUTION_STATE_GROUP,
    LIQUIDITY_STATE_GROUP,
    OPEN_ORDERS_STATE_GROUP,
    ORDER_ACTION_STATE_GROUP,
    POSITIONS_STATE_GROUP,
    QUOTE_STATE_GROUP,
    REDEEM_ACTION_STATE_GROUP,
    REDEEM_RUNNER_STATE_GROUP,
    REGIME_STATE_GROUP,
    RUNNER_STATE_GROUP,
    SIGNAL_STATE_GROUP,
    StateGroupSpec,
)


def _make_target_state_dir_method(name: str, spec: StateGroupSpec):
    def method(self, *, market: str, cycle: str, profile: str, target: str) -> Path:
        return self._target_group_state_dir(spec, market, cycle, profile, target)

    method.__name__ = name
    return method


def _make_latest_target_path_method(name: str, spec: StateGroupSpec):
    def method(self, *, market: str, cycle: str, profile: str, target: str) -> Path:
        return self._latest_target_group_path(spec, market, cycle, profile, target)

    method.__name__ = name
    return method


def _make_target_snapshot_path_method(name: str, spec: StateGroupSpec):
    def method(
        self,
        *,
        market: str,
        cycle: str,
        profile: str,
        target: str,
        snapshot_ts: str,
    ) -> Path:
        return self._snapshot_target_group_path(spec, market, cycle, profile, target, snapshot_ts)

    method.__name__ = name
    return method


def _make_profile_state_dir_method(name: str, spec: StateGroupSpec):
    def method(self, *, market: str, cycle: str, profile: str) -> Path:
        return self._profile_group_state_dir(spec, market, cycle, profile)

    method.__name__ = name
    return method


def _make_latest_profile_path_method(name: str, spec: StateGroupSpec):
    def method(self, *, market: str, cycle: str, profile: str) -> Path:
        return self._latest_profile_group_path(spec, market, cycle, profile)

    method.__name__ = name
    return method


def _make_profile_snapshot_path_method(name: str, spec: StateGroupSpec):
    def method(
        self,
        *,
        market: str,
        cycle: str,
        profile: str,
        snapshot_ts: str,
    ) -> Path:
        return self._snapshot_profile_group_path(spec, market, cycle, profile, snapshot_ts)

    method.__name__ = name
    return method


def _make_asset_state_dir_method(name: str, spec: StateGroupSpec):
    def method(self, *, market: str) -> Path:
        return self._asset_group_state_dir(spec, market)

    method.__name__ = name
    return method


def _make_latest_asset_path_method(name: str, spec: StateGroupSpec):
    def method(self, *, market: str) -> Path:
        return self._latest_asset_group_path(spec, market)

    method.__name__ = name
    return method


def _make_asset_snapshot_path_method(name: str, spec: StateGroupSpec):
    def method(self, *, market: str, snapshot_ts: str) -> Path:
        return self._snapshot_asset_group_path(spec, market, snapshot_ts)

    method.__name__ = name
    return method


@dataclass(frozen=True)
class LiveStateLayout:
    rewrite_root: Path

    @classmethod
    def discover(cls, root: Path | None = None) -> "LiveStateLayout":
        return cls(rewrite_root=Path(root) if root is not None else rewrite_root())

    @property
    def live_var_root(self) -> Path:
        return self.rewrite_root / "var" / "live"

    @property
    def state_root(self) -> Path:
        return self.live_var_root / "state"

    @property
    def logs_root(self) -> Path:
        return self.live_var_root / "logs"

    def _target_state_dir(
        self,
        *,
        spec: StateGroupSpec,
        market: str,
        cycle: str,
        profile: str,
        target: str,
    ) -> Path:
        return state_scope_with_target(
            state_root=self.state_root,
            group=spec.group,
            market=market,
            cycle=cycle,
            profile=profile,
            target=target,
        )

    def _latest_target_path(
        self,
        *,
        spec: StateGroupSpec,
        market: str,
        cycle: str,
        profile: str,
        target: str,
    ) -> Path:
        return latest_payload_path(
            self._target_state_dir(
                spec=spec,
                market=market,
                cycle=cycle,
                profile=profile,
                target=target,
            )
        )

    def _snapshot_target_path(
        self,
        *,
        spec: StateGroupSpec,
        market: str,
        cycle: str,
        profile: str,
        target: str,
        snapshot_ts: str,
    ) -> Path:
        return snapshot_payload_path(
            self._target_state_dir(
                spec=spec,
                market=market,
                cycle=cycle,
                profile=profile,
                target=target,
            ),
            snapshot_ts=snapshot_ts,
            filename=spec.snapshot_filename,
        )

    def _profile_state_dir(
        self,
        *,
        spec: StateGroupSpec,
        market: str,
        cycle: str,
        profile: str,
    ) -> Path:
        return state_scope_with_profile(
            state_root=self.state_root,
            group=spec.group,
            market=market,
            cycle=cycle,
            profile=profile,
        )

    def _latest_profile_path(
        self,
        *,
        spec: StateGroupSpec,
        market: str,
        cycle: str,
        profile: str,
    ) -> Path:
        return latest_payload_path(
            self._profile_state_dir(spec=spec, market=market, cycle=cycle, profile=profile)
        )

    def _snapshot_profile_path(
        self,
        *,
        spec: StateGroupSpec,
        market: str,
        cycle: str,
        profile: str,
        snapshot_ts: str,
    ) -> Path:
        return snapshot_payload_path(
            self._profile_state_dir(spec=spec, market=market, cycle=cycle, profile=profile),
            snapshot_ts=snapshot_ts,
            filename=spec.snapshot_filename,
        )

    def _asset_state_dir(self, *, spec: StateGroupSpec, market: str) -> Path:
        return state_scope_asset_only(state_root=self.state_root, group=spec.group, market=market)

    def _latest_asset_path(self, *, spec: StateGroupSpec, market: str) -> Path:
        return latest_payload_path(self._asset_state_dir(spec=spec, market=market))

    def _snapshot_asset_path(
        self,
        *,
        spec: StateGroupSpec,
        market: str,
        snapshot_ts: str,
    ) -> Path:
        return snapshot_payload_path(
            self._asset_state_dir(spec=spec, market=market),
            snapshot_ts=snapshot_ts,
            filename=spec.snapshot_filename,
        )

    def _target_group_state_dir(
        self,
        spec: StateGroupSpec,
        market: str,
        cycle: str,
        profile: str,
        target: str,
    ) -> Path:
        return self._target_state_dir(
            spec=spec,
            market=market,
            cycle=cycle,
            profile=profile,
            target=target,
        )

    def _latest_target_group_path(
        self,
        spec: StateGroupSpec,
        market: str,
        cycle: str,
        profile: str,
        target: str,
    ) -> Path:
        return self._latest_target_path(
            spec=spec,
            market=market,
            cycle=cycle,
            profile=profile,
            target=target,
        )

    def _snapshot_target_group_path(
        self,
        spec: StateGroupSpec,
        market: str,
        cycle: str,
        profile: str,
        target: str,
        snapshot_ts: str,
    ) -> Path:
        return self._snapshot_target_path(
            spec=spec,
            market=market,
            cycle=cycle,
            profile=profile,
            target=target,
            snapshot_ts=snapshot_ts,
        )

    def _profile_group_state_dir(
        self,
        spec: StateGroupSpec,
        market: str,
        cycle: str,
        profile: str,
    ) -> Path:
        return self._profile_state_dir(spec=spec, market=market, cycle=cycle, profile=profile)

    def _latest_profile_group_path(
        self,
        spec: StateGroupSpec,
        market: str,
        cycle: str,
        profile: str,
    ) -> Path:
        return self._latest_profile_path(spec=spec, market=market, cycle=cycle, profile=profile)

    def _snapshot_profile_group_path(
        self,
        spec: StateGroupSpec,
        market: str,
        cycle: str,
        profile: str,
        snapshot_ts: str,
    ) -> Path:
        return self._snapshot_profile_path(
            spec=spec,
            market=market,
            cycle=cycle,
            profile=profile,
            snapshot_ts=snapshot_ts,
        )

    def _asset_group_state_dir(self, spec: StateGroupSpec, market: str) -> Path:
        return self._asset_state_dir(spec=spec, market=market)

    def _latest_asset_group_path(self, spec: StateGroupSpec, market: str) -> Path:
        return self._latest_asset_path(spec=spec, market=market)

    def _snapshot_asset_group_path(
        self,
        spec: StateGroupSpec,
        market: str,
        snapshot_ts: str,
    ) -> Path:
        return self._snapshot_asset_path(spec=spec, market=market, snapshot_ts=snapshot_ts)

    signal_state_dir = _make_target_state_dir_method("signal_state_dir", SIGNAL_STATE_GROUP)
    latest_signal_path = _make_latest_target_path_method("latest_signal_path", SIGNAL_STATE_GROUP)
    signal_snapshot_path = _make_target_snapshot_path_method("signal_snapshot_path", SIGNAL_STATE_GROUP)

    decision_state_dir = _make_target_state_dir_method("decision_state_dir", DECISION_STATE_GROUP)
    latest_decision_path = _make_latest_target_path_method("latest_decision_path", DECISION_STATE_GROUP)
    decision_snapshot_path = _make_target_snapshot_path_method("decision_snapshot_path", DECISION_STATE_GROUP)

    quote_state_dir = _make_target_state_dir_method("quote_state_dir", QUOTE_STATE_GROUP)
    latest_quote_path = _make_latest_target_path_method("latest_quote_path", QUOTE_STATE_GROUP)
    quote_snapshot_path = _make_target_snapshot_path_method("quote_snapshot_path", QUOTE_STATE_GROUP)

    liquidity_state_dir = _make_profile_state_dir_method("liquidity_state_dir", LIQUIDITY_STATE_GROUP)
    latest_liquidity_path = _make_latest_profile_path_method("latest_liquidity_path", LIQUIDITY_STATE_GROUP)
    liquidity_snapshot_path = _make_profile_snapshot_path_method(
        "liquidity_snapshot_path", LIQUIDITY_STATE_GROUP
    )

    regime_state_dir = _make_profile_state_dir_method("regime_state_dir", REGIME_STATE_GROUP)
    latest_regime_path = _make_latest_profile_path_method("latest_regime_path", REGIME_STATE_GROUP)
    regime_snapshot_path = _make_profile_snapshot_path_method("regime_snapshot_path", REGIME_STATE_GROUP)

    open_orders_state_dir = _make_asset_state_dir_method("open_orders_state_dir", OPEN_ORDERS_STATE_GROUP)
    latest_open_orders_path = _make_latest_asset_path_method("latest_open_orders_path", OPEN_ORDERS_STATE_GROUP)
    open_orders_snapshot_path = _make_asset_snapshot_path_method(
        "open_orders_snapshot_path", OPEN_ORDERS_STATE_GROUP
    )

    positions_state_dir = _make_asset_state_dir_method("positions_state_dir", POSITIONS_STATE_GROUP)
    latest_positions_path = _make_latest_asset_path_method("latest_positions_path", POSITIONS_STATE_GROUP)
    positions_snapshot_path = _make_asset_snapshot_path_method("positions_snapshot_path", POSITIONS_STATE_GROUP)

    cancel_action_state_dir = _make_profile_state_dir_method("cancel_action_state_dir", CANCEL_ACTION_STATE_GROUP)
    latest_cancel_action_path = _make_latest_profile_path_method(
        "latest_cancel_action_path", CANCEL_ACTION_STATE_GROUP
    )
    cancel_action_snapshot_path = _make_profile_snapshot_path_method(
        "cancel_action_snapshot_path", CANCEL_ACTION_STATE_GROUP
    )

    redeem_action_state_dir = _make_profile_state_dir_method("redeem_action_state_dir", REDEEM_ACTION_STATE_GROUP)
    latest_redeem_action_path = _make_latest_profile_path_method(
        "latest_redeem_action_path", REDEEM_ACTION_STATE_GROUP
    )
    redeem_action_snapshot_path = _make_profile_snapshot_path_method(
        "redeem_action_snapshot_path", REDEEM_ACTION_STATE_GROUP
    )

    redeem_runner_state_dir = _make_profile_state_dir_method("redeem_runner_state_dir", REDEEM_RUNNER_STATE_GROUP)
    latest_redeem_runner_path = _make_latest_profile_path_method(
        "latest_redeem_runner_path", REDEEM_RUNNER_STATE_GROUP
    )
    redeem_runner_snapshot_path = _make_profile_snapshot_path_method(
        "redeem_runner_snapshot_path", REDEEM_RUNNER_STATE_GROUP
    )

    def redeem_runner_log_dir(self, *, market: str, cycle: str, profile: str) -> Path:
        return profile_log_scope(
            logs_root=self.logs_root,
            group="redeem_runner",
            market=market,
            cycle=cycle,
            profile=profile,
        )

    def redeem_runner_log_path(self, *, market: str, cycle: str, profile: str) -> Path:
        return self.redeem_runner_log_dir(market=market, cycle=cycle, profile=profile) / "redeem_runner.jsonl"

    order_action_state_dir = _make_target_state_dir_method("order_action_state_dir", ORDER_ACTION_STATE_GROUP)
    latest_order_action_path = _make_latest_target_path_method("latest_order_action_path", ORDER_ACTION_STATE_GROUP)
    order_action_snapshot_path = _make_target_snapshot_path_method(
        "order_action_snapshot_path", ORDER_ACTION_STATE_GROUP
    )

    runner_state_dir = _make_target_state_dir_method("runner_state_dir", RUNNER_STATE_GROUP)
    latest_runner_path = _make_latest_target_path_method("latest_runner_path", RUNNER_STATE_GROUP)
    runner_snapshot_path = _make_target_snapshot_path_method("runner_snapshot_path", RUNNER_STATE_GROUP)

    def runner_log_dir(self, *, market: str, cycle: str, profile: str, target: str) -> Path:
        return runner_log_scope(
            logs_root=self.logs_root,
            market=market,
            cycle=cycle,
            profile=profile,
            target=target,
        )

    def runner_log_path(self, *, market: str, cycle: str, profile: str, target: str) -> Path:
        return self.runner_log_dir(market=market, cycle=cycle, profile=profile, target=target) / "runner.jsonl"

    execution_state_dir = _make_target_state_dir_method("execution_state_dir", EXECUTION_STATE_GROUP)
    latest_execution_path = _make_latest_target_path_method("latest_execution_path", EXECUTION_STATE_GROUP)
    execution_snapshot_path = _make_target_snapshot_path_method(
        "execution_snapshot_path", EXECUTION_STATE_GROUP
    )


def utc_date_label(now: datetime | None = None) -> str:
    ts = datetime.now(timezone.utc) if now is None else now.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%d")
