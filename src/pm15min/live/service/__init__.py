from __future__ import annotations

from pm15min.core.config import LiveConfig
from pm15min.data.layout import utc_snapshot_label
from . import facade_helpers as _service_facade_helpers
from ..account import build_account_state_snapshot
from ..actions import apply_cancel_policy, apply_redeem_policy, submit_execution_payload
from ..execution import build_execution_snapshot, persist_execution_snapshot
from ..gateway.checks import check_live_trading_gateway as _check_live_trading_gateway_impl
from ..liquidity import build_liquidity_state_snapshot
from ..profiles import resolve_live_profile_spec
from ..readiness import show_live_latest_runner as _show_live_latest_runner_impl
from ..readiness import show_live_ready as _show_live_ready_impl
from ..runtime import describe_live_config as _describe_live_config_impl
from ..runtime import describe_live_runtime as _describe_live_runtime_impl
from .operation import (
    execute_live_cancel_policy as _execute_live_cancel_policy_impl,
    execute_live_latest as _execute_live_latest_impl,
    execute_live_redeem_policy as _execute_live_redeem_policy_impl,
    simulate_live_execution as _simulate_live_execution_impl,
    sync_live_account_state as _sync_live_account_state_impl,
    sync_live_liquidity_state as _sync_live_liquidity_state_impl,
)
from ..signal.service import (
    check_live_latest as _check_live_latest_impl,
    decide_live_latest as _decide_live_latest_impl,
    quote_live_latest as _quote_live_latest_impl,
    score_live_latest as _score_live_latest_impl,
)
from ..trading.gateway import LiveTradingGateway
from ..trading.service import (
    load_live_trading_env_configs,
    build_live_trading_gateway_from_env,
    describe_live_trading_gateway,
    normalize_live_trading_adapter,
)
from pm15min.research.bundles.loader import read_bundle_config, read_model_bundle_manifest, resolve_model_bundle_dir
from pm15min.research.inference.scorer import score_bundle_offset
from pm15min.research.service import get_active_bundle_selection

_module_available = _service_facade_helpers.module_available
_supports_feature_set = _service_facade_helpers.supports_feature_set
_build_live_feature_frame = _service_facade_helpers.build_live_feature_frame
_feature_coverage = _service_facade_helpers.feature_coverage
_resolve_live_blacklist = _service_facade_helpers.resolve_live_blacklist
_apply_live_blacklist = _service_facade_helpers.apply_live_blacklist
_latest_nan_feature_columns = _service_facade_helpers.latest_nan_feature_columns
_persist_live_signal_snapshot = _service_facade_helpers.persist_live_signal_snapshot
_extract_feature_snapshot = _service_facade_helpers.extract_feature_snapshot
_iso_or_none = _service_facade_helpers.iso_or_none


def describe_live_runtime(cfg: LiveConfig) -> dict[str, object]:
    return _describe_live_runtime_impl(cfg)


def show_live_latest_runner(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    risk_only: bool = False,
) -> dict[str, object]:
    return _show_live_latest_runner_impl(cfg, target=target, risk_only=risk_only)


def show_live_ready(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    adapter: str | None = None,
) -> dict[str, object]:
    return _show_live_ready_impl(
        cfg,
        target=target,
        adapter=adapter,
        check_live_trading_gateway_fn=check_live_trading_gateway,
        show_live_latest_runner_fn=show_live_latest_runner,
    )


def describe_live_config(cfg: LiveConfig) -> dict[str, object]:
    return _describe_live_config_impl(cfg)


def check_live_trading_gateway(
    cfg: LiveConfig,
    *,
    gateway: LiveTradingGateway | None = None,
    adapter: str | None = None,
    probe_open_orders: bool = False,
    probe_positions: bool = False,
) -> dict[str, object]:
    return _check_live_trading_gateway_impl(
        cfg,
        gateway=gateway,
        adapter=adapter,
        probe_open_orders=probe_open_orders,
        probe_positions=probe_positions,
        **_gateway_check_wiring(),
    )


def score_live_latest(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist: bool = True,
) -> dict[str, object]:
    return _score_live_latest_impl(
        cfg,
        target=target,
        feature_set=feature_set,
        persist=persist,
        **_score_live_latest_wiring(),
    )


def check_live_latest(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    feature_set: str | None = None,
) -> dict[str, object]:
    return _check_live_latest_impl(
        cfg,
        target=target,
        feature_set=feature_set,
        **_signal_check_wiring(),
    )


def decide_live_latest(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist: bool = True,
    session_state: dict[str, object] | None = None,
    orderbook_provider=None,
) -> dict[str, object]:
    return _decide_live_latest_impl(
        cfg,
        target=target,
        feature_set=feature_set,
        persist=persist,
        session_state=session_state,
        orderbook_provider=orderbook_provider,
        **_decision_wiring(),
    )


def quote_live_latest(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist: bool = True,
) -> dict[str, object]:
    return _quote_live_latest_impl(
        cfg,
        target=target,
        feature_set=feature_set,
        persist=persist,
        **_quote_wiring(),
    )


def sync_live_account_state(
    cfg: LiveConfig,
    *,
    persist: bool = True,
    adapter: str | None = None,
    gateway: LiveTradingGateway | None = None,
) -> dict[str, object]:
    return _sync_live_account_state_impl(
        cfg,
        persist=persist,
        adapter=adapter,
        gateway=gateway,
        **_sync_account_wiring(),
    )


def sync_live_liquidity_state(
    cfg: LiveConfig,
    *,
    persist: bool = True,
    force_refresh: bool = False,
) -> dict[str, object]:
    return _sync_live_liquidity_state_impl(
        cfg,
        persist=persist,
        force_refresh=force_refresh,
        **_sync_liquidity_wiring(),
    )


def execute_live_cancel_policy(
    cfg: LiveConfig,
    *,
    persist: bool = True,
    refresh_account_state: bool = True,
    dry_run: bool = False,
    adapter: str | None = None,
    gateway: LiveTradingGateway | None = None,
) -> dict[str, object]:
    return _execute_live_cancel_policy_impl(
        cfg,
        persist=persist,
        refresh_account_state=refresh_account_state,
        dry_run=dry_run,
        adapter=adapter,
        gateway=gateway,
        **_execute_cancel_wiring(),
    )


def execute_live_redeem_policy(
    cfg: LiveConfig,
    *,
    persist: bool = True,
    refresh_account_state: bool = True,
    dry_run: bool = False,
    max_conditions: int | None = None,
    adapter: str | None = None,
    gateway: LiveTradingGateway | None = None,
) -> dict[str, object]:
    return _execute_live_redeem_policy_impl(
        cfg,
        persist=persist,
        refresh_account_state=refresh_account_state,
        dry_run=dry_run,
        max_conditions=max_conditions,
        adapter=adapter,
        gateway=gateway,
        **_execute_redeem_wiring(),
    )


def execute_live_latest(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist: bool = True,
    dry_run: bool = False,
    refresh_account_state: bool = True,
    adapter: str | None = None,
    gateway: LiveTradingGateway | None = None,
) -> dict[str, object]:
    return _execute_live_latest_impl(
        cfg,
        target=target,
        feature_set=feature_set,
        persist=persist,
        dry_run=dry_run,
        refresh_account_state=refresh_account_state,
        adapter=adapter,
        gateway=gateway,
        **_execute_latest_wiring(),
    )


def simulate_live_execution(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist: bool = True,
) -> dict[str, object]:
    return _simulate_live_execution_impl(
        cfg,
        target=target,
        feature_set=feature_set,
        persist=persist,
        **_simulate_execution_wiring(),
    )


def _gateway_check_wiring() -> dict[str, object]:
    return _service_facade_helpers.build_gateway_check_wiring(globals())


def _score_live_latest_wiring() -> dict[str, object]:
    return _service_facade_helpers.build_score_live_latest_wiring(globals())


def _signal_check_wiring() -> dict[str, object]:
    return _service_facade_helpers.build_signal_check_wiring(globals())


def _decision_wiring() -> dict[str, object]:
    return _service_facade_helpers.build_decision_wiring(globals())


def _quote_wiring() -> dict[str, object]:
    return _service_facade_helpers.build_quote_wiring(globals())


def _sync_account_wiring() -> dict[str, object]:
    return _service_facade_helpers.build_sync_account_wiring(globals())


def _sync_liquidity_wiring() -> dict[str, object]:
    return _service_facade_helpers.build_sync_liquidity_wiring(globals())


def _execute_cancel_wiring() -> dict[str, object]:
    return _service_facade_helpers.build_execute_cancel_wiring(globals())


def _execute_redeem_wiring() -> dict[str, object]:
    return _service_facade_helpers.build_execute_redeem_wiring(globals())


def _execute_latest_wiring() -> dict[str, object]:
    return _service_facade_helpers.build_execute_latest_wiring(globals())


def _simulate_execution_wiring() -> dict[str, object]:
    return _service_facade_helpers.build_simulate_execution_wiring(globals())


def _load_live_account_context(cfg: LiveConfig) -> dict[str, object]:
    return _service_facade_helpers.load_live_account_context(cfg, utc_snapshot_label_fn=utc_snapshot_label)
