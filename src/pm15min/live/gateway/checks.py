from __future__ import annotations

from collections.abc import Callable

from pm15min.core.config import LiveConfig
from .capabilities import build_trading_capabilities
from .probes import (
    build_live_trading_smoke_runs,
    module_available as _module_available,
    probe_gateway_call,
)
from .service import check_live_trading_gateway as _check_live_trading_gateway_impl
from ..trading.gateway import LiveTradingGateway
from ..trading.service import (
    build_live_trading_gateway_from_env,
    describe_live_trading_gateway,
    load_live_trading_env_configs,
    normalize_live_trading_adapter,
)


def check_live_trading_gateway(
    cfg: LiveConfig,
    *,
    gateway: LiveTradingGateway | None = None,
    adapter: str | None = None,
    probe_open_orders: bool = False,
    probe_positions: bool = False,
    load_env_configs_fn: Callable[[], tuple[object, object, object]] = load_live_trading_env_configs,
    build_gateway_from_env_fn: Callable[..., LiveTradingGateway] = build_live_trading_gateway_from_env,
    describe_gateway_fn: Callable[..., dict[str, object]] = describe_live_trading_gateway,
    normalize_adapter_fn: Callable[[str | None], str] = normalize_live_trading_adapter,
    module_available_fn: Callable[[str], bool] | None = None,
) -> dict[str, object]:
    return _check_live_trading_gateway_impl(
        cfg,
        gateway=gateway,
        adapter=adapter,
        probe_open_orders=probe_open_orders,
        probe_positions=probe_positions,
        load_env_configs_fn=load_env_configs_fn,
        build_gateway_from_env_fn=build_gateway_from_env_fn,
        describe_gateway_fn=describe_gateway_fn,
        normalize_adapter_fn=normalize_adapter_fn,
        module_available_fn=_module_available if module_available_fn is None else module_available_fn,
        build_trading_capabilities_fn=build_trading_capabilities,
        build_live_trading_smoke_runs_fn=build_live_trading_smoke_runs,
        probe_gateway_call_fn=probe_gateway_call,
    )
