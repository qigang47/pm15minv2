from __future__ import annotations

from pm15min.core.config import LiveConfig
from . import run_live_runner
from ..trading.gateway import LiveTradingGateway
from ..trading.service import build_live_trading_gateway_from_env


def run_live_runner_once(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist: bool = True,
    run_foundation: bool = True,
    foundation_include_direct_oracle: bool = True,
    foundation_include_orderbooks: bool = True,
    apply_side_effects: bool = True,
    side_effect_dry_run: bool = False,
    adapter: str | None = None,
    gateway: LiveTradingGateway | None = None,
) -> dict[str, object]:
    resolved_gateway = gateway if gateway is not None else (
        None if adapter is None else build_live_trading_gateway_from_env(adapter_override=adapter)
    )
    return run_live_runner(
        cfg,
        target=target,
        feature_set=feature_set,
        iterations=1,
        loop=False,
        persist=persist,
        run_foundation=run_foundation,
        foundation_include_direct_oracle=foundation_include_direct_oracle,
        foundation_include_orderbooks=foundation_include_orderbooks,
        apply_side_effects=apply_side_effects,
        side_effect_dry_run=side_effect_dry_run,
        gateway=resolved_gateway,
    )


def run_live_runner_loop(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    iterations: int = 1,
    sleep_sec: float = 1.0,
    persist: bool = True,
    run_foundation: bool = True,
    foundation_include_direct_oracle: bool = True,
    foundation_include_orderbooks: bool = True,
    apply_side_effects: bool = True,
    side_effect_dry_run: bool = False,
    adapter: str | None = None,
    gateway: LiveTradingGateway | None = None,
) -> dict[str, object]:
    resolved_gateway = gateway if gateway is not None else (
        None if adapter is None else build_live_trading_gateway_from_env(adapter_override=adapter)
    )
    return run_live_runner(
        cfg,
        target=target,
        feature_set=feature_set,
        iterations=iterations,
        loop=True,
        sleep_sec=sleep_sec,
        persist=persist,
        run_foundation=run_foundation,
        foundation_include_direct_oracle=foundation_include_direct_oracle,
        foundation_include_orderbooks=foundation_include_orderbooks,
        apply_side_effects=apply_side_effects,
        side_effect_dry_run=side_effect_dry_run,
        gateway=resolved_gateway,
    )
