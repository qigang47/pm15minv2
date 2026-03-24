from __future__ import annotations

import argparse

from .common import (
    build_live_cfg,
    build_show_config_cfg,
    enforce_canonical_live_target,
    print_payload,
)
from .parser import attach_live_subcommands as _attach_live_subcommands_impl
from ..gateway.checks import check_live_trading_gateway
from ..readiness import show_live_latest_runner, show_live_ready
from ..redeem import run_live_redeem_loop
from ..runner.api import run_live_runner_loop, run_live_runner_once
from ..runtime import describe_live_config, describe_live_runtime
from ..service import (
    check_live_latest,
    decide_live_latest,
    execute_live_cancel_policy,
    execute_live_latest,
    execute_live_redeem_policy,
    quote_live_latest,
    score_live_latest,
    simulate_live_execution,
    sync_live_account_state,
    sync_live_liquidity_state,
)

def attach_live_subcommands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    _attach_live_subcommands_impl(subparsers)


def _handle_show_config(args: argparse.Namespace) -> dict[str, object]:
    return describe_live_config(build_show_config_cfg(args))


def _handle_show_layout(args: argparse.Namespace) -> dict[str, object]:
    return describe_live_runtime(build_live_cfg(args))


def _handle_check_trading_gateway(args: argparse.Namespace) -> dict[str, object]:
    return check_live_trading_gateway(
        build_live_cfg(args),
        adapter=args.adapter,
        probe_open_orders=bool(args.probe_open_orders),
        probe_positions=bool(args.probe_positions),
    )


def _handle_show_latest_runner(args: argparse.Namespace) -> dict[str, object]:
    return show_live_latest_runner(
        build_live_cfg(args),
        target=args.target,
        risk_only=bool(args.risk_only),
    )


def _handle_show_ready(args: argparse.Namespace) -> dict[str, object]:
    return show_live_ready(
        build_live_cfg(args),
        target=args.target,
        adapter=args.adapter,
    )


def _handle_score_latest(args: argparse.Namespace) -> dict[str, object]:
    enforce_canonical_live_target(live_command=args.live_command, target=args.target)
    return score_live_latest(
        build_live_cfg(args),
        target=args.target,
        feature_set=args.feature_set,
        persist=not args.no_persist,
    )


def _handle_quote_latest(args: argparse.Namespace) -> dict[str, object]:
    enforce_canonical_live_target(live_command=args.live_command, target=args.target)
    return quote_live_latest(
        build_live_cfg(args),
        target=args.target,
        feature_set=args.feature_set,
        persist=not args.no_persist,
    )


def _handle_check_latest(args: argparse.Namespace) -> dict[str, object]:
    enforce_canonical_live_target(live_command=args.live_command, target=args.target)
    return check_live_latest(
        build_live_cfg(args),
        target=args.target,
        feature_set=args.feature_set,
    )


def _handle_decide_latest(args: argparse.Namespace) -> dict[str, object]:
    enforce_canonical_live_target(live_command=args.live_command, target=args.target)
    return decide_live_latest(
        build_live_cfg(args),
        target=args.target,
        feature_set=args.feature_set,
        persist=not args.no_persist,
    )


def _handle_runner_once(args: argparse.Namespace) -> dict[str, object]:
    enforce_canonical_live_target(live_command=args.live_command, target=args.target)
    return run_live_runner_once(
        build_live_cfg(args),
        target=args.target,
        feature_set=args.feature_set,
        persist=not args.no_persist,
        run_foundation=not args.no_foundation,
        foundation_include_direct_oracle=not args.no_direct_oracle,
        foundation_include_orderbooks=not args.no_orderbooks,
        apply_side_effects=not args.no_side_effects,
        side_effect_dry_run=bool(args.dry_run_side_effects),
        adapter=args.adapter,
    )


def _handle_runner_loop(args: argparse.Namespace) -> dict[str, object]:
    enforce_canonical_live_target(live_command=args.live_command, target=args.target)
    return run_live_runner_loop(
        build_live_cfg(args),
        target=args.target,
        feature_set=args.feature_set,
        iterations=int(args.iterations),
        sleep_sec=float(args.sleep_sec),
        persist=not args.no_persist,
        run_foundation=not args.no_foundation,
        foundation_include_direct_oracle=not args.no_direct_oracle,
        foundation_include_orderbooks=not args.no_orderbooks,
        apply_side_effects=not args.no_side_effects,
        side_effect_dry_run=bool(args.dry_run_side_effects),
        adapter=args.adapter,
    )


def _handle_execution_simulate(args: argparse.Namespace) -> dict[str, object]:
    enforce_canonical_live_target(live_command=args.live_command, target=args.target)
    return simulate_live_execution(
        build_live_cfg(args),
        target=args.target,
        feature_set=args.feature_set,
        persist=not args.no_persist,
    )


def _handle_execute_latest(args: argparse.Namespace) -> dict[str, object]:
    enforce_canonical_live_target(live_command=args.live_command, target=args.target)
    return execute_live_latest(
        build_live_cfg(args),
        target=args.target,
        feature_set=args.feature_set,
        persist=not args.no_persist,
        dry_run=bool(args.dry_run),
        refresh_account_state=not args.no_refresh_account_state,
        adapter=args.adapter,
    )


def _handle_sync_account_state(args: argparse.Namespace) -> dict[str, object]:
    return sync_live_account_state(
        build_live_cfg(args),
        persist=not args.no_persist,
        adapter=args.adapter,
    )


def _handle_sync_liquidity_state(args: argparse.Namespace) -> dict[str, object]:
    return sync_live_liquidity_state(
        build_live_cfg(args),
        persist=not args.no_persist,
        force_refresh=bool(args.force_refresh),
    )


def _handle_apply_cancel_policy(args: argparse.Namespace) -> dict[str, object]:
    return execute_live_cancel_policy(
        build_live_cfg(args),
        persist=not args.no_persist,
        refresh_account_state=not args.no_refresh_account_state,
        dry_run=bool(args.dry_run),
        adapter=args.adapter,
    )


def _handle_apply_redeem_policy(args: argparse.Namespace) -> dict[str, object]:
    return execute_live_redeem_policy(
        build_live_cfg(args),
        persist=not args.no_persist,
        refresh_account_state=not args.no_refresh_account_state,
        dry_run=bool(args.dry_run),
        max_conditions=args.max_conditions,
        adapter=args.adapter,
    )


def _handle_redeem_loop(args: argparse.Namespace) -> dict[str, object]:
    return run_live_redeem_loop(
        build_live_cfg(args),
        iterations=int(args.iterations),
        loop=bool(args.loop),
        sleep_sec=float(args.sleep_sec),
        persist=not args.no_persist,
        refresh_account_state=not args.no_refresh_account_state,
        dry_run=bool(args.dry_run),
        max_conditions=args.max_conditions,
        adapter=args.adapter,
    )


_LIVE_COMMAND_HANDLERS = {
    "show-config": _handle_show_config,
    "show-layout": _handle_show_layout,
    "check-trading-gateway": _handle_check_trading_gateway,
    "show-latest-runner": _handle_show_latest_runner,
    "show-ready": _handle_show_ready,
    "score-latest": _handle_score_latest,
    "quote-latest": _handle_quote_latest,
    "check-latest": _handle_check_latest,
    "decide-latest": _handle_decide_latest,
    "runner-once": _handle_runner_once,
    "runner-loop": _handle_runner_loop,
    "execution-simulate": _handle_execution_simulate,
    "execute-latest": _handle_execute_latest,
    "sync-account-state": _handle_sync_account_state,
    "sync-liquidity-state": _handle_sync_liquidity_state,
    "apply-cancel-policy": _handle_apply_cancel_policy,
    "apply-redeem-policy": _handle_apply_redeem_policy,
    "redeem-loop": _handle_redeem_loop,
}


def run_live_command(args: argparse.Namespace) -> int:
    handler = _LIVE_COMMAND_HANDLERS.get(str(args.live_command or ""))
    if handler is None:
        raise SystemExit("Missing live subcommand.")
    return print_payload(handler(args))
