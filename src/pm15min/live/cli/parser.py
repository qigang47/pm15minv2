from __future__ import annotations

import argparse

from .common import (
    add_adapter_arg,
    add_feature_set_arg,
    add_market_profile_args,
    add_market_profile_cycle_args,
    add_no_persist_arg,
    add_target_arg,
)


def attach_live_subcommands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("live", help="Live trading domain.")
    live_sub = parser.add_subparsers(dest="live_command")

    show_config = live_sub.add_parser("show-config", help="Show live config plus canonical-scope boundary metadata.")
    add_market_profile_cycle_args(show_config)
    show_config.add_argument("--loop", action="store_true")
    show_config.add_argument("--refresh-interval-minutes", type=int, default=30)
    show_config.add_argument("--decision-poll-interval-sec", type=float, default=1.0)

    show_layout = live_sub.add_parser("show-layout", help="Show live runtime layout plus canonical-scope boundary metadata.")
    add_market_profile_args(show_layout)

    check_trading_gateway_parser = live_sub.add_parser("check-trading-gateway", help="Validate the current live trading adapter config and optional read-only probes.")
    add_market_profile_cycle_args(check_trading_gateway_parser)
    add_adapter_arg(check_trading_gateway_parser)
    check_trading_gateway_parser.add_argument("--probe-open-orders", action="store_true")
    check_trading_gateway_parser.add_argument("--probe-positions", action="store_true")

    show_latest_runner = live_sub.add_parser("show-latest-runner", help="Show the latest persisted runner summary and risk state.")
    add_market_profile_cycle_args(show_latest_runner)
    add_target_arg(show_latest_runner)
    show_latest_runner.add_argument("--risk-only", action="store_true")

    show_ready_parser = live_sub.add_parser("show-ready", help="Show whether canonical live is ready for side effects.")
    add_market_profile_cycle_args(show_ready_parser)
    add_target_arg(show_ready_parser)
    add_adapter_arg(show_ready_parser)

    score_latest = live_sub.add_parser("score-latest", help="Score the latest live snapshot with the active bundle.")
    add_market_profile_cycle_args(score_latest)
    add_target_arg(score_latest)
    add_feature_set_arg(score_latest)
    add_no_persist_arg(score_latest)

    quote_latest = live_sub.add_parser("quote-latest", help="Build the canonical live quote snapshot from signal + market/orderbook inputs.")
    add_market_profile_cycle_args(quote_latest)
    add_target_arg(quote_latest)
    add_feature_set_arg(quote_latest)
    add_no_persist_arg(quote_latest)

    check_latest = live_sub.add_parser("check-latest", help="Run itemized checks on the latest live signal path.")
    add_market_profile_cycle_args(check_latest)
    add_target_arg(check_latest)
    add_feature_set_arg(check_latest)

    decide_latest = live_sub.add_parser("decide-latest", help="Build the minimal live decision snapshot from the latest signal path.")
    add_market_profile_cycle_args(decide_latest)
    add_target_arg(decide_latest)
    add_feature_set_arg(decide_latest)
    add_no_persist_arg(decide_latest)

    runner_once = live_sub.add_parser("runner-once", help="Run one canonical live pipeline iteration.")
    add_market_profile_cycle_args(runner_once)
    add_target_arg(runner_once)
    add_feature_set_arg(runner_once)
    add_no_persist_arg(runner_once)
    runner_once.add_argument("--no-foundation", action="store_true")
    runner_once.add_argument("--no-direct-oracle", action="store_true")
    runner_once.add_argument("--no-orderbooks", action="store_true")
    runner_once.add_argument("--no-side-effects", action="store_true")
    runner_once.add_argument("--dry-run-side-effects", action="store_true")
    add_adapter_arg(runner_once)

    runner_loop = live_sub.add_parser("runner-loop", help="Run the canonical live pipeline loop.")
    add_market_profile_cycle_args(runner_loop)
    add_target_arg(runner_loop)
    add_feature_set_arg(runner_loop)
    runner_loop.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Max loop iterations; use 0 to run forever.",
    )
    runner_loop.add_argument("--sleep-sec", type=float, default=0.35)
    add_no_persist_arg(runner_loop)
    runner_loop.add_argument("--no-foundation", action="store_true")
    runner_loop.add_argument("--no-direct-oracle", action="store_true")
    runner_loop.add_argument("--no-orderbooks", action="store_true")
    runner_loop.add_argument("--no-side-effects", action="store_true")
    runner_loop.add_argument("--dry-run-side-effects", action="store_true")
    add_adapter_arg(runner_loop)

    execution_simulate = live_sub.add_parser("execution-simulate", help="Build the canonical execution simulation snapshot without placing orders.")
    add_market_profile_cycle_args(execution_simulate)
    add_target_arg(execution_simulate)
    add_feature_set_arg(execution_simulate)
    add_no_persist_arg(execution_simulate)

    execute_latest = live_sub.add_parser("execute-latest", help="Build the latest execution plan and submit the real order.")
    add_market_profile_cycle_args(execute_latest)
    add_target_arg(execute_latest)
    add_feature_set_arg(execute_latest)
    execute_latest.add_argument("--dry-run", action="store_true")
    execute_latest.add_argument("--no-refresh-account-state", action="store_true")
    add_no_persist_arg(execute_latest)
    add_adapter_arg(execute_latest)

    sync_account_state = live_sub.add_parser("sync-account-state", help="Fetch and persist the latest live open-orders + positions snapshots.")
    add_market_profile_cycle_args(sync_account_state)
    add_no_persist_arg(sync_account_state)
    add_adapter_arg(sync_account_state)

    sync_liquidity_state = live_sub.add_parser("sync-liquidity-state", help="Fetch and persist the latest live Binance spot/perp liquidity snapshot.")
    add_market_profile_cycle_args(sync_liquidity_state)
    sync_liquidity_state.add_argument("--force-refresh", action="store_true")
    add_no_persist_arg(sync_liquidity_state)

    apply_cancel_policy = live_sub.add_parser("apply-cancel-policy", help="Apply the cancel policy to open orders near market end.")
    add_market_profile_cycle_args(apply_cancel_policy)
    apply_cancel_policy.add_argument("--dry-run", action="store_true")
    apply_cancel_policy.add_argument("--no-refresh-account-state", action="store_true")
    add_no_persist_arg(apply_cancel_policy)
    add_adapter_arg(apply_cancel_policy)

    apply_redeem_policy = live_sub.add_parser("apply-redeem-policy", help="Apply the redeem policy to redeemable positions.")
    add_market_profile_cycle_args(apply_redeem_policy)
    apply_redeem_policy.add_argument("--dry-run", action="store_true")
    apply_redeem_policy.add_argument("--max-conditions", type=int, default=None)
    apply_redeem_policy.add_argument("--no-refresh-account-state", action="store_true")
    add_no_persist_arg(apply_redeem_policy)
    add_adapter_arg(apply_redeem_policy)

    redeem_loop = live_sub.add_parser("redeem-loop", help="Run the canonical auto-redeem daemon loop.")
    add_market_profile_cycle_args(redeem_loop)
    redeem_loop.add_argument("--dry-run", action="store_true")
    redeem_loop.add_argument("--max-conditions", type=int, default=None)
    redeem_loop.add_argument("--no-refresh-account-state", action="store_true")
    redeem_loop.add_argument("--loop", action="store_true")
    redeem_loop.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Max loop iterations; use 0 with --loop to run forever.",
    )
    redeem_loop.add_argument("--sleep-sec", type=float, default=7200.0)
    add_no_persist_arg(redeem_loop)
    add_adapter_arg(redeem_loop)
