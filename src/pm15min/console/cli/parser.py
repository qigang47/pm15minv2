from __future__ import annotations

import argparse


def _add_market_cycle_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--market", default="sol")
    parser.add_argument("--cycle", default="15m")


def attach_console_subcommands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "console",
        help="Read-only console over canonical v2 data/research artifacts.",
    )
    console_sub = parser.add_subparsers(dest="console_command")

    console_sub.add_parser(
        "show-home",
        help="Show the console home payload and available sections.",
    )

    console_sub.add_parser(
        "show-runtime-state",
        help="Show persisted console runtime state and recent task summary.",
    )

    console_sub.add_parser(
        "show-runtime-history",
        help="Show persisted console runtime history window and retention metadata.",
    )

    serve = console_sub.add_parser(
        "serve",
        help="Run the read-only JSON HTTP server for the console domain.",
    )
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--poll-interval", type=float, default=0.5)

    console_sub.add_parser(
        "show-actions",
        help="Show the console action catalog.",
    )

    build_action = console_sub.add_parser(
        "build-action",
        help="Build one normalized console action request from JSON input.",
    )
    build_action.add_argument("--action-id", required=True)
    build_action.add_argument("--request-json", default="{}")

    execute_action = console_sub.add_parser(
        "execute-action",
        help="Execute one console action through existing pm15min CLI entrypoints.",
    )
    execute_action.add_argument("--action-id", required=True)
    execute_action.add_argument("--request-json", default="{}")
    execute_action.add_argument("--execution-mode", default="sync", choices=["sync", "async"])

    list_tasks = console_sub.add_parser(
        "list-tasks",
        help="List console background tasks.",
    )
    list_tasks.add_argument("--action-id", action="append", default=None)
    list_tasks.add_argument("--status", default=None)
    list_tasks.add_argument("--status-group", default=None, choices=["active", "terminal", "failed"])
    list_tasks.add_argument("--marker", default=None, choices=["latest", "active", "terminal", "failed"])
    list_tasks.add_argument("--group-by", default=None, choices=["action_id", "status", "status_group"])
    list_tasks.add_argument("--limit", type=int, default=20)

    show_task = console_sub.add_parser(
        "show-task",
        help="Show one console background task.",
    )
    show_task.add_argument("--task-id", required=True)

    show_data = console_sub.add_parser(
        "show-data-overview",
        help="Show canonical data overview for one market/surface.",
    )
    _add_market_cycle_args(show_data)
    show_data.add_argument("--surface", default="backtest", choices=["live", "backtest"])

    list_training = console_sub.add_parser(
        "list-training-runs",
        help="List canonical training runs for one market.",
    )
    _add_market_cycle_args(list_training)
    list_training.add_argument("--model-family", default=None)
    list_training.add_argument("--target", default=None)
    list_training.add_argument("--prefix", default=None)

    show_training = console_sub.add_parser(
        "show-training-run",
        help="Show one canonical training run and its offset summaries.",
    )
    _add_market_cycle_args(show_training)
    show_training.add_argument("--run-label", required=True)
    show_training.add_argument("--model-family", default="deep_otm")
    show_training.add_argument("--target", default="direction")

    list_bundles = console_sub.add_parser(
        "list-bundles",
        help="List canonical model bundles for one market.",
    )
    _add_market_cycle_args(list_bundles)
    list_bundles.add_argument("--profile", default=None)
    list_bundles.add_argument("--target", default=None)
    list_bundles.add_argument("--prefix", default=None)

    show_bundle = console_sub.add_parser(
        "show-bundle",
        help="Show one canonical model bundle and active selection state.",
    )
    _add_market_cycle_args(show_bundle)
    show_bundle.add_argument("--bundle-label", required=True)
    show_bundle.add_argument("--profile", default="deep_otm")
    show_bundle.add_argument("--target", default="direction")

    list_backtests = console_sub.add_parser(
        "list-backtests",
        help="List canonical backtest runs for one market.",
    )
    _add_market_cycle_args(list_backtests)
    list_backtests.add_argument("--profile", default=None)
    list_backtests.add_argument("--spec", default=None)
    list_backtests.add_argument("--prefix", default=None)

    show_backtest = console_sub.add_parser(
        "show-backtest",
        help="Show one canonical backtest run summary and artifact inventory.",
    )
    _add_market_cycle_args(show_backtest)
    show_backtest.add_argument("--run-label", required=True)
    show_backtest.add_argument("--profile", default="deep_otm")
    show_backtest.add_argument("--spec", default="baseline_truth")

    list_experiments = console_sub.add_parser(
        "list-experiments",
        help="List canonical experiment runs.",
    )
    list_experiments.add_argument("--suite", default=None)
    list_experiments.add_argument("--prefix", default=None)

    show_experiment = console_sub.add_parser(
        "show-experiment",
        help="Show one canonical experiment run summary and artifact inventory.",
    )
    show_experiment.add_argument("--suite", required=True)
    show_experiment.add_argument("--run-label", required=True)
