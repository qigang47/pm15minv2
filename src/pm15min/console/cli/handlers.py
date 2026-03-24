from __future__ import annotations

import argparse
import json

from ..http import serve_console_http
from ..service import (
    build_console_action_request,
    execute_console_action,
    list_console_tasks,
    list_console_backtests,
    list_console_bundles,
    list_console_experiments,
    list_console_training_runs,
    load_console_action_catalog,
    load_console_backtest,
    load_console_bundle,
    load_console_data_overview,
    load_console_experiment,
    load_console_home,
    load_console_runtime_history,
    load_console_runtime_state,
    load_console_task,
    load_console_training_run,
    submit_console_action_task,
)


def _print_payload(payload: object) -> int:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


def _parse_request_json(raw: str) -> dict[str, object]:
    try:
        payload = json.loads(str(raw or "{}"))
    except Exception as exc:
        raise SystemExit(f"Invalid --request-json: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("Invalid --request-json: expected a JSON object.")
    return {str(key): value for key, value in payload.items()}


def run_console_command(args: argparse.Namespace) -> int:
    if args.console_command == "show-home":
        return _print_payload(load_console_home())

    if args.console_command == "show-runtime-state":
        return _print_payload(load_console_runtime_state())

    if args.console_command == "show-runtime-history":
        return _print_payload(load_console_runtime_history())

    if args.console_command == "show-actions":
        return _print_payload(load_console_action_catalog())

    if args.console_command == "build-action":
        return _print_payload(
            build_console_action_request(
                action_id=args.action_id,
                request=_parse_request_json(args.request_json),
            )
        )

    if args.console_command == "execute-action":
        runner = submit_console_action_task if args.execution_mode == "async" else execute_console_action
        return _print_payload(
            runner(
                action_id=args.action_id,
                request=_parse_request_json(args.request_json),
            )
        )

    if args.console_command == "list-tasks":
        return _print_payload(
            list_console_tasks(
                action_ids=args.action_id,
                status=args.status,
                status_group=args.status_group,
                marker=args.marker,
                group_by=args.group_by,
                limit=args.limit,
            )
        )

    if args.console_command == "show-task":
        return _print_payload(
            load_console_task(
                task_id=args.task_id,
            )
        )

    if args.console_command == "show-data-overview":
        return _print_payload(
            load_console_data_overview(
                market=args.market,
                cycle=args.cycle,
                surface=args.surface,
            )
        )

    if args.console_command == "list-training-runs":
        return _print_payload(
            list_console_training_runs(
                market=args.market,
                cycle=args.cycle,
                model_family=args.model_family,
                target=args.target,
                prefix=args.prefix,
            )
        )

    if args.console_command == "show-training-run":
        return _print_payload(
            load_console_training_run(
                market=args.market,
                cycle=args.cycle,
                model_family=args.model_family,
                target=args.target,
                run_label=args.run_label,
            )
        )

    if args.console_command == "list-bundles":
        return _print_payload(
            list_console_bundles(
                market=args.market,
                cycle=args.cycle,
                profile=args.profile,
                target=args.target,
                prefix=args.prefix,
            )
        )

    if args.console_command == "show-bundle":
        return _print_payload(
            load_console_bundle(
                market=args.market,
                cycle=args.cycle,
                profile=args.profile,
                target=args.target,
                bundle_label=args.bundle_label,
            )
        )

    if args.console_command == "list-backtests":
        return _print_payload(
            list_console_backtests(
                market=args.market,
                cycle=args.cycle,
                profile=args.profile,
                spec_name=args.spec,
                prefix=args.prefix,
            )
        )

    if args.console_command == "show-backtest":
        return _print_payload(
            load_console_backtest(
                market=args.market,
                cycle=args.cycle,
                profile=args.profile,
                spec_name=args.spec,
                run_label=args.run_label,
            )
        )

    if args.console_command == "list-experiments":
        return _print_payload(
            list_console_experiments(
                suite_name=args.suite,
                prefix=args.prefix,
            )
        )

    if args.console_command == "show-experiment":
        return _print_payload(
            load_console_experiment(
                suite_name=args.suite,
                run_label=args.run_label,
            )
        )

    if args.console_command == "serve":
        serve_console_http(
            host=args.host,
            port=int(args.port),
            poll_interval=float(args.poll_interval),
        )
        return 0

    raise SystemExit("Missing console subcommand.")
