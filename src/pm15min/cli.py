from __future__ import annotations

import argparse
import json
import sys

from pm15min.console.cli import attach_console_subcommands, run_console_command
from pm15min.data.layout import DataLayout
from pm15min.data.cli import attach_data_subcommands, run_data_command
from pm15min.live.cli import attach_live_subcommands, run_live_command
from pm15min.research.cli import attach_research_subcommands, run_research_command


def _print_json(payload: dict[str, object]) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pm15min",
        description="pm15min v2 clean-room CLI",
    )
    subparsers = parser.add_subparsers(dest="domain")

    layout_parser = subparsers.add_parser(
        "layout",
        help="Show the canonical 15m market layout.",
    )
    layout_parser.add_argument("--market", default="btc", help="Market slug: btc/eth/sol/xrp.")
    layout_parser.add_argument("--cycle", default="15m", choices=("15m",), help="Cycle slug: 15m only.")
    layout_parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of key/value lines.",
    )

    attach_live_subcommands(subparsers)
    attach_research_subcommands(subparsers)
    attach_data_subcommands(subparsers)
    attach_console_subcommands(subparsers)
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    if not argv:
        parser.print_help()
        return 0

    args = parser.parse_args(argv)
    if args.domain == "layout":
        payload = DataLayout.discover().for_market(args.market, args.cycle).to_dict()
        if args.json:
            _print_json(payload)
        else:
            for key, value in payload.items():
                print(f"{key}: {value}")
        return 0
    if args.domain == "live":
        return run_live_command(args)
    if args.domain == "research":
        return run_research_command(args)
    if args.domain == "data":
        return run_data_command(args)
    if args.domain == "console":
        return run_console_command(args)

    parser.print_help()
    return 1
