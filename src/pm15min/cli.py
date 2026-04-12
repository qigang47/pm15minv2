from __future__ import annotations

import argparse
import importlib
import json
import sys

from pm15min.data.layout import DataLayout


_DOMAIN_LOADERS = {
    "console": ("pm15min.console.cli", "attach_console_subcommands", "run_console_command"),
    "data": ("pm15min.data.cli", "attach_data_subcommands", "run_data_command"),
    "live": ("pm15min.live.cli", "attach_live_subcommands", "run_live_command"),
    "research": ("pm15min.research.cli", "attach_research_subcommands", "run_research_command"),
}
_DOMAIN_HELP = {
    "console": "Console commands.",
    "data": "Data commands.",
    "live": "Live commands.",
    "research": "Research commands.",
}


def _print_json(payload: dict[str, object]) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))


def _load_domain_cli(domain: str) -> tuple[object, object]:
    module_name, attach_name, run_name = _DOMAIN_LOADERS[domain]
    module = importlib.import_module(module_name)
    return getattr(module, attach_name), getattr(module, run_name)


def _requested_domain(argv: list[str]) -> str | None:
    for token in argv:
        if token.startswith("-"):
            continue
        if token in _DOMAIN_LOADERS or token == "layout":
            return token
        break
    return None


def build_parser(requested_domain: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pm15min",
        description="pm15min v2 clean-room CLI",
    )
    subparsers = parser.add_subparsers(dest="domain")

    layout_parser = subparsers.add_parser(
        "layout",
        help="Show the canonical v2 market layout.",
    )
    layout_parser.add_argument("--market", default="btc", help="Market slug: btc/eth/sol/xrp.")
    layout_parser.add_argument("--cycle", default="15m", help="Cycle slug: 5m or 15m.")
    layout_parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of key/value lines.",
    )

    for domain in ("live", "research", "data", "console"):
        if requested_domain == domain:
            attach_subcommands, _ = _load_domain_cli(domain)
            attach_subcommands(subparsers)
        else:
            subparsers.add_parser(domain, help=_DOMAIN_HELP[domain])
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser(_requested_domain(argv))
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
        _, run_live_command = _load_domain_cli("live")
        return run_live_command(args)
    if args.domain == "research":
        _, run_research_command = _load_domain_cli("research")
        return run_research_command(args)
    if args.domain == "data":
        _, run_data_command = _load_domain_cli("data")
        return run_data_command(args)
    if args.domain == "console":
        _, run_console_command = _load_domain_cli("console")
        return run_console_command(args)

    parser.print_help()
    return 1
