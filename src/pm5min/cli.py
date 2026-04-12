from __future__ import annotations

import argparse
import importlib
import json
import sys

from pm5min.defaults import DEFAULT_CYCLE, DEFAULT_CYCLE_MINUTES, DEFAULT_LIVE_PROFILE


_DOMAIN_LOADERS = {
    "console": ("pm15min.console.cli", "attach_console_subcommands", "run_console_command"),
    "data": ("pm15min.data.cli", "attach_data_subcommands", "run_data_command"),
    "live": ("pm15min.live.cli", "attach_live_subcommands", "run_live_command"),
    "research": ("pm15min.research.cli", "attach_research_subcommands", "run_research_command"),
}


def _has_flag(argv: list[str], flag: str) -> bool:
    return any(arg == flag or arg.startswith(f"{flag}=") for arg in argv)


def _data_command_supports_cycle(argv: list[str]) -> bool:
    if len(argv) < 2 or argv[1].startswith("-"):
        return False
    command = argv[1]
    if command in {"show-config", "show-layout", "show-summary", "show-orderbook-coverage"}:
        return True
    if len(argv) < 3 or argv[2].startswith("-"):
        return False
    subcommand = argv[2]
    return (command, subcommand) in {
        ("sync", "market-catalog"),
        ("sync", "direct-oracle-prices"),
        ("sync", "legacy-market-catalog"),
        ("sync", "legacy-orderbook-depth"),
        ("sync", "settlement-truth-rpc"),
        ("sync", "legacy-settlement-truth"),
        ("build", "oracle-prices-15m"),
        ("build", "truth-15m"),
        ("build", "orderbook-index"),
        ("export", "oracle-prices-15m"),
        ("export", "truth-15m"),
        ("record", "orderbooks"),
        ("run", "orderbook-fleet"),
        ("run", "live-foundation"),
        ("run", "backfill-direct-oracle"),
        ("run", "backfill-cycle-labels-gamma"),
    }


def _load_data_layout() -> object:
    module = importlib.import_module("pm15min.data.layout")
    return getattr(module, "DataLayout")


def _load_domain_cli(domain: str) -> tuple[object, object]:
    module_name, attach_name, run_name = _DOMAIN_LOADERS[domain]
    module = importlib.import_module(module_name)
    return getattr(module, attach_name), getattr(module, run_name)


def _requested_domain(argv: list[str]) -> str | None:
    for token in argv:
        if token.startswith("-"):
            continue
        if token == "layout" or token in _DOMAIN_LOADERS:
            return token
        break
    return None


def _print_json(payload: dict[str, object]) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))


def build_parser(requested_domain: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pm5min",
        description="pm5min v2 clean-room CLI",
    )
    subparsers = parser.add_subparsers(dest="domain")

    layout_parser = subparsers.add_parser(
        "layout",
        help="Show the canonical 5m market layout.",
    )
    layout_parser.add_argument("--market", default="btc", help="Market slug: btc/eth/sol/xrp.")
    layout_parser.add_argument("--cycle", default=DEFAULT_CYCLE, choices=(DEFAULT_CYCLE,), help="Cycle slug: 5m only.")
    layout_parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of key/value lines.",
    )

    for domain in ("live", "research", "data", "console"):
        if requested_domain in (None, domain):
            attach_subcommands, _ = _load_domain_cli(domain)
            attach_subcommands(subparsers)
        else:
            subparsers.add_parser(domain)
    return parser


def rewrite_pm5min_argv(argv: list[str]) -> list[str]:
    out = list(argv)
    if not out:
        return out
    domain = out[0]
    if domain in {"layout", "research"} and not _has_flag(out, "--cycle"):
        out.extend(["--cycle", DEFAULT_CYCLE])
    if domain == "data" and _data_command_supports_cycle(out) and not _has_flag(out, "--cycle"):
        out.extend(["--cycle", DEFAULT_CYCLE])
    if domain == "live":
        live_command = out[1] if len(out) > 1 and not out[1].startswith("-") else None
        if live_command is None:
            return out
        if not _has_flag(out, "--cycle-minutes"):
            out.extend(["--cycle-minutes", str(DEFAULT_CYCLE_MINUTES)])
        if not _has_flag(out, "--profile"):
            out.extend(["--profile", DEFAULT_LIVE_PROFILE])
    return out


def main(argv: list[str] | None = None) -> int:
    rewritten_argv = rewrite_pm5min_argv(list(sys.argv[1:] if argv is None else argv))
    parser = build_parser(_requested_domain(rewritten_argv))
    if not rewritten_argv:
        parser.print_help()
        return 0

    args = parser.parse_args(rewritten_argv)
    if args.domain == "layout":
        payload = _load_data_layout().discover().for_market(args.market, args.cycle).to_dict()
        if args.json:
            _print_json(payload)
        else:
            for key, value in payload.items():
                print(f"{key}: {value}")
        return 0
    if args.domain in _DOMAIN_LOADERS:
        _, run_domain_command = _load_domain_cli(args.domain)
        return run_domain_command(args)

    parser.print_help()
    return 1
