from __future__ import annotations

import sys

from pm15min.cli import main as pm15min_main
from pm5min.defaults import DEFAULT_CYCLE, DEFAULT_CYCLE_MINUTES, DEFAULT_LIVE_PROFILE


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
    return pm15min_main(rewrite_pm5min_argv(list(sys.argv[1:] if argv is None else argv)))
