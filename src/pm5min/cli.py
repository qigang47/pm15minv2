from __future__ import annotations

import sys

from pm15min.cli import main as pm15min_main
from pm5min.defaults import DEFAULT_CYCLE, DEFAULT_CYCLE_MINUTES, DEFAULT_LIVE_PROFILE


def _has_flag(argv: list[str], flag: str) -> bool:
    return any(arg == flag or arg.startswith(f"{flag}=") for arg in argv)


def rewrite_pm5min_argv(argv: list[str]) -> list[str]:
    out = list(argv)
    if not out:
        return out
    domain = out[0]
    if domain in {"layout", "research"} and not _has_flag(out, "--cycle"):
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
