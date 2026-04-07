from __future__ import annotations

import sys

from pm15min.cli import main as pm15min_main
from pm5min.defaults import DEFAULT_CYCLE, DEFAULT_CYCLE_MINUTES, DEFAULT_LIVE_PROFILE


def rewrite_pm5min_argv(argv: list[str]) -> list[str]:
    out = list(argv)
    if not out:
        return out
    domain = out[0]
    if domain in {"layout", "data", "research", "console"} and "--cycle" not in out:
        out.extend(["--cycle", DEFAULT_CYCLE])
    if domain == "live":
        if "--cycle-minutes" not in out:
            out.extend(["--cycle-minutes", str(DEFAULT_CYCLE_MINUTES)])
        if "--profile" not in out:
            out.extend(["--profile", DEFAULT_LIVE_PROFILE])
    return out


def main(argv: list[str] | None = None) -> int:
    return pm15min_main(rewrite_pm5min_argv(list(sys.argv[1:] if argv is None else argv)))
