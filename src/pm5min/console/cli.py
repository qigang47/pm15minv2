from __future__ import annotations

import argparse

from .compat import attach_pm15min_console_subcommands, run_pm15min_console_command


def attach_console_subcommands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    attach_pm15min_console_subcommands(subparsers)


def run_console_command(args: argparse.Namespace) -> int:
    return run_pm15min_console_command(args)
