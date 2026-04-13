from __future__ import annotations

import argparse

from .handlers import run_console_command as _run_console_command_impl
from .parser import attach_console_subcommands as _attach_console_subcommands_impl


def attach_console_subcommands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    _attach_console_subcommands_impl(subparsers)


def run_console_command(args: argparse.Namespace) -> int:
    return _run_console_command_impl(args)
