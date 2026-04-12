from __future__ import annotations

import argparse

from .compat import build_pm15min_data_deps, run_pm15min_data_command
from .parser import attach_data_subcommands as _attach_data_subcommands_impl


def attach_data_subcommands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    _attach_data_subcommands_impl(subparsers)


def run_data_command(args: argparse.Namespace) -> int:
    return run_pm15min_data_command(args, deps=build_pm15min_data_deps())
