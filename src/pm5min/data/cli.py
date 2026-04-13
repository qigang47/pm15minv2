from __future__ import annotations

import argparse

from .handlers import run_data_command as _run_data_command_impl
from .parser import attach_data_subcommands as _attach_data_subcommands_impl


def attach_data_subcommands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    _attach_data_subcommands_impl(subparsers)


def run_data_command(args: argparse.Namespace) -> int:
    return _run_data_command_impl(args)
