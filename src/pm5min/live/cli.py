from __future__ import annotations

import argparse

from .common import build_live_cfg, build_show_config_cfg, print_payload
from .compat import run_pm15min_live_command
from .parser import attach_live_subcommands as _attach_live_subcommands_impl
from .runtime import describe_live_config, describe_live_runtime


def attach_live_subcommands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    _attach_live_subcommands_impl(subparsers)


def run_live_command(args: argparse.Namespace) -> int:
    live_command = str(args.live_command or "")
    if live_command == "show-config":
        return print_payload(describe_live_config(build_show_config_cfg(args)))
    if live_command == "show-layout":
        return print_payload(describe_live_runtime(build_live_cfg(args)))
    return run_pm15min_live_command(args)
