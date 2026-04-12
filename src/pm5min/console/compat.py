from __future__ import annotations

import argparse
import importlib


def _load_console_cli_module() -> object:
    return importlib.import_module("pm15min.console.cli")


def attach_pm15min_console_subcommands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    module = _load_console_cli_module()
    getattr(module, "attach_console_subcommands")(subparsers)


def run_pm15min_console_command(args: argparse.Namespace) -> int:
    module = _load_console_cli_module()
    return getattr(module, "run_console_command")(args)
