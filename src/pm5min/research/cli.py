from __future__ import annotations

import argparse

from .handlers import run_research_command as _run_research_command_impl
from .parser import attach_research_subcommands as _attach_research_subcommands_impl


def attach_research_subcommands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    _attach_research_subcommands_impl(subparsers)


def run_research_command(args: argparse.Namespace) -> int:
    return _run_research_command_impl(args)
