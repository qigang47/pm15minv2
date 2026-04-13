from __future__ import annotations

import argparse
import importlib


def _load_live_cli_module() -> object:
    return importlib.import_module("pm15min.live.cli")


def run_pm15min_live_command(args: argparse.Namespace) -> int:
    module = _load_live_cli_module()
    return getattr(module, "run_live_command")(args)
