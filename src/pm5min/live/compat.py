from __future__ import annotations

import argparse
import importlib


def _load_live_runtime_module() -> object:
    return importlib.import_module("pm15min.live.runtime")


def _load_live_cli_module() -> object:
    return importlib.import_module("pm15min.live.cli")


def describe_live_runtime(cfg) -> dict[str, object]:
    module = _load_live_runtime_module()
    return getattr(module, "describe_live_runtime")(cfg)


def describe_live_config(cfg) -> dict[str, object]:
    module = _load_live_runtime_module()
    return getattr(module, "describe_live_config")(cfg)


def run_pm15min_live_command(args: argparse.Namespace) -> int:
    module = _load_live_cli_module()
    return getattr(module, "run_live_command")(args)
