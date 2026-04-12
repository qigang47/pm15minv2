from __future__ import annotations

import argparse
import importlib
from typing import Any

from pm5min.data.config import DataConfig
from pm5min.data.layout import DataLayout


def _load_pm15min_data_cli_module() -> object:
    return importlib.import_module("pm15min.data.cli")


def _load_pm15min_data_handlers_module() -> object:
    return importlib.import_module("pm15min.data.cli.handlers")


def build_pm15min_data_deps() -> object:
    module = _load_pm15min_data_cli_module()
    base_deps = getattr(module, "_build_cli_deps")()
    deps_type = type(base_deps)
    payload: dict[str, Any] = dict(base_deps.__dict__)
    payload["DataConfig"] = DataConfig
    payload["DataLayout"] = DataLayout
    return deps_type(**payload)


def run_pm15min_data_command(args: argparse.Namespace, *, deps: object) -> int:
    module = _load_pm15min_data_handlers_module()
    return getattr(module, "run_data_command")(args, deps=deps)
