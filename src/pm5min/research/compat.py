from __future__ import annotations

import argparse
import importlib
from typing import Any


def _load_research_cli_module() -> object:
    return importlib.import_module("pm15min.research.cli")


def _load_research_cli_handlers_module() -> object:
    return importlib.import_module("pm15min.research.cli_handlers")


def run_pm15min_research_command(args: argparse.Namespace, *, deps: object) -> int:
    module = _load_research_cli_handlers_module()
    return getattr(module, "run_research_command")(args, deps=deps)


def build_pm15min_research_deps(*, research_config_type: type, describe_runtime_fn) -> object:
    module = _load_research_cli_module()
    base_deps = getattr(module, "_build_cli_deps")()
    deps_type = type(base_deps)
    payload: dict[str, Any] = dict(base_deps.__dict__)
    payload["ResearchConfig"] = research_config_type
    payload["describe_research_runtime"] = describe_runtime_fn
    return deps_type(**payload)
