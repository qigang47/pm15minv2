from __future__ import annotations

import importlib
import importlib.abc
import sys


class _BlockingFinder(importlib.abc.MetaPathFinder):
    def __init__(self, blocked: set[str]) -> None:
        self._blocked = blocked

    def find_spec(self, fullname: str, path, target=None):  # type: ignore[override]
        if fullname in self._blocked:
            raise ImportError(f"blocked import: {fullname}")
        return None


def _clear_imports(*prefixes: str) -> None:
    for name in list(sys.modules):
        if any(name == prefix or name.startswith(f"{prefix}.") for prefix in prefixes):
            sys.modules.pop(name, None)


def test_experiment_runner_import_does_not_require_trading_stack() -> None:
    blocker = _BlockingFinder(
        {
            "pm15min.live.account",
            "pm15min.live.execution.policy",
            "pm15min.live.trading",
            "py_builder_relayer_client",
            "py_builder_relayer_client.client",
            "web3",
        }
    )
    sys.meta_path.insert(0, blocker)
    try:
        _clear_imports(
            "pm15min.live.account",
            "pm15min.live.execution",
            "pm15min.live.trading",
            "pm15min.research.backtests.engine",
            "pm15min.research.experiments.runner",
        )
        module = importlib.import_module("pm15min.research.experiments.runner")
    finally:
        sys.meta_path.remove(blocker)
    assert callable(module.run_experiment_suite)
