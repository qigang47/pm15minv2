from __future__ import annotations

import importlib

from pm15min.research.backtests import runtime_cache as runtime_cache_module


def test_backtest_runtime_cache_reads_env_override(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES", "1")

    reloaded = importlib.reload(runtime_cache_module)

    try:
        cache = reloaded.process_backtest_runtime_cache()
        assert cache._max_entries == 1
    finally:
        monkeypatch.delenv("PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES", raising=False)
        importlib.reload(runtime_cache_module)
