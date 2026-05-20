from __future__ import annotations

import importlib

import pandas as pd

from pm15min.research.backtests import runtime_cache as runtime_cache_module


def test_backtest_runtime_cache_reads_env_override(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES", "1")
    monkeypatch.setenv("PM15MIN_BACKTEST_SURFACE_RUNTIME_CACHE_MAX_ENTRIES", "3")

    reloaded = importlib.reload(runtime_cache_module)

    try:
        cache = reloaded.process_backtest_runtime_cache()
        surface_cache = reloaded.process_backtest_surface_runtime_cache()
        assert cache._max_entries == 1
        assert surface_cache._max_entries == 3
    finally:
        monkeypatch.delenv("PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES", raising=False)
        monkeypatch.delenv("PM15MIN_BACKTEST_SURFACE_RUNTIME_CACHE_MAX_ENTRIES", raising=False)
        importlib.reload(runtime_cache_module)


def test_surface_runtime_cache_default_is_disabled_for_memory_lean_runs(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES", "1")
    monkeypatch.delenv("PM15MIN_BACKTEST_SURFACE_RUNTIME_CACHE_MAX_ENTRIES", raising=False)

    reloaded = importlib.reload(runtime_cache_module)

    try:
        assert reloaded.process_backtest_runtime_cache()._max_entries == 1
        assert reloaded.process_backtest_surface_runtime_cache()._max_entries == 0
    finally:
        monkeypatch.delenv("PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES", raising=False)
        importlib.reload(runtime_cache_module)


def test_primary_runtime_cache_can_be_disabled_without_losing_return_value(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES", "0")
    reloaded = importlib.reload(runtime_cache_module)

    key = reloaded.BacktestSharedRuntimeKey(
        rewrite_root="/tmp/root",
        market="btc",
        cycle="15m",
        source_surface="backtest",
        bundle_dir="/tmp/bundle",
        feature_set="features",
        label_set="truth",
        profile_spec_key="{}",
        liquidity_proxy_mode="spot_kline_mirror",
        decision_start="2026-04-15",
        decision_end="2026-05-07",
    )
    prepared = reloaded.BacktestPreparedRuntime(
        bundle_dir="/tmp/bundle",
        feature_set="features",
        label_set="truth",
        features=pd.DataFrame({"a": [1]}),
        labels=pd.DataFrame({"b": [2]}),
        raw_klines=pd.DataFrame({"c": [3]}),
        available_offsets=(7,),
        replay=pd.DataFrame({"d": [4]}),
        replay_summary=object(),
        depth_replay=pd.DataFrame({"e": [5]}),
        depth_replay_summary=object(),
        depth_candidate_lookup={},
        runtime_replay=pd.DataFrame({"f": [6]}),
        quote_summary=object(),
        state_summary=object(),
        source_mtimes=(),
    )

    try:
        cache = reloaded.BacktestRuntimeStageCache(max_entries=0)
        returned = cache.put(key, prepared)

        assert returned.features.equals(prepared.features)
        assert cache.get(key) is None
    finally:
        monkeypatch.delenv("PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES", raising=False)
        importlib.reload(runtime_cache_module)


def test_primary_runtime_cache_reports_disabled_when_max_entries_is_zero() -> None:
    cache = runtime_cache_module.BacktestRuntimeStageCache(max_entries=0)
    enabled_cache = runtime_cache_module.BacktestRuntimeStageCache(max_entries=1)

    assert cache.enabled is False
    assert enabled_cache.enabled is True


def test_surface_runtime_cache_reports_disabled_when_max_entries_is_zero() -> None:
    cache = runtime_cache_module.BacktestSurfaceRuntimeStageCache(max_entries=0)
    enabled_cache = runtime_cache_module.BacktestSurfaceRuntimeStageCache(max_entries=1)

    assert cache.enabled is False
    assert enabled_cache.enabled is True


def test_surface_runtime_cache_can_be_disabled_with_zero_entries() -> None:
    key = runtime_cache_module.BacktestSurfaceRuntimeKey(
        rewrite_root="/tmp/root",
        market="btc",
        cycle="15m",
        source_surface="backtest",
        feature_set="features",
        label_set="truth",
        profile_spec_key="{}",
        liquidity_proxy_mode="spot_kline_mirror",
        raw_depth_fak_refresh_enabled=True,
        decision_start="2026-04-15",
        decision_end="2026-05-07",
        available_offsets=(7,),
        surface_input_signature="demo",
    )
    prepared = runtime_cache_module.BacktestSurfaceRuntime(
        depth_replay=pd.DataFrame({"a": [1]}),
        depth_replay_summary=object(),
        depth_candidate_lookup={},
        runtime_replay=pd.DataFrame({"b": [2]}),
        quote_summary=object(),
        state_summary=object(),
        source_mtimes=(),
    )

    cache = runtime_cache_module.BacktestSurfaceRuntimeStageCache(max_entries=0)
    returned = cache.put(key, prepared)

    assert returned.runtime_replay.equals(prepared.runtime_replay)
    assert cache.get(key) is None
