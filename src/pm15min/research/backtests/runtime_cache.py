from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

import pandas as pd


DEFAULT_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES = 8


@dataclass(frozen=True)
class BacktestSharedRuntimeKey:
    rewrite_root: str
    market: str
    cycle: str
    source_surface: str
    bundle_dir: str
    feature_set: str
    label_set: str
    profile_spec_key: str
    liquidity_proxy_mode: str
    decision_start: str
    decision_end: str


@dataclass(frozen=True)
class BacktestPreparedRuntime:
    bundle_dir: str
    feature_set: str
    label_set: str
    features: pd.DataFrame
    labels: pd.DataFrame
    raw_klines: pd.DataFrame
    available_offsets: tuple[int, ...]
    replay: pd.DataFrame
    replay_summary: object
    depth_replay: pd.DataFrame
    depth_replay_summary: object
    depth_candidate_lookup: dict[tuple[object, ...], list[dict[str, object]]]
    runtime_replay: pd.DataFrame
    quote_summary: object
    state_summary: object
    source_mtimes: tuple[tuple[str, int | None], ...]

    def is_current(self) -> bool:
        return all(_path_mtime_ns(Path(raw_path)) == expected for raw_path, expected in self.source_mtimes)

    def clone(self) -> "BacktestPreparedRuntime":
        # Downstream stages already copy before mutation; shallow frame copies avoid reparsing without doubling memory.
        return BacktestPreparedRuntime(
            bundle_dir=self.bundle_dir,
            feature_set=self.feature_set,
            label_set=self.label_set,
            features=self.features.copy(deep=False),
            labels=self.labels.copy(deep=False),
            raw_klines=self.raw_klines.copy(deep=False),
            available_offsets=tuple(self.available_offsets),
            replay=self.replay.copy(deep=False),
            replay_summary=self.replay_summary,
            depth_replay=self.depth_replay.copy(deep=False),
            depth_replay_summary=self.depth_replay_summary,
            depth_candidate_lookup=self.depth_candidate_lookup,
            runtime_replay=self.runtime_replay.copy(deep=False),
            quote_summary=self.quote_summary,
            state_summary=self.state_summary,
            source_mtimes=tuple(self.source_mtimes),
        )


class BacktestRuntimeStageCache:
    def __init__(self, *, max_entries: int = DEFAULT_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES) -> None:
        self._max_entries = max(1, int(max_entries))
        self._entries: OrderedDict[BacktestSharedRuntimeKey, BacktestPreparedRuntime] = OrderedDict()
        self._lock = RLock()

    def get(self, key: BacktestSharedRuntimeKey) -> BacktestPreparedRuntime | None:
        with self._lock:
            cached = self._entries.get(key)
            if cached is None:
                return None
            if not cached.is_current():
                self._entries.pop(key, None)
                return None
            self._entries.move_to_end(key)
            return cached.clone()

    def put(self, key: BacktestSharedRuntimeKey, prepared: BacktestPreparedRuntime) -> BacktestPreparedRuntime:
        stored = prepared.clone()
        with self._lock:
            self._entries[key] = stored
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)
        return stored.clone()

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


_PROCESS_BACKTEST_RUNTIME_CACHE = BacktestRuntimeStageCache()


def process_backtest_runtime_cache() -> BacktestRuntimeStageCache:
    return _PROCESS_BACKTEST_RUNTIME_CACHE


def clear_process_backtest_runtime_cache() -> None:
    _PROCESS_BACKTEST_RUNTIME_CACHE.clear()


def snapshot_source_mtimes(paths: list[Path]) -> tuple[tuple[str, int | None], ...]:
    unique_paths = {str(path): path for path in paths if str(path)}
    return tuple(sorted((raw_path, _path_mtime_ns(path)) for raw_path, path in unique_paths.items()))


def _path_mtime_ns(path: Path) -> int | None:
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return None
    except Exception:
        return None


__all__ = [
    "BacktestPreparedRuntime",
    "BacktestRuntimeStageCache",
    "BacktestSharedRuntimeKey",
    "clear_process_backtest_runtime_cache",
    "process_backtest_runtime_cache",
    "snapshot_source_mtimes",
]
