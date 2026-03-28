from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import threading
import time

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.pipelines.binance_klines import sync_binance_klines_1m
from pm15min.data.pipelines.market_catalog import sync_market_catalog
from pm15min.data.queries.loaders import load_binance_klines_1m, load_oracle_prices_table
from pm15min.live.oracle import build_live_runtime_oracle_prices
from ..account import summarize_account_state_payload, summarize_open_orders_snapshot, summarize_positions_snapshot
from ..account import load_latest_open_orders_snapshot, load_latest_positions_snapshot
from ..layout import LiveStateLayout
from ..persistence import write_live_payload_pair
from pm15min.research.features.builders import build_feature_frame as build_feature_frame_df
from pm15min.research.features.registry import feature_set_columns

DEFAULT_LIVE_TRADE_INPUT_REFRESH_RETRY_SECONDS = 30.0
DEFAULT_LIVE_MARKET_CATALOG_MAX_AGE_SECONDS = 300.0
DEFAULT_LIVE_BINANCE_KLINES_MAX_AGE_SECONDS = 120.0
DEFAULT_LIVE_MARKET_CATALOG_LOOKBACK_HOURS = 24
DEFAULT_LIVE_MARKET_CATALOG_LOOKAHEAD_HOURS = 24
DEFAULT_LIVE_BINANCE_LOOKBACK_MINUTES = 2880
DEFAULT_LIVE_BINANCE_BATCH_LIMIT = 1000

_LIVE_FEATURE_FRAME_CACHE: dict[tuple[str, str, int, str, tuple[int, ...], int], tuple[float, pd.DataFrame]] = {}
_LIVE_FEATURE_FRAME_CACHE_LOCK = threading.Lock()
_LIVE_ACCOUNT_CONTEXT_CACHE: dict[tuple[str, str], tuple[float, tuple[int | None, int | None], dict[str, object]]] = {}
_LIVE_ACCOUNT_CONTEXT_CACHE_LOCK = threading.Lock()
_LIVE_KLINES_TAIL_CACHE: dict[tuple[str, str], tuple[tuple[int | None, int | None], pd.DataFrame]] = {}
_LIVE_KLINES_TAIL_CACHE_LOCK = threading.Lock()


def supports_feature_set(feature_set: str) -> bool:
    try:
        feature_set_columns(feature_set)
        return True
    except Exception:
        return False


def build_live_feature_frame(
    cfg,
    *,
    feature_set: str,
    retain_offsets: tuple[int, ...] | None = None,
) -> pd.DataFrame:
    cache_ttl_seconds = _env_float("PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC", default=0.0)
    resolved_retain_offsets = tuple(retain_offsets or _env_int_list("PM15MIN_LIVE_FEATURE_RETAIN_OFFSETS"))
    tail_cycles = _env_int("PM15MIN_LIVE_FEATURE_TAIL_CYCLES", default=2)
    cache_key = (
        str(cfg.layout.rewrite.root),
        str(cfg.asset.slug),
        int(cfg.cycle_minutes),
        str(feature_set),
        tuple(resolved_retain_offsets),
        int(tail_cycles),
    )
    if cache_ttl_seconds > 0.0:
        cached = _load_cached_live_feature_frame(cache_key=cache_key)
        if cached is not None:
            return cached
    data_cfg = DataConfig.build(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        surface="live",
        root=cfg.layout.rewrite.root,
    )
    ensure_live_trade_inputs_fresh(cfg)
    raw_tail_bars = _live_feature_build_tail_bars(cycle_minutes=int(cfg.cycle_minutes), tail_cycles=tail_cycles)
    raw_klines = _load_live_binance_klines_tail(
        data_cfg=data_cfg,
        tail_bars=raw_tail_bars,
    )
    oracle_prices = build_live_runtime_oracle_prices(
        data_cfg=data_cfg,
        market_slug=cfg.asset.slug,
        raw_klines=raw_klines,
        oracle_prices_table=load_oracle_prices_table(data_cfg),
    )
    btc_klines = None
    if cfg.asset.slug != "btc":
        btc_cfg = DataConfig.build(
            market="btc",
            cycle=f"{int(cfg.cycle_minutes)}m",
            surface="live",
            root=cfg.layout.rewrite.root,
        )
        btc_klines = _load_live_binance_klines_tail(
            data_cfg=btc_cfg,
            symbol="BTCUSDT",
            tail_bars=raw_tail_bars,
        )
    features = build_feature_frame_df(
        raw_klines,
        feature_set=feature_set,
        oracle_prices=oracle_prices,
        btc_klines=btc_klines,
        cycle=f"{int(cfg.cycle_minutes)}m",
    )
    features = _trim_live_feature_frame(
        features,
        cycle_minutes=int(cfg.cycle_minutes),
        retain_offsets=resolved_retain_offsets,
        tail_cycles=tail_cycles,
    )
    if cache_ttl_seconds > 0.0:
        _store_cached_live_feature_frame(
            cache_key=cache_key,
            cache_ttl_seconds=cache_ttl_seconds,
            features=features,
        )
    return features


def _load_cached_live_feature_frame(*, cache_key: tuple[str, str, int, str, tuple[int, ...], int]) -> pd.DataFrame | None:
    now_monotonic = time.monotonic()
    with _LIVE_FEATURE_FRAME_CACHE_LOCK:
        cached = _LIVE_FEATURE_FRAME_CACHE.get(cache_key)
        if cached is None:
            return None
        expires_at, frame = cached
        if now_monotonic >= float(expires_at):
            _LIVE_FEATURE_FRAME_CACHE.pop(cache_key, None)
            return None
        return frame.copy(deep=False)


def _store_cached_live_feature_frame(
    *,
    cache_key: tuple[str, str, int, str, tuple[int, ...], int],
    cache_ttl_seconds: float,
    features: pd.DataFrame,
) -> None:
    with _LIVE_FEATURE_FRAME_CACHE_LOCK:
        _LIVE_FEATURE_FRAME_CACHE[cache_key] = (
            time.monotonic() + max(0.0, float(cache_ttl_seconds)),
            features.copy(deep=False),
        )


def _trim_live_feature_frame(
    features: pd.DataFrame,
    *,
    cycle_minutes: int,
    retain_offsets: tuple[int, ...],
    tail_cycles: int,
) -> pd.DataFrame:
    if not isinstance(features, pd.DataFrame) or features.empty:
        return features
    if int(cycle_minutes) <= 0:
        return features
    rows = features.copy()
    cycle_start = pd.to_datetime(rows.get("cycle_start_ts"), utc=True, errors="coerce")
    decision_ts = pd.to_datetime(rows.get("decision_ts"), utc=True, errors="coerce")
    if not isinstance(cycle_start, pd.Series):
        return rows
    valid_cycle_start = cycle_start.dropna().drop_duplicates().sort_values()
    if valid_cycle_start.empty:
        if not isinstance(decision_ts, pd.Series):
            return rows
        tail_rows = max(int(cycle_minutes) * max(1, int(tail_cycles)), int(cycle_minutes))
        return rows.sort_values("decision_ts").tail(tail_rows).reset_index(drop=True)

    keep_cycle_count = max(1, int(tail_cycles))
    latest_cycle_start = valid_cycle_start.iloc[-1]
    keep_cycle_starts = set(valid_cycle_start.iloc[-keep_cycle_count:].tolist())
    latest_cycle_mask = cycle_start.eq(latest_cycle_start)
    keep_mask = latest_cycle_mask | cycle_start.isin(keep_cycle_starts)
    if retain_offsets:
        offset_series = pd.to_numeric(rows.get("offset"), errors="coerce")
        active_offset_mask = offset_series.isin(list(retain_offsets))
        keep_mask = latest_cycle_mask | (cycle_start.isin(keep_cycle_starts) & active_offset_mask)
    trimmed = rows.loc[keep_mask].copy()
    if trimmed.empty:
        trimmed = rows.sort_values("decision_ts").tail(max(int(cycle_minutes), 1)).copy()
    if "decision_ts" in trimmed.columns:
        trimmed = trimmed.sort_values("decision_ts")
    return trimmed.reset_index(drop=True)


def _live_feature_build_tail_bars(*, cycle_minutes: int, tail_cycles: int) -> int:
    requested = _env_int("PM15MIN_LIVE_FEATURE_BUILD_TAIL_BARS", default=384)
    minimum = max(256, int(cycle_minutes) * max(4, int(tail_cycles) + 2))
    return max(int(requested), int(minimum))


def _load_live_binance_klines_tail(
    *,
    data_cfg: DataConfig,
    symbol: str | None = None,
    tail_bars: int,
) -> pd.DataFrame:
    path = data_cfg.layout.binance_klines_path(symbol=symbol)
    cache_key = (str(path), str(symbol or data_cfg.asset.binance_symbol))
    signature = _path_signature(path)
    if signature[0] is not None:
        with _LIVE_KLINES_TAIL_CACHE_LOCK:
            cached = _LIVE_KLINES_TAIL_CACHE.get(cache_key)
            if cached is not None and cached[0] == signature:
                return cached[1].copy(deep=False)
    raw = load_binance_klines_1m(data_cfg, symbol=symbol)
    if isinstance(raw, pd.DataFrame) and not raw.empty and int(tail_bars) > 0 and len(raw) > int(tail_bars):
        raw = raw.tail(int(tail_bars)).reset_index(drop=True)
    if signature[0] is not None:
        with _LIVE_KLINES_TAIL_CACHE_LOCK:
            _LIVE_KLINES_TAIL_CACHE[cache_key] = (
                signature,
                raw.copy(deep=False),
            )
    return raw


def ensure_live_trade_inputs_fresh(cfg) -> dict[str, object]:
    now_utc = datetime.now(timezone.utc)
    cycle = f"{int(cfg.cycle_minutes)}m"
    data_cfg = DataConfig.build(
        market=cfg.asset.slug,
        cycle=cycle,
        surface="live",
        root=cfg.layout.rewrite.root,
    )
    summaries = {
        "market_catalog": _ensure_live_market_catalog_fresh(data_cfg=data_cfg, now_utc=now_utc),
        "binance_primary": _ensure_live_binance_klines_fresh(data_cfg=data_cfg, now_utc=now_utc),
    }
    if cfg.asset.slug != "btc":
        btc_cfg = DataConfig.build(
            market="btc",
            cycle=cycle,
            surface="live",
            root=cfg.layout.rewrite.root,
        )
        summaries["binance_btc"] = _ensure_live_binance_klines_fresh(data_cfg=btc_cfg, now_utc=now_utc)
    return summaries


def _ensure_live_market_catalog_fresh(*, data_cfg: DataConfig, now_utc: datetime) -> dict[str, object]:
    target_path = data_cfg.layout.market_catalog_table_path
    max_age_seconds = _env_float(
        "PM15MIN_LIVE_MARKET_CATALOG_MAX_AGE_SECONDS",
        default=DEFAULT_LIVE_MARKET_CATALOG_MAX_AGE_SECONDS,
    )
    if _path_is_fresh(target_path=target_path, now_utc=now_utc, max_age_seconds=max_age_seconds):
        return {"status": "fresh", "path": str(target_path)}
    _refresh_if_needed(
        data_cfg=data_cfg,
        lock_name=f"market_catalog_{data_cfg.asset.slug}",
        target_path=target_path,
        now_utc=now_utc,
        max_age_seconds=max_age_seconds,
        refresh_fn=lambda: sync_market_catalog(
            data_cfg,
            start_ts=int((now_utc - timedelta(hours=DEFAULT_LIVE_MARKET_CATALOG_LOOKBACK_HOURS)).timestamp()),
            end_ts=int((now_utc + timedelta(hours=DEFAULT_LIVE_MARKET_CATALOG_LOOKAHEAD_HOURS)).timestamp()),
            now=now_utc,
        ),
    )
    _require_fresh_path(
        target_path=target_path,
        now_utc=now_utc,
        max_age_seconds=max_age_seconds,
        dataset_name=f"market_catalog:{data_cfg.asset.slug}",
    )
    return {"status": "refreshed", "path": str(target_path)}


def _ensure_live_binance_klines_fresh(*, data_cfg: DataConfig, now_utc: datetime) -> dict[str, object]:
    target_path = data_cfg.layout.binance_klines_path()
    max_age_seconds = _env_float(
        "PM15MIN_LIVE_BINANCE_MAX_AGE_SECONDS",
        default=DEFAULT_LIVE_BINANCE_KLINES_MAX_AGE_SECONDS,
    )
    if _path_is_fresh(target_path=target_path, now_utc=now_utc, max_age_seconds=max_age_seconds):
        return {"status": "fresh", "path": str(target_path)}
    _refresh_if_needed(
        data_cfg=data_cfg,
        lock_name=f"binance_klines_{data_cfg.asset.binance_symbol}",
        target_path=target_path,
        now_utc=now_utc,
        max_age_seconds=max_age_seconds,
        refresh_fn=lambda: sync_binance_klines_1m(
            data_cfg,
            symbol=data_cfg.asset.binance_symbol,
            now=now_utc,
            lookback_minutes=DEFAULT_LIVE_BINANCE_LOOKBACK_MINUTES,
            batch_limit=DEFAULT_LIVE_BINANCE_BATCH_LIMIT,
        ),
    )
    _require_fresh_path(
        target_path=target_path,
        now_utc=now_utc,
        max_age_seconds=max_age_seconds,
        dataset_name=f"binance_klines:{data_cfg.asset.binance_symbol}",
    )
    return {"status": "refreshed", "path": str(target_path)}


def _refresh_if_needed(
    *,
    data_cfg: DataConfig,
    lock_name: str,
    target_path: Path,
    now_utc: datetime,
    max_age_seconds: float,
    refresh_fn,
) -> None:
    lock_path, state_path = _refresh_coordination_paths(data_cfg=data_cfg, lock_name=lock_name)
    with _exclusive_lock(lock_path):
        if _path_is_fresh(target_path=target_path, now_utc=now_utc, max_age_seconds=max_age_seconds):
            return
        last_attempt_ts = _load_last_refresh_attempt(state_path)
        retry_seconds = _env_float(
            "PM15MIN_LIVE_TRADE_INPUT_REFRESH_RETRY_SECONDS",
            default=DEFAULT_LIVE_TRADE_INPUT_REFRESH_RETRY_SECONDS,
        )
        if (
            last_attempt_ts is not None
            and retry_seconds > 0.0
            and (now_utc - last_attempt_ts).total_seconds() < retry_seconds
        ):
            return
        _store_last_refresh_attempt(state_path=state_path, attempted_at=now_utc)
        refresh_fn()
        if _path_is_fresh(target_path=target_path, now_utc=now_utc, max_age_seconds=max_age_seconds):
            _store_last_refresh_attempt(state_path=state_path, attempted_at=now_utc, success=True)


def _refresh_coordination_paths(*, data_cfg: DataConfig, lock_name: str) -> tuple[Path, Path]:
    lock_dir = data_cfg.layout.surface_var_root / "locks" / "trade_inputs"
    lock_dir.mkdir(parents=True, exist_ok=True)
    return (
        lock_dir / f"{lock_name}.lock",
        lock_dir / f"{lock_name}.state.json",
    )


@contextmanager
def _exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _load_last_refresh_attempt(state_path: Path) -> datetime | None:
    if not state_path.exists():
        return None
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    raw = payload.get("last_attempt_ts")
    if raw in (None, ""):
        return None
    try:
        return pd.Timestamp(raw).to_pydatetime()
    except Exception:
        return None


def _store_last_refresh_attempt(*, state_path: Path, attempted_at: datetime, success: bool = False) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_attempt_ts": pd.Timestamp(attempted_at).isoformat(),
        "last_attempt_success": bool(success),
    }
    state_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _path_exists(path: Path) -> bool:
    return path.exists() and path.is_file()


def _path_is_fresh(*, target_path: Path, now_utc: datetime, max_age_seconds: float) -> bool:
    if not _path_exists(target_path):
        return False
    age = max(0.0, now_utc.timestamp() - target_path.stat().st_mtime)
    return age <= max(0.0, float(max_age_seconds))


def _require_fresh_path(*, target_path: Path, now_utc: datetime, max_age_seconds: float, dataset_name: str) -> None:
    if _path_is_fresh(target_path=target_path, now_utc=now_utc, max_age_seconds=max_age_seconds):
        return
    age_seconds = None
    if _path_exists(target_path):
        age_seconds = max(0.0, now_utc.timestamp() - target_path.stat().st_mtime)
    raise RuntimeError(
        f"live_trade_input_stale:{dataset_name}:path={target_path}:age_seconds={age_seconds}"
    )


def _env_float(name: str, *, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_int(name: str, *, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_int_list(name: str) -> tuple[int, ...]:
    raw = os.getenv(name)
    if raw in (None, ""):
        return tuple()
    values: list[int] = []
    for token in str(raw).split(","):
        text = token.strip()
        if not text:
            continue
        try:
            values.append(int(text))
        except Exception:
            continue
    return tuple(sorted(set(values)))


def load_live_account_context(cfg, *, utc_snapshot_label_fn) -> dict[str, object]:
    cache_ttl_seconds = _env_float("PM15MIN_LIVE_ACCOUNT_CONTEXT_CACHE_SEC", default=60.0)
    cache_key = (
        str(cfg.layout.rewrite.root),
        str(cfg.asset.slug),
    )
    open_orders_path = LiveStateLayout.discover(root=cfg.layout.rewrite.root).latest_open_orders_path(market=cfg.asset.slug)
    positions_path = LiveStateLayout.discover(root=cfg.layout.rewrite.root).latest_positions_path(market=cfg.asset.slug)
    signature = (
        _path_mtime_ns(open_orders_path),
        _path_mtime_ns(positions_path),
    )
    if cache_ttl_seconds > 0.0:
        cached = _load_cached_live_account_context(
            cache_key=cache_key,
            signature=signature,
        )
        if cached is not None:
            return cached
    open_orders = load_latest_open_orders_snapshot(
        rewrite_root=cfg.layout.rewrite.root,
        market=cfg.asset.slug,
    )
    positions = load_latest_positions_snapshot(
        rewrite_root=cfg.layout.rewrite.root,
        market=cfg.asset.slug,
    )
    open_orders_compact = _compact_live_open_orders_snapshot(open_orders)
    positions_compact = _compact_live_positions_snapshot(positions)
    payload = {
        "snapshot_ts": utc_snapshot_label_fn(),
        "open_orders": open_orders_compact,
        "positions": positions_compact,
    }
    payload["summary"] = summarize_account_state_payload(payload, include_heavy_fields=False)
    resolved = {
        **payload,
    }
    if cache_ttl_seconds > 0.0:
        _store_cached_live_account_context(
            cache_key=cache_key,
            cache_ttl_seconds=cache_ttl_seconds,
            signature=signature,
            payload=resolved,
        )
    return resolved


def _load_cached_live_account_context(
    *,
    cache_key: tuple[str, str],
    signature: tuple[int | None, int | None],
) -> dict[str, object] | None:
    now_monotonic = time.monotonic()
    with _LIVE_ACCOUNT_CONTEXT_CACHE_LOCK:
        cached = _LIVE_ACCOUNT_CONTEXT_CACHE.get(cache_key)
        if cached is None:
            return None
        expires_at, cached_signature, payload = cached
        if now_monotonic >= float(expires_at) or cached_signature != signature:
            _LIVE_ACCOUNT_CONTEXT_CACHE.pop(cache_key, None)
            return None
        return dict(payload)


def _store_cached_live_account_context(
    *,
    cache_key: tuple[str, str],
    cache_ttl_seconds: float,
    signature: tuple[int | None, int | None],
    payload: dict[str, object],
) -> None:
    with _LIVE_ACCOUNT_CONTEXT_CACHE_LOCK:
        _LIVE_ACCOUNT_CONTEXT_CACHE[cache_key] = (
            time.monotonic() + max(0.0, float(cache_ttl_seconds)),
            signature,
            dict(payload),
        )


def _path_mtime_ns(path: Path) -> int | None:
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return None
    except Exception:
        return None


def _path_signature(path: Path) -> tuple[int | None, int | None]:
    try:
        stat = path.stat()
        return (int(stat.st_mtime_ns), int(stat.st_size))
    except FileNotFoundError:
        return (None, None)
    except Exception:
        return (None, None)


def _compact_live_open_orders_snapshot(payload: dict[str, object] | None) -> dict[str, object] | None:
    if not isinstance(payload, dict):
        return None
    open_orders_summary = summarize_open_orders_snapshot(payload)
    return {
        "status": payload.get("status"),
        "reason": payload.get("reason"),
        "snapshot_ts": payload.get("snapshot_ts"),
        "summary": open_orders_summary if isinstance(open_orders_summary, dict) else None,
    }


def _compact_live_positions_snapshot(payload: dict[str, object] | None) -> dict[str, object] | None:
    if not isinstance(payload, dict):
        return None
    positions_summary = summarize_positions_snapshot(payload, include_heavy_fields=False)
    return {
        "status": payload.get("status"),
        "reason": payload.get("reason"),
        "snapshot_ts": payload.get("snapshot_ts"),
        "cash_balance_usd": payload.get("cash_balance_usd"),
        "cash_balance_status": payload.get("cash_balance_status"),
        "summary": positions_summary if isinstance(positions_summary, dict) else None,
    }


def feature_coverage(
    *,
    available_columns: set[str],
    required_columns: list[str],
    blacklisted_columns: list[str],
    not_allowed_blacklist_columns: list[str],
    nan_feature_columns: list[str],
) -> dict[str, object]:
    blacklisted = [column for column in required_columns if column in set(blacklisted_columns)]
    present = [column for column in required_columns if column in available_columns]
    missing = [column for column in required_columns if column not in available_columns]
    effective_missing = [column for column in missing if column not in set(blacklisted)]
    nan_columns = sorted({str(column) for column in nan_feature_columns if str(column)})
    return {
        "required_feature_count": len(required_columns),
        "present_feature_count": len(present),
        "missing_feature_count": len(missing),
        "effective_missing_feature_count": len(effective_missing),
        "coverage_ratio": float(len(present) / len(required_columns)) if required_columns else 1.0,
        "present_columns": present,
        "missing_columns": missing,
        "blacklisted_columns": blacklisted,
        "not_allowed_blacklist_columns": list(not_allowed_blacklist_columns),
        "not_allowed_blacklist_count": len(not_allowed_blacklist_columns),
        "effective_missing_columns": effective_missing,
        "nan_feature_columns": nan_columns,
        "nan_feature_count": len(nan_columns),
    }


def resolve_live_blacklist(
    *,
    profile_blacklist: list[str],
    bundle_allowed_blacklist: list[str],
) -> tuple[list[str], list[str]]:
    profile_set = {str(column) for column in profile_blacklist}
    allowed_set = {str(column) for column in bundle_allowed_blacklist}
    effective = sorted(profile_set & allowed_set)
    not_allowed = sorted(profile_set - allowed_set)
    return effective, not_allowed


def apply_live_blacklist(features: pd.DataFrame, *, blacklist_columns: list[str]) -> None:
    if not blacklist_columns:
        return
    masked_cols = [column for column in blacklist_columns if column in features.columns]
    if masked_cols:
        features.loc[:, masked_cols] = 0.0


def latest_nan_feature_columns(
    *,
    features: pd.DataFrame,
    offset: int,
    decision_ts,
    required_columns: list[str],
) -> list[str]:
    rows = features[pd.to_numeric(features.get("offset"), errors="coerce") == int(offset)].copy()
    if rows.empty:
        return []
    if decision_ts is not None:
        target_ts = pd.to_datetime(decision_ts, utc=True, errors="coerce")
        if not pd.isna(target_ts):
            rows = rows[pd.to_datetime(rows["decision_ts"], utc=True, errors="coerce") == target_ts]
    if rows.empty:
        return []
    row = rows.sort_values("decision_ts").tail(1)
    nan_columns: list[str] = []
    for column in required_columns:
        if column not in row.columns:
            continue
        series = pd.to_numeric(row[column], errors="coerce").replace([float("inf"), float("-inf")], pd.NA)
        if bool(series.isna().any()):
            nan_columns.append(str(column))
    return sorted(set(nan_columns))


def persist_live_signal_snapshot(
    cfg,
    *,
    target: str,
    snapshot_ts: str,
    payload: dict[str, object],
) -> dict[str, Path]:
    layout = LiveStateLayout.discover(root=cfg.layout.rewrite.root)
    latest_path = layout.latest_signal_path(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        profile=cfg.profile,
        target=target,
    )
    snapshot_path = layout.signal_snapshot_path(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        profile=cfg.profile,
        target=target,
        snapshot_ts=snapshot_ts,
    )
    return write_live_payload_pair(
        payload=payload,
        latest_path=latest_path,
        snapshot_path=snapshot_path,
        write_snapshot_history=False,
    )


def extract_feature_snapshot(features: pd.DataFrame, *, offset: int, decision_ts) -> dict[str, object]:
    rows = features[pd.to_numeric(features.get("offset"), errors="coerce") == int(offset)].copy()
    if rows.empty:
        return {}
    if decision_ts is not None:
        target_ts = pd.to_datetime(decision_ts, utc=True, errors="coerce")
        if not pd.isna(target_ts):
            rows = rows[pd.to_datetime(rows["decision_ts"], utc=True, errors="coerce") == target_ts]
    if rows.empty:
        return {}
    row = rows.sort_values("decision_ts").tail(1).iloc[0]
    snapshot: dict[str, object] = {}
    excluded_columns = {"decision_ts", "cycle_start_ts", "cycle_end_ts", "offset"}
    for column in rows.columns:
        if column in excluded_columns:
            continue
        value = row.get(column)
        if pd.isna(value):
            snapshot[column] = None
            continue
        if isinstance(value, bool):
            snapshot[column] = bool(value)
            continue
        numeric_value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(numeric_value):
            continue
        float_value = float(numeric_value)
        snapshot[column] = int(float_value) if float_value.is_integer() else float_value
    return snapshot


def iso_or_none(value) -> str | None:
    if value is None:
        return None
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.isoformat()
