from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
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
from pm15min.data.sources.binance_spot import BinanceSpotKlinesClient, BinanceSpotKlinesRequest
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

@dataclass
class LiveFeatureState:
    source_signature: tuple[object, ...]
    features: pd.DataFrame
    build_tail_bars: int
    build_mode: str


class LiveClosedBarNotReadyError(RuntimeError):
    """Raised when live feature refresh would otherwise compute on stale 1m inputs."""


_LIVE_FEATURE_FRAME_CACHE: dict[
    tuple[str, str, int, str, tuple[int, ...], int, tuple[str, ...]],
    tuple[float, tuple[object, ...], pd.DataFrame],
] = {}
_LIVE_FEATURE_FRAME_CACHE_LOCK = threading.Lock()
_LIVE_FEATURE_STATE_CACHE: dict[
    tuple[str, str, int, str, tuple[int, ...], int, tuple[str, ...]],
    LiveFeatureState,
] = {}
_LIVE_FEATURE_STATE_CACHE_LOCK = threading.Lock()
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
    allow_preview_open_bar: bool = False,
    required_feature_columns: set[str] | None = None,
) -> pd.DataFrame:
    total_started = time.perf_counter()
    cache_ttl_seconds = _env_float("PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC", default=0.0)
    resolved_retain_offsets = tuple(retain_offsets or _env_int_list("PM15MIN_LIVE_FEATURE_RETAIN_OFFSETS"))
    tail_cycles = _env_int("PM15MIN_LIVE_FEATURE_TAIL_CYCLES", default=2)
    selected_required_feature_columns = _resolve_live_feature_columns_request(
        feature_set=feature_set,
        required_feature_columns=required_feature_columns,
    )
    cache_key = _live_feature_cache_key(
        rewrite_root=cfg.layout.rewrite.root,
        market=cfg.asset.slug,
        cycle_minutes=int(cfg.cycle_minutes),
        feature_set=feature_set,
        retain_offsets=resolved_retain_offsets,
        tail_cycles=int(tail_cycles),
        required_feature_columns=selected_required_feature_columns,
    )
    data_cfg = DataConfig.build(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        surface="live",
        root=cfg.layout.rewrite.root,
    )
    timings_ms: dict[str, float | bool] = {}
    refresh_started = time.perf_counter()
    ensure_live_trade_inputs_fresh(cfg)
    timings_ms["trade_inputs_refresh_stage_ms"] = _elapsed_ms(refresh_started)
    btc_cfg = None
    if cfg.asset.slug != "btc":
        btc_cfg = DataConfig.build(
            market="btc",
            cycle=f"{int(cfg.cycle_minutes)}m",
            surface="live",
            root=cfg.layout.rewrite.root,
        )
    now_utc = pd.Timestamp.now(tz="UTC")
    if not allow_preview_open_bar:
        closed_bar_wait_started = time.perf_counter()
        _ensure_live_closed_bar_inputs_ready(
            data_cfg=data_cfg,
            btc_cfg=btc_cfg,
            now_utc=now_utc,
            cycle_minutes=int(cfg.cycle_minutes),
            retain_offsets=resolved_retain_offsets,
        )
        timings_ms["closed_bar_wait_stage_ms"] = _elapsed_ms(closed_bar_wait_started)
    raw_tail_bars = _live_feature_build_tail_bars(cycle_minutes=int(cfg.cycle_minutes), tail_cycles=tail_cycles)
    primary_kline_started = time.perf_counter()
    raw_klines = _load_live_binance_klines_tail(
        data_cfg=data_cfg,
        tail_bars=raw_tail_bars,
        allow_preview_open_bar=allow_preview_open_bar,
        now_utc=now_utc,
    )
    timings_ms["primary_kline_load_stage_ms"] = _elapsed_ms(primary_kline_started)
    oracle_started = time.perf_counter()
    oracle_prices = build_live_runtime_oracle_prices(
        data_cfg=data_cfg,
        market_slug=cfg.asset.slug,
        raw_klines=raw_klines,
        oracle_prices_table=load_oracle_prices_table(data_cfg),
    )
    timings_ms["oracle_stage_ms"] = _elapsed_ms(oracle_started)
    btc_klines = None
    if btc_cfg is not None:
        btc_kline_started = time.perf_counter()
        btc_klines = _load_live_binance_klines_tail(
            data_cfg=btc_cfg,
            symbol="BTCUSDT",
            tail_bars=raw_tail_bars,
            allow_preview_open_bar=allow_preview_open_bar,
            now_utc=now_utc,
        )
        timings_ms["btc_kline_load_stage_ms"] = _elapsed_ms(btc_kline_started)
    source_signature = _live_feature_source_signature(
        raw_klines=raw_klines,
        btc_klines=btc_klines,
        oracle_prices=oracle_prices,
    )
    state_cached = _load_live_feature_state(
        cache_key=cache_key,
        source_signature=source_signature,
    )
    if state_cached is not None:
        _attach_live_feature_timings(
            state_cached,
            timings_ms={
                **timings_ms,
                "feature_frame_cache_hit": True,
                "feature_frame_total_stage_ms": _elapsed_ms(total_started),
            },
        )
        return state_cached
    if cache_ttl_seconds > 0.0:
        cached = _load_cached_live_feature_frame(
            cache_key=cache_key,
            source_signature=source_signature,
        )
        if cached is not None:
            _store_live_feature_state(
                cache_key=cache_key,
                source_signature=source_signature,
                features=cached,
                build_tail_bars=int(raw_tail_bars),
                build_mode="ttl_cache_hit",
            )
            _attach_live_feature_timings(
                cached,
                timings_ms={
                    **timings_ms,
                    "feature_frame_cache_hit": True,
                    "feature_frame_total_stage_ms": _elapsed_ms(total_started),
                },
            )
            return cached
    build_tail_bars = int(raw_tail_bars)
    build_mode = "full_tail"
    prior_state = _load_any_live_feature_state(cache_key=cache_key)
    if prior_state is not None and _can_incrementally_refresh_feature_state(
        prior_state=prior_state,
        source_signature=source_signature,
    ):
        build_tail_bars = _live_feature_incremental_build_tail_bars(
            cycle_minutes=int(cfg.cycle_minutes),
            tail_cycles=tail_cycles,
            full_tail_bars=int(raw_tail_bars),
        )
        build_mode = "incremental_tail"
    builder_raw_klines = _slice_feature_builder_tail(raw_klines, tail_bars=build_tail_bars)
    builder_btc_klines = _slice_feature_builder_tail(btc_klines, tail_bars=build_tail_bars)
    builder_started = time.perf_counter()
    builder_kwargs = {
        "feature_set": feature_set,
        "oracle_prices": oracle_prices,
        "btc_klines": builder_btc_klines,
        "cycle": f"{int(cfg.cycle_minutes)}m",
    }
    if selected_required_feature_columns is not None:
        builder_kwargs["requested_columns"] = selected_required_feature_columns
    try:
        features = build_feature_frame_df(
            builder_raw_klines,
            **builder_kwargs,
        )
    except TypeError as exc:
        if "requested_columns" not in builder_kwargs or "requested_columns" not in str(exc):
            raise
        builder_kwargs.pop("requested_columns", None)
        features = build_feature_frame_df(
            builder_raw_klines,
            **builder_kwargs,
        )
    timings_ms["builder_call_stage_ms"] = _elapsed_ms(builder_started)
    trim_started = time.perf_counter()
    features = _trim_live_feature_frame(
        features,
        cycle_minutes=int(cfg.cycle_minutes),
        retain_offsets=resolved_retain_offsets,
        tail_cycles=tail_cycles,
    )
    timings_ms["trim_stage_ms"] = _elapsed_ms(trim_started)
    builder_timings = {
        f"builder_{key}": value
        for key, value in dict(getattr(features, "attrs", {}).get("timings_ms") or {}).items()
    }
    _attach_live_feature_timings(
        features,
        timings_ms={
            **timings_ms,
            **builder_timings,
            "feature_frame_cache_hit": False,
            "feature_frame_total_stage_ms": _elapsed_ms(total_started),
        },
    )
    _store_live_feature_state(
        cache_key=cache_key,
        source_signature=source_signature,
        features=features,
        build_tail_bars=build_tail_bars,
        build_mode=build_mode,
    )
    if cache_ttl_seconds > 0.0:
        _store_cached_live_feature_frame(
            cache_key=cache_key,
            cache_ttl_seconds=cache_ttl_seconds,
            source_signature=source_signature,
            features=features,
        )
    return features


def _attach_live_feature_timings(features: pd.DataFrame, *, timings_ms: dict[str, object]) -> None:
    attrs = dict(getattr(features, "attrs", {}) or {})
    attrs["timings_ms"] = {
        str(key): value
        for key, value in timings_ms.items()
        if value is not None
    }
    features.attrs = attrs


def _load_cached_live_feature_frame(
    *,
    cache_key: tuple[str, str, int, str, tuple[int, ...], int, tuple[str, ...]],
    source_signature: tuple[object, ...],
) -> pd.DataFrame | None:
    now_monotonic = time.monotonic()
    with _LIVE_FEATURE_FRAME_CACHE_LOCK:
        cached = _LIVE_FEATURE_FRAME_CACHE.get(cache_key)
        if cached is None:
            return None
        expires_at, cached_source_signature, frame = cached
        if now_monotonic >= float(expires_at) or cached_source_signature != source_signature:
            _LIVE_FEATURE_FRAME_CACHE.pop(cache_key, None)
            return None
        return frame.copy(deep=False)


def _store_cached_live_feature_frame(
    *,
    cache_key: tuple[str, str, int, str, tuple[int, ...], int, tuple[str, ...]],
    cache_ttl_seconds: float,
    source_signature: tuple[object, ...],
    features: pd.DataFrame,
) -> None:
    with _LIVE_FEATURE_FRAME_CACHE_LOCK:
        _LIVE_FEATURE_FRAME_CACHE[cache_key] = (
            time.monotonic() + max(0.0, float(cache_ttl_seconds)),
            tuple(source_signature),
            features.copy(deep=False),
        )


def _live_feature_source_signature(
    *,
    raw_klines: pd.DataFrame,
    btc_klines: pd.DataFrame | None,
    oracle_prices: pd.DataFrame,
) -> tuple[object, ...]:
    return (
        _logical_frame_signature(raw_klines, key_columns=("open_time",), value_columns=("close",)),
        _logical_frame_signature(btc_klines, key_columns=("open_time",), value_columns=("close",))
        if isinstance(btc_klines, pd.DataFrame)
        else None,
        _logical_frame_signature(
            oracle_prices,
            key_columns=("cycle_start_ts", "cycle_end_ts"),
            value_columns=("price_to_beat", "final_price"),
        ),
    )


def _logical_frame_signature(
    frame: pd.DataFrame | None,
    *,
    key_columns: tuple[str, ...],
    value_columns: tuple[str, ...],
) -> tuple[object, ...]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return (0, None)
    selected_columns = [column for column in [*key_columns, *value_columns] if column in frame.columns]
    if not selected_columns:
        return (int(len(frame)), tuple())
    subset = frame.loc[:, selected_columns].copy()
    row_hashes = pd.util.hash_pandas_object(subset, index=False)
    digest = hashlib.blake2b(
        row_hashes.to_numpy(dtype="uint64", copy=False).tobytes(),
        digest_size=16,
    ).hexdigest()
    return (int(len(frame)), tuple(selected_columns), digest)


def _normalize_signature_value(value: object) -> object:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.tz_convert("UTC").isoformat() if value.tzinfo is not None else value.isoformat()
    if isinstance(value, datetime):
        timestamp = pd.Timestamp(value)
        return timestamp.tz_convert("UTC").isoformat() if timestamp.tzinfo is not None else timestamp.isoformat()
    if isinstance(value, (int, str, bool)):
        return value
    if isinstance(value, float):
        return round(float(value), 10)
    return str(value)


def _load_live_feature_state(
    *,
    cache_key: tuple[str, str, int, str, tuple[int, ...], int, tuple[str, ...]],
    source_signature: tuple[object, ...],
) -> pd.DataFrame | None:
    with _LIVE_FEATURE_STATE_CACHE_LOCK:
        state = _LIVE_FEATURE_STATE_CACHE.get(cache_key)
        if state is None or state.source_signature != source_signature:
            return None
        return state.features.copy(deep=False)


def _load_any_live_feature_state(
    *,
    cache_key: tuple[str, str, int, str, tuple[int, ...], int, tuple[str, ...]],
) -> LiveFeatureState | None:
    with _LIVE_FEATURE_STATE_CACHE_LOCK:
        state = _LIVE_FEATURE_STATE_CACHE.get(cache_key)
        if state is None:
            return None
        return LiveFeatureState(
            source_signature=tuple(state.source_signature),
            features=state.features.copy(deep=False),
            build_tail_bars=int(state.build_tail_bars),
            build_mode=str(state.build_mode),
        )


def _store_live_feature_state(
    *,
    cache_key: tuple[str, str, int, str, tuple[int, ...], int, tuple[str, ...]],
    source_signature: tuple[object, ...],
    features: pd.DataFrame,
    build_tail_bars: int,
    build_mode: str,
) -> None:
    with _LIVE_FEATURE_STATE_CACHE_LOCK:
        _LIVE_FEATURE_STATE_CACHE[cache_key] = LiveFeatureState(
            source_signature=tuple(source_signature),
            features=features.copy(deep=False),
            build_tail_bars=int(build_tail_bars),
            build_mode=str(build_mode),
        )


def _resolve_live_feature_columns_request(
    *,
    feature_set: str,
    required_feature_columns: set[str] | None,
) -> set[str] | None:
    if required_feature_columns is not None:
        return {str(column) for column in required_feature_columns}
    if str(feature_set).strip().lower() == "v6_user_core":
        return {
            *feature_set_columns("v6_user_core"),
            *feature_set_columns("bs_q_replace_direction"),
        }
    return None


def _live_feature_cache_key(
    *,
    rewrite_root: Path,
    market: str,
    cycle_minutes: int,
    feature_set: str,
    retain_offsets: tuple[int, ...],
    tail_cycles: int,
    required_feature_columns: set[str] | None,
) -> tuple[str, str, int, str, tuple[int, ...], int, tuple[str, ...]]:
    required_columns_key = ()
    if required_feature_columns is not None:
        required_columns_key = tuple(sorted(str(column) for column in required_feature_columns))
    return (
        str(rewrite_root),
        str(market),
        int(cycle_minutes),
        str(feature_set),
        tuple(retain_offsets),
        int(tail_cycles),
        required_columns_key,
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


def _live_feature_incremental_build_tail_bars(
    *,
    cycle_minutes: int,
    tail_cycles: int,
    full_tail_bars: int,
) -> int:
    requested = _env_int("PM15MIN_LIVE_FEATURE_INCREMENTAL_BUILD_TAIL_BARS", default=320)
    minimum = max(260, int(cycle_minutes) * max(4, int(tail_cycles) + 2))
    return max(int(min(full_tail_bars, requested)), int(minimum))


def _slice_feature_builder_tail(frame: pd.DataFrame | None, *, tail_bars: int) -> pd.DataFrame | None:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame
    limit = max(1, int(tail_bars))
    if len(frame) <= limit:
        return frame
    return frame.tail(limit).reset_index(drop=True)


def _can_incrementally_refresh_feature_state(
    *,
    prior_state: LiveFeatureState,
    source_signature: tuple[object, ...],
) -> bool:
    return tuple(prior_state.source_signature) != tuple(source_signature)


def _load_live_binance_klines_tail(
    *,
    data_cfg: DataConfig,
    symbol: str | None = None,
    tail_bars: int,
    allow_preview_open_bar: bool = False,
    now_utc: pd.Timestamp | None = None,
) -> pd.DataFrame:
    path = data_cfg.layout.binance_klines_path(symbol=symbol)
    cache_key = (str(path), str(symbol or data_cfg.asset.binance_symbol))
    signature = _path_signature(path)
    cached_frame: pd.DataFrame | None = None
    stale_cached_frame: pd.DataFrame | None = None
    if signature[0] is not None:
        with _LIVE_KLINES_TAIL_CACHE_LOCK:
            cached = _LIVE_KLINES_TAIL_CACHE.get(cache_key)
            if cached is not None:
                stale_cached_frame = cached[1].copy(deep=False)
                if cached[0] == signature:
                    cached_frame = stale_cached_frame
    marker_frame = _load_live_binance_latest_tail_marker(
        data_cfg=data_cfg,
        symbol=symbol,
    )
    base_frame: pd.DataFrame | None = None
    merge_source = stale_cached_frame if isinstance(stale_cached_frame, pd.DataFrame) and not stale_cached_frame.empty else cached_frame
    if isinstance(merge_source, pd.DataFrame) and not merge_source.empty and isinstance(marker_frame, pd.DataFrame):
        merged = _merge_cached_kline_tail_with_marker(
            cached_frame=merge_source,
            marker_frame=marker_frame,
            tail_bars=tail_bars,
        )
        if merged is not None:
            if signature[0] is not None:
                with _LIVE_KLINES_TAIL_CACHE_LOCK:
                    _LIVE_KLINES_TAIL_CACHE[cache_key] = (
                        signature,
                        merged.copy(deep=False),
                    )
            base_frame = merged
    if base_frame is None and isinstance(cached_frame, pd.DataFrame) and not cached_frame.empty:
        base_frame = cached_frame
    if base_frame is None:
        raw = load_binance_klines_1m(data_cfg, symbol=symbol)
        if isinstance(raw, pd.DataFrame) and not raw.empty and int(tail_bars) > 0 and len(raw) > int(tail_bars):
            raw = raw.tail(int(tail_bars)).reset_index(drop=True)
        base_frame = raw
    if signature[0] is not None:
        with _LIVE_KLINES_TAIL_CACHE_LOCK:
            _LIVE_KLINES_TAIL_CACHE[cache_key] = (
                signature,
                base_frame.copy(deep=False),
            )
    if not allow_preview_open_bar:
        return base_frame
    preview_frame = _load_live_binance_preview_tail(
        data_cfg=data_cfg,
        symbol=symbol,
        now_utc=now_utc,
    )
    merged_preview = _merge_cached_kline_tail_with_marker(
        cached_frame=base_frame,
        marker_frame=preview_frame,
        tail_bars=tail_bars,
    )
    return merged_preview if merged_preview is not None else base_frame


def _load_live_binance_latest_tail_marker(
    *,
    data_cfg: DataConfig,
    symbol: str | None,
) -> pd.DataFrame | None:
    path = _live_binance_latest_tail_path(data_cfg=data_cfg, symbol=symbol)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    rows = payload.get("tail_rows")
    if not isinstance(rows, list) or not rows:
        return None
    frame = pd.DataFrame(rows)
    if frame.empty:
        return None
    if "open_time" in frame.columns:
        frame["open_time"] = pd.to_datetime(frame["open_time"], utc=True, errors="coerce")
    if "close_time" in frame.columns:
        frame["close_time"] = pd.to_datetime(frame["close_time"], utc=True, errors="coerce")
    for column in (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_quote_volume",
        "number_of_trades",
    ):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["open_time", "close"]).sort_values("open_time").drop_duplicates(subset=["open_time"], keep="last")
    if frame.empty:
        return None
    return frame.reset_index(drop=True)


def _load_live_binance_preview_tail(
    *,
    data_cfg: DataConfig,
    symbol: str | None,
    now_utc: pd.Timestamp | None,
) -> pd.DataFrame | None:
    resolved_now = pd.to_datetime(now_utc, utc=True, errors="coerce")
    if resolved_now is None or pd.isna(resolved_now):
        resolved_now = pd.Timestamp.now(tz="UTC")
    resolved_symbol = str(symbol or data_cfg.asset.binance_symbol).strip().upper()
    minute_floor = resolved_now.floor("min")
    start_time = minute_floor - pd.Timedelta(minutes=2)
    client = BinanceSpotKlinesClient(timeout_sec=max(2.0, data_cfg.orderbook_timeout_sec * 4.0))
    frame = client.fetch_klines(
        BinanceSpotKlinesRequest(
            symbol=resolved_symbol,
            interval="1m",
            start_time_ms=int(start_time.timestamp() * 1000),
            end_time_ms=int(resolved_now.timestamp() * 1000),
            limit=5,
        )
    )
    preview = _normalize_preview_kline_frame(frame=frame, now_utc=resolved_now)
    if preview.empty:
        return None
    return preview.tail(2).reset_index(drop=True)


def _normalize_preview_kline_frame(*, frame: pd.DataFrame, now_utc: pd.Timestamp) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    out = frame.copy()
    out["open_time"] = pd.to_datetime(pd.to_numeric(out.get("open_time"), errors="coerce"), unit="ms", utc=True)
    out["close_time"] = pd.to_datetime(pd.to_numeric(out.get("close_time"), errors="coerce"), unit="ms", utc=True)
    for column in (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_quote_volume",
        "number_of_trades",
    ):
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    out = out.dropna(subset=["open_time", "close"]).copy()
    out = out.loc[out["open_time"] <= now_utc.floor("min")].copy()
    if out.empty:
        return pd.DataFrame()
    out = out.sort_values("open_time").drop_duplicates(subset=["open_time"], keep="last").reset_index(drop=True)
    return out


def _read_live_binance_latest_open_time(
    *,
    path: Path,
) -> pd.Timestamp | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    latest_open_time = pd.to_datetime(payload.get("latest_open_time"), utc=True, errors="coerce")
    if latest_open_time is not None and not pd.isna(latest_open_time):
        return latest_open_time
    rows = payload.get("tail_rows")
    if not isinstance(rows, list) or not rows:
        return None
    frame = pd.DataFrame(rows)
    if frame.empty or "open_time" not in frame.columns:
        return None
    open_time = pd.to_datetime(frame["open_time"], utc=True, errors="coerce").dropna()
    if open_time.empty:
        return None
    return open_time.max()


def _expected_latest_closed_bar_open_time(*, now_utc: pd.Timestamp) -> pd.Timestamp:
    normalized = pd.to_datetime(now_utc, utc=True, errors="coerce")
    if normalized is None or pd.isna(normalized):
        normalized = pd.Timestamp.now(tz="UTC")
    return normalized.floor("min") - pd.Timedelta(minutes=1)


def _expected_boundary_closed_bar_open_time(
    *,
    now_utc: pd.Timestamp,
    cycle_minutes: int,
    retain_offsets: tuple[int, ...],
) -> pd.Timestamp | None:
    if int(cycle_minutes) <= 0:
        return None
    offsets = tuple(sorted({int(offset) for offset in retain_offsets if 0 <= int(offset) < int(cycle_minutes)}))
    if not offsets:
        return None
    normalized = pd.to_datetime(now_utc, utc=True, errors="coerce")
    if normalized is None or pd.isna(normalized):
        normalized = pd.Timestamp.now(tz="UTC")
    minute_floor = normalized.floor("min")
    cycle_start = minute_floor.floor(f"{int(cycle_minutes)}min")
    minute_offset = int((minute_floor - cycle_start).total_seconds() // 60)
    boundary_minutes = {int(offset) for offset in offsets}
    if minute_offset not in boundary_minutes:
        return None
    return minute_floor - pd.Timedelta(minutes=1)


def _ensure_live_closed_bar_inputs_ready(
    *,
    data_cfg: DataConfig,
    btc_cfg: DataConfig | None,
    now_utc: pd.Timestamp,
    cycle_minutes: int,
    retain_offsets: tuple[int, ...],
) -> None:
    if not _env_bool("PM15MIN_LIVE_ENFORCE_EXPECTED_CLOSED_BAR", default=False):
        return
    expected_open_time = _expected_boundary_closed_bar_open_time(
        now_utc=now_utc,
        cycle_minutes=int(cycle_minutes),
        retain_offsets=retain_offsets,
    )
    if expected_open_time is None:
        return
    wait_sec = max(0.0, _env_float("PM15MIN_LIVE_EXPECTED_CLOSED_BAR_WAIT_SEC", default=1.5))
    poll_sec = max(0.01, _env_float("PM15MIN_LIVE_EXPECTED_CLOSED_BAR_POLL_SEC", default=0.05))
    checks: list[tuple[str, Path]] = [
        (
            str(data_cfg.asset.binance_symbol).strip().upper(),
            _live_binance_latest_tail_path(data_cfg=data_cfg, symbol=data_cfg.asset.binance_symbol),
        ),
    ]
    if btc_cfg is not None:
        checks.append(
            (
                "BTCUSDT",
                _live_binance_latest_tail_path(data_cfg=btc_cfg, symbol="BTCUSDT"),
            )
        )
    deadline = time.monotonic() + wait_sec
    latest_by_symbol: dict[str, pd.Timestamp | None] = {}
    while True:
        stale_symbols: list[str] = []
        for symbol, path in checks:
            latest_open_time = _read_live_binance_latest_open_time(path=path)
            latest_by_symbol[symbol] = latest_open_time
            if latest_open_time is None or latest_open_time < expected_open_time:
                stale_symbols.append(symbol)
        if not stale_symbols:
            return
        if time.monotonic() >= deadline:
            observed = ", ".join(
                f"{symbol}={None if latest_by_symbol.get(symbol) is None else latest_by_symbol[symbol].isoformat()}"
                for symbol, _ in checks
            )
            raise LiveClosedBarNotReadyError(
                "live_closed_bar_not_ready "
                f"expected_open_time={expected_open_time.isoformat()} "
                f"stale_symbols={','.join(stale_symbols)} "
                f"observed={observed}"
            )
        time.sleep(poll_sec)


def _live_binance_latest_tail_path(*, data_cfg: DataConfig, symbol: str | None) -> Path:
    resolved_symbol = str(symbol or data_cfg.asset.binance_symbol).strip().upper()
    return (
        data_cfg.layout.surface_var_root
        / "state"
        / "binance_klines_1m"
        / f"symbol={resolved_symbol}"
        / "latest_tail.json"
    )


def _merge_cached_kline_tail_with_marker(
    *,
    cached_frame: pd.DataFrame,
    marker_frame: pd.DataFrame,
    tail_bars: int,
) -> pd.DataFrame | None:
    cached = _normalize_kline_tail_frame(cached_frame)
    marker = _normalize_kline_tail_frame(marker_frame)
    if cached.empty or marker.empty:
        return None
    cached_last_open = pd.to_datetime(cached["open_time"], utc=True, errors="coerce").max()
    marker_last_open = pd.to_datetime(marker["open_time"], utc=True, errors="coerce").max()
    if pd.isna(cached_last_open) or pd.isna(marker_last_open):
        return None
    if marker_last_open <= cached_last_open:
        return None
    expected_next_open = cached_last_open + pd.Timedelta(minutes=1)
    marker_after_cached = marker.loc[pd.to_datetime(marker["open_time"], utc=True, errors="coerce") >= expected_next_open].copy()
    if marker_after_cached.empty:
        return None
    earliest_open = pd.to_datetime(marker_after_cached["open_time"], utc=True, errors="coerce").min()
    if earliest_open > expected_next_open:
        return None
    if not _kline_open_times_are_contiguous(marker_after_cached["open_time"]):
        return None
    combined = (
        pd.concat([cached, marker_after_cached], ignore_index=True, sort=False)
        .sort_values("open_time")
        .drop_duplicates(subset=["open_time"], keep="last")
        .reset_index(drop=True)
    )
    if int(tail_bars) > 0 and len(combined) > int(tail_bars):
        combined = combined.tail(int(tail_bars)).reset_index(drop=True)
    return combined


def _normalize_kline_tail_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    out = frame.copy()
    out["open_time"] = pd.to_datetime(out.get("open_time"), utc=True, errors="coerce")
    if "close_time" in out.columns:
        out["close_time"] = pd.to_datetime(out.get("close_time"), utc=True, errors="coerce")
    out = out.dropna(subset=["open_time"]).sort_values("open_time").drop_duplicates(subset=["open_time"], keep="last")
    return out.reset_index(drop=True)


def _kline_open_times_are_contiguous(series: pd.Series) -> bool:
    open_times = pd.to_datetime(series, utc=True, errors="coerce").dropna().sort_values()
    if open_times.empty:
        return False
    diffs = open_times.diff().dropna()
    if diffs.empty:
        return True
    return bool((diffs == pd.Timedelta(minutes=1)).all())


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


def _env_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw in (None, ""):
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


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


def _elapsed_ms(started_at: float) -> float:
    return round(max(0.0, (time.perf_counter() - float(started_at)) * 1000.0), 3)
