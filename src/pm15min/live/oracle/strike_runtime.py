from __future__ import annotations

import asyncio
from dataclasses import dataclass
from functools import lru_cache
import importlib
import os
from pathlib import Path
import threading
import time

import pandas as pd

from pm15min.data import DataConfig
from pm15min.data.queries.loaders import load_streams_source
from pm15min.data.sources.polymarket_oracle_api import PolymarketOracleApiClient
from pm15min.live.oracle.strike_cache import StrikeCacheRecord

DEFAULT_POLYMARKET_SYMBOL: dict[str, str] = {
    "btc": "BTC",
    "eth": "ETH",
    "sol": "SOL",
    "xrp": "XRP",
}
DEFAULT_CHAINLINK_SYMBOL: dict[str, str] = {
    "btc": "btc/usd",
    "eth": "eth/usd",
    "sol": "sol/usd",
    "xrp": "xrp/usd",
}
_EXACT_CACHE_SOURCES = {"polymarket_open_price_api", "streams_parquet"}
_STREAMS_REFRESH_SECONDS = 60.0
_OPEN_PRICE_MIN_REFRESH_SECONDS = 60.0
_OPEN_PRICE_EMPTY_RETRY_MAX_ATTEMPTS = 1
_OPEN_PRICE_EMPTY_RETRY_BASE_SECONDS = 0.15
_OPEN_PRICE_EMPTY_RETRY_MAX_SECONDS = 1.2
_OPEN_PRICE_EMPTY_RETRY_MULTIPLIER = 2.0
_DEFAULT_RTDS_MAX_SKEW_MS = 1000
_OPEN_PRICE_CACHE: dict[tuple[str, int, int], "StrikeQuote"] = {}
_OPEN_PRICE_LAST_ATTEMPT: dict[tuple[str, int, int], float] = {}
_STREAMS_BOUNDARY_CACHE: dict[tuple[str, str, str], "_StreamsBoundaryState"] = {}
_RTDS_PROVIDER_CACHE: dict[tuple[str, str, int], "RTDSBoundaryStrikeProvider | None"] = {}
_RTDS_PROVIDER_LOCK = threading.Lock()


@dataclass(frozen=True)
class StrikeQuote:
    price: float
    ts_ms: int
    source: str


@dataclass
class _StreamsBoundaryState:
    last_refresh_monotonic: float
    signature: tuple[tuple[str, int], ...]
    prices_by_ts: dict[int, float]


class RTDSBoundaryStrikeProvider:
    """Buffered RTDS boundary fallback matching the legacy live strike provider."""

    def __init__(
        self,
        *,
        chainlink_symbol: str,
        ws_url: str = "wss://ws-live-data.polymarket.com",
        buffer_seconds: int = 3600,
    ) -> None:
        self.chainlink_symbol = str(chainlink_symbol or "").strip().lower()
        self.ws_url = str(ws_url or "").strip() or "wss://ws-live-data.polymarket.com"
        self.buffer_ms = max(60_000, int(buffer_seconds) * 1000)
        self._lock = threading.Lock()
        self._ticks: list[tuple[int, float]] = []
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if _legacy_rtds_chainlink_client_class() is None:
            return
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name=f"pm15min_rtds_{self.chainlink_symbol}", daemon=True)
        self._thread.start()

    def _append_tick(self, ts_ms: int, price: float) -> None:
        cutoff = int(time.time() * 1000) - self.buffer_ms
        with self._lock:
            self._ticks.append((int(ts_ms), float(price)))
            self._ticks.sort(key=lambda item: item[0])
            idx = 0
            while idx < len(self._ticks) and self._ticks[idx][0] < cutoff:
                idx += 1
            if idx > 0:
                self._ticks = self._ticks[idx:]

    async def _run_async(self) -> None:
        while not self._stop.is_set():
            try:
                client_class = _legacy_rtds_chainlink_client_class()
                if client_class is None:
                    return
                client = client_class(symbol=self.chainlink_symbol, ws_url=self.ws_url)
                async for point in client.stream():
                    if self._stop.is_set():
                        return
                    if float(point.value) <= 0.0:
                        continue
                    self._append_tick(int(point.timestamp_ms), float(point.value))
            except Exception:
                await asyncio.sleep(1.0)

    def _run(self) -> None:
        asyncio.run(self._run_async())

    def price_near(self, cycle_start_ts: pd.Timestamp, *, max_skew_ms: int) -> StrikeQuote | None:
        ts = _normalize_cycle_start_ts(cycle_start_ts)
        if ts is None:
            return None
        target_ms = int(ts.timestamp() * 1000)
        max_skew_ms = max(0, int(max_skew_ms))
        with self._lock:
            ticks = list(self._ticks)
        if not ticks:
            return None
        best = min(ticks, key=lambda item: abs(int(item[0]) - int(target_ms)))
        if abs(int(best[0]) - int(target_ms)) > max_skew_ms:
            return None
        return StrikeQuote(
            price=float(best[1]),
            ts_ms=int(best[0]),
            source="rtds_chainlink_boundary",
        )


class LiveRuntimeStrikeResolver:
    """Legacy-faithful runtime strike resolver for the first three fallback layers."""

    def __init__(
        self,
        *,
        data_cfg: DataConfig,
        market_slug: str,
        oracle_client: PolymarketOracleApiClient | None = None,
        cache_path: str | Path | None = None,
        rtds_provider: RTDSBoundaryStrikeProvider | None = None,
        rtds_max_skew_ms: int = _DEFAULT_RTDS_MAX_SKEW_MS,
    ) -> None:
        self.data_cfg = data_cfg
        self.market_slug = str(market_slug or "").strip().lower()
        self.oracle_client = oracle_client or PolymarketOracleApiClient()
        from pm15min.live.oracle.strike_cache import StrikeCache
        self.cache = StrikeCache(
            path=(Path(cache_path) if cache_path is not None else _default_cache_path(data_cfg, self.market_slug)),
            asset_slug=self.market_slug,
        )
        self.rtds_max_skew_ms = max(0, int(rtds_max_skew_ms))
        self.rtds_provider = rtds_provider or _default_rtds_provider(
            market_slug=self.market_slug,
        )
        if self.rtds_provider is not None:
            self.rtds_provider.start()

    def strike_at(self, cycle_start_ts: pd.Timestamp) -> StrikeQuote | None:
        normalized_ts = _normalize_cycle_start_ts(cycle_start_ts)
        if normalized_ts is None:
            return None
        cycle_start_sec = int(normalized_ts.timestamp())
        cached = _cached_quote(
            cache=self.cache,
            cycle_start_sec=cycle_start_sec,
            target_ms=int(normalized_ts.timestamp() * 1000),
            max_skew_ms=self.rtds_max_skew_ms,
        )
        if cached is not None and str(cached.source or "") in _EXACT_CACHE_SOURCES:
            return cached

        quote = _resolve_open_price_quote(
            market_slug=self.market_slug,
            cycle_start_ts=normalized_ts,
            cycle_seconds=int(self.data_cfg.layout.cycle_seconds),
            oracle_client=self.oracle_client,
        )
        if quote is not None:
            if cached is None or str(cached.source or "") != "streams_parquet":
                self.cache.put(
                    StrikeCacheRecord(
                        cycle_start_ts=cycle_start_sec,
                        strike_price=float(quote.price),
                        observed_ts_ms=int(quote.ts_ms),
                        source=str(quote.source),
                    )
                )
            return quote

        quote = _resolve_streams_boundary_quote(
            data_cfg=self.data_cfg,
            market_slug=self.market_slug,
            cycle_start_ts=normalized_ts,
        )
        if quote is not None:
            self.cache.put(
                StrikeCacheRecord(
                    cycle_start_ts=cycle_start_sec,
                    strike_price=float(quote.price),
                    observed_ts_ms=int(quote.ts_ms),
                    source=str(quote.source),
                )
            )
            return quote

        if cached is None:
            quote = _resolve_rtds_boundary_quote(
                provider=self.rtds_provider,
                cycle_start_ts=normalized_ts,
                max_skew_ms=self.rtds_max_skew_ms,
            )
            if quote is None:
                return None
            previous = self.cache.get(cycle_start_sec)
            if previous is None or str(previous.source or "") != "streams_parquet":
                self.cache.put(
                    StrikeCacheRecord(
                        cycle_start_ts=cycle_start_sec,
                        strike_price=float(quote.price),
                        observed_ts_ms=int(quote.ts_ms),
                        source=str(quote.source),
                    )
                )
            return quote
        return cached


def build_live_runtime_oracle_prices(
    *,
    data_cfg: DataConfig,
    market_slug: str,
    raw_klines: pd.DataFrame,
    oracle_prices_table: pd.DataFrame | None = None,
    oracle_client: PolymarketOracleApiClient | None = None,
    cache_path: str | Path | None = None,
    rtds_provider: RTDSBoundaryStrikeProvider | None = None,
    rtds_max_skew_ms: int = _DEFAULT_RTDS_MAX_SKEW_MS,
) -> pd.DataFrame:
    base = _normalize_oracle_prices_table(oracle_prices_table)
    latest_cycle_start_ts = _latest_cycle_start_ts(raw_klines, cycle_seconds=int(data_cfg.layout.cycle_seconds))
    if latest_cycle_start_ts is None:
        return base
    resolver = LiveRuntimeStrikeResolver(
        data_cfg=data_cfg,
        market_slug=market_slug,
        oracle_client=oracle_client,
        cache_path=cache_path,
        rtds_provider=rtds_provider,
        rtds_max_skew_ms=rtds_max_skew_ms,
    )
    quote = resolver.strike_at(latest_cycle_start_ts)
    if quote is None:
        return base
    return _overlay_runtime_quote(
        base,
        market_slug=market_slug,
        cycle_start_ts=latest_cycle_start_ts,
        cycle_seconds=int(data_cfg.layout.cycle_seconds),
        quote=quote,
    )


def _resolve_open_price_quote(
    *,
    market_slug: str,
    cycle_start_ts: pd.Timestamp,
    cycle_seconds: int,
    oracle_client: PolymarketOracleApiClient,
    min_refresh_seconds: float = _OPEN_PRICE_MIN_REFRESH_SECONDS,
) -> StrikeQuote | None:
    cycle_start_sec = int(cycle_start_ts.timestamp())
    cache_key = (str(market_slug), int(cycle_seconds), int(cycle_start_sec))
    cached = _OPEN_PRICE_CACHE.get(cache_key)
    if cached is not None:
        return cached
    now = time.monotonic()
    last_attempt = _OPEN_PRICE_LAST_ATTEMPT.get(cache_key, 0.0)
    if now - last_attempt < max(1.0, float(min_refresh_seconds)):
        return None
    _OPEN_PRICE_LAST_ATTEMPT[cache_key] = now
    symbol = DEFAULT_POLYMARKET_SYMBOL.get(str(market_slug), str(market_slug).upper())
    max_attempts = _env_int(
        "PM15MIN_OPEN_PRICE_EMPTY_RETRY_MAX_ATTEMPTS",
        default=_OPEN_PRICE_EMPTY_RETRY_MAX_ATTEMPTS,
    )
    for attempt in range(max(1, int(max_attempts))):
        try:
            payload = oracle_client.fetch_crypto_price(
                symbol=symbol,
                cycle_start_ts=cycle_start_sec,
                cycle_seconds=int(cycle_seconds),
            )
        except Exception:
            return None
        open_price = pd.to_numeric(payload.get("openPrice"), errors="coerce")
        if not pd.isna(open_price) and float(open_price) > 0.0:
            quote = StrikeQuote(
                price=float(open_price),
                ts_ms=int(cycle_start_sec * 1000),
                source="polymarket_open_price_api",
            )
            _OPEN_PRICE_CACHE[cache_key] = quote
            return quote
        if attempt + 1 >= max_attempts:
            break
        time.sleep(_open_price_retry_sleep_seconds(attempt))
    return None


def _resolve_streams_boundary_quote(
    *,
    data_cfg: DataConfig,
    market_slug: str,
    cycle_start_ts: pd.Timestamp,
) -> StrikeQuote | None:
    cycle_start_sec = int(cycle_start_ts.timestamp())
    prices_by_ts = _load_streams_boundary_prices(data_cfg=data_cfg, market_slug=market_slug)
    price = prices_by_ts.get(cycle_start_sec)
    if price is None or float(price) <= 0.0:
        return None
    return StrikeQuote(
        price=float(price),
        ts_ms=int(cycle_start_sec * 1000),
        source="streams_parquet",
    )


def _resolve_rtds_boundary_quote(
    *,
    provider: RTDSBoundaryStrikeProvider | None,
    cycle_start_ts: pd.Timestamp,
    max_skew_ms: int,
) -> StrikeQuote | None:
    if provider is None:
        return None
    return provider.price_near(cycle_start_ts, max_skew_ms=int(max_skew_ms))


def _load_streams_boundary_prices(*, data_cfg: DataConfig, market_slug: str) -> dict[int, float]:
    cache_key = (str(data_cfg.layout.streams_source_root), str(market_slug or "").strip().lower(), str(data_cfg.cycle))
    now = time.monotonic()
    state = _STREAMS_BOUNDARY_CACHE.get(cache_key)
    if state is not None and now - state.last_refresh_monotonic < _STREAMS_REFRESH_SECONDS:
        return dict(state.prices_by_ts)

    signature = _streams_partition_signature(data_cfg.layout.streams_source_root)
    if state is not None and signature == state.signature:
        state.last_refresh_monotonic = now
        return dict(state.prices_by_ts)

    prices_by_ts = _build_streams_prices_by_ts(data_cfg=data_cfg, market_slug=market_slug)
    _STREAMS_BOUNDARY_CACHE[cache_key] = _StreamsBoundaryState(
        last_refresh_monotonic=now,
        signature=signature,
        prices_by_ts=prices_by_ts,
    )
    return dict(prices_by_ts)


def _build_streams_prices_by_ts(*, data_cfg: DataConfig, market_slug: str) -> dict[int, float]:
    streams = load_streams_source(data_cfg)
    if streams.empty:
        return {}
    frame = streams.copy()
    frame["asset"] = frame.get("asset", pd.Series("", index=frame.index)).astype(str).str.lower()
    frame = frame.loc[frame["asset"].eq(str(market_slug or "").strip().lower())].copy()
    if frame.empty:
        return {}
    frame["extra_ts"] = pd.to_numeric(frame.get("extra_ts"), errors="coerce")
    if "price" in frame.columns:
        frame["price"] = pd.to_numeric(frame.get("price"), errors="coerce")
    else:
        frame["benchmark_price_raw"] = pd.to_numeric(frame.get("benchmark_price_raw"), errors="coerce")
        frame["price"] = frame["benchmark_price_raw"] / 1e18
    frame = frame.dropna(subset=["extra_ts", "price"]).copy()
    frame["extra_ts"] = frame["extra_ts"].astype("int64")
    frame = frame.loc[frame["extra_ts"].mod(int(data_cfg.layout.cycle_seconds)).eq(0)].copy()
    if frame.empty:
        return {}
    sort_columns = [column for column in ("extra_ts", "tx_hash", "perform_idx", "value_idx") if column in frame.columns]
    if sort_columns:
        frame = frame.sort_values(sort_columns, kind="stable")
    frame = frame.drop_duplicates(subset=["extra_ts"], keep="last")
    return {
        int(row.extra_ts): float(row.price)
        for row in frame.itertuples(index=False)
        if float(row.price) > 0.0
    }


def _normalize_oracle_prices_table(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(
            columns=[
                "asset",
                "cycle_start_ts",
                "cycle_end_ts",
                "price_to_beat",
                "final_price",
                "source_price_to_beat",
                "source_final_price",
                "has_price_to_beat",
                "has_final_price",
                "has_both",
            ]
        )
    out = frame.copy()
    out["cycle_start_ts"] = pd.to_numeric(out.get("cycle_start_ts"), errors="coerce")
    out["cycle_end_ts"] = pd.to_numeric(out.get("cycle_end_ts"), errors="coerce")
    out["price_to_beat"] = pd.to_numeric(out.get("price_to_beat"), errors="coerce")
    if "final_price" in out.columns:
        out["final_price"] = pd.to_numeric(out.get("final_price"), errors="coerce")
    return out


def _overlay_runtime_quote(
    base: pd.DataFrame,
    *,
    market_slug: str,
    cycle_start_ts: pd.Timestamp,
    cycle_seconds: int,
    quote: StrikeQuote,
) -> pd.DataFrame:
    out = base.copy()
    cycle_start_sec = int(cycle_start_ts.timestamp())
    cycle_end_sec = cycle_start_sec + int(cycle_seconds)
    mask = (
        pd.to_numeric(out.get("cycle_start_ts"), errors="coerce").eq(cycle_start_sec)
        if not out.empty
        else pd.Series(dtype=bool)
    )
    if mask.any():
        out.loc[mask, "price_to_beat"] = float(quote.price)
        out.loc[mask, "source_price_to_beat"] = str(quote.source)
        out.loc[mask, "has_price_to_beat"] = True
        if "final_price" not in out.columns:
            out["final_price"] = pd.NA
        out["has_final_price"] = pd.to_numeric(out.get("final_price"), errors="coerce").notna()
        out["has_both"] = out["has_price_to_beat"].astype(bool) & out["has_final_price"].astype(bool)
        return out

    appended = pd.DataFrame(
        [
            {
                "asset": str(market_slug),
                "cycle_start_ts": int(cycle_start_sec),
                "cycle_end_ts": int(cycle_end_sec),
                "price_to_beat": float(quote.price),
                "final_price": pd.NA,
                "source_price_to_beat": str(quote.source),
                "source_final_price": "",
                "has_price_to_beat": True,
                "has_final_price": False,
                "has_both": False,
            }
        ]
    )
    if out.empty:
        return appended
    return pd.concat([out, appended], ignore_index=True, sort=False)


def _latest_cycle_start_ts(raw_klines: pd.DataFrame, *, cycle_seconds: int) -> pd.Timestamp | None:
    if raw_klines.empty:
        return None
    open_time = pd.to_datetime(raw_klines.get("open_time"), utc=True, errors="coerce").dropna()
    if open_time.empty:
        return None
    latest = open_time.max()
    cycle_start_sec = int(latest.timestamp()) // int(cycle_seconds) * int(cycle_seconds)
    return pd.Timestamp(cycle_start_sec, unit="s", tz="UTC")


def _normalize_cycle_start_ts(value: pd.Timestamp) -> pd.Timestamp | None:
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts)


def _default_cache_path(data_cfg: DataConfig, market_slug: str) -> Path:
    return data_cfg.layout.cache_root / "live_oracle" / f"strike_cache_{str(market_slug or '').strip().lower()}.csv"


def _streams_partition_signature(root: Path) -> tuple[tuple[str, int], ...]:
    rows: list[tuple[str, int]] = []
    for path in sorted(root.glob("year=*/month=*/data.parquet")):
        try:
            rows.append((str(path), int(path.stat().st_mtime_ns)))
        except Exception:
            continue
    return tuple(rows)


def _cached_quote(
    *,
    cache: StrikeCache,
    cycle_start_sec: int,
    target_ms: int,
    max_skew_ms: int,
) -> StrikeQuote | None:
    cached = cache.get(cycle_start_sec)
    if cached is None:
        return None
    source = str(cached.source or "").strip()
    if source.startswith("rtds_chainlink") and abs(int(cached.observed_ts_ms) - int(target_ms)) > int(max_skew_ms):
        return None
    quote_source = source if source in _EXACT_CACHE_SOURCES else f"strike_cache:{source}"
    return StrikeQuote(
        price=float(cached.strike_price),
        ts_ms=int(cached.observed_ts_ms),
        source=quote_source,
    )


def _default_rtds_provider(*, market_slug: str) -> RTDSBoundaryStrikeProvider | None:
    enabled = (os.getenv("PM15MIN_ENABLE_STRIKE_RTDS") or "1").strip().lower() not in {"0", "false", "no", "off"}
    if not enabled or _legacy_rtds_chainlink_client_class() is None:
        return None
    symbol = DEFAULT_CHAINLINK_SYMBOL.get(str(market_slug or "").strip().lower(), f"{market_slug}/usd")
    ws_url = (os.getenv("PM15MIN_CHAINLINK_WS_URL") or "").strip() or "wss://ws-live-data.polymarket.com"
    buffer_seconds = int(float((os.getenv("PM15MIN_CHAINLINK_STRIKE_BUFFER_SEC") or "3600").strip() or "3600"))
    key = (str(symbol), str(ws_url), int(buffer_seconds))
    with _RTDS_PROVIDER_LOCK:
        provider = _RTDS_PROVIDER_CACHE.get(key)
        if provider is None:
            provider = RTDSBoundaryStrikeProvider(
                chainlink_symbol=symbol,
                ws_url=ws_url,
                buffer_seconds=buffer_seconds,
            )
            _RTDS_PROVIDER_CACHE[key] = provider
        return provider


def _open_price_retry_sleep_seconds(attempt: int) -> float:
    base_seconds = _env_float(
        "PM15MIN_OPEN_PRICE_EMPTY_RETRY_BASE_SECONDS",
        default=_OPEN_PRICE_EMPTY_RETRY_BASE_SECONDS,
    )
    max_seconds = _env_float(
        "PM15MIN_OPEN_PRICE_EMPTY_RETRY_MAX_SECONDS",
        default=_OPEN_PRICE_EMPTY_RETRY_MAX_SECONDS,
    )
    multiplier = _env_float(
        "PM15MIN_OPEN_PRICE_EMPTY_RETRY_MULTIPLIER",
        default=_OPEN_PRICE_EMPTY_RETRY_MULTIPLIER,
    )
    base_seconds = max(0.0, float(base_seconds))
    max_seconds = max(base_seconds, float(max_seconds))
    multiplier = max(1.0, float(multiplier))
    sleep_seconds = base_seconds * (multiplier ** max(0, int(attempt)))
    return min(max_seconds, sleep_seconds)


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
        return int(float(raw))
    except Exception:
        return int(default)


@lru_cache(maxsize=1)
def _legacy_rtds_chainlink_client_class():
    try:
        module = importlib.import_module("live_trading.oracle.rtds_chainlink_client")
    except Exception:  # pragma: no cover - optional dependency
        return None
    return getattr(module, "RTDSChainlinkClient", None)
