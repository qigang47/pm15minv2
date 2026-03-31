from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Protocol

import requests

from .polymarket_clob import PolymarketClobClient


class OrderbookProvider(Protocol):
    def get_orderbook_summary(
        self,
        token_id: str,
        *,
        levels: int = 0,
        timeout: float = 1.2,
        force_refresh: bool = False,
    ) -> dict[str, Any] | None: ...

    def sync_subscriptions(
        self,
        token_ids: list[str],
        *,
        replace: bool = True,
        prefetch: bool = False,
        levels: int = 0,
        timeout: float = 1.2,
    ) -> dict[str, Any] | None: ...


@dataclass
class DirectOrderbookProvider:
    client: PolymarketClobClient

    def get_orderbook_summary(
        self,
        token_id: str,
        *,
        levels: int = 0,
        timeout: float = 1.2,
        force_refresh: bool = False,
    ) -> dict[str, Any] | None:
        del force_refresh
        return self.client.fetch_book(token_id, levels=levels, timeout_sec=timeout)

    def sync_subscriptions(
        self,
        token_ids: list[str],
        *,
        replace: bool = True,
        prefetch: bool = False,
        levels: int = 0,
        timeout: float = 1.2,
    ) -> dict[str, Any] | None:
        del token_ids, replace, prefetch, levels, timeout
        return {"ok": True, "skipped": True, "provider": "direct"}


@dataclass
class _CachedOrderbookEntry:
    payload: dict[str, Any]
    levels: int
    fetched_monotonic: float
    snapshot_ts_ms: int | None


class InMemoryCachedOrderbookProvider:
    def __init__(
        self,
        *,
        wrapped: OrderbookProvider,
        max_age_ms: int = 300,
        monotonic_fn=None,
        now_ms_fn=None,
        background_refresh_ms: int = 0,
        refresh_timeout_sec: float = 1.2,
    ) -> None:
        self.wrapped = wrapped
        self.max_age_ms = max(0, int(max_age_ms))
        self.monotonic_fn = monotonic_fn or time.monotonic
        self.now_ms_fn = now_ms_fn or (lambda: int(time.time() * 1000))
        self.background_refresh_ms = max(0, int(background_refresh_ms))
        self.refresh_timeout_sec = max(0.1, float(refresh_timeout_sec))
        self._cache: dict[tuple[str, int], _CachedOrderbookEntry] = {}
        self._tracked_levels: dict[str, int] = {}
        self._last_synced_tokens: tuple[tuple[str, int], ...] | None = None
        self._lock = threading.Lock()
        if self.background_refresh_ms > 0:
            self._refresh_thread = threading.Thread(
                target=self._refresh_loop,
                name="orderbook-hot-cache-refresh",
                daemon=True,
            )
            self._refresh_thread.start()

    def get_orderbook_summary(
        self,
        token_id: str,
        *,
        levels: int = 0,
        timeout: float = 1.2,
        force_refresh: bool = False,
    ) -> dict[str, Any] | None:
        token = str(token_id or "").strip()
        requested_levels = max(0, int(levels or 0))
        self._track_token(token_id=token, levels=requested_levels)
        cached = None if force_refresh else self._lookup_cached(token_id=token, levels=requested_levels)
        if cached is not None:
            return dict(cached)
        payload = self._fetch_fresh_payload(
            token_id=token,
            levels=requested_levels,
            timeout=timeout,
            force_refresh=force_refresh,
        )
        if isinstance(payload, dict):
            self._store_cached(token_id=token, levels=requested_levels, payload=payload)
            return dict(payload)
        return payload

    def sync_subscriptions(
        self,
        token_ids: list[str],
        *,
        replace: bool = True,
        prefetch: bool = False,
        levels: int = 0,
        timeout: float = 1.2,
    ) -> dict[str, Any] | None:
        requested_levels = max(0, int(levels or 0))
        requested = {
            str(token_id).strip(): requested_levels
            for token_id in token_ids
            if str(token_id).strip()
        }
        requested_state = tuple(sorted((token, int(token_levels)) for token, token_levels in requested.items()))
        with self._lock:
            if replace and self._last_synced_tokens == requested_state:
                return {
                    "ok": True,
                    "skipped": True,
                    "provider": "in_memory_cached",
                    "token_count": len(requested),
                }
            if replace:
                self._tracked_levels = dict(requested)
                self._last_synced_tokens = requested_state
                tracked_tokens = set(requested)
                self._cache = {
                    key: entry
                    for key, entry in self._cache.items()
                    if key[0] in tracked_tokens
                }
            else:
                for token_id, token_levels in requested.items():
                    existing = self._tracked_levels.get(token_id)
                    if existing is None or int(token_levels) > int(existing):
                        self._tracked_levels[token_id] = int(token_levels)
                self._last_synced_tokens = None
            tracked_snapshot = list(self._tracked_levels.items())
        wrapped_result = self.wrapped.sync_subscriptions(
            token_ids,
            replace=replace,
            prefetch=prefetch,
            levels=levels,
            timeout=timeout,
        )
        if not prefetch:
            return wrapped_result
        prefetched = 0
        cache_hits = 0
        errors = 0
        for token_id, token_levels in tracked_snapshot:
            cached = self._lookup_cached(token_id=str(token_id), levels=int(token_levels))
            if cached is not None:
                cache_hits += 1
                continue
            try:
                payload = self._fetch_fresh_payload(
                    token_id=str(token_id),
                    levels=int(token_levels),
                    timeout=timeout,
                    force_refresh=False,
                )
            except Exception:
                errors += 1
                continue
            if isinstance(payload, dict):
                self._store_cached(token_id=str(token_id), levels=int(token_levels), payload=payload)
                prefetched += 1
        summary = {
            "ok": True,
            "provider": "in_memory_cached",
            "token_count": len(tracked_snapshot),
            "prefetched": int(prefetched),
            "cache_hits": int(cache_hits),
            "errors": int(errors),
        }
        if isinstance(wrapped_result, dict):
            summary["wrapped"] = dict(wrapped_result)
        return summary

    def _lookup_cached(self, *, token_id: str, levels: int) -> dict[str, Any] | None:
        now_monotonic = float(self.monotonic_fn())
        now_ms = int(self.now_ms_fn())
        with self._lock:
            candidates: list[tuple[int, _CachedOrderbookEntry]] = []
            for (cached_token, cached_levels), entry in self._cache.items():
                if cached_token != token_id:
                    continue
                age_ms = _entry_age_ms(
                    entry=entry,
                    now_ms=now_ms,
                    now_monotonic=now_monotonic,
                )
                if self.max_age_ms > 0 and age_ms > float(self.max_age_ms):
                    continue
                if cached_levels == levels:
                    return dict(entry.payload)
                if levels > 0 and (cached_levels == 0 or cached_levels >= levels):
                    priority = 0 if cached_levels == 0 else int(cached_levels)
                    candidates.append((priority, entry))
            if not candidates:
                return None
            candidates.sort(key=lambda item: item[0])
            return dict(candidates[0][1].payload)

    def _store_cached(self, *, token_id: str, levels: int, payload: dict[str, Any]) -> None:
        with self._lock:
            self._cache[(str(token_id), max(0, int(levels)))] = _CachedOrderbookEntry(
                payload=dict(payload),
                levels=max(0, int(levels)),
                fetched_monotonic=float(self.monotonic_fn()),
                snapshot_ts_ms=_payload_snapshot_ts_ms(payload),
            )
            tracked_levels = self._tracked_levels.get(str(token_id))
            if tracked_levels is None or int(levels) > int(tracked_levels):
                self._tracked_levels[str(token_id)] = max(0, int(levels))

    def _fetch_fresh_payload(
        self,
        *,
        token_id: str,
        levels: int,
        timeout: float,
        force_refresh: bool,
    ) -> dict[str, Any] | None:
        payload = self.wrapped.get_orderbook_summary(
            token_id,
            levels=levels,
            timeout=timeout,
            force_refresh=force_refresh,
        )
        if not isinstance(payload, dict):
            return payload
        payload_age_ms = _payload_age_ms(payload=payload, now_ms=int(self.now_ms_fn()))
        if self.max_age_ms > 0 and payload_age_ms is not None and payload_age_ms > int(self.max_age_ms):
            retry_payload = self.wrapped.get_orderbook_summary(
                token_id,
                levels=levels,
                timeout=timeout,
                force_refresh=True,
            )
            if isinstance(retry_payload, dict):
                retry_age_ms = _payload_age_ms(payload=retry_payload, now_ms=int(self.now_ms_fn()))
                if retry_age_ms is not None and retry_age_ms > int(self.max_age_ms):
                    return None
                return retry_payload
            return retry_payload
        return payload

    def _track_token(self, *, token_id: str, levels: int) -> None:
        token = str(token_id or "").strip()
        if not token:
            return
        with self._lock:
            existing = self._tracked_levels.get(token)
            if existing is None or int(levels) > int(existing):
                self._tracked_levels[token] = max(0, int(levels))

    def _refresh_loop(self) -> None:
        while True:
            time.sleep(max(0.05, float(self.background_refresh_ms) / 1000.0))
            with self._lock:
                tracked = list(self._tracked_levels.items())
            if not tracked:
                continue
            now_monotonic = float(self.monotonic_fn())
            now_ms = int(self.now_ms_fn())
            for token_id, levels in tracked:
                entry = None
                with self._lock:
                    entry = self._cache.get((str(token_id), int(levels)))
                if entry is not None:
                    age_ms = _entry_age_ms(entry=entry, now_ms=now_ms, now_monotonic=now_monotonic)
                    if age_ms < float(self.background_refresh_ms):
                        continue
                try:
                    payload = self.wrapped.get_orderbook_summary(
                        str(token_id),
                        levels=int(levels),
                        timeout=float(self.refresh_timeout_sec),
                        force_refresh=False,
                    )
                except Exception:
                    continue
                if isinstance(payload, dict):
                    self._store_cached(token_id=str(token_id), levels=int(levels), payload=payload)


class HubOrderbookProvider:
    def __init__(
        self,
        *,
        hub_url: str,
        source_name: str,
        fallback_provider: OrderbookProvider | None = None,
        subscribe_on_read: bool = False,
        session: requests.Session | None = None,
    ) -> None:
        self.hub_url = str(hub_url).rstrip("/")
        self.source_name = str(source_name).strip() or "pm15min"
        self.fallback_provider = fallback_provider
        self.subscribe_on_read = bool(subscribe_on_read)
        self.session = session or requests.Session()
        self._subscribed: set[str] = set()
        self._lock = threading.Lock()
        self._last_synced_tokens: set[str] | None = None

    def _subscribe_token(self, token_id: str, *, timeout: float) -> None:
        if not self.subscribe_on_read:
            return
        with self._lock:
            if token_id in self._subscribed:
                return
        payload = {"token_ids": [str(token_id)], "source": self.source_name}
        resp = self.session.post(f"{self.hub_url}/v1/subscribe", json=payload, timeout=timeout)
        resp.raise_for_status()
        with self._lock:
            self._subscribed.add(str(token_id))

    def sync_subscriptions(
        self,
        token_ids: list[str],
        *,
        replace: bool = True,
        prefetch: bool = False,
        levels: int = 0,
        timeout: float = 1.2,
    ) -> dict[str, Any] | None:
        tokens = sorted({str(token_id).strip() for token_id in token_ids if str(token_id).strip()})
        token_set = set(tokens)
        if replace and self._last_synced_tokens == token_set:
            return {
                "ok": True,
                "skipped": True,
                "provider": "hub",
                "token_count": len(tokens),
            }

        payload = {
            "token_ids": tokens,
            "source": self.source_name,
            "prefetch": bool(prefetch),
            "limit": max(0, int(levels or 0)),
            "timeout_sec": max(0.1, float(timeout)),
        }
        url = f"{self.hub_url}/v1/subscriptions/replace" if replace else f"{self.hub_url}/v1/subscribe"
        resp = self.session.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        body = resp.json()
        if replace:
            self._last_synced_tokens = token_set
            with self._lock:
                self._subscribed = token_set
        return body if isinstance(body, dict) else {"ok": True, "provider": "hub"}

    def _fetch_from_hub(
        self,
        token_id: str,
        *,
        levels: int,
        timeout: float,
        force_refresh: bool,
    ) -> dict[str, Any] | None:
        self._subscribe_token(token_id, timeout=timeout)
        params = {
            "token_id": str(token_id),
            "limit": max(0, int(levels or 0)),
            "force_refresh": "1" if force_refresh else "0",
            "source": self.source_name,
        }
        resp = self.session.get(f"{self.hub_url}/v1/orderbook", params=params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            return None
        orderbook = payload.get("orderbook")
        if not isinstance(orderbook, dict):
            return None
        fetched_at = payload.get("fetched_at")
        if fetched_at in (None, ""):
            return orderbook
        tagged = dict(orderbook)
        tagged["__hub_fetched_at"] = fetched_at
        return tagged

    def get_orderbook_summary(
        self,
        token_id: str,
        *,
        levels: int = 0,
        timeout: float = 1.2,
        force_refresh: bool = False,
    ) -> dict[str, Any] | None:
        try:
            payload = self._fetch_from_hub(
                token_id,
                levels=levels,
                timeout=timeout,
                force_refresh=force_refresh,
            )
            if payload:
                return payload
        except Exception:
            pass
        if self.fallback_provider is None:
            return None
        return self.fallback_provider.get_orderbook_summary(
            token_id,
            levels=levels,
            timeout=timeout,
            force_refresh=force_refresh,
        )


def build_orderbook_provider(
    *,
    client: PolymarketClobClient | None = None,
    hub_url: str | None = None,
    source_name: str,
    subscribe_on_read: bool = False,
    fallback_to_direct: bool = True,
) -> OrderbookProvider:
    direct = DirectOrderbookProvider(client=client or PolymarketClobClient())
    if not hub_url:
        return direct
    return HubOrderbookProvider(
        hub_url=hub_url,
        source_name=source_name,
        fallback_provider=direct if fallback_to_direct else None,
        subscribe_on_read=subscribe_on_read,
    )


def build_orderbook_provider_from_env(
    *,
    client: PolymarketClobClient | None = None,
    source_name: str,
    subscribe_on_read: bool = False,
) -> OrderbookProvider:
    hub_url = (os.getenv("PM15MIN_ORDERBOOK_HUB_URL") or "").strip() or None
    strict = _env_bool("PM15MIN_ORDERBOOK_HUB_STRICT", default=False)
    return build_orderbook_provider(
        client=client,
        hub_url=hub_url,
        source_name=source_name,
        subscribe_on_read=subscribe_on_read,
        fallback_to_direct=not strict,
    )


def build_in_memory_cached_orderbook_provider(
    *,
    provider: OrderbookProvider,
    max_age_ms: int = 300,
    background_refresh_ms: int = 0,
    refresh_timeout_sec: float = 1.2,
) -> OrderbookProvider:
    return InMemoryCachedOrderbookProvider(
        wrapped=provider,
        max_age_ms=max_age_ms,
        background_refresh_ms=background_refresh_ms,
        refresh_timeout_sec=refresh_timeout_sec,
    )


def _entry_age_ms(
    *,
    entry: _CachedOrderbookEntry,
    now_ms: int,
    now_monotonic: float,
) -> float:
    if entry.snapshot_ts_ms is not None:
        return float(max(0, int(now_ms) - int(entry.snapshot_ts_ms)))
    return max(0.0, (float(now_monotonic) - float(entry.fetched_monotonic)) * 1000.0)


def _payload_age_ms(*, payload: dict[str, Any], now_ms: int) -> int | None:
    snapshot_ts_ms = _payload_snapshot_ts_ms(payload)
    if snapshot_ts_ms is None:
        return None
    return max(0, int(now_ms) - int(snapshot_ts_ms))


def _payload_snapshot_ts_ms(payload: dict[str, Any]) -> int | None:
    for key in ("timestamp", "ts", "orderbook_ts", "source_ts_ms", "captured_ts_ms"):
        parsed = _timestamp_to_ms(payload.get(key))
        if parsed is not None:
            return parsed
    return _timestamp_to_ms(payload.get("__hub_fetched_at"))


def _timestamp_to_ms(raw: object) -> int | None:
    if raw in (None, ""):
        return None
    if isinstance(raw, (int, float)):
        value = int(raw)
        return value if value > 10_000_000_000 else value * 1000
    text = str(raw).strip()
    if not text:
        return None
    if text.isdigit():
        value = int(text)
        return value if value > 10_000_000_000 else value * 1000
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def _env_bool(name: str, *, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw in (None, ""):
        return bool(default)
    token = str(raw).strip().lower()
    if token in {"1", "true", "yes", "y", "on"}:
        return True
    if token in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)
