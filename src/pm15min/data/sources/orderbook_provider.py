from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import threading
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Protocol

import requests

from .polymarket_clob import PolymarketClobClient

try:
    import websockets
except Exception:  # pragma: no cover - optional dependency fallback
    websockets = None


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

    def get_update_marker(self) -> object | None: ...

    def wait_for_update(
        self,
        *,
        since_marker: object | None,
        timeout_sec: float,
    ) -> object | None: ...


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
        payload = self.client.fetch_book(token_id, levels=levels, timeout_sec=timeout)
        if not isinstance(payload, dict):
            return payload
        out = dict(payload)
        out.setdefault("__provider_source", "direct")
        out["__provider_fetched_at_ms"] = int(time.time() * 1000)
        out["__provider_cache_hit"] = False
        out["__provider_force_refresh_used"] = False
        return out

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

    def get_update_marker(self) -> object | None:
        return None

    def wait_for_update(
        self,
        *,
        since_marker: object | None,
        timeout_sec: float,
    ) -> object | None:
        del since_marker, timeout_sec
        return None


@dataclass
class _StreamingOrderbookEntry:
    token_id: str
    market: str | None
    asks: list[dict[str, float]]
    bids: list[dict[str, float]]
    best_ask: float | None
    best_bid: float | None
    snapshot_ts_ms: int | None
    received_at_ms: int
    event_type: str
    depth_complete: bool
    truncated_asks: bool
    truncated_bids: bool


class WebsocketOrderbookProvider:
    def __init__(
        self,
        *,
        ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        fallback_provider: OrderbookProvider | None = None,
        subscribe_on_read: bool = False,
        ping_interval_sec: float = 10.0,
        connect_timeout_sec: float = 3.0,
        reconnect_backoff_sec: float = 1.0,
        stream_wait_ms: int = 120,
        max_cached_levels: int = 20,
    ) -> None:
        self.ws_url = str(ws_url or "").strip() or "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        self.fallback_provider = fallback_provider
        self.subscribe_on_read = bool(subscribe_on_read)
        self.ping_interval_sec = max(1.0, float(ping_interval_sec))
        self.connect_timeout_sec = max(0.5, float(connect_timeout_sec))
        self.reconnect_backoff_sec = max(0.2, float(reconnect_backoff_sec))
        self.stream_wait_ms = max(0, int(stream_wait_ms))
        self.max_cached_levels = max(1, int(max_cached_levels))
        self._entries: dict[str, _StreamingOrderbookEntry] = {}
        self._desired_levels: dict[str, int] = {}
        self._subscription_version = 0
        self._update_marker = 0
        self._connected = False
        self._last_error: str | None = None
        self._last_pong_monotonic: float | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._data_ready = threading.Condition(self._lock)

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
        if not token:
            return None
        if self.subscribe_on_read:
            self._update_desired_tokens([token], replace=False, levels=requested_levels)
        self._ensure_started()

        if not force_refresh:
            entry = self._get_entry(token)
            if entry is not None and self._entry_satisfies(entry=entry, levels=requested_levels):
                return self._build_payload(token_id=token, entry=entry, levels=requested_levels)

        wait_sec = min(max(0.0, float(timeout)), float(self.stream_wait_ms) / 1000.0)
        if not force_refresh and wait_sec > 0.0:
            self._wait_for_entry(token_id=token, levels=requested_levels, timeout_sec=wait_sec)
            entry = self._get_entry(token)
            if entry is not None and self._entry_satisfies(entry=entry, levels=requested_levels):
                return self._build_payload(token_id=token, entry=entry, levels=requested_levels)

        if self.fallback_provider is None:
            entry = self._get_entry(token)
            if entry is None:
                return None
            return self._build_payload(token_id=token, entry=entry, levels=requested_levels)

        payload = self.fallback_provider.get_orderbook_summary(
            token,
            levels=requested_levels,
            timeout=timeout,
            force_refresh=force_refresh,
        )
        if isinstance(payload, dict):
            self._store_fallback_payload(token_id=token, payload=payload)
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
        tokens = sorted({str(token_id).strip() for token_id in token_ids if str(token_id).strip()})
        unchanged = self._update_desired_tokens(tokens, replace=replace, levels=requested_levels)
        self._ensure_started()
        if not prefetch:
            return {
                "ok": True,
                "provider": "websocket",
                "token_count": len(tokens),
                "connected": self._is_connected(),
                "skipped": bool(replace and unchanged),
            }

        stream_timeout_sec = min(max(0.0, float(timeout)), float(self.stream_wait_ms) / 1000.0)
        stream_hits = self._wait_for_tokens(tokens=tokens, levels=requested_levels, timeout_sec=stream_timeout_sec)
        missing = [token for token in tokens if token not in stream_hits]
        prefetched_direct = 0
        errors = 0
        if missing and self.fallback_provider is not None:
            workers = min(len(missing), _prefetch_worker_count())
            if workers > 0:
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {
                        executor.submit(
                            self.fallback_provider.get_orderbook_summary,
                            token,
                            levels=requested_levels,
                            timeout=timeout,
                            force_refresh=False,
                        ): token
                        for token in missing
                    }
                    for future in as_completed(futures):
                        token = futures[future]
                        try:
                            payload = future.result()
                        except Exception:
                            errors += 1
                            continue
                        if not isinstance(payload, dict):
                            continue
                        self._store_fallback_payload(token_id=token, payload=payload)
                        prefetched_direct += 1
        return {
            "ok": True,
            "provider": "websocket",
            "token_count": len(tokens),
            "connected": self._is_connected(),
            "prefetched_stream": len(stream_hits),
            "prefetched_direct": int(prefetched_direct),
            "errors": int(errors),
            "missing": max(0, len(missing) - int(prefetched_direct)),
            "skipped": bool(replace and unchanged),
        }

    def get_update_marker(self) -> object | None:
        with self._lock:
            return int(self._update_marker)

    def wait_for_update(
        self,
        *,
        since_marker: object | None,
        timeout_sec: float,
    ) -> object | None:
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        baseline = -1 if since_marker is None else int(since_marker)
        with self._data_ready:
            while True:
                if int(self._update_marker) > baseline:
                    return int(self._update_marker)
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    return None
                self._data_ready.wait(timeout=remaining)

    def _ensure_started(self) -> None:
        if websockets is None:
            return
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run_thread,
                name="pm15min-orderbook-ws",
                daemon=True,
            )
            self._thread.start()

    def _run_thread(self) -> None:
        asyncio.run(self._run_async())

    async def _run_async(self) -> None:
        backoff = float(self.reconnect_backoff_sec)
        while not self._stop.is_set():
            tokens, version = self._subscription_snapshot()
            if not tokens:
                await asyncio.sleep(0.1)
                continue
            try:
                async with websockets.connect(
                    self.ws_url,
                    open_timeout=self.connect_timeout_sec,
                    close_timeout=1.0,
                    ping_interval=None,
                ) as websocket:
                    with self._lock:
                        self._connected = True
                        self._last_error = None
                        self._data_ready.notify_all()
                    await self._send_initial_subscription(websocket, tokens)
                    applied_tokens = set(tokens)
                    applied_version = int(version)
                    next_ping_at = time.monotonic() + float(self.ping_interval_sec)
                    while not self._stop.is_set():
                        current_tokens, current_version = self._subscription_snapshot()
                        if current_version != applied_version:
                            if not current_tokens:
                                await websocket.close()
                                break
                            await self._apply_subscription_delta(
                                websocket,
                                previous_tokens=applied_tokens,
                                current_tokens=set(current_tokens),
                            )
                            applied_tokens = set(current_tokens)
                            applied_version = int(current_version)
                        now_monotonic = time.monotonic()
                        if now_monotonic >= next_ping_at:
                            await websocket.send("PING")
                            next_ping_at = now_monotonic + float(self.ping_interval_sec)
                        try:
                            raw_message = await asyncio.wait_for(websocket.recv(), timeout=0.5)
                        except TimeoutError:
                            continue
                        if isinstance(raw_message, bytes):
                            raw_message = raw_message.decode("utf-8", errors="ignore")
                        if not isinstance(raw_message, str):
                            continue
                        stripped = raw_message.strip()
                        if not stripped:
                            continue
                        upper = stripped.upper()
                        if upper == "PONG":
                            with self._lock:
                                self._last_pong_monotonic = time.monotonic()
                            continue
                        if upper == "PING":
                            await websocket.send("PONG")
                            continue
                        self._handle_stream_message(stripped)
                backoff = float(self.reconnect_backoff_sec)
            except Exception as exc:
                with self._lock:
                    self._connected = False
                    self._last_error = repr(exc)
                    self._data_ready.notify_all()
                await asyncio.sleep(backoff)
                backoff = min(max(0.2, backoff * 2.0), 5.0)
            else:
                with self._lock:
                    self._connected = False
                    self._data_ready.notify_all()
        with self._lock:
            self._connected = False
            self._data_ready.notify_all()

    async def _send_initial_subscription(self, websocket, tokens: list[str]) -> None:
        payload = {
            "assets_ids": list(tokens),
            "type": "market",
            "custom_feature_enabled": True,
        }
        await websocket.send(json.dumps(payload))

    async def _apply_subscription_delta(
        self,
        websocket,
        *,
        previous_tokens: set[str],
        current_tokens: set[str],
    ) -> None:
        to_unsubscribe = sorted(previous_tokens - current_tokens)
        to_subscribe = sorted(current_tokens - previous_tokens)
        if to_unsubscribe:
            await websocket.send(
                json.dumps(
                    {
                        "assets_ids": to_unsubscribe,
                        "operation": "unsubscribe",
                    }
                )
            )
        if to_subscribe:
            await websocket.send(
                json.dumps(
                    {
                        "assets_ids": to_subscribe,
                        "operation": "subscribe",
                        "custom_feature_enabled": True,
                    }
                )
            )

    def _handle_stream_message(self, raw_message: str) -> None:
        try:
            payload = json.loads(raw_message)
        except Exception:
            return
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    self._handle_stream_payload(item)
            return
        if isinstance(payload, dict):
            self._handle_stream_payload(payload)

    def _handle_stream_payload(self, payload: dict[str, Any]) -> None:
        event_type = str(payload.get("event_type") or "").strip().lower()
        if event_type == "book":
            self._record_book_event(payload)
            return
        if event_type == "price_change":
            self._record_price_change_event(payload)
            return
        if event_type == "best_bid_ask":
            self._record_best_bid_ask_event(payload)
            return

    def _record_book_event(self, payload: dict[str, Any]) -> None:
        token_id = str(payload.get("asset_id") or payload.get("token_id") or "").strip()
        if not token_id:
            return
        raw_asks = _normalize_price_levels(payload.get("asks"), reverse=False)
        raw_bids = _normalize_price_levels(payload.get("bids"), reverse=True)
        asks = raw_asks[: self.max_cached_levels]
        bids = raw_bids[: self.max_cached_levels]
        snapshot_ts_ms = _timestamp_to_ms(payload.get("timestamp"))
        received_at_ms = int(time.time() * 1000)
        entry = _StreamingOrderbookEntry(
            token_id=token_id,
            market=str(payload.get("market") or "").strip() or None,
            asks=asks,
            bids=bids,
            best_ask=asks[0]["price"] if asks else None,
            best_bid=bids[0]["price"] if bids else None,
            snapshot_ts_ms=snapshot_ts_ms,
            received_at_ms=received_at_ms,
            event_type="book",
            depth_complete=True,
            truncated_asks=len(raw_asks) > len(asks),
            truncated_bids=len(raw_bids) > len(bids),
        )
        self._publish_entry(token_id=token_id, entry=entry)

    def _record_price_change_event(self, payload: dict[str, Any]) -> None:
        snapshot_ts_ms = _timestamp_to_ms(payload.get("timestamp"))
        received_at_ms = int(time.time() * 1000)
        for change in list(payload.get("price_changes") or []):
            if not isinstance(change, dict):
                continue
            token_id = str(change.get("asset_id") or "").strip()
            if not token_id:
                continue
            with self._lock:
                previous = self._entries.get(token_id)
            asks = list(previous.asks) if previous is not None else []
            bids = list(previous.bids) if previous is not None else []
            if str(change.get("side") or "").strip().upper() == "BUY":
                bids = _apply_price_change(
                    levels=bids,
                    price=change.get("price"),
                    size=change.get("size"),
                    reverse=True,
                    limit=self.max_cached_levels,
                )
            elif str(change.get("side") or "").strip().upper() == "SELL":
                asks = _apply_price_change(
                    levels=asks,
                    price=change.get("price"),
                    size=change.get("size"),
                    reverse=False,
                    limit=self.max_cached_levels,
                )
            best_bid = _float_or_none(change.get("best_bid"))
            best_ask = _float_or_none(change.get("best_ask"))
            entry = _StreamingOrderbookEntry(
                token_id=token_id,
                market=str(payload.get("market") or (previous.market if previous is not None else "")).strip() or None,
                asks=asks,
                bids=bids,
                best_ask=best_ask if best_ask is not None else (asks[0]["price"] if asks else (previous.best_ask if previous is not None else None)),
                best_bid=best_bid if best_bid is not None else (bids[0]["price"] if bids else (previous.best_bid if previous is not None else None)),
                snapshot_ts_ms=snapshot_ts_ms if snapshot_ts_ms is not None else (previous.snapshot_ts_ms if previous is not None else None),
                received_at_ms=received_at_ms,
                event_type="price_change",
                depth_complete=bool(previous.depth_complete) if previous is not None else False,
                truncated_asks=bool(previous.truncated_asks) if previous is not None else False,
                truncated_bids=bool(previous.truncated_bids) if previous is not None else False,
            )
            self._publish_entry(token_id=token_id, entry=entry)

    def _record_best_bid_ask_event(self, payload: dict[str, Any]) -> None:
        token_id = str(payload.get("asset_id") or payload.get("token_id") or "").strip()
        if not token_id:
            return
        snapshot_ts_ms = _timestamp_to_ms(payload.get("timestamp"))
        received_at_ms = int(time.time() * 1000)
        with self._lock:
            previous = self._entries.get(token_id)
        entry = _StreamingOrderbookEntry(
            token_id=token_id,
            market=str(payload.get("market") or (previous.market if previous is not None else "")).strip() or None,
            asks=list(previous.asks) if previous is not None else [],
            bids=list(previous.bids) if previous is not None else [],
            best_ask=_float_or_none(payload.get("best_ask")) if _float_or_none(payload.get("best_ask")) is not None else (previous.best_ask if previous is not None else None),
            best_bid=_float_or_none(payload.get("best_bid")) if _float_or_none(payload.get("best_bid")) is not None else (previous.best_bid if previous is not None else None),
            snapshot_ts_ms=snapshot_ts_ms if snapshot_ts_ms is not None else (previous.snapshot_ts_ms if previous is not None else None),
            received_at_ms=received_at_ms,
            event_type="best_bid_ask",
            depth_complete=bool(previous.depth_complete) if previous is not None else False,
            truncated_asks=bool(previous.truncated_asks) if previous is not None else False,
            truncated_bids=bool(previous.truncated_bids) if previous is not None else False,
        )
        self._publish_entry(token_id=token_id, entry=entry)

    def _update_desired_tokens(self, token_ids: list[str], *, replace: bool, levels: int) -> bool:
        normalized = {str(token_id).strip(): max(0, int(levels)) for token_id in token_ids if str(token_id).strip()}
        with self._lock:
            current = tuple(sorted((token, int(token_levels)) for token, token_levels in self._desired_levels.items()))
            if replace:
                next_levels = dict(normalized)
            else:
                next_levels = dict(self._desired_levels)
                for token_id, token_levels in normalized.items():
                    existing = next_levels.get(token_id)
                    if existing is None or int(token_levels) > int(existing):
                        next_levels[token_id] = int(token_levels)
            next_state = tuple(sorted((token, int(token_levels)) for token, token_levels in next_levels.items()))
            unchanged = current == next_state
            if not unchanged:
                self._desired_levels = next_levels
                self._subscription_version += 1
                tracked = set(self._desired_levels)
                self._entries = {
                    token_id: entry
                    for token_id, entry in self._entries.items()
                    if token_id in tracked
                }
                self._data_ready.notify_all()
            return unchanged

    def _subscription_snapshot(self) -> tuple[list[str], int]:
        with self._lock:
            return sorted(self._desired_levels), int(self._subscription_version)

    def _get_entry(self, token_id: str) -> _StreamingOrderbookEntry | None:
        with self._lock:
            return self._entries.get(str(token_id))

    def _entry_satisfies(self, *, entry: _StreamingOrderbookEntry, levels: int) -> bool:
        requested_levels = max(0, int(levels))
        if requested_levels <= 1:
            return (
                entry.best_ask is not None
                or entry.best_bid is not None
                or bool(entry.asks)
                or bool(entry.bids)
            )
        if not entry.depth_complete:
            return False
        if entry.truncated_asks and len(entry.asks) < requested_levels:
            return False
        if entry.truncated_bids and len(entry.bids) < requested_levels:
            return False
        return True

    def _wait_for_entry(self, *, token_id: str, levels: int, timeout_sec: float) -> None:
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        with self._data_ready:
            while time.monotonic() < deadline:
                entry = self._entries.get(str(token_id))
                if entry is not None and self._entry_satisfies(entry=entry, levels=levels):
                    return
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                self._data_ready.wait(timeout=remaining)

    def _wait_for_tokens(self, *, tokens: list[str], levels: int, timeout_sec: float) -> set[str]:
        if not tokens or timeout_sec <= 0.0:
            return {
                token
                for token in tokens
                if (entry := self._entries.get(token)) is not None and self._entry_satisfies(entry=entry, levels=levels)
            }
        deadline = time.monotonic() + max(0.0, float(timeout_sec))
        with self._data_ready:
            while time.monotonic() < deadline:
                ready = {
                    token
                    for token in tokens
                    if (entry := self._entries.get(token)) is not None and self._entry_satisfies(entry=entry, levels=levels)
                }
                if len(ready) >= len(tokens):
                    return ready
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return ready
                self._data_ready.wait(timeout=remaining)
            return {
                token
                for token in tokens
                if (entry := self._entries.get(token)) is not None and self._entry_satisfies(entry=entry, levels=levels)
            }

    def _build_payload(
        self,
        *,
        token_id: str,
        entry: _StreamingOrderbookEntry,
        levels: int,
    ) -> dict[str, Any]:
        requested_levels = max(0, int(levels))
        asks = entry.asks if requested_levels <= 0 else entry.asks[:requested_levels]
        bids = entry.bids if requested_levels <= 0 else entry.bids[:requested_levels]
        payload: dict[str, Any] = {
            "asset_id": str(token_id),
            "token_id": str(token_id),
            "market": entry.market,
            "asks": _serialize_price_levels(asks),
            "bids": _serialize_price_levels(bids),
            "timestamp": entry.snapshot_ts_ms,
            "event_type": entry.event_type,
            "__provider_source": "websocket",
            "__provider_fetched_at_ms": int(entry.received_at_ms),
            "__provider_cache_hit": True,
            "__provider_cache_age_ms": (
                max(0, int(time.time() * 1000) - int(entry.snapshot_ts_ms))
                if entry.snapshot_ts_ms is not None
                else None
            ),
            "__provider_force_refresh_used": False,
            "__provider_stream_connected": self._is_connected(),
            "__provider_stream_event_type": entry.event_type,
        }
        best_ask = entry.best_ask if entry.best_ask is not None else (asks[0]["price"] if asks else None)
        best_bid = entry.best_bid if entry.best_bid is not None else (bids[0]["price"] if bids else None)
        if best_ask is not None:
            payload["best_ask"] = _format_decimal(best_ask)
        if best_bid is not None:
            payload["best_bid"] = _format_decimal(best_bid)
        if best_ask is not None and best_bid is not None:
            payload["spread"] = _format_decimal(best_ask - best_bid)
        return payload

    def _store_fallback_payload(self, *, token_id: str, payload: dict[str, Any]) -> None:
        asks = _normalize_price_levels(payload.get("asks"), reverse=False)
        bids = _normalize_price_levels(payload.get("bids"), reverse=True)
        snapshot_ts_ms = _payload_snapshot_ts_ms(payload)
        entry = _StreamingOrderbookEntry(
            token_id=str(token_id),
            market=str(payload.get("market") or "").strip() or None,
            asks=asks[: self.max_cached_levels],
            bids=bids[: self.max_cached_levels],
            best_ask=asks[0]["price"] if asks else _float_or_none(payload.get("best_ask")),
            best_bid=bids[0]["price"] if bids else _float_or_none(payload.get("best_bid")),
            snapshot_ts_ms=snapshot_ts_ms,
            received_at_ms=int(time.time() * 1000),
            event_type=str(payload.get("event_type") or "direct_fallback"),
            depth_complete=True,
            truncated_asks=False,
            truncated_bids=False,
        )
        self._publish_entry(token_id=str(token_id), entry=entry)

    def _is_connected(self) -> bool:
        with self._lock:
            return bool(self._connected)

    def _publish_entry(self, *, token_id: str, entry: _StreamingOrderbookEntry) -> None:
        with self._lock:
            self._entries[str(token_id)] = entry
            self._update_marker += 1
            self._data_ready.notify_all()


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
            age_ms = _payload_age_ms(payload=cached, now_ms=int(self.now_ms_fn()))
            out = dict(cached)
            out["__provider_cache_hit"] = True
            out["__provider_cache_age_ms"] = age_ms
            out["__provider_force_refresh_used"] = False
            return out
        payload = self._fetch_fresh_payload(
            token_id=token,
            levels=requested_levels,
            timeout=timeout,
            force_refresh=force_refresh,
        )
        if isinstance(payload, dict):
            self._store_cached(token_id=token, levels=requested_levels, payload=payload)
            out = dict(payload)
            out["__provider_cache_hit"] = False
            out["__provider_cache_age_ms"] = 0
            out["__provider_force_refresh_used"] = bool(force_refresh or out.get("__provider_force_refresh_used"))
            return out
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
        prefetch_targets: list[tuple[str, int]] = []
        for token_id, token_levels in tracked_snapshot:
            cached = self._lookup_cached(token_id=str(token_id), levels=int(token_levels))
            if cached is not None:
                cache_hits += 1
                continue
            prefetch_targets.append((str(token_id), int(token_levels)))
        workers = min(len(prefetch_targets), _prefetch_worker_count())
        if workers > 0:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(
                        self._fetch_fresh_payload,
                        token_id=token_id,
                        levels=token_levels,
                        timeout=timeout,
                        force_refresh=False,
                    ): (token_id, token_levels)
                    for token_id, token_levels in prefetch_targets
                }
                for future in as_completed(futures):
                    token_id, token_levels = futures[future]
                    try:
                        payload = future.result()
                    except Exception:
                        errors += 1
                        continue
                    if isinstance(payload, dict):
                        self._store_cached(token_id=token_id, levels=token_levels, payload=payload)
                        prefetched += 1
        summary = {
            "ok": True,
            "provider": "in_memory_cached",
            "token_count": len(tracked_snapshot),
            "prefetched": int(prefetched),
            "cache_hits": int(cache_hits),
            "errors": int(errors),
            "prefetch_workers": int(workers),
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
        payload = dict(payload)
        payload.setdefault("__provider_force_refresh_used", bool(force_refresh))
        payload_age_ms = _payload_age_ms(payload=payload, now_ms=int(self.now_ms_fn()))
        if self.max_age_ms > 0 and payload_age_ms is not None and payload_age_ms > int(self.max_age_ms):
            retry_payload = self.wrapped.get_orderbook_summary(
                token_id,
                levels=levels,
                timeout=timeout,
                force_refresh=True,
            )
            if isinstance(retry_payload, dict):
                retry_payload = dict(retry_payload)
                retry_payload["__provider_force_refresh_used"] = True
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

    def get_update_marker(self) -> object | None:
        getter = getattr(self.wrapped, "get_update_marker", None)
        if not callable(getter):
            return None
        try:
            return getter()
        except Exception:
            return None

    def wait_for_update(
        self,
        *,
        since_marker: object | None,
        timeout_sec: float,
    ) -> object | None:
        waiter = getattr(self.wrapped, "wait_for_update", None)
        if not callable(waiter):
            return None
        try:
            return waiter(since_marker=since_marker, timeout_sec=timeout_sec)
        except Exception:
            return None


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

    def get_update_marker(self) -> object | None:
        getter = getattr(self.fallback_provider, "get_update_marker", None)
        if not callable(getter):
            return None
        try:
            return getter()
        except Exception:
            return None

    def wait_for_update(
        self,
        *,
        since_marker: object | None,
        timeout_sec: float,
    ) -> object | None:
        waiter = getattr(self.fallback_provider, "wait_for_update", None)
        if not callable(waiter):
            return None
        try:
            return waiter(since_marker=since_marker, timeout_sec=timeout_sec)
        except Exception:
            return None


def build_orderbook_provider(
    *,
    client: PolymarketClobClient | None = None,
    hub_url: str | None = None,
    source_name: str,
    subscribe_on_read: bool = False,
    fallback_to_direct: bool = True,
    streaming: bool = False,
    ws_url: str | None = None,
    stream_wait_ms: int = 120,
    stream_max_levels: int = 20,
) -> OrderbookProvider:
    direct = DirectOrderbookProvider(client=client or PolymarketClobClient())
    if hub_url:
        return HubOrderbookProvider(
            hub_url=hub_url,
            source_name=source_name,
            fallback_provider=direct if fallback_to_direct else None,
            subscribe_on_read=subscribe_on_read,
        )
    if streaming:
        return WebsocketOrderbookProvider(
            ws_url=ws_url or "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            fallback_provider=direct if fallback_to_direct else None,
            subscribe_on_read=subscribe_on_read,
            ping_interval_sec=_env_float("PM15MIN_ORDERBOOK_WS_PING_INTERVAL_SEC", default=10.0),
            connect_timeout_sec=_env_float("PM15MIN_ORDERBOOK_WS_CONNECT_TIMEOUT_SEC", default=3.0),
            reconnect_backoff_sec=_env_float("PM15MIN_ORDERBOOK_WS_RECONNECT_BACKOFF_SEC", default=1.0),
            stream_wait_ms=stream_wait_ms,
            max_cached_levels=stream_max_levels,
        )
    return direct


def build_orderbook_provider_from_env(
    *,
    client: PolymarketClobClient | None = None,
    source_name: str,
    subscribe_on_read: bool = False,
) -> OrderbookProvider:
    hub_url = (os.getenv("PM15MIN_ORDERBOOK_HUB_URL") or "").strip() or None
    strict = _env_bool("PM15MIN_ORDERBOOK_HUB_STRICT", default=False)
    streaming = _env_bool("PM15MIN_ORDERBOOK_STREAMING", default=False)
    ws_url = (os.getenv("PM15MIN_ORDERBOOK_WS_URL") or "").strip() or None
    stream_wait_ms = _env_int("PM15MIN_ORDERBOOK_STREAM_WAIT_MS", default=120)
    stream_max_levels = _env_int(
        "PM15MIN_ORDERBOOK_STREAM_MAX_LEVELS",
        default=_env_int("PM15MIN_LIVE_ORDERBOOK_PROVIDER_LEVELS", default=20),
    )
    return build_orderbook_provider(
        client=client,
        hub_url=hub_url,
        source_name=source_name,
        subscribe_on_read=subscribe_on_read,
        fallback_to_direct=not strict,
        streaming=streaming,
        ws_url=ws_url,
        stream_wait_ms=stream_wait_ms,
        stream_max_levels=stream_max_levels,
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
    for key in ("timestamp", "ts", "orderbook_ts", "source_ts_ms", "captured_ts_ms", "__provider_fetched_at_ms"):
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


def _env_int(name: str, *, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_float(name: str, *, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _prefetch_worker_count() -> int:
    return max(1, _env_int("PM15MIN_ORDERBOOK_PREFETCH_WORKERS", default=4))


def _normalize_price_levels(levels: object, *, reverse: bool) -> list[dict[str, float]]:
    out: list[dict[str, float]] = []
    if not isinstance(levels, list):
        return out
    for level in levels:
        if not isinstance(level, dict):
            continue
        price = _float_or_none(level.get("price"))
        size = _float_or_none(level.get("size") or level.get("qty"))
        if price is None or price <= 0 or size is None or size < 0:
            continue
        out.append(
            {
                "price": round(price, 8),
                "size": round(size, 8),
            }
        )
    out.sort(key=lambda item: item["price"], reverse=reverse)
    return out


def _apply_price_change(
    *,
    levels: list[dict[str, float]],
    price: object,
    size: object,
    reverse: bool,
    limit: int,
) -> list[dict[str, float]]:
    normalized_price = _float_or_none(price)
    normalized_size = _float_or_none(size)
    if normalized_price is None:
        return list(levels)
    level_map = {
        round(float(level.get("price", 0.0)), 8): round(float(level.get("size", 0.0)), 8)
        for level in list(levels)
        if _float_or_none(level.get("price")) is not None and _float_or_none(level.get("size")) is not None
    }
    price_key = round(float(normalized_price), 8)
    if normalized_size is None or normalized_size <= 0.0:
        level_map.pop(price_key, None)
    else:
        level_map[price_key] = round(float(normalized_size), 8)
    updated = [{"price": price_key, "size": size_value} for price_key, size_value in level_map.items()]
    updated.sort(key=lambda item: item["price"], reverse=reverse)
    return updated[: max(1, int(limit))]


def _serialize_price_levels(levels: list[dict[str, float]]) -> list[dict[str, str]]:
    return [
        {
            "price": _format_decimal(level.get("price")),
            "size": _format_decimal(level.get("size")),
        }
        for level in list(levels)
        if _float_or_none(level.get("price")) is not None and _float_or_none(level.get("size")) is not None
    ]


def _format_decimal(value: object) -> str:
    normalized = _float_or_none(value)
    if normalized is None:
        return ""
    return f"{float(normalized):.8f}".rstrip("0").rstrip(".") or "0"


def _float_or_none(raw: object) -> float | None:
    if raw in (None, ""):
        return None
    try:
        return float(raw)
    except Exception:
        return None
