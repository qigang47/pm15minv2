from __future__ import annotations

import json
import threading
import time

from pm15min.data.sources.orderbook_provider import (
    DirectOrderbookProvider,
    HubOrderbookProvider,
    InMemoryCachedOrderbookProvider,
    WebsocketOrderbookProvider,
    build_orderbook_provider,
)


class _FakeClobClient:
    def fetch_book(self, token_id: str, *, levels: int = 0, timeout_sec: float = 1.2):
        return {
            "token_id": token_id,
            "levels": levels,
            "timeout_sec": timeout_sec,
            "asks": [{"price": "0.12", "size": "5"}],
            "bids": [{"price": "0.11", "size": "4"}],
        }


class _FakeSession:
    def __init__(self, payload: dict | None = None, raise_on_get: Exception | None = None) -> None:
        self.payload = payload or {}
        self.raise_on_get = raise_on_get
        self.posts: list[tuple[str, dict, float]] = []
        self.gets: list[tuple[str, dict, float]] = []

    def post(self, url, json, timeout):
        self.posts.append((url, json, timeout))
        return _FakeResponse({"ok": True})

    def get(self, url, params, timeout):
        self.gets.append((url, params, timeout))
        if self.raise_on_get is not None:
            raise self.raise_on_get
        return _FakeResponse(self.payload)


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self.payload


def test_build_orderbook_provider_defaults_to_direct() -> None:
    provider = build_orderbook_provider(client=_FakeClobClient(), hub_url=None, source_name="test")
    assert isinstance(provider, DirectOrderbookProvider)


def test_build_orderbook_provider_can_use_websocket_streaming() -> None:
    provider = build_orderbook_provider(
        client=_FakeClobClient(),
        hub_url=None,
        source_name="test",
        streaming=True,
    )

    assert isinstance(provider, WebsocketOrderbookProvider)


def test_direct_orderbook_provider_tags_fetch_time_metadata() -> None:
    provider = DirectOrderbookProvider(client=_FakeClobClient())

    payload = provider.get_orderbook_summary("token-1", levels=20, timeout=1.2)

    assert payload is not None
    assert payload["token_id"] == "token-1"
    assert payload["__provider_source"] == "direct"
    assert isinstance(payload["__provider_fetched_at_ms"], int)
    assert payload["__provider_cache_hit"] is False
    assert payload["__provider_force_refresh_used"] is False


def test_hub_orderbook_provider_falls_back_to_direct() -> None:
    session = _FakeSession(raise_on_get=RuntimeError("hub down"))
    provider = HubOrderbookProvider(
        hub_url="http://127.0.0.1:18115",
        source_name="test",
        fallback_provider=DirectOrderbookProvider(client=_FakeClobClient()),
        session=session,
    )

    payload = provider.get_orderbook_summary("token-1", levels=0, timeout=1.2)

    assert payload is not None
    assert payload["token_id"] == "token-1"


def test_hub_orderbook_provider_syncs_subscriptions() -> None:
    session = _FakeSession(payload={"orderbook": {"asks": [], "bids": []}})
    provider = HubOrderbookProvider(
        hub_url="http://127.0.0.1:18115",
        source_name="test",
        fallback_provider=None,
        session=session,
    )

    out = provider.sync_subscriptions(["token-1", "token-2"], replace=True, prefetch=True, levels=0, timeout=1.2)

    assert out == {"ok": True}
    assert session.posts


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)

    def __call__(self) -> float:
        return float(self.now)


class _FakeWallClock:
    def __init__(self, now_ms: int) -> None:
        self.now_ms = int(now_ms)

    def advance(self, ms: int) -> None:
        self.now_ms += int(ms)

    def __call__(self) -> int:
        return int(self.now_ms)


def test_in_memory_cached_orderbook_provider_reuses_fresh_payload() -> None:
    clock = _FakeClock()
    wall_clock = _FakeWallClock(1_700_000_000_000)
    client = _FakeClobClient()
    wrapped = DirectOrderbookProvider(client=client)
    provider = InMemoryCachedOrderbookProvider(
        wrapped=wrapped,
        max_age_ms=500,
        monotonic_fn=clock,
        now_ms_fn=wall_clock,
    )

    first = provider.get_orderbook_summary("token-1", levels=0, timeout=1.2)
    second = provider.get_orderbook_summary("token-1", levels=0, timeout=1.2)

    assert first is not None
    assert second is not None
    assert first["token_id"] == "token-1"
    assert second["token_id"] == "token-1"


def test_in_memory_cached_orderbook_provider_refreshes_after_max_age() -> None:
    clock = _FakeClock()
    wall_clock = _FakeWallClock(1_700_000_000_000)

    class _CountingProvider:
        def __init__(self) -> None:
            self.calls = 0

        def get_orderbook_summary(self, token_id, *, levels=0, timeout=1.2, force_refresh=False):
            self.calls += 1
            return {
                "token_id": token_id,
                "levels": levels,
                "calls": self.calls,
                "timestamp": wall_clock(),
                "asks": [{"price": "0.12", "size": "5"}],
                "bids": [{"price": "0.11", "size": "4"}],
            }

        def sync_subscriptions(self, token_ids, *, replace=True, prefetch=False, levels=0, timeout=1.2):
            return {"ok": True}

    wrapped = _CountingProvider()
    provider = InMemoryCachedOrderbookProvider(
        wrapped=wrapped,
        max_age_ms=500,
        monotonic_fn=clock,
        now_ms_fn=wall_clock,
    )

    first = provider.get_orderbook_summary("token-1", levels=0, timeout=1.2)
    clock.advance(0.2)
    wall_clock.advance(200)
    second = provider.get_orderbook_summary("token-1", levels=0, timeout=1.2)
    clock.advance(0.6)
    wall_clock.advance(600)
    third = provider.get_orderbook_summary("token-1", levels=0, timeout=1.2)

    assert first["calls"] == 1
    assert second["calls"] == 1
    assert third["calls"] == 2


def test_in_memory_cached_orderbook_provider_retries_when_payload_timestamp_is_stale() -> None:
    clock = _FakeClock()
    wall_clock = _FakeWallClock(1_700_000_001_000)

    class _CountingProvider:
        def __init__(self) -> None:
            self.calls = 0

        def get_orderbook_summary(self, token_id, *, levels=0, timeout=1.2, force_refresh=False):
            self.calls += 1
            snapshot_ts_ms = wall_clock() - (600 if self.calls == 1 else 50)
            return {
                "token_id": token_id,
                "levels": levels,
                "calls": self.calls,
                "timestamp": snapshot_ts_ms,
                "asks": [{"price": "0.12", "size": "5"}],
                "bids": [{"price": "0.11", "size": "4"}],
            }

        def sync_subscriptions(self, token_ids, *, replace=True, prefetch=False, levels=0, timeout=1.2):
            return {"ok": True}

    provider = InMemoryCachedOrderbookProvider(
        wrapped=_CountingProvider(),
        max_age_ms=300,
        monotonic_fn=clock,
        now_ms_fn=wall_clock,
    )

    payload = provider.get_orderbook_summary("token-1", levels=0, timeout=1.2)

    assert payload["calls"] == 2


def test_in_memory_cached_orderbook_provider_rejects_retry_payload_when_still_stale() -> None:
    clock = _FakeClock()
    wall_clock = _FakeWallClock(1_700_000_001_000)

    class _CountingProvider:
        def __init__(self) -> None:
            self.calls = 0

        def get_orderbook_summary(self, token_id, *, levels=0, timeout=1.2, force_refresh=False):
            self.calls += 1
            snapshot_ts_ms = wall_clock() - 600
            return {
                "token_id": token_id,
                "levels": levels,
                "calls": self.calls,
                "timestamp": snapshot_ts_ms,
                "asks": [{"price": "0.12", "size": "5"}],
                "bids": [{"price": "0.11", "size": "4"}],
            }

        def sync_subscriptions(self, token_ids, *, replace=True, prefetch=False, levels=0, timeout=1.2):
            return {"ok": True}

    provider = InMemoryCachedOrderbookProvider(
        wrapped=_CountingProvider(),
        max_age_ms=300,
        monotonic_fn=clock,
        now_ms_fn=wall_clock,
    )

    payload = provider.get_orderbook_summary("token-1", levels=0, timeout=1.2)

    assert payload is None


def test_in_memory_cached_orderbook_provider_background_refreshes_tracked_tokens() -> None:
    clock = _FakeClock()
    wall_clock = _FakeWallClock(1_700_000_000_000)

    class _CountingProvider:
        def __init__(self) -> None:
            self.calls = 0

        def get_orderbook_summary(self, token_id, *, levels=0, timeout=1.2, force_refresh=False):
            self.calls += 1
            return {
                "token_id": token_id,
                "levels": levels,
                "calls": self.calls,
                "timestamp": wall_clock(),
                "asks": [{"price": "0.12", "size": "5"}],
                "bids": [{"price": "0.11", "size": "4"}],
            }

        def sync_subscriptions(self, token_ids, *, replace=True, prefetch=False, levels=0, timeout=1.2):
            return {"ok": True}

    provider = InMemoryCachedOrderbookProvider(
        wrapped=_CountingProvider(),
        max_age_ms=300,
        monotonic_fn=clock,
        now_ms_fn=wall_clock,
        background_refresh_ms=1,
        refresh_timeout_sec=0.1,
    )
    provider.get_orderbook_summary("token-1", levels=50, timeout=1.2)
    wall_clock.advance(10)
    clock.advance(0.01)
    import time as _time

    _time.sleep(0.05)
    payload = provider.get_orderbook_summary("token-1", levels=50, timeout=1.2)
    assert payload["calls"] >= 1


def test_in_memory_cached_orderbook_provider_prefetches_and_skips_unchanged_replace() -> None:
    clock = _FakeClock()
    wall_clock = _FakeWallClock(1_700_000_000_000)

    class _CountingProvider:
        def __init__(self) -> None:
            self.get_calls = 0
            self.sync_calls = 0

        def get_orderbook_summary(self, token_id, *, levels=0, timeout=1.2, force_refresh=False):
            self.get_calls += 1
            return {
                "token_id": token_id,
                "levels": levels,
                "calls": self.get_calls,
                "timestamp": wall_clock(),
                "asks": [{"price": "0.12", "size": "5"}],
                "bids": [{"price": "0.11", "size": "4"}],
            }

        def sync_subscriptions(self, token_ids, *, replace=True, prefetch=False, levels=0, timeout=1.2):
            self.sync_calls += 1
            return {
                "ok": True,
                "token_ids": list(token_ids),
                "replace": bool(replace),
                "prefetch": bool(prefetch),
                "levels": int(levels),
            }

    wrapped = _CountingProvider()
    provider = InMemoryCachedOrderbookProvider(
        wrapped=wrapped,
        max_age_ms=300,
        monotonic_fn=clock,
        now_ms_fn=wall_clock,
    )

    first = provider.sync_subscriptions(["token-1"], replace=True, prefetch=True, levels=20, timeout=1.2)
    second = provider.sync_subscriptions(["token-1"], replace=True, prefetch=True, levels=20, timeout=1.2)
    payload = provider.get_orderbook_summary("token-1", levels=20, timeout=1.2)

    assert first["prefetched"] == 1
    assert first["cache_hits"] == 0
    assert second["skipped"] is True
    assert wrapped.sync_calls == 1
    assert wrapped.get_calls == 1
    assert payload["calls"] == 1


def test_in_memory_cached_orderbook_provider_prefetches_in_parallel() -> None:
    clock = _FakeClock()
    wall_clock = _FakeWallClock(1_700_000_000_000)

    class _ConcurrentProvider:
        def __init__(self) -> None:
            self.lock = threading.Lock()
            self.active = 0
            self.max_active = 0

        def get_orderbook_summary(self, token_id, *, levels=0, timeout=1.2, force_refresh=False):
            del timeout, force_refresh
            with self.lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            time.sleep(0.05)
            with self.lock:
                self.active -= 1
            return {
                "token_id": token_id,
                "levels": levels,
                "timestamp": wall_clock(),
                "asks": [{"price": "0.12", "size": "5"}],
                "bids": [{"price": "0.11", "size": "4"}],
            }

        def sync_subscriptions(self, token_ids, *, replace=True, prefetch=False, levels=0, timeout=1.2):
            del token_ids, replace, prefetch, levels, timeout
            return {"ok": True}

    wrapped = _ConcurrentProvider()
    provider = InMemoryCachedOrderbookProvider(
        wrapped=wrapped,
        max_age_ms=300,
        monotonic_fn=clock,
        now_ms_fn=wall_clock,
    )

    out = provider.sync_subscriptions(
        ["token-1", "token-2", "token-3"],
        replace=True,
        prefetch=True,
        levels=20,
        timeout=1.2,
    )

    assert out["prefetched"] == 3
    assert out["prefetch_workers"] >= 2
    assert wrapped.max_active >= 2


def test_websocket_orderbook_provider_uses_stream_cache_before_fallback() -> None:
    class _FallbackProvider:
        def __init__(self) -> None:
            self.calls = 0

        def get_orderbook_summary(self, token_id, *, levels=0, timeout=1.2, force_refresh=False):
            del token_id, levels, timeout, force_refresh
            self.calls += 1
            return None

        def sync_subscriptions(self, token_ids, *, replace=True, prefetch=False, levels=0, timeout=1.2):
            del token_ids, replace, prefetch, levels, timeout
            return {"ok": True}

    fallback = _FallbackProvider()
    provider = WebsocketOrderbookProvider(
        fallback_provider=fallback,
        subscribe_on_read=True,
        stream_wait_ms=0,
        max_cached_levels=20,
    )
    provider._ensure_started = lambda: None  # type: ignore[method-assign]
    provider.sync_subscriptions(["token-1"], replace=True, prefetch=False, levels=20, timeout=1.2)

    provider._handle_stream_message(
        json.dumps(
            {
                "event_type": "book",
                "asset_id": "token-1",
                "market": "market-1",
                "timestamp": "2026-03-31T12:00:00Z",
                "asks": [
                    {"price": "0.25", "size": "4"},
                    {"price": "0.26", "size": "3"},
                ],
                "bids": [
                    {"price": "0.22", "size": "5"},
                    {"price": "0.21", "size": "6"},
                ],
            }
        )
    )
    provider._handle_stream_message(
        json.dumps(
            {
                "event_type": "price_change",
                "market": "market-1",
                "timestamp": "2026-03-31T12:00:00.100Z",
                "price_changes": [
                    {
                        "asset_id": "token-1",
                        "side": "SELL",
                        "price": "0.24",
                        "size": "9",
                        "best_bid": "0.22",
                        "best_ask": "0.24",
                    }
                ],
            }
        )
    )

    payload = provider.get_orderbook_summary("token-1", levels=20, timeout=0.01)

    assert payload is not None
    assert payload["__provider_source"] == "websocket"
    assert payload["best_ask"] == "0.24"
    assert payload["best_bid"] == "0.22"
    assert payload["asks"][0]["price"] == "0.24"
    assert fallback.calls == 0


def test_websocket_orderbook_provider_does_not_let_older_fallback_overwrite_newer_stream() -> None:
    class _FallbackProvider:
        def get_orderbook_summary(self, token_id, *, levels=0, timeout=1.2, force_refresh=False):
            del token_id, levels, timeout, force_refresh
            return {
                "timestamp": "2026-03-31T11:59:59Z",
                "__provider_fetched_at_ms": 1_743_422_399_000,
                "asks": [{"price": "0.10", "size": "9"}],
                "bids": [{"price": "0.09", "size": "8"}],
            }

        def sync_subscriptions(self, token_ids, *, replace=True, prefetch=False, levels=0, timeout=1.2):
            del token_ids, replace, prefetch, levels, timeout
            return {"ok": True}

    provider = WebsocketOrderbookProvider(
        fallback_provider=_FallbackProvider(),
        subscribe_on_read=True,
        stream_wait_ms=0,
        max_cached_levels=20,
    )
    provider._ensure_started = lambda: None  # type: ignore[method-assign]
    provider._handle_stream_message(
        json.dumps(
            {
                "event_type": "book",
                "asset_id": "token-1",
                "market": "market-1",
                "timestamp": "2026-03-31T12:00:00Z",
                "asks": [{"price": "0.25", "size": "4"}],
                "bids": [{"price": "0.24", "size": "5"}],
            }
        )
    )

    first = provider.get_orderbook_summary("token-1", levels=20, timeout=0.01)
    second = provider.get_orderbook_summary("token-1", levels=20, timeout=0.01, force_refresh=True)
    third = provider.get_orderbook_summary("token-1", levels=20, timeout=0.01)

    assert first is not None
    assert second is not None
    assert third is not None
    assert first["asks"][0]["price"] == "0.25"
    assert second["asks"][0]["price"] == "0.10"
    assert third["asks"][0]["price"] == "0.25"


def test_websocket_orderbook_provider_does_not_treat_truncated_fallback_as_full_deeper_cache() -> None:
    class _FallbackProvider:
        def __init__(self) -> None:
            self.calls = 0

        def get_orderbook_summary(self, token_id, *, levels=0, timeout=1.2, force_refresh=False):
            del token_id, timeout, force_refresh
            self.calls += 1
            return {
                "timestamp": "2026-03-31T12:00:00Z",
                "__provider_fetched_at_ms": 1_743_422_400_000,
                "asks": [
                    {"price": f"{0.10 + idx * 0.01:.2f}", "size": "1"}
                    for idx in range(max(0, int(levels)))
                ],
                "bids": [
                    {"price": f"{0.09 - idx * 0.01:.2f}", "size": "1"}
                    for idx in range(max(0, int(levels)))
                ],
            }

        def sync_subscriptions(self, token_ids, *, replace=True, prefetch=False, levels=0, timeout=1.2):
            del token_ids, replace, prefetch, levels, timeout
            return {"ok": True}

    fallback = _FallbackProvider()
    provider = WebsocketOrderbookProvider(
        fallback_provider=fallback,
        subscribe_on_read=True,
        stream_wait_ms=0,
        max_cached_levels=20,
    )
    provider._ensure_started = lambda: None  # type: ignore[method-assign]

    first = provider.get_orderbook_summary("token-1", levels=50, timeout=0.01)
    second = provider.get_orderbook_summary("token-1", levels=50, timeout=0.01)
    third = provider.get_orderbook_summary("token-1", levels=20, timeout=0.01)

    assert first is not None
    assert second is not None
    assert third is not None
    assert len(first["asks"]) == 50
    assert len(second["asks"]) == 50
    assert len(third["asks"]) == 20
    assert fallback.calls == 2
