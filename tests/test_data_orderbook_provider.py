from __future__ import annotations

from pm15min.data.sources.orderbook_provider import (
    DirectOrderbookProvider,
    HubOrderbookProvider,
    InMemoryCachedOrderbookProvider,
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
