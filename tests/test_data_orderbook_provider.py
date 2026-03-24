from __future__ import annotations

from pm15min.data.sources.orderbook_provider import (
    DirectOrderbookProvider,
    HubOrderbookProvider,
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
