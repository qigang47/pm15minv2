from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Protocol

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


def build_orderbook_provider_from_env(
    *,
    client: PolymarketClobClient | None = None,
    source_name: str | None = None,
    subscribe_on_read: bool = False,
) -> OrderbookProvider:
    del source_name, subscribe_on_read
    return DirectOrderbookProvider(client=client or PolymarketClobClient())
