from __future__ import annotations

import os
import threading
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
