from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests


def _normalize_levels(levels: object, *, reverse: bool) -> list[dict[str, float]]:
    out: list[dict[str, float]] = []
    if not isinstance(levels, list):
        return out
    for level in levels:
        if not isinstance(level, dict):
            continue
        try:
            price = float(level.get("price"))
            size = float(level.get("size") or level.get("qty"))
        except Exception:
            continue
        if price <= 0 or size <= 0:
            continue
        out.append({"price": round(price, 8), "size": round(size, 8)})
    out.sort(key=lambda item: item["price"], reverse=reverse)
    return out


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


class PolymarketClobClient:
    def __init__(self, session: requests.Session | None = None, base_url: str = "https://clob.polymarket.com") -> None:
        self.session = session or requests.Session()
        self.base_url = base_url.rstrip("/")

    def fetch_book(self, token_id: str, *, levels: int = 0, timeout_sec: float = 1.2) -> dict[str, Any]:
        params: dict[str, Any] = {"token_id": str(token_id)}
        if int(levels) > 0:
            params["limit"] = int(levels)
        resp = self.session.get(f"{self.base_url}/book", params=params, timeout=timeout_sec)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            raise ValueError("Invalid CLOB payload.")
        return payload


def normalize_book(payload: dict[str, Any]) -> tuple[list[dict[str, float]], list[dict[str, float]], int | None]:
    asks = _normalize_levels(payload.get("asks"), reverse=False)
    bids = _normalize_levels(payload.get("bids"), reverse=True)
    ts_ms = _timestamp_to_ms(payload.get("timestamp") or payload.get("ts"))
    return asks, bids, ts_ms
