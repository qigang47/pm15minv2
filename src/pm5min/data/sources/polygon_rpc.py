from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


DEFAULT_RPC_URLS = [
    "https://polygon.drpc.org",
    "https://polygon-bor-rpc.publicnode.com",
]


@dataclass
class PolygonRpcClient:
    urls: list[str]
    timeout_sec: float = 25.0

    def __init__(self, urls: list[str] | None = None, timeout_sec: float = 25.0) -> None:
        resolved = [url.strip() for url in (urls or DEFAULT_RPC_URLS) if str(url).strip()]
        if not resolved:
            raise ValueError("No RPC URLs configured.")
        self.urls = list(dict.fromkeys(resolved))
        self.timeout_sec = float(timeout_sec)
        self.primary_url = self.urls[0]
        self._session = requests.Session()

    def _ordered_urls(self) -> list[str]:
        if self.primary_url in self.urls:
            return [self.primary_url] + [url for url in self.urls if url != self.primary_url]
        return list(self.urls)

    def call(self, method: str, params: list[Any], retries: int = 4) -> Any:
        last_err: str | None = None
        for _ in range(max(1, int(retries))):
            for url in self._ordered_urls():
                payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
                try:
                    resp = self._session.post(url, json=payload, timeout=self.timeout_sec)
                    resp.raise_for_status()
                    obj = resp.json()
                except Exception as exc:
                    last_err = f"{url}: request failed: {exc}"
                    continue
                if "error" in obj:
                    last_err = f"{url}: rpc error: {obj['error']}"
                    continue
                self.primary_url = url
                return obj.get("result")
        raise RuntimeError(last_err or f"RPC call failed: {method}")

    def eth_block_number(self) -> int:
        return int(self.call("eth_blockNumber", [], retries=5), 16)

    def eth_block_timestamp(self, block_number: int, cache: dict[int, int] | None = None) -> int:
        cache = {} if cache is None else cache
        if block_number in cache:
            return cache[block_number]
        blk = self.call("eth_getBlockByNumber", [hex(int(block_number)), False], retries=5)
        ts = int((blk or {}).get("timestamp", "0x0"), 16)
        cache[block_number] = ts
        return ts

    def find_first_block_at_or_after_ts(self, target_ts: int, lo_block: int, hi_block: int) -> int:
        cache: dict[int, int] = {}
        lo = max(1, int(lo_block))
        hi = max(lo, int(hi_block))
        while lo < hi:
            mid = (lo + hi) // 2
            mid_ts = self.eth_block_timestamp(mid, cache)
            if mid_ts < int(target_ts):
                lo = mid + 1
            else:
                hi = mid
        return lo
