from __future__ import annotations

import hashlib
import hmac
import os
from typing import Callable

import requests

from .chainlink_rpc import _decode_signed_report_payload


DEFAULT_BASE_URL = "https://api.dataengine.chain.link"


def _clean_env(*names: str) -> str:
    for name in names:
        raw = str(os.getenv(name) or "").strip()
        if raw:
            return raw
    return ""


class ChainlinkDataStreamsApiClient:
    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        api_key: str | None = None,
        api_secret: str | None = None,
        base_url: str | None = None,
        timeout_sec: float = 20.0,
        now_ms: Callable[[], int] | None = None,
    ) -> None:
        self.session = session or requests.Session()
        self.api_key = str(api_key or _clean_env("PM15MIN_CHAINLINK_STREAMS_API_KEY", "PM15MIN_CHAINLINK_STREAMS_USER_ID")).strip()
        self.api_secret = str(
            api_secret or _clean_env("PM15MIN_CHAINLINK_STREAMS_API_SECRET", "PM15MIN_CHAINLINK_STREAMS_USER_SECRET")
        ).strip()
        self.base_url = str(base_url or os.getenv("PM15MIN_CHAINLINK_STREAMS_API_BASE_URL") or DEFAULT_BASE_URL).strip().rstrip("/")
        self.timeout_sec = float(timeout_sec)
        self._now_ms = now_ms or (lambda: int(__import__("time").time() * 1000))

    def is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def fetch_report(self, *, feed_id: str, timestamp: int) -> dict[str, object] | None:
        if not self.is_configured():
            return None
        feed_id = str(feed_id or "").strip().lower()
        query = f"feedID={feed_id}&timestamp={int(timestamp)}"
        path = f"/api/v1/reports?{query}"
        response = self.session.get(
            f"{self.base_url}{path}",
            headers=self._auth_headers(method="GET", full_path=path),
            timeout=self.timeout_sec,
        )
        if response.status_code in {400, 404}:
            return None
        if response.status_code != 200:
            raise RuntimeError(f"chainlink_data_streams_report_failed:{response.status_code}:{(response.text or '')[:200]}")
        payload = response.json()
        report = payload.get("report") if isinstance(payload, dict) else None
        if not isinstance(report, dict):
            return None
        decoded = _decode_full_report(report.get("fullReport"))
        benchmark_price_raw = decoded.get("benchmark_price_raw")
        price = None if benchmark_price_raw is None else float(benchmark_price_raw) / 1e18
        return {
            "feed_id": str(report.get("feedID") or feed_id).strip().lower(),
            "valid_from_ts": int(report.get("validFromTimestamp") or decoded.get("valid_from_ts") or 0) or None,
            "observation_ts": int(report.get("observationsTimestamp") or decoded.get("observation_ts") or 0) or None,
            "expires_at_ts": decoded.get("expires_at_ts"),
            "benchmark_price_raw": benchmark_price_raw,
            "price": price,
            "source": "chainlink_data_streams_rest_api",
        }

    def _auth_headers(self, *, method: str, full_path: str) -> dict[str, str]:
        timestamp_ms = int(self._now_ms())
        body_hash = hashlib.sha256(b"").hexdigest()
        string_to_sign = f"{str(method).upper()} {full_path} {body_hash} {self.api_key} {timestamp_ms}"
        signature = hmac.new(self.api_secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        return {
            "Authorization": self.api_key,
            "X-Authorization-Timestamp": str(timestamp_ms),
            "X-Authorization-Signature-SHA256": signature,
        }


def _decode_full_report(full_report: object) -> dict[str, object]:
    raw = str(full_report or "").strip()
    if not raw:
        return {}
    if raw.startswith("0x"):
        raw = raw[2:]
    payload = bytes.fromhex(raw)
    decoded = _decode_signed_report_payload(payload)
    return {
        "valid_from_ts": decoded.get("valid_from_ts"),
        "observation_ts": decoded.get("observation_ts"),
        "expires_at_ts": decoded.get("expires_at_ts"),
        "benchmark_price_raw": decoded.get("benchmark_price_raw"),
    }
