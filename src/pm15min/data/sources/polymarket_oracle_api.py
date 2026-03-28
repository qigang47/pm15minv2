from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import requests


PAST_RESULTS_URL = "https://polymarket.com/api/past-results"
CRYPTO_PRICE_URL = "https://polymarket.com/api/crypto/crypto-price"


def _iso_z(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class PolymarketOracleApiClient:
    def __init__(self, session: requests.Session | None = None, timeout_sec: float = 20.0) -> None:
        self.session = session or requests.Session()
        self.timeout_sec = float(timeout_sec)

    def fetch_past_results_batch(
        self,
        *,
        symbol: str,
        current_event_start_time: str,
        cycle_seconds: int = 900,
        count: int = 50,
        sleep_sec: float = 0.15,
        max_retries: int = 6,
    ) -> list[dict[str, Any]]:
        params = {
            "symbol": symbol.upper(),
            "variant": "fiveminute" if int(cycle_seconds) == 300 else "fifteen",
            "assetType": "crypto",
            "currentEventStartTime": current_event_start_time,
            "count": int(count),
        }
        last_err = ""
        for attempt in range(max(1, int(max_retries))):
            try:
                resp = self.session.get(PAST_RESULTS_URL, params=params, timeout=self.timeout_sec)
            except Exception as exc:
                last_err = str(exc)
                time.sleep(min(5.0, 0.3 * (attempt + 1)))
                continue
            if resp.status_code == 200:
                obj = resp.json()
                rows = (obj.get("data") or {}).get("results") or []
                if sleep_sec > 0:
                    time.sleep(float(sleep_sec))
                return [row for row in rows if isinstance(row, dict)]
            if resp.status_code in {429, 500, 502, 503, 504}:
                last_err = (resp.text or "")[:200]
                time.sleep(min(30.0, 0.8 * (attempt + 1)))
                continue
            raise RuntimeError(f"/api/past-results failed status={resp.status_code}: {(resp.text or '')[:200]}")
        raise RuntimeError(f"/api/past-results failed after retries: {last_err}")

    def fetch_crypto_price(
        self,
        *,
        symbol: str,
        cycle_start_ts: int,
        cycle_seconds: int,
        sleep_sec: float = 0.02,
        max_retries: int = 4,
    ) -> dict[str, Any]:
        params = {
            "symbol": symbol.upper(),
            "eventStartTime": _iso_z(cycle_start_ts),
            "variant": "five" if int(cycle_seconds) == 300 else "fifteen",
            "endDate": _iso_z(int(cycle_start_ts) + int(cycle_seconds)),
        }
        last_err = ""
        for attempt in range(max(1, int(max_retries))):
            try:
                resp = self.session.get(CRYPTO_PRICE_URL, params=params, timeout=self.timeout_sec)
            except Exception as exc:
                last_err = str(exc)
                time.sleep(min(1.5, 0.2 * (attempt + 1)))
                continue
            if resp.status_code == 200:
                obj = resp.json()
                if sleep_sec > 0:
                    time.sleep(float(sleep_sec))
                return obj if isinstance(obj, dict) else {}
            if resp.status_code in {429, 500, 502, 503, 504}:
                last_err = (resp.text or "")[:200]
                time.sleep(min(10.0, 0.5 * (attempt + 1)))
                continue
            if resp.status_code == 400 and "Timestamp too old" in (resp.text or ""):
                return {}
            raise RuntimeError(f"/api/crypto/crypto-price failed status={resp.status_code}: {(resp.text or '')[:200]}")
        raise RuntimeError(f"/api/crypto/crypto-price failed after retries: {last_err}")
