from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Any

import requests


PAST_RESULTS_URL = "https://polymarket.com/api/past-results"
CRYPTO_PRICE_URL = "https://polymarket.com/api/crypto/crypto-price"
EVENT_PAGE_URL = "https://polymarket.com/event/{slug}"


def _iso_z(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _variant_slug(cycle_seconds: int) -> str:
    return "fiveminute" if int(cycle_seconds) == 300 else "fifteen"


def _event_slug_candidates(*, symbol: str, cycle_start_ts: int, cycle_seconds: int) -> tuple[str, ...]:
    cycle_label = "5m" if int(cycle_seconds) == 300 else "15m"
    symbol_slug = str(symbol or "").strip().lower()
    return (
        f"{symbol_slug}-updown-{cycle_label}-{int(cycle_start_ts)}",
        f"{symbol_slug}-up-or-down-{cycle_label}-{int(cycle_start_ts)}",
    )


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
            "variant": _variant_slug(cycle_seconds),
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

    def fetch_crypto_price_from_event_page(
        self,
        *,
        symbol: str,
        cycle_start_ts: int,
        cycle_seconds: int,
        sleep_sec: float = 0.02,
        max_retries: int = 3,
    ) -> dict[str, Any]:
        query_key = (
            f'"queryKey":["crypto-prices","price","{str(symbol).upper()}","{_iso_z(cycle_start_ts)}",'
            f'"{_variant_slug(cycle_seconds)}","{_iso_z(int(cycle_start_ts) + int(cycle_seconds))}"]'
        )
        state_re = re.compile(r'"state":\{"data":\{"openPrice":([^,]+),"closePrice":([^}]+)\}')
        last_err = ""
        for slug in _event_slug_candidates(symbol=symbol, cycle_start_ts=cycle_start_ts, cycle_seconds=cycle_seconds):
            url = EVENT_PAGE_URL.format(slug=slug)
            for attempt in range(max(1, int(max_retries))):
                try:
                    resp = self.session.get(url, timeout=self.timeout_sec)
                except Exception as exc:
                    last_err = str(exc)
                    time.sleep(min(2.0, 0.3 * (attempt + 1)))
                    continue
                if resp.status_code in {429, 500, 502, 503, 504}:
                    last_err = (resp.text or "")[:200]
                    time.sleep(min(5.0, 0.5 * (attempt + 1)))
                    continue
                if resp.status_code != 200:
                    last_err = f"status={resp.status_code}"
                    break
                text = resp.text or ""
                idx = text.find(query_key)
                if idx < 0:
                    last_err = "query_key_missing"
                    break
                window = text[max(0, idx - 800):idx]
                match = state_re.search(window)
                if match is None:
                    last_err = "state_payload_missing"
                    break
                try:
                    open_price = float(match.group(1))
                    close_price = float(match.group(2))
                except Exception as exc:
                    last_err = str(exc)
                    break
                payload = {
                    "openPrice": open_price,
                    "closePrice": close_price,
                    "completed": True,
                    "incomplete": False,
                    "cached": True,
                    "source": "event_page_dehydrated_state",
                }
                if sleep_sec > 0:
                    time.sleep(float(sleep_sec))
                return payload
        return {} if not last_err else {}

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
        timestamp_too_old = False
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
                last_err = (resp.text or "")[:200]
                timestamp_too_old = True
                break
            last_err = f"status={resp.status_code}: {(resp.text or '')[:200]}"
            break
        event_page_obj = self.fetch_crypto_price_from_event_page(
            symbol=symbol,
            cycle_start_ts=cycle_start_ts,
            cycle_seconds=cycle_seconds,
            sleep_sec=sleep_sec,
            max_retries=min(3, max(1, int(max_retries))),
        )
        if event_page_obj:
            return event_page_obj
        if timestamp_too_old:
            return {}
        raise RuntimeError(f"/api/crypto/crypto-price failed after retries: {last_err}")
