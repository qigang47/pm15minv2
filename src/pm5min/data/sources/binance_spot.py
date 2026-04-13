from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd
import requests


BINANCE_BASE_URLS = (
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
)

BINANCE_KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]


@dataclass(frozen=True)
class BinanceSpotKlinesRequest:
    symbol: str
    interval: str
    start_time_ms: int | None = None
    end_time_ms: int | None = None
    limit: int = 1000


class BinanceSpotKlinesClient:
    def __init__(
        self,
        *,
        base_urls: Iterable[str] | None = None,
        session: requests.Session | None = None,
        timeout_sec: float = 10.0,
    ) -> None:
        self.base_urls = tuple(base_urls or BINANCE_BASE_URLS)
        self.session = session or requests.Session()
        self.timeout_sec = max(1.0, float(timeout_sec))

    def fetch_klines(self, request: BinanceSpotKlinesRequest) -> pd.DataFrame:
        last_error: Exception | None = None
        for base_url in self.base_urls:
            try:
                return self._fetch_from_base(base_url, request)
            except Exception as exc:
                last_error = exc
                continue
        if last_error is None:
            raise RuntimeError("No Binance base URLs configured.")
        raise RuntimeError(f"Failed to fetch Binance klines for {request.symbol}: {last_error}") from last_error

    def _fetch_from_base(self, base_url: str, request: BinanceSpotKlinesRequest) -> pd.DataFrame:
        params: dict[str, object] = {
            "symbol": request.symbol,
            "interval": request.interval,
            "limit": max(1, min(int(request.limit), 1000)),
        }
        if request.start_time_ms is not None:
            params["startTime"] = int(request.start_time_ms)
        if request.end_time_ms is not None:
            params["endTime"] = int(request.end_time_ms)
        response = self.session.get(
            f"{base_url}/api/v3/klines",
            params=params,
            timeout=self.timeout_sec,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list) or not payload:
            return pd.DataFrame(columns=BINANCE_KLINE_COLUMNS)
        return pd.DataFrame(payload, columns=BINANCE_KLINE_COLUMNS)
