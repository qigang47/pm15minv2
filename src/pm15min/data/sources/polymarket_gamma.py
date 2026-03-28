from __future__ import annotations

import ast
import json
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests

from ..contracts import MarketCatalogRecord
from ..layout import cycle_seconds, normalize_cycle


GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
STREAM_RE = re.compile(r"data\.chain\.link/streams/([a-z0-9]+)-usd", re.I)
SLUG_RE_BY_CYCLE = {
    "5m": re.compile(r"^(btc|eth|sol|xrp)-(?:updown|up-or-down)-5m-(\d+)$", re.I),
    "15m": re.compile(r"^(btc|eth|sol|xrp)-(?:updown|up-or-down)-15m-(\d+)$", re.I),
}


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_ts(raw: str | None) -> int | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    if len(text) == 10:
        dt = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp())


def _extract_asset(payload: dict[str, Any], cycle: str) -> str:
    resolution_source = str(payload.get("resolutionSource") or "").lower()
    match = STREAM_RE.search(resolution_source)
    if match:
        return match.group(1).lower()

    slug = str(payload.get("slug") or "").lower()
    slug_match = SLUG_RE_BY_CYCLE[cycle].match(slug)
    if slug_match:
        return slug_match.group(1).lower()

    title = str(payload.get("title") or payload.get("question") or "").lower()
    for asset, needle in (("btc", "bitcoin"), ("eth", "ethereum"), ("sol", "solana"), ("xrp", "xrp")):
        if needle in title:
            return asset
    return ""


def _cycle_start_from_slug(slug: str, cycle: str) -> int | None:
    match = SLUG_RE_BY_CYCLE[cycle].match(str(slug or "").strip().lower())
    if not match:
        return None
    try:
        return int(match.group(2))
    except Exception:
        return None


def _coerce_list(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return []
    for parser in (json.loads, ast.literal_eval):
        try:
            parsed = parser(text)
        except Exception:
            continue
        if isinstance(parsed, list):
            return parsed
    return []


def _map_token_ids(outcomes: object, clob_token_ids: object) -> tuple[str, str]:
    outs = [str(x or "").strip().lower() for x in _coerce_list(outcomes)]
    toks = [str(x or "").strip() for x in _coerce_list(clob_token_ids)]

    if len(toks) != 2:
        return "", ""
    if len(outs) == 2:
        token_up = ""
        token_down = ""
        for idx, outcome in enumerate(outs):
            if outcome in {"up", "yes"}:
                token_up = toks[idx]
            elif outcome in {"down", "no"}:
                token_down = toks[idx]
        if token_up or token_down:
            return token_up, token_down
    return toks[0], toks[1]


def _normalize_outcome_side(value: object) -> str:
    token = str(value or "").strip().lower()
    if token in {"up", "yes"}:
        return "UP"
    if token in {"down", "no"}:
        return "DOWN"
    return str(value or "").strip().upper()


def resolve_winner_side_from_market(market: dict[str, Any]) -> str:
    winner = _normalize_outcome_side(market.get("winner") or market.get("winningOutcome"))
    if winner in {"UP", "DOWN"}:
        return winner

    outcomes = [str(item or "").strip() for item in _coerce_list(market.get("outcomes"))]
    prices_raw = _coerce_list(market.get("outcomePrices"))
    if len(outcomes) != len(prices_raw) or not outcomes:
        return ""

    prices: list[float] = []
    for item in prices_raw:
        try:
            prices.append(float(item))
        except Exception:
            return ""
    if not prices:
        return ""
    best_price = max(prices)
    if best_price <= 0:
        return ""
    if sum(abs(price - best_price) <= 1e-12 for price in prices) != 1:
        return ""
    winner_idx = max(range(len(prices)), key=prices.__getitem__)
    return _normalize_outcome_side(outcomes[winner_idx])


def _is_cycle_stream_event(event: dict[str, Any], cycle: str) -> bool:
    resolution_source = str(event.get("resolutionSource") or "").lower()
    if "data.chain.link/streams/" not in resolution_source:
        return False
    series_slug = str(event.get("seriesSlug") or "").lower()
    if series_slug.endswith(f"-up-or-down-{cycle}"):
        return True

    series = event.get("series") or []
    if isinstance(series, list):
        for item in series:
            recurrence = str((item or {}).get("recurrence") or "").lower().strip()
            slug = str((item or {}).get("slug") or "").lower()
            if recurrence == cycle:
                return True
            if cycle == "5m" and recurrence in {"5m", "5min", "5mins", "5 minutes"}:
                return True
            if slug.endswith(f"-up-or-down-{cycle}"):
                return True

    return bool(SLUG_RE_BY_CYCLE[cycle].match(str(event.get("slug") or "").lower()))


def _coerce_event_payloads(value: object) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in _coerce_list(value):
        if isinstance(item, dict):
            out.append(item)
    return out


def _first_event(value: object) -> dict[str, Any]:
    events = _coerce_event_payloads(value)
    if events:
        return events[0]
    return {}


def _build_market_catalog_record(
    *,
    market: dict[str, Any],
    event: dict[str, Any] | None,
    asset: str,
    cycle: str,
    snapshot_ts: str,
) -> MarketCatalogRecord | None:
    event = event or {}
    market_id = str(market.get("id") or market.get("market_id") or "").strip()
    if not market_id:
        return None
    resolved_asset = _extract_asset(market, cycle) or _extract_asset(event, cycle)
    if resolved_asset != asset:
        return None

    slug = str(market.get("slug") or "").strip()
    cycle_start_ts = _cycle_start_from_slug(slug, cycle)
    cycle_end_ts = _parse_ts(str(market.get("endDate") or event.get("endDate") or market.get("end_date") or ""))
    if cycle_start_ts is None and cycle_end_ts is not None:
        cycle_start_ts = cycle_end_ts - cycle_seconds(cycle)
    if cycle_start_ts is None or cycle_end_ts is None:
        return None

    token_up, token_down = _map_token_ids(market.get("outcomes"), market.get("clobTokenIds"))
    if not token_up and not token_down:
        token_up, token_down = _map_token_ids(event.get("outcomes"), event.get("clobTokenIds"))
    closed_ts = _parse_ts(str(market.get("closedTime") or event.get("closedTime") or ""))
    series_slug = str(event.get("seriesSlug") or "")
    if not series_slug:
        series = _coerce_list(event.get("series"))
        if series:
            first_series = series[0]
            if isinstance(first_series, dict):
                series_slug = str(first_series.get("slug") or "")

    return MarketCatalogRecord(
        market_id=market_id,
        condition_id=str(market.get("conditionId") or market.get("condition_id") or ""),
        asset=asset,
        cycle=cycle,
        cycle_start_ts=int(cycle_start_ts),
        cycle_end_ts=int(cycle_end_ts),
        token_up=token_up,
        token_down=token_down,
        slug=slug,
        question=str(market.get("question") or ""),
        resolution_source=str(market.get("resolutionSource") or event.get("resolutionSource") or ""),
        event_id=str(event.get("id") or ""),
        event_slug=str(event.get("slug") or ""),
        event_title=str(event.get("title") or ""),
        series_slug=series_slug,
        closed_ts=closed_ts,
        source_snapshot_ts=snapshot_ts,
    )


class GammaEventsClient:
    def __init__(
        self,
        session: requests.Session | None = None,
        base_url: str = GAMMA_EVENTS_URL,
        markets_url: str = GAMMA_MARKETS_URL,
    ) -> None:
        self.session = session or requests.Session()
        self.base_url = base_url
        self.markets_url = markets_url

    def fetch_closed_events(
        self,
        *,
        start_ts: int,
        end_ts: int,
        limit: int,
        max_pages: int | None,
        sleep_sec: float,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        limit = max(1, int(limit))
        page = 0
        page_cap = None if max_pages is None else max(1, int(max_pages))
        while page_cap is None or page < page_cap:
            params = {
                "closed": "true",
                "end_date_min": _iso(start_ts),
                "end_date_max": _iso(end_ts),
                "limit": limit,
                "offset": page * limit,
            }
            resp = self.session.get(self.base_url, params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, list) or not payload:
                break
            rows.extend(item for item in payload if isinstance(item, dict))
            if len(payload) < limit:
                break
            page += 1
            if sleep_sec > 0:
                time.sleep(float(sleep_sec))
        return rows

    def fetch_closed_markets(
        self,
        *,
        start_ts: int,
        end_ts: int,
        limit: int,
        max_pages: int | None,
        sleep_sec: float,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        limit = max(1, int(limit))
        page = 0
        page_cap = None if max_pages is None else max(1, int(max_pages))
        while page_cap is None or page < page_cap:
            params = {
                "closed": "true",
                "end_date_min": _iso(start_ts),
                "end_date_max": _iso(end_ts),
                "limit": limit,
                "offset": page * limit,
            }
            resp = self.session.get(self.markets_url, params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, list) or not payload:
                break
            rows.extend(item for item in payload if isinstance(item, dict))
            if len(payload) < limit:
                break
            page += 1
            if sleep_sec > 0:
                time.sleep(float(sleep_sec))
        return rows

    def fetch_markets_by_ids(
        self,
        market_ids: list[str],
        *,
        sleep_sec: float = 0.0,
        max_retries: int = 6,
    ) -> list[dict[str, Any]]:
        ids = [str(market_id).strip() for market_id in market_ids if str(market_id).strip()]
        if not ids:
            return []
        last_err = ""
        for attempt in range(max(1, int(max_retries))):
            resp = self.session.get(self.markets_url, params={"id": ids}, timeout=30)
            status_code = int(getattr(resp, "status_code", 200))
            if status_code == 200:
                payload = resp.json()
                if sleep_sec > 0:
                    time.sleep(float(sleep_sec))
                if not isinstance(payload, list):
                    return []
                return [item for item in payload if isinstance(item, dict)]
            last_err = f"status={status_code}: {(getattr(resp, 'text', '') or '')[:200]}"
            if status_code in {429, 500, 502, 503, 504}:
                time.sleep(min(20.0, 0.5 * (attempt + 1)))
                continue
            resp.raise_for_status()
        raise RuntimeError(f"/markets by ids failed after retries: {last_err}")

    def fetch_active_markets(
        self,
        *,
        start_ts: int,
        end_ts: int,
        limit: int,
        max_pages: int | None,
        sleep_sec: float,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        limit = max(1, int(limit))
        page = 0
        page_cap = None if max_pages is None else max(1, int(max_pages))
        while page_cap is None or page < page_cap:
            params = {
                "active": "true",
                "closed": "false",
                "end_date_min": _iso(start_ts),
                "end_date_max": _iso(end_ts),
                "limit": limit,
                "offset": page * limit,
            }
            resp = self.session.get(self.markets_url, params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, list) or not payload:
                break
            rows.extend(item for item in payload if isinstance(item, dict))
            if len(payload) < limit:
                break
            page += 1
            if sleep_sec > 0:
                time.sleep(float(sleep_sec))
        return rows


def build_market_catalog_records(
    *,
    events: list[dict[str, Any]],
    asset: str,
    cycle: str,
    snapshot_ts: str,
) -> list[MarketCatalogRecord]:
    cycle = normalize_cycle(cycle)
    asset = asset.strip().lower()
    cycle_sec = cycle_seconds(cycle)
    by_market_id: dict[str, MarketCatalogRecord] = {}

    for event in events:
        if not _is_cycle_stream_event(event, cycle):
            continue
        markets = event.get("markets") or []
        if not isinstance(markets, list):
            continue
        for market in markets:
            if not isinstance(market, dict):
                continue
            market_id = str(market.get("id") or "").strip()
            if not market_id:
                continue
            record = _build_market_catalog_record(
                market=market,
                event=event,
                asset=asset,
                cycle=cycle,
                snapshot_ts=snapshot_ts,
            )
            if record is not None:
                by_market_id[market_id] = record

    return sorted(by_market_id.values(), key=lambda rec: (rec.cycle_start_ts, rec.market_id))


def build_market_catalog_records_from_markets(
    *,
    markets: list[dict[str, Any]],
    asset: str,
    cycle: str,
    snapshot_ts: str,
    include_closed: bool = False,
) -> list[MarketCatalogRecord]:
    cycle = normalize_cycle(cycle)
    asset = asset.strip().lower()
    by_market_id: dict[str, MarketCatalogRecord] = {}

    for market in markets:
        if not isinstance(market, dict):
            continue
        if not include_closed and bool(market.get("closed", False)):
            continue
        if not include_closed and not bool(market.get("active", False)):
            continue
        event = _first_event(market.get("events"))
        if not (_is_cycle_stream_event(market, cycle) or _is_cycle_stream_event(event, cycle)):
            continue
        record = _build_market_catalog_record(
            market=market,
            event=event,
            asset=asset,
            cycle=cycle,
            snapshot_ts=snapshot_ts,
        )
        if record is not None:
            by_market_id[record.market_id] = record

    return sorted(by_market_id.values(), key=lambda rec: (rec.cycle_start_ts, rec.market_id))
