from __future__ import annotations

from pm15min.data.sources.polymarket_oracle_api import PolymarketOracleApiClient


class _FakeResponse:
    def __init__(self, *, status_code: int = 200, payload: dict[str, object] | None = None, text: str = "") -> None:
        self.status_code = int(status_code)
        self._payload = payload if payload is not None else {"data": {"results": []}}
        self.text = text

    def json(self) -> dict[str, object]:
        return self._payload


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse] | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self._responses = list(responses or [])

    def get(self, url: str, *, params: dict[str, object] | None = None, timeout: float):
        self.calls.append({"url": url, "params": dict(params or {}), "timeout": timeout})
        if self._responses:
            return self._responses.pop(0)
        return _FakeResponse()


def test_fetch_past_results_batch_uses_fiveminute_variant_for_5m() -> None:
    session = _FakeSession()
    client = PolymarketOracleApiClient(session=session, timeout_sec=12.0)

    client.fetch_past_results_batch(
        symbol="btc",
        current_event_start_time="2026-03-28T00:00:00Z",
        cycle_seconds=300,
        count=5,
        sleep_sec=0.0,
        max_retries=1,
    )

    assert len(session.calls) == 1
    assert session.calls[0]["params"]["variant"] == "fiveminute"


def test_fetch_past_results_batch_uses_fifteen_variant_for_15m() -> None:
    session = _FakeSession()
    client = PolymarketOracleApiClient(session=session, timeout_sec=12.0)

    client.fetch_past_results_batch(
        symbol="btc",
        current_event_start_time="2026-03-28T00:00:00Z",
        cycle_seconds=900,
        count=5,
        sleep_sec=0.0,
        max_retries=1,
    )

    assert len(session.calls) == 1
    assert session.calls[0]["params"]["variant"] == "fifteen"


def test_fetch_crypto_price_falls_back_to_event_page_dehydrated_state() -> None:
    session = _FakeSession(
        responses=[
            _FakeResponse(status_code=400, payload={}, text='{"status":"error","error":"unsupported"}'),
            _FakeResponse(
                text=(
                    '{"state":{"data":{"openPrice":82.74,"closePrice":83.11},"status":"success"},'
                    '"queryKey":["crypto-prices","price","SOL","2026-03-27T23:55:00Z","fiveminute","2026-03-28T00:00:00Z"]}'
                )
            ),
        ]
    )
    client = PolymarketOracleApiClient(session=session, timeout_sec=12.0)

    payload = client.fetch_crypto_price(
        symbol="sol",
        cycle_start_ts=1_774_655_700,
        cycle_seconds=300,
        sleep_sec=0.0,
        max_retries=1,
    )

    assert payload["openPrice"] == 82.74
    assert payload["closePrice"] == 83.11
    assert payload["source"] == "event_page_dehydrated_state"
    assert session.calls[0]["url"].endswith("/api/crypto/crypto-price")
    assert session.calls[1]["url"].endswith("/event/sol-updown-5m-1774655700")


def test_fetch_crypto_price_timestamp_too_old_still_tries_event_page_fallback() -> None:
    session = _FakeSession(
        responses=[
            _FakeResponse(status_code=400, payload={}, text='{"status":"error","error":"Timestamp too old"}'),
            _FakeResponse(
                text=(
                    '{"state":{"data":{"openPrice":1985.51,"closePrice":1992.75},"status":"success"},'
                    '"queryKey":["crypto-prices","price","ETH","2026-03-27T23:55:00Z","fiveminute","2026-03-28T00:00:00Z"]}'
                )
            ),
        ]
    )
    client = PolymarketOracleApiClient(session=session, timeout_sec=12.0)

    payload = client.fetch_crypto_price(
        symbol="eth",
        cycle_start_ts=1_774_655_700,
        cycle_seconds=300,
        sleep_sec=0.0,
        max_retries=1,
    )

    assert payload["openPrice"] == 1985.51
    assert payload["closePrice"] == 1992.75
    assert payload["source"] == "event_page_dehydrated_state"
