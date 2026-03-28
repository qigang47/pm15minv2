from __future__ import annotations

from pm15min.data.sources.polymarket_oracle_api import PolymarketOracleApiClient


class _FakeResponse:
    def __init__(self) -> None:
        self.status_code = 200

    def json(self) -> dict[str, object]:
        return {"data": {"results": []}}


class _FakeSession:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, *, params: dict[str, object], timeout: float):
        self.calls.append({"url": url, "params": dict(params), "timeout": timeout})
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
