from __future__ import annotations

from pm15min.data.sources.polymarket_gamma import GammaEventsClient, build_market_catalog_records


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def get(self, url, *, params=None, timeout=None):
        index = len(self.calls)
        self.calls.append({"url": url, "params": dict(params or {}), "timeout": timeout})
        return _FakeResponse(self.payloads[index])


def test_build_market_catalog_records_for_15m() -> None:
    events = [
        {
            "id": "event-1",
            "slug": "btc-up-or-down-15m-1700000000",
            "title": "Bitcoin Up or Down 15m",
            "seriesSlug": "btc-up-or-down-15m",
            "resolutionSource": "https://data.chain.link/streams/btc-usd",
            "closed": True,
            "markets": [
                {
                    "id": "market-1",
                    "conditionId": "cond-1",
                    "slug": "btc-up-or-down-15m-1700000000",
                    "question": "Bitcoin Up or Down",
                    "endDate": "2023-11-14T22:28:20Z",
                    "closedTime": "2023-11-14T22:29:00Z",
                    "outcomes": ["Up", "Down"],
                    "clobTokenIds": ["token-up", "token-down"],
                }
            ],
        }
    ]

    records = build_market_catalog_records(
        events=events,
        asset="btc",
        cycle="15m",
        snapshot_ts="2026-03-19T09-00-00Z",
    )

    assert len(records) == 1
    row = records[0].to_row()
    assert row["market_id"] == "market-1"
    assert row["asset"] == "btc"
    assert row["cycle"] == "15m"
    assert row["cycle_start_ts"] == 1700000000
    assert row["cycle_end_ts"] == 1700000900
    assert row["token_up"] == "token-up"
    assert row["token_down"] == "token-down"


def test_fetch_closed_events_without_page_cap_runs_until_short_page() -> None:
    session = _FakeSession(
        [
            [{"id": "event-1"}, {"id": "event-2"}],
            [{"id": "event-3"}, {"id": "event-4"}],
            [{"id": "event-5"}],
        ]
    )
    client = GammaEventsClient(session=session)

    rows = client.fetch_closed_events(
        start_ts=1_700_000_000,
        end_ts=1_700_086_400,
        limit=2,
        max_pages=None,
        sleep_sec=0.0,
    )

    assert [row["id"] for row in rows] == ["event-1", "event-2", "event-3", "event-4", "event-5"]
    assert [call["params"]["offset"] for call in session.calls] == [0, 2, 4]


def test_fetch_closed_events_respects_explicit_page_cap() -> None:
    session = _FakeSession(
        [
            [{"id": "event-1"}, {"id": "event-2"}],
            [{"id": "event-3"}, {"id": "event-4"}],
            [{"id": "event-5"}],
        ]
    )
    client = GammaEventsClient(session=session)

    rows = client.fetch_closed_events(
        start_ts=1_700_000_000,
        end_ts=1_700_086_400,
        limit=2,
        max_pages=2,
        sleep_sec=0.0,
    )

    assert [row["id"] for row in rows] == ["event-1", "event-2", "event-3", "event-4"]
    assert [call["params"]["offset"] for call in session.calls] == [0, 2]
