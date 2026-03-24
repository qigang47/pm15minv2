from __future__ import annotations

from pm15min.data.sources.polymarket_gamma import build_market_catalog_records


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
