from __future__ import annotations

from pathlib import Path

from pm15min.data.config import DataConfig
from pm15min.data.io.ndjson_zst import append_ndjson_zst
from pm15min.data.service.orderbook_coverage import build_orderbook_coverage_report


def test_build_orderbook_coverage_report_infers_provenance_and_missing_dates(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=tmp_path / "v2")

    append_ndjson_zst(
        cfg.layout.orderbook_depth_path("2026-03-22"),
        [
            {
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "logged_at": "2026-03-22T00:05:00Z",
                "orderbook_ts": "2026-03-22T00:05:01Z",
                "decision_ts": "2026-03-22T00:05:00Z",
                "offset": 5,
                "asks": [],
                "bids": [],
            },
            {
                "market_id": "market-2",
                "token_id": "token-down",
                "side": "down",
                "logged_at": "2026-03-22T00:20:00Z",
                "orderbook_ts": "2026-03-22T00:20:01Z",
                "decision_ts": "2026-03-22T00:20:00Z",
                "offset": 5,
                "asks": [],
                "bids": [],
            },
        ],
    )
    append_ndjson_zst(
        cfg.layout.orderbook_depth_path("2026-03-24"),
        [
            {
                "captured_ts_ms": 1_774_314_010_000,
                "source_ts_ms": 1_774_314_015_000,
                "market_id": "market-3",
                "token_id": "token-up",
                "side": "up",
                "asset": "sol",
                "cycle": "15m",
                "source": "clob",
                "asks": [],
                "bids": [],
            }
        ],
    )

    payload = build_orderbook_coverage_report(
        cfg,
        date_from="2026-03-22",
        date_to="2026-03-24",
    )

    assert payload["expected_daily_market_count"] == 96
    assert payload["missing_dates"] == ["2026-03-23"]

    by_date = {item["date"]: item for item in payload["days"]}
    assert by_date["2026-03-22"]["provenance"] == "legacy_import_or_legacy_writer"
    assert by_date["2026-03-22"]["unique_market_count"] == 2
    assert by_date["2026-03-24"]["provenance"] == "v2_native_recorder"
    assert by_date["2026-03-24"]["unique_market_count"] == 1
    assert by_date["2026-03-23"]["status"] == "missing"
