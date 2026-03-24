from __future__ import annotations

from pathlib import Path

from pm15min.data.layout import DataLayout


def test_market_layout_paths_are_partitioned(tmp_path: Path) -> None:
    storage = DataLayout.discover(root=tmp_path / "v2")
    scope = storage.for_market("sol", "15m", surface="backtest")

    assert scope.market_catalog_snapshot_root == (
        tmp_path
        / "v2"
        / "data"
        / "backtest"
        / "sources"
        / "polymarket"
        / "market_catalogs"
        / "cycle=15m"
        / "asset=sol"
    )
    assert scope.market_catalog_table_path == (
        tmp_path / "v2" / "data" / "backtest" / "tables" / "markets" / "cycle=15m" / "asset=sol" / "data.parquet"
    )
    assert scope.datafeeds_partition_path(2026, 3) == (
        tmp_path
        / "v2"
        / "data"
        / "backtest"
        / "sources"
        / "chainlink"
        / "datafeeds"
        / "asset=sol"
        / "year=2026"
        / "month=03"
        / "data.parquet"
    )
    assert scope.orderbook_depth_path("2026-03-19") == (
        tmp_path
        / "v2"
        / "data"
        / "backtest"
        / "sources"
        / "polymarket"
        / "orderbooks"
        / "cycle=15m"
        / "asset=sol"
        / "date=2026-03-19"
        / "depth.ndjson.zst"
    )
    assert scope.latest_summary_path == (
        tmp_path
        / "v2"
        / "var"
        / "backtest"
        / "state"
        / "summary"
        / "cycle=15m"
        / "asset=sol"
        / "latest.json"
    )
    assert scope.latest_summary_manifest_path == (
        tmp_path
        / "v2"
        / "var"
        / "backtest"
        / "state"
        / "summary"
        / "cycle=15m"
        / "asset=sol"
        / "latest.manifest.json"
    )
    assert scope.summary_manifest_snapshot_path("2026-03-20T00-00-00Z") == (
        tmp_path
        / "v2"
        / "var"
        / "backtest"
        / "state"
        / "summary"
        / "cycle=15m"
        / "asset=sol"
        / "snapshots"
        / "snapshot_ts=2026-03-20T00-00-00Z"
        / "manifest.json"
    )
