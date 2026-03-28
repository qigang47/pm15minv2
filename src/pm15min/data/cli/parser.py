from __future__ import annotations

import argparse

from .args import add_market_arg, add_market_cycle_surface_args, add_surface_arg


def attach_data_subcommands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("data", help="Data ingestion and recording domain.")
    data_sub = parser.add_subparsers(dest="data_command")

    show_config = data_sub.add_parser("show-config", help="Show the canonical data config.")
    add_market_cycle_surface_args(show_config)
    show_config.add_argument("--poll-interval-sec", type=float, default=1.0)
    show_config.add_argument("--timeout-sec", type=float, default=1.2)
    show_config.add_argument("--recent-window-minutes", type=int, default=15)
    show_config.add_argument("--market-depth", type=int, default=1)
    show_config.add_argument("--market-start-offset", type=int, default=0)

    show_layout = data_sub.add_parser("show-layout", help="Show the canonical data layout.")
    add_market_cycle_surface_args(show_layout)

    show_summary = data_sub.add_parser("show-summary", help="Show a canonical data surface summary and basic quality stats.")
    add_market_cycle_surface_args(show_summary)
    show_summary.add_argument("--write-state", action="store_true")

    show_orderbook_coverage = data_sub.add_parser(
        "show-orderbook-coverage",
        help="Report raw orderbook depth partition coverage and inferred writer provenance.",
    )
    add_market_cycle_surface_args(show_orderbook_coverage)
    show_orderbook_coverage.add_argument("--date-from", default=None, help="Inclusive UTC date, YYYY-MM-DD.")
    show_orderbook_coverage.add_argument("--date-to", default=None, help="Inclusive UTC date, YYYY-MM-DD.")

    sync = data_sub.add_parser("sync", help="Sync raw source datasets into canonical source storage.")
    sync_sub = sync.add_subparsers(dest="data_sync_command")

    market_catalog = sync_sub.add_parser("market-catalog", help="Fetch and normalize the Polymarket market catalog.")
    add_market_cycle_surface_args(market_catalog)
    market_catalog.add_argument("--start-date", default=None, help="UTC start date or ISO8601 timestamp.")
    market_catalog.add_argument("--end-date", default=None, help="UTC end date or ISO8601 timestamp.")
    market_catalog.add_argument("--lookback-days", type=int, default=35)
    market_catalog.add_argument("--limit", type=int, default=500)
    market_catalog.add_argument("--max-pages", type=int, default=None, help="Optional page cap; default fetches until Gamma is exhausted.")
    market_catalog.add_argument("--sleep-sec", type=float, default=0.03)

    streams_rpc = sync_sub.add_parser(
        "streams-rpc",
        help="Fetch Chainlink streams directly from Polygon RPC and store them in v2 source partitions.",
    )
    add_market_arg(streams_rpc)
    add_surface_arg(streams_rpc)
    streams_rpc.add_argument("--start-date", default=None, help="UTC start date or ISO8601 timestamp.")
    streams_rpc.add_argument("--end-date", default=None, help="UTC end date or ISO8601 timestamp.")
    streams_rpc.add_argument("--lookback-days", type=int, default=35)
    streams_rpc.add_argument("--include-block-timestamp", action="store_true")
    streams_rpc.add_argument("--chunk-blocks", type=int, default=1000)
    streams_rpc.add_argument("--sleep-sec", type=float, default=0.02)

    datafeeds_rpc = sync_sub.add_parser(
        "datafeeds-rpc",
        help="Fetch Chainlink datafeeds AnswerUpdated logs directly from Polygon RPC and store them in v2 source partitions.",
    )
    add_market_arg(datafeeds_rpc)
    add_surface_arg(datafeeds_rpc)
    datafeeds_rpc.add_argument("--start-date", default=None, help="UTC start date or ISO8601 timestamp.")
    datafeeds_rpc.add_argument("--end-date", default=None, help="UTC end date or ISO8601 timestamp.")
    datafeeds_rpc.add_argument("--lookback-days", type=int, default=35)
    datafeeds_rpc.add_argument("--chunk-blocks", type=int, default=5000)
    datafeeds_rpc.add_argument("--sleep-sec", type=float, default=0.02)

    binance_klines = sync_sub.add_parser(
        "binance-klines-1m",
        help="Fetch canonical Binance spot 1m klines directly into v2 source storage.",
    )
    add_market_arg(binance_klines)
    add_surface_arg(binance_klines, default="live")
    binance_klines.add_argument("--symbol", default=None, help="Override symbol, default uses asset mapping like SOLUSDT.")
    binance_klines.add_argument("--lookback-minutes", type=int, default=1440)
    binance_klines.add_argument("--start-time-ms", type=int, default=None)
    binance_klines.add_argument("--end-time-ms", type=int, default=None)
    binance_klines.add_argument("--batch-limit", type=int, default=1000)

    settlement_rpc = sync_sub.add_parser(
        "settlement-truth-rpc",
        help="Fetch settlement truth directly from Polygon RPC and store it in v2 source storage.",
    )
    add_market_arg(settlement_rpc)
    add_surface_arg(settlement_rpc)
    settlement_rpc.add_argument("--start-date", default=None, help="Optional UTC start date override.")
    settlement_rpc.add_argument("--end-date", default=None, help="Optional UTC end date override.")
    settlement_rpc.add_argument("--chunk-blocks", type=int, default=3000)
    settlement_rpc.add_argument("--sleep-sec", type=float, default=0.01)

    direct_oracle = sync_sub.add_parser(
        "direct-oracle-prices",
        help="Fetch direct Polymarket price_to_beat/final_price data from Polymarket APIs.",
    )
    add_market_cycle_surface_args(direct_oracle, surface_default="live")
    direct_oracle.add_argument("--start-date", default=None, help="Optional UTC start date override.")
    direct_oracle.add_argument("--end-date", default=None, help="Optional UTC end date override.")
    direct_oracle.add_argument("--lookback-days", type=int, default=35)
    direct_oracle.add_argument("--timeout-sec", type=float, default=20.0)
    direct_oracle.add_argument("--count", type=int, default=50)
    direct_oracle.add_argument("--sleep-sec", type=float, default=0.15)
    direct_oracle.add_argument("--max-requests", type=int, default=400)
    direct_oracle.add_argument("--no-single-fallback", action="store_true")

    legacy_streams = sync_sub.add_parser(
        "legacy-streams",
        help="Import the latest legacy streams CSV into v2 partitioned source storage.",
    )
    add_market_arg(legacy_streams)
    add_surface_arg(legacy_streams)
    legacy_streams.add_argument("--source-path", default=None)

    legacy_market_catalog = sync_sub.add_parser(
        "legacy-market-catalog",
        help="Import the latest legacy market-catalog CSV into the canonical v2 market table.",
    )
    add_market_cycle_surface_args(legacy_market_catalog, surface_default="live")
    legacy_market_catalog.add_argument("--source-path", default=None)

    legacy_orderbook_depth = sync_sub.add_parser(
        "legacy-orderbook-depth",
        help="Copy legacy orderbook depth .ndjson.zst files into canonical v2 source paths.",
    )
    add_market_cycle_surface_args(legacy_orderbook_depth, surface_default="live")
    legacy_orderbook_depth.add_argument("--date-from", default=None, help="Inclusive UTC date, YYYY-MM-DD.")
    legacy_orderbook_depth.add_argument("--date-to", default=None, help="Inclusive UTC date, YYYY-MM-DD.")
    legacy_orderbook_depth.add_argument("--overwrite", action="store_true")

    legacy_settlement = sync_sub.add_parser(
        "legacy-settlement-truth",
        help="Import the latest legacy settlement-truth CSV into v2 source storage.",
    )
    add_market_arg(legacy_settlement)
    add_surface_arg(legacy_settlement)
    legacy_settlement.add_argument("--source-path", default=None)

    build = data_sub.add_parser("build", help="Build canonical tables from v2 sources.")
    build_sub = build.add_subparsers(dest="data_build_command")

    build_oracle = build_sub.add_parser("oracle-prices-15m", help="Build canonical oracle price tables for 15m.")
    add_market_cycle_surface_args(build_oracle)

    build_truth = build_sub.add_parser("truth-15m", help="Build canonical truth tables for 15m.")
    add_market_cycle_surface_args(build_truth)

    build_orderbook_index = build_sub.add_parser("orderbook-index", help="Build the daily orderbook index from canonical depth.ndjson.zst.")
    add_market_cycle_surface_args(build_orderbook_index, surface_default="live")
    build_orderbook_index.add_argument("--date", required=True, help="UTC date partition, YYYY-MM-DD.")

    export = data_sub.add_parser("export", help="Export canonical tables into human-facing files.")
    export_sub = export.add_subparsers(dest="data_export_command")

    export_oracle = export_sub.add_parser("oracle-prices-15m", help="Export oracle price table to CSV.")
    add_market_cycle_surface_args(export_oracle)

    export_truth = export_sub.add_parser("truth-15m", help="Export truth table to CSV.")
    add_market_cycle_surface_args(export_truth)

    record = data_sub.add_parser("record", help="Record high-frequency source datasets.")
    record_sub = record.add_subparsers(dest="data_record_command")
    orderbooks = record_sub.add_parser("orderbooks", help="Record raw orderbook depth and canonical daily index.")
    add_market_cycle_surface_args(orderbooks, surface_default="live")
    orderbooks.add_argument("--poll-interval-sec", type=float, default=0.35)
    orderbooks.add_argument("--timeout-sec", type=float, default=1.2)
    orderbooks.add_argument("--recent-window-minutes", type=int, default=15)
    orderbooks.add_argument("--market-depth", type=int, default=1)
    orderbooks.add_argument("--market-start-offset", type=int, default=0)
    orderbooks.add_argument("--sleep-sec", type=float, default=None)
    orderbooks.add_argument("--loop", action="store_true")
    orderbooks.add_argument("--iterations", type=int, default=1)

    run = data_sub.add_parser("run", help="Run canonical data-domain runtimes.")
    run_sub = run.add_subparsers(dest="data_run_command")
    orderbook_fleet = run_sub.add_parser("orderbook-fleet", help="Run the canonical multi-market orderbook recorder fleet.")
    orderbook_fleet.add_argument("--markets", default="btc,eth,sol,xrp", help="Comma-separated markets, default btc,eth,sol,xrp.")
    orderbook_fleet.add_argument("--cycle", default="15m", choices=["5m", "15m"])
    orderbook_fleet.add_argument("--surface", default="live", choices=["live", "backtest"])
    orderbook_fleet.add_argument("--poll-interval-sec", type=float, default=0.35)
    orderbook_fleet.add_argument("--timeout-sec", type=float, default=1.2)
    orderbook_fleet.add_argument("--recent-window-minutes", type=int, default=15)
    orderbook_fleet.add_argument("--market-depth", type=int, default=1)
    orderbook_fleet.add_argument("--market-start-offset", type=int, default=0)
    orderbook_fleet.add_argument("--loop", action="store_true")
    orderbook_fleet.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Max loop iterations; use 0 with --loop to run forever.",
    )
    orderbook_fleet.add_argument("--sleep-sec", type=float, default=None)

    foundation = run_sub.add_parser("live-foundation", help="Run the canonical live data foundation refresh loop.")
    add_market_cycle_surface_args(foundation, market_default="sol", surface_default="live", surface_choices=("live",))
    foundation.add_argument("--market-depth", type=int, default=1)
    foundation.add_argument("--timeout-sec", type=float, default=1.2)
    foundation.add_argument("--recent-window-minutes", type=int, default=15)
    foundation.add_argument("--loop", action="store_true")
    foundation.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Max loop iterations; use 0 with --loop to run forever.",
    )
    foundation.add_argument("--sleep-sec", type=float, default=1.0)
    foundation.add_argument("--market-catalog-refresh-sec", type=float, default=300.0)
    foundation.add_argument("--binance-refresh-sec", type=float, default=60.0)
    foundation.add_argument("--oracle-refresh-sec", type=float, default=60.0)
    foundation.add_argument("--streams-refresh-sec", type=float, default=300.0)
    foundation.add_argument("--orderbook-refresh-sec", type=float, default=0.35)
    foundation.add_argument("--market-catalog-lookback-hours", type=int, default=24)
    foundation.add_argument("--market-catalog-lookahead-hours", type=int, default=24)
    foundation.add_argument("--binance-lookback-minutes", type=int, default=2880)
    foundation.add_argument("--binance-batch-limit", type=int, default=1000)
    foundation.add_argument("--oracle-lookback-days", type=int, default=2)
    foundation.add_argument("--oracle-lookahead-hours", type=int, default=24)
    foundation.add_argument("--no-direct-oracle", action="store_true")
    foundation.add_argument("--no-streams", action="store_true")
    foundation.add_argument("--no-orderbooks", action="store_true")

    backtest_refresh = run_sub.add_parser("backtest-refresh", help="One-click refresh the canonical backtest data surface.")
    backtest_refresh.add_argument("--markets", default="btc,eth,sol,xrp", help="Comma-separated markets, default btc,eth,sol,xrp.")

    backfill_direct_oracle = run_sub.add_parser(
        "backfill-direct-oracle",
        help="Backfill direct Polymarket oracle rows and rebuild oracle/truth/label outputs.",
    )
    add_market_cycle_surface_args(backfill_direct_oracle, surface_default="backtest")
    backfill_direct_oracle.add_argument("--workers", type=int, default=1)
    backfill_direct_oracle.add_argument("--flush-every", type=int, default=200)
    backfill_direct_oracle.add_argument("--timeout-sec", type=float, default=30.0)
    backfill_direct_oracle.add_argument("--max-retries", type=int, default=6)
    backfill_direct_oracle.add_argument("--sleep-sec", type=float, default=0.0)

    backfill_gamma = run_sub.add_parser(
        "backfill-cycle-labels-gamma",
        help="Backfill market catalog and truth/label frames from Gamma outcome prices.",
    )
    backfill_gamma.add_argument("--markets", default="btc,eth,sol,xrp", help="Comma-separated markets or all.")
    backfill_gamma.add_argument("--cycle", default="15m", choices=["5m", "15m"])
    backfill_gamma.add_argument("--surface", default="backtest", choices=["backtest", "live"])
    backfill_gamma.add_argument("--start-date", default=None, help="UTC start date or ISO8601 timestamp.")
    backfill_gamma.add_argument("--end-date", default=None, help="UTC end date or ISO8601 timestamp.")
    backfill_gamma.add_argument("--window-days", type=int, default=7)
    backfill_gamma.add_argument("--workers", type=int, default=4)
    backfill_gamma.add_argument("--skip-market-catalog-refresh", action="store_true")
