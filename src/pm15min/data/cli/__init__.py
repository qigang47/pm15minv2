from __future__ import annotations

import argparse
from datetime import datetime

from .handlers import DataCliDeps, parse_utc_datetime, run_data_command as _run_data_command_impl
from .parser import attach_data_subcommands as _attach_data_subcommands_impl
from ..config import DataConfig
from ..layout import DataLayout
from ..service import show_data_summary
from ..pipelines.binance_klines import sync_binance_klines_1m
from ..pipelines.backtest_refresh import run_backtest_data_refresh
from ..pipelines.direct_sync import sync_datafeeds_from_rpc, sync_settlement_truth_from_rpc, sync_streams_from_rpc
from ..pipelines.export_tables import export_oracle_prices_15m, export_truth_15m
from ..pipelines.foundation_runtime import run_live_data_foundation
from ..pipelines.market_catalog import sync_market_catalog
from ..pipelines.oracle_prices import build_oracle_prices_15m
from ..pipelines.orderbook_fleet import run_orderbook_recorder_fleet
from ..pipelines.orderbook_recording import build_orderbook_index_from_depth
from ..pipelines.orderbook_runtime import run_orderbook_recorder
from ..pipelines.source_ingest import (
    import_legacy_market_catalog,
    import_legacy_orderbook_depth,
    import_legacy_settlement_truth,
    import_legacy_streams,
)
from ..pipelines.truth import build_truth_15m


def _sync_polymarket_oracle_prices_direct(*args, **kwargs):
    # Keep the direct-oracle import lazy so this facade keeps the previous import behavior.
    from ..pipelines.direct_oracle_prices import sync_polymarket_oracle_prices_direct

    return sync_polymarket_oracle_prices_direct(*args, **kwargs)


def _build_cli_deps() -> DataCliDeps:
    return DataCliDeps(
        DataConfig=DataConfig,
        DataLayout=DataLayout,
        show_data_summary=show_data_summary,
        sync_market_catalog=sync_market_catalog,
        sync_streams_from_rpc=sync_streams_from_rpc,
        sync_datafeeds_from_rpc=sync_datafeeds_from_rpc,
        sync_binance_klines_1m=sync_binance_klines_1m,
        sync_settlement_truth_from_rpc=sync_settlement_truth_from_rpc,
        sync_polymarket_oracle_prices_direct=_sync_polymarket_oracle_prices_direct,
        import_legacy_streams=import_legacy_streams,
        import_legacy_market_catalog=import_legacy_market_catalog,
        import_legacy_orderbook_depth=import_legacy_orderbook_depth,
        import_legacy_settlement_truth=import_legacy_settlement_truth,
        build_oracle_prices_15m=build_oracle_prices_15m,
        build_truth_15m=build_truth_15m,
        build_orderbook_index_from_depth=build_orderbook_index_from_depth,
        export_oracle_prices_15m=export_oracle_prices_15m,
        export_truth_15m=export_truth_15m,
        run_orderbook_recorder=run_orderbook_recorder,
        run_orderbook_recorder_fleet=run_orderbook_recorder_fleet,
        run_live_data_foundation=run_live_data_foundation,
        run_backtest_data_refresh=run_backtest_data_refresh,
    )


def attach_data_subcommands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    _attach_data_subcommands_impl(subparsers)


def run_data_command(args: argparse.Namespace) -> int:
    return _run_data_command_impl(args, deps=_build_cli_deps())


def _parse_utc_datetime(raw: str) -> datetime:
    return parse_utc_datetime(raw)
