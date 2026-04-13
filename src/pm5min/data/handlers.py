from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import DataConfig
from .layout import DataLayout
from .pipelines import backtest_refresh as backtest_refresh_pipeline
from .pipelines import binance_klines as binance_klines_pipeline
from .pipelines import direct_oracle_prices as direct_oracle_prices_pipeline
from .pipelines import direct_sync as direct_sync_pipeline
from .pipelines import export_tables as export_tables_pipeline
from .pipelines import foundation_runtime as foundation_runtime_pipeline
from .pipelines import market_catalog as market_catalog_pipeline
from .pipelines import oracle_prices as oracle_prices_pipeline
from .pipelines import orderbook_fleet as orderbook_fleet_pipeline
from .pipelines import orderbook_recording as orderbook_recording_pipeline
from .pipelines import orderbook_runtime as orderbook_runtime_pipeline
from .pipelines import source_ingest as source_ingest_pipeline
from .pipelines import truth as truth_pipeline
from .service import build_orderbook_coverage_report, show_data_summary


def _print_payload(payload: object) -> int:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


def _build_data_cfg(args: argparse.Namespace, **overrides: object) -> DataConfig:
    cycle = overrides.pop("cycle", getattr(args, "cycle", "5m"))
    surface = overrides.pop("surface", getattr(args, "surface", "backtest"))
    return DataConfig.build(
        market=args.market,
        cycle=cycle,
        surface=surface,
        **overrides,
    )


def _handle_show_config(args: argparse.Namespace) -> int:
    cfg = _build_data_cfg(
        args,
        poll_interval_sec=args.poll_interval_sec,
        orderbook_timeout_sec=args.timeout_sec,
        recent_window_minutes=args.recent_window_minutes,
        market_depth=args.market_depth,
        market_start_offset=args.market_start_offset,
    )
    return _print_payload(cfg.to_dict())


def _handle_show_layout(args: argparse.Namespace) -> int:
    return _print_payload(DataLayout.discover().for_market(args.market, args.cycle, surface=args.surface).to_dict())


def _handle_show_summary(args: argparse.Namespace) -> int:
    return _print_payload(show_data_summary(_build_data_cfg(args), persist=bool(args.write_state)))


def _handle_show_orderbook_coverage(args: argparse.Namespace) -> int:
    return _print_payload(
        build_orderbook_coverage_report(
            _build_data_cfg(args),
            date_from=args.date_from,
            date_to=args.date_to,
        )
    )


def _handle_sync_settlement_truth_rpc(args: argparse.Namespace) -> int:
    return _print_payload(
        direct_sync_pipeline.sync_settlement_truth_from_rpc(
            _build_data_cfg(args),
            start_ts=None,
            end_ts=None,
            chunk_blocks=int(args.chunk_blocks),
            sleep_sec=float(args.sleep_sec),
        )
    )


def _sync_window(args: argparse.Namespace) -> tuple[int, int]:
    end_dt = _parse_utc_datetime(args.end_date) if args.end_date else datetime.now(timezone.utc)
    start_dt = _parse_utc_datetime(args.start_date) if args.start_date else end_dt - timedelta(days=int(args.lookback_days))
    return int(start_dt.timestamp()), int(end_dt.timestamp())


def _handle_sync_market_catalog(args: argparse.Namespace) -> int:
    start_ts, end_ts = _sync_window(args)
    cfg = _build_data_cfg(
        args,
        gamma_limit=args.limit,
        max_pages=args.max_pages,
        sleep_sec=args.sleep_sec,
    )
    return _print_payload(market_catalog_pipeline.sync_market_catalog(cfg, start_ts=start_ts, end_ts=end_ts))


def _handle_sync_streams_rpc(args: argparse.Namespace) -> int:
    start_ts, end_ts = _sync_window(args)
    cfg = _build_data_cfg(args, sleep_sec=args.sleep_sec)
    return _print_payload(
        direct_sync_pipeline.sync_streams_from_rpc(
            cfg,
            start_ts=start_ts,
            end_ts=end_ts,
            include_block_timestamp=bool(args.include_block_timestamp),
            chunk_blocks=int(args.chunk_blocks),
            sleep_sec=float(args.sleep_sec),
        )
    )


def _handle_sync_datafeeds_rpc(args: argparse.Namespace) -> int:
    start_ts, end_ts = _sync_window(args)
    cfg = _build_data_cfg(args, sleep_sec=args.sleep_sec)
    return _print_payload(
        direct_sync_pipeline.sync_datafeeds_from_rpc(
            cfg,
            start_ts=start_ts,
            end_ts=end_ts,
            chunk_blocks=int(args.chunk_blocks),
            sleep_sec=float(args.sleep_sec),
        )
    )


def _handle_sync_binance_klines(args: argparse.Namespace) -> int:
    return _print_payload(
        binance_klines_pipeline.sync_binance_klines_1m(
            _build_data_cfg(args),
            symbol=args.symbol,
            start_time_ms=args.start_time_ms,
            end_time_ms=args.end_time_ms,
            lookback_minutes=int(args.lookback_minutes),
            batch_limit=int(args.batch_limit),
        )
    )


def _handle_sync_direct_oracle_prices(args: argparse.Namespace) -> int:
    start_ts = int(_parse_utc_datetime(args.start_date).timestamp()) if args.start_date else None
    end_ts = int(_parse_utc_datetime(args.end_date).timestamp()) if args.end_date else None
    return _print_payload(
        direct_oracle_prices_pipeline.sync_polymarket_oracle_prices_direct(
            _build_data_cfg(args),
            start_ts=start_ts,
            end_ts=end_ts,
            lookback_days=int(args.lookback_days),
            timeout_sec=float(args.timeout_sec),
            count=int(args.count),
            sleep_sec=float(args.sleep_sec),
            max_requests=int(args.max_requests),
            fallback_single=not bool(args.no_single_fallback),
        )
    )


def _handle_sync_legacy_market_catalog(args: argparse.Namespace) -> int:
    return _print_payload(
        source_ingest_pipeline.import_legacy_market_catalog(
            _build_data_cfg(args),
            source_path=Path(args.source_path) if args.source_path else None,
        )
    )


def _handle_sync_legacy_streams(args: argparse.Namespace) -> int:
    return _print_payload(
        source_ingest_pipeline.import_legacy_streams(
            _build_data_cfg(args),
            source_path=Path(args.source_path) if args.source_path else None,
        )
    )


def _handle_sync_legacy_orderbook_depth(args: argparse.Namespace) -> int:
    return _print_payload(
        source_ingest_pipeline.import_legacy_orderbook_depth(
            _build_data_cfg(args),
            date_from=args.date_from,
            date_to=args.date_to,
            overwrite=bool(args.overwrite),
        )
    )


def _handle_sync_legacy_settlement_truth(args: argparse.Namespace) -> int:
    return _print_payload(
        source_ingest_pipeline.import_legacy_settlement_truth(
            _build_data_cfg(args),
            source_path=Path(args.source_path) if args.source_path else None,
        )
    )


def _handle_run_live_foundation(args: argparse.Namespace) -> int:
    cfg = _build_data_cfg(
        args,
        orderbook_timeout_sec=args.timeout_sec,
        recent_window_minutes=args.recent_window_minutes,
        market_depth=args.market_depth,
    )
    return _print_payload(
        foundation_runtime_pipeline.run_live_data_foundation(
            cfg,
            iterations=int(args.iterations),
            loop=bool(args.loop),
            sleep_sec=float(args.sleep_sec),
            market_catalog_refresh_sec=float(args.market_catalog_refresh_sec),
            binance_refresh_sec=float(args.binance_refresh_sec),
            oracle_refresh_sec=float(args.oracle_refresh_sec),
            streams_refresh_sec=float(args.streams_refresh_sec),
            orderbook_refresh_sec=float(args.orderbook_refresh_sec),
            market_catalog_lookback_hours=int(args.market_catalog_lookback_hours),
            market_catalog_lookahead_hours=int(args.market_catalog_lookahead_hours),
            binance_lookback_minutes=int(args.binance_lookback_minutes),
            binance_batch_limit=int(args.binance_batch_limit),
            oracle_lookback_days=int(args.oracle_lookback_days),
            oracle_lookahead_hours=int(args.oracle_lookahead_hours),
            include_direct_oracle=not bool(args.no_direct_oracle),
            include_streams=not bool(args.no_streams),
            include_orderbooks=not bool(args.no_orderbooks),
        )
    )


def _handle_build_truth(args: argparse.Namespace) -> int:
    return _print_payload(truth_pipeline.build_truth_15m(_build_data_cfg(args)))


def _handle_build_oracle_prices(args: argparse.Namespace) -> int:
    return _print_payload(oracle_prices_pipeline.build_oracle_prices_15m(_build_data_cfg(args)))


def _handle_build_orderbook_index(args: argparse.Namespace) -> int:
    return _print_payload(orderbook_recording_pipeline.build_orderbook_index_from_depth(_build_data_cfg(args), date_str=args.date))


def _handle_export_truth(args: argparse.Namespace) -> int:
    return _print_payload(export_tables_pipeline.export_truth_15m(_build_data_cfg(args)))


def _handle_export_oracle_prices(args: argparse.Namespace) -> int:
    return _print_payload(export_tables_pipeline.export_oracle_prices_15m(_build_data_cfg(args)))


def _handle_record_orderbooks(args: argparse.Namespace) -> int:
    cfg = _build_data_cfg(
        args,
        poll_interval_sec=args.poll_interval_sec,
        orderbook_timeout_sec=args.timeout_sec,
        recent_window_minutes=args.recent_window_minutes,
        market_depth=args.market_depth,
        market_start_offset=args.market_start_offset,
    )
    return _print_payload(
        orderbook_runtime_pipeline.run_orderbook_recorder(
            cfg,
            iterations=int(args.iterations),
            loop=bool(args.loop),
            sleep_sec=args.sleep_sec,
        )
    )


def _handle_run_backfill_direct_oracle(args: argparse.Namespace) -> int:
    return _print_payload(
        direct_oracle_prices_pipeline.backfill_direct_oracle_prices(
            _build_data_cfg(args),
            workers=int(args.workers),
            flush_every=int(args.flush_every),
            timeout_sec=float(args.timeout_sec),
            max_retries=int(args.max_retries),
            sleep_sec=float(args.sleep_sec),
        )
    )


def _handle_run_orderbook_fleet(args: argparse.Namespace) -> int:
    return _print_payload(
        orderbook_fleet_pipeline.run_orderbook_recorder_fleet(
            markets=args.markets,
            cycle=str(args.cycle),
            surface=str(args.surface),
            poll_interval_sec=float(args.poll_interval_sec),
            orderbook_timeout_sec=float(args.timeout_sec),
            recent_window_minutes=int(args.recent_window_minutes),
            market_depth=int(args.market_depth),
            market_start_offset=int(args.market_start_offset),
            iterations=int(args.iterations),
            loop=bool(args.loop),
            sleep_sec=args.sleep_sec,
        )
    )


def _handle_run_backtest_refresh(args: argparse.Namespace) -> int:
    markets = [item.strip().lower() for item in str(args.markets or "").split(",") if item.strip()]
    return _print_payload(
        backtest_refresh_pipeline.run_backtest_data_refresh(
            markets=markets or ["btc", "eth", "sol", "xrp"],
            options=backtest_refresh_pipeline.BacktestRefreshOptions(),
        )
    )


def _handle_run_backfill_cycle_labels_gamma(args: argparse.Namespace) -> int:
    markets = [item.strip().lower() for item in str(args.markets or "").split(",") if item.strip()]
    if any(item == "all" for item in markets):
        markets = ["btc", "eth", "sol", "xrp"]
    start_ts = int(_parse_utc_datetime(args.start_date).timestamp()) if args.start_date else None
    end_ts = int(_parse_utc_datetime(args.end_date).timestamp()) if args.end_date else None
    return _print_payload(
        backtest_refresh_pipeline.backfill_cycle_labels_from_gamma(
            markets=markets or ["btc", "eth", "sol", "xrp"],
            cycle=str(args.cycle),
            surface=str(args.surface),
            start_ts=start_ts,
            end_ts=end_ts,
            window_days=int(args.window_days),
            workers=int(args.workers),
            refresh_market_catalog=not bool(args.skip_market_catalog_refresh),
        )
    )


def _parse_utc_datetime(raw: str) -> datetime:
    text = str(raw).strip()
    if not text:
        raise ValueError("empty datetime")
    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=timezone.utc)
    if len(text) == 10:
        return datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


_ROOT_HANDLERS = {
    "show-config": _handle_show_config,
    "show-layout": _handle_show_layout,
    "show-summary": _handle_show_summary,
    "show-orderbook-coverage": _handle_show_orderbook_coverage,
}

_SYNC_HANDLERS = {
    "market-catalog": _handle_sync_market_catalog,
    "streams-rpc": _handle_sync_streams_rpc,
    "datafeeds-rpc": _handle_sync_datafeeds_rpc,
    "binance-klines-1m": _handle_sync_binance_klines,
    "direct-oracle-prices": _handle_sync_direct_oracle_prices,
    "legacy-streams": _handle_sync_legacy_streams,
    "legacy-market-catalog": _handle_sync_legacy_market_catalog,
    "legacy-orderbook-depth": _handle_sync_legacy_orderbook_depth,
    "settlement-truth-rpc": _handle_sync_settlement_truth_rpc,
    "legacy-settlement-truth": _handle_sync_legacy_settlement_truth,
}

_BUILD_HANDLERS = {
    "orderbook-index": _handle_build_orderbook_index,
    "oracle-prices-15m": _handle_build_oracle_prices,
    "truth-15m": _handle_build_truth,
}

_EXPORT_HANDLERS = {
    "oracle-prices-15m": _handle_export_oracle_prices,
    "truth-15m": _handle_export_truth,
}

_RECORD_HANDLERS = {
    "orderbooks": _handle_record_orderbooks,
}

_RUN_HANDLERS = {
    "orderbook-fleet": _handle_run_orderbook_fleet,
    "backtest-refresh": _handle_run_backtest_refresh,
    "live-foundation": _handle_run_live_foundation,
    "backfill-direct-oracle": _handle_run_backfill_direct_oracle,
    "backfill-cycle-labels-gamma": _handle_run_backfill_cycle_labels_gamma,
}


def run_data_command(args: argparse.Namespace) -> int:
    data_command = str(args.data_command or "")
    root_handler = _ROOT_HANDLERS.get(data_command)
    if root_handler is not None:
        return root_handler(args)

    if data_command == "sync":
        handler = _SYNC_HANDLERS.get(str(args.data_sync_command or ""))
        if handler is not None:
            return handler(args)

    if data_command == "build":
        handler = _BUILD_HANDLERS.get(str(args.data_build_command or ""))
        if handler is not None:
            return handler(args)

    if data_command == "export":
        handler = _EXPORT_HANDLERS.get(str(args.data_export_command or ""))
        if handler is not None:
            return handler(args)

    if data_command == "record":
        handler = _RECORD_HANDLERS.get(str(args.data_record_command or ""))
        if handler is not None:
            return handler(args)

    if data_command == "run":
        handler = _RUN_HANDLERS.get(str(args.data_run_command or ""))
        if handler is not None:
            return handler(args)

    raise SystemExit("Missing data subcommand.")
