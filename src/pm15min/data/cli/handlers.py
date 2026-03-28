from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable


@dataclass(frozen=True)
class DataCliDeps:
    DataConfig: type
    DataLayout: type
    show_data_summary: Callable[..., dict[str, Any]]
    build_orderbook_coverage_report: Callable[..., dict[str, Any]]
    sync_market_catalog: Callable[..., dict[str, Any]]
    sync_streams_from_rpc: Callable[..., dict[str, Any]]
    sync_datafeeds_from_rpc: Callable[..., dict[str, Any]]
    sync_binance_klines_1m: Callable[..., dict[str, Any]]
    sync_settlement_truth_from_rpc: Callable[..., dict[str, Any]]
    sync_polymarket_oracle_prices_direct: Callable[..., dict[str, Any]]
    import_legacy_streams: Callable[..., dict[str, Any]]
    import_legacy_market_catalog: Callable[..., dict[str, Any]]
    import_legacy_orderbook_depth: Callable[..., dict[str, Any]]
    import_legacy_settlement_truth: Callable[..., dict[str, Any]]
    build_oracle_prices_15m: Callable[..., dict[str, Any]]
    build_truth_15m: Callable[..., dict[str, Any]]
    build_orderbook_index_from_depth: Callable[..., dict[str, Any]]
    export_oracle_prices_15m: Callable[..., dict[str, Any]]
    export_truth_15m: Callable[..., dict[str, Any]]
    run_orderbook_recorder: Callable[..., dict[str, Any]]
    run_orderbook_recorder_fleet: Callable[..., dict[str, Any]]
    run_live_data_foundation: Callable[..., dict[str, Any]]
    run_backtest_data_refresh: Callable[..., dict[str, Any]]


def _print_payload(payload: dict[str, Any]) -> int:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


def _build_data_cfg(args: argparse.Namespace, deps: DataCliDeps, **overrides: Any):
    cycle = overrides.pop("cycle", getattr(args, "cycle", "15m"))
    surface = overrides.pop("surface", getattr(args, "surface", "backtest"))
    return deps.DataConfig.build(
        market=args.market,
        cycle=cycle,
        surface=surface,
        **overrides,
    )


def _handle_show_config(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    cfg = _build_data_cfg(
        args,
        deps,
        poll_interval_sec=args.poll_interval_sec,
        orderbook_timeout_sec=args.timeout_sec,
        recent_window_minutes=args.recent_window_minutes,
        market_depth=args.market_depth,
        market_start_offset=args.market_start_offset,
    )
    return cfg.to_dict()


def _handle_show_layout(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    layout = deps.DataLayout.discover().for_market(args.market, args.cycle, surface=args.surface)
    return layout.to_dict()


def _handle_show_summary(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    cfg = _build_data_cfg(args, deps)
    return deps.show_data_summary(cfg, persist=bool(args.write_state))


def _handle_show_orderbook_coverage(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    cfg = _build_data_cfg(args, deps)
    return deps.build_orderbook_coverage_report(
        cfg,
        date_from=args.date_from,
        date_to=args.date_to,
    )


def _sync_window(args: argparse.Namespace) -> tuple[int, int]:
    end_dt = parse_utc_datetime(args.end_date) if args.end_date else datetime.now(timezone.utc)
    start_dt = parse_utc_datetime(args.start_date) if args.start_date else end_dt - timedelta(days=int(args.lookback_days))
    return int(start_dt.timestamp()), int(end_dt.timestamp())


def _handle_sync_market_catalog(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    start_ts, end_ts = _sync_window(args)
    cfg = _build_data_cfg(
        args,
        deps,
        gamma_limit=args.limit,
        max_pages=args.max_pages,
        sleep_sec=args.sleep_sec,
    )
    return deps.sync_market_catalog(cfg, start_ts=start_ts, end_ts=end_ts)


def _handle_sync_streams_rpc(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    start_ts, end_ts = _sync_window(args)
    cfg = _build_data_cfg(args, deps, cycle="15m", sleep_sec=args.sleep_sec)
    return deps.sync_streams_from_rpc(
        cfg,
        start_ts=start_ts,
        end_ts=end_ts,
        include_block_timestamp=bool(args.include_block_timestamp),
        chunk_blocks=int(args.chunk_blocks),
        sleep_sec=float(args.sleep_sec),
    )


def _handle_sync_datafeeds_rpc(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    start_ts, end_ts = _sync_window(args)
    cfg = _build_data_cfg(args, deps, cycle="15m", sleep_sec=args.sleep_sec)
    return deps.sync_datafeeds_from_rpc(
        cfg,
        start_ts=start_ts,
        end_ts=end_ts,
        chunk_blocks=int(args.chunk_blocks),
        sleep_sec=float(args.sleep_sec),
    )


def _handle_sync_binance_klines(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    cfg = _build_data_cfg(args, deps, cycle="15m")
    return deps.sync_binance_klines_1m(
        cfg,
        symbol=args.symbol,
        start_time_ms=args.start_time_ms,
        end_time_ms=args.end_time_ms,
        lookback_minutes=int(args.lookback_minutes),
        batch_limit=int(args.batch_limit),
    )


def _handle_sync_settlement_truth_rpc(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    start_ts = int(parse_utc_datetime(args.start_date).timestamp()) if args.start_date else None
    end_ts = int(parse_utc_datetime(args.end_date).timestamp()) if args.end_date else None
    cfg = _build_data_cfg(args, deps, cycle="15m")
    return deps.sync_settlement_truth_from_rpc(
        cfg,
        start_ts=start_ts,
        end_ts=end_ts,
        chunk_blocks=int(args.chunk_blocks),
        sleep_sec=float(args.sleep_sec),
    )


def _handle_sync_direct_oracle_prices(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    start_ts = int(parse_utc_datetime(args.start_date).timestamp()) if args.start_date else None
    end_ts = int(parse_utc_datetime(args.end_date).timestamp()) if args.end_date else None
    cfg = _build_data_cfg(args, deps)
    return deps.sync_polymarket_oracle_prices_direct(
        cfg,
        start_ts=start_ts,
        end_ts=end_ts,
        lookback_days=int(args.lookback_days),
        timeout_sec=float(args.timeout_sec),
        count=int(args.count),
        sleep_sec=float(args.sleep_sec),
        max_requests=int(args.max_requests),
        fallback_single=not bool(args.no_single_fallback),
    )


def _handle_sync_legacy_streams(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    cfg = _build_data_cfg(args, deps, cycle="15m")
    return deps.import_legacy_streams(cfg, source_path=Path(args.source_path) if args.source_path else None)


def _handle_sync_legacy_market_catalog(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    cfg = _build_data_cfg(args, deps)
    return deps.import_legacy_market_catalog(cfg, source_path=Path(args.source_path) if args.source_path else None)


def _handle_sync_legacy_orderbook_depth(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    cfg = _build_data_cfg(args, deps)
    return deps.import_legacy_orderbook_depth(
        cfg,
        date_from=args.date_from,
        date_to=args.date_to,
        overwrite=bool(args.overwrite),
    )


def _handle_sync_legacy_settlement_truth(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    cfg = _build_data_cfg(args, deps, cycle="15m")
    return deps.import_legacy_settlement_truth(cfg, source_path=Path(args.source_path) if args.source_path else None)


def _handle_build_oracle_prices(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    return deps.build_oracle_prices_15m(_build_data_cfg(args, deps))


def _handle_build_truth(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    return deps.build_truth_15m(_build_data_cfg(args, deps))


def _handle_build_orderbook_index(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    return deps.build_orderbook_index_from_depth(_build_data_cfg(args, deps), date_str=args.date)


def _handle_export_oracle_prices(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    return deps.export_oracle_prices_15m(_build_data_cfg(args, deps))


def _handle_export_truth(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    return deps.export_truth_15m(_build_data_cfg(args, deps))


def _handle_record_orderbooks(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    cfg = _build_data_cfg(
        args,
        deps,
        poll_interval_sec=args.poll_interval_sec,
        orderbook_timeout_sec=args.timeout_sec,
        recent_window_minutes=args.recent_window_minutes,
        market_depth=args.market_depth,
        market_start_offset=args.market_start_offset,
    )
    return deps.run_orderbook_recorder(
        cfg,
        iterations=int(args.iterations),
        loop=bool(args.loop),
        sleep_sec=args.sleep_sec,
    )


def _handle_run_orderbook_fleet(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    return deps.run_orderbook_recorder_fleet(
        markets=args.markets,
        cycle=args.cycle,
        surface=args.surface,
        poll_interval_sec=float(args.poll_interval_sec),
        orderbook_timeout_sec=float(args.timeout_sec),
        recent_window_minutes=int(args.recent_window_minutes),
        market_depth=int(args.market_depth),
        market_start_offset=int(args.market_start_offset),
        iterations=int(args.iterations),
        loop=bool(args.loop),
        sleep_sec=args.sleep_sec,
    )


def _handle_run_live_foundation(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    cfg = _build_data_cfg(
        args,
        deps,
        orderbook_timeout_sec=args.timeout_sec,
        recent_window_minutes=args.recent_window_minutes,
        market_depth=args.market_depth,
    )
    return deps.run_live_data_foundation(
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


def _handle_run_backtest_refresh(args: argparse.Namespace, deps: DataCliDeps) -> dict[str, Any]:
    markets = [item.strip().lower() for item in str(args.markets or "").split(",") if item.strip()]
    return deps.run_backtest_data_refresh(
        markets=markets or ["btc", "eth", "sol", "xrp"],
        options=None,
    )


_ROOT_HANDLERS: dict[str, Callable[[argparse.Namespace, DataCliDeps], dict[str, Any]]] = {
    "show-config": _handle_show_config,
    "show-layout": _handle_show_layout,
    "show-summary": _handle_show_summary,
    "show-orderbook-coverage": _handle_show_orderbook_coverage,
}

_SYNC_HANDLERS: dict[str, Callable[[argparse.Namespace, DataCliDeps], dict[str, Any]]] = {
    "market-catalog": _handle_sync_market_catalog,
    "streams-rpc": _handle_sync_streams_rpc,
    "datafeeds-rpc": _handle_sync_datafeeds_rpc,
    "binance-klines-1m": _handle_sync_binance_klines,
    "settlement-truth-rpc": _handle_sync_settlement_truth_rpc,
    "direct-oracle-prices": _handle_sync_direct_oracle_prices,
    "legacy-streams": _handle_sync_legacy_streams,
    "legacy-market-catalog": _handle_sync_legacy_market_catalog,
    "legacy-orderbook-depth": _handle_sync_legacy_orderbook_depth,
    "legacy-settlement-truth": _handle_sync_legacy_settlement_truth,
}

_BUILD_HANDLERS: dict[str, Callable[[argparse.Namespace, DataCliDeps], dict[str, Any]]] = {
    "oracle-prices-15m": _handle_build_oracle_prices,
    "truth-15m": _handle_build_truth,
    "orderbook-index": _handle_build_orderbook_index,
}

_EXPORT_HANDLERS: dict[str, Callable[[argparse.Namespace, DataCliDeps], dict[str, Any]]] = {
    "oracle-prices-15m": _handle_export_oracle_prices,
    "truth-15m": _handle_export_truth,
}

_RECORD_HANDLERS: dict[str, Callable[[argparse.Namespace, DataCliDeps], dict[str, Any]]] = {
    "orderbooks": _handle_record_orderbooks,
}

_RUN_HANDLERS: dict[str, Callable[[argparse.Namespace, DataCliDeps], dict[str, Any]]] = {
    "orderbook-fleet": _handle_run_orderbook_fleet,
    "live-foundation": _handle_run_live_foundation,
    "backtest-refresh": _handle_run_backtest_refresh,
}


def run_data_command(args: argparse.Namespace, *, deps: DataCliDeps) -> int:
    root_handler = _ROOT_HANDLERS.get(str(args.data_command or ""))
    if root_handler is not None:
        return _print_payload(root_handler(args, deps))

    if args.data_command == "sync":
        handler = _SYNC_HANDLERS.get(str(args.data_sync_command or ""))
        if handler is not None:
            return _print_payload(handler(args, deps))
        raise SystemExit("Missing data subcommand.")

    if args.data_command == "build":
        handler = _BUILD_HANDLERS.get(str(args.data_build_command or ""))
        if handler is not None:
            return _print_payload(handler(args, deps))
        raise SystemExit("Missing data subcommand.")

    if args.data_command == "export":
        handler = _EXPORT_HANDLERS.get(str(args.data_export_command or ""))
        if handler is not None:
            return _print_payload(handler(args, deps))
        raise SystemExit("Missing data subcommand.")

    if args.data_command == "record":
        handler = _RECORD_HANDLERS.get(str(args.data_record_command or ""))
        if handler is not None:
            return _print_payload(handler(args, deps))
        raise SystemExit("Missing data subcommand.")

    if args.data_command == "run":
        handler = _RUN_HANDLERS.get(str(args.data_run_command or ""))
        if handler is not None:
            return _print_payload(handler(args, deps))
        raise SystemExit("Missing data subcommand.")

    raise SystemExit("Missing data subcommand.")


def parse_utc_datetime(raw: str) -> datetime:
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
