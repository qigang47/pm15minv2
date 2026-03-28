from __future__ import annotations

import argparse
import json

from ..config import DataConfig
from .foundation_runtime import run_live_data_foundation_shared


def _parse_markets(raw: str) -> list[str]:
    markets: list[str] = []
    for item in str(raw or "").split(","):
        market = item.strip().lower()
        if not market:
            continue
        if market not in {"btc", "eth", "sol", "xrp"}:
            raise ValueError(f"unsupported shared live foundation market: {market}")
        if market not in markets:
            markets.append(market)
    if not markets:
        raise ValueError("shared live foundation requires at least one market.")
    return markets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the shared live foundation refresh loop.")
    parser.add_argument("--markets", required=True, help="Comma-separated markets, e.g. btc,eth,sol,xrp")
    parser.add_argument("--cycle", default="15m")
    parser.add_argument("--surface", default="live")
    parser.add_argument("--market-depth", type=int, default=1)
    parser.add_argument("--timeout-sec", type=float, default=1.2)
    parser.add_argument("--recent-window-minutes", type=int, default=15)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--sleep-sec", type=float, default=1.0)
    parser.add_argument("--market-catalog-refresh-sec", type=float, default=300.0)
    parser.add_argument("--binance-refresh-sec", type=float, default=60.0)
    parser.add_argument("--oracle-refresh-sec", type=float, default=60.0)
    parser.add_argument("--streams-refresh-sec", type=float, default=300.0)
    parser.add_argument("--orderbook-refresh-sec", type=float, default=0.35)
    parser.add_argument("--market-catalog-lookback-hours", type=int, default=24)
    parser.add_argument("--market-catalog-lookahead-hours", type=int, default=24)
    parser.add_argument("--binance-lookback-minutes", type=int, default=2880)
    parser.add_argument("--binance-batch-limit", type=int, default=1000)
    parser.add_argument("--oracle-lookback-days", type=int, default=2)
    parser.add_argument("--oracle-lookahead-hours", type=int, default=24)
    parser.add_argument("--no-direct-oracle", action="store_true")
    parser.add_argument("--no-streams", action="store_true")
    parser.add_argument("--no-orderbooks", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    cfgs = [
        DataConfig.build(
            market=market,
            cycle=args.cycle,
            surface=args.surface,
            market_depth=int(args.market_depth),
            recent_window_minutes=int(args.recent_window_minutes),
            orderbook_timeout_sec=float(args.timeout_sec),
        )
        for market in _parse_markets(args.markets)
    ]
    payload = run_live_data_foundation_shared(
        cfgs,
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
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
