from __future__ import annotations

import argparse
from collections.abc import Sequence


def add_market_arg(parser: argparse.ArgumentParser, *, default: str = "btc") -> None:
    parser.add_argument("--market", default=default)


def add_cycle_arg(parser: argparse.ArgumentParser, *, default: str = "15m") -> None:
    parser.add_argument("--cycle", default=default)


def add_surface_arg(
    parser: argparse.ArgumentParser,
    *,
    default: str = "backtest",
    choices: Sequence[str] = ("live", "backtest"),
) -> None:
    parser.add_argument("--surface", default=default, choices=list(choices))


def add_market_cycle_surface_args(
    parser: argparse.ArgumentParser,
    *,
    market_default: str = "btc",
    cycle_default: str = "15m",
    surface_default: str = "backtest",
    surface_choices: Sequence[str] = ("live", "backtest"),
) -> None:
    add_market_arg(parser, default=market_default)
    add_cycle_arg(parser, default=cycle_default)
    add_surface_arg(parser, default=surface_default, choices=surface_choices)
