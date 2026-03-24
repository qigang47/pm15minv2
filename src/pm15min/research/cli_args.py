from __future__ import annotations

import argparse
from collections.abc import Sequence


def add_market_arg(parser: argparse.ArgumentParser, *, default: str = "btc") -> None:
    parser.add_argument("--market", default=default)


def add_cycle_arg(parser: argparse.ArgumentParser, *, default: str = "15m") -> None:
    parser.add_argument("--cycle", default=default)


def add_profile_arg(parser: argparse.ArgumentParser, *, default: str = "default") -> None:
    parser.add_argument("--profile", default=default)


def add_target_arg(
    parser: argparse.ArgumentParser,
    *,
    default: str = "direction",
    choices: Sequence[str] = ("direction", "reversal"),
) -> None:
    parser.add_argument("--target", default=default, choices=list(choices))


def add_market_cycle_args(
    parser: argparse.ArgumentParser,
    *,
    market_default: str = "btc",
    cycle_default: str = "15m",
) -> None:
    add_market_arg(parser, default=market_default)
    add_cycle_arg(parser, default=cycle_default)


def add_market_cycle_profile_args(
    parser: argparse.ArgumentParser,
    *,
    market_default: str = "btc",
    cycle_default: str = "15m",
    profile_default: str = "default",
) -> None:
    add_market_cycle_args(parser, market_default=market_default, cycle_default=cycle_default)
    add_profile_arg(parser, default=profile_default)

