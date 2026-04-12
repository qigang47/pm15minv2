from __future__ import annotations

import argparse
import json

from pm5min.core.config import LiveConfig


def print_payload(payload: dict[str, object]) -> int:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


def build_live_cfg(args: argparse.Namespace) -> LiveConfig:
    return LiveConfig.build(
        market=args.market,
        profile=args.profile,
        cycle_minutes=getattr(args, "cycle_minutes", 15),
    )


def build_show_config_cfg(args: argparse.Namespace) -> LiveConfig:
    return LiveConfig.build(
        market=args.market,
        profile=args.profile,
        cycle_minutes=args.cycle_minutes,
        loop=args.loop,
        refresh_interval_minutes=args.refresh_interval_minutes,
        decision_poll_interval_sec=args.decision_poll_interval_sec,
    )


def add_market_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--market", default="sol")


def add_profile_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", default="deep_otm")


def add_cycle_minutes_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cycle-minutes", type=int, default=15)


def add_market_profile_args(parser: argparse.ArgumentParser) -> None:
    add_market_arg(parser)
    add_profile_arg(parser)


def add_market_profile_cycle_args(parser: argparse.ArgumentParser) -> None:
    add_market_profile_args(parser)
    add_cycle_minutes_arg(parser)


def add_target_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--target", default="direction", choices=["direction"])


def add_feature_set_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--feature-set", default=None)


def add_no_persist_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--no-persist", action="store_true")


def add_adapter_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--adapter", default=None, choices=["legacy", "direct"])
