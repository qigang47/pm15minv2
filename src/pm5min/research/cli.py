from __future__ import annotations

import argparse
import json

from .compat import (
    attach_pm15min_research_subcommands,
    build_pm15min_research_deps,
    run_pm15min_research_command,
)
from .config import ResearchConfig
from .service import describe_research_runtime


def attach_research_subcommands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    attach_pm15min_research_subcommands(subparsers)


def _print_payload(payload: dict[str, object]) -> int:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


def _build_config(args: argparse.Namespace) -> ResearchConfig:
    return ResearchConfig.build(
        market=args.market,
        cycle=getattr(args, "cycle", "15m"),
        profile=getattr(args, "profile", "default"),
        source_surface=getattr(args, "source_surface", "backtest"),
        feature_set=getattr(args, "feature_set", "deep_otm_v1"),
        label_set=getattr(args, "label_set", "truth"),
        target=getattr(args, "target", "direction"),
        model_family=getattr(args, "model_family", "deep_otm"),
        run_prefix=getattr(args, "run_prefix", None),
    )


def run_research_command(args: argparse.Namespace) -> int:
    if args.research_command == "show-config":
        return _print_payload(describe_research_runtime(_build_config(args)))
    if args.research_command == "show-layout":
        return _print_payload(_build_config(args).layout.to_dict())
    deps = build_pm15min_research_deps(
        research_config_type=ResearchConfig,
        describe_runtime_fn=describe_research_runtime,
    )
    return run_pm15min_research_command(args, deps=deps)
