from __future__ import annotations

import argparse
import json

from .compat import build_pm15min_research_deps, run_pm15min_research_command
from .config import ResearchConfig
from .service import (
    activate_model_bundle,
    describe_research_runtime,
    get_active_bundle_selection,
    list_model_bundles,
    list_training_runs,
)


def _print_payload(payload: object, *, sort_keys: bool = True) -> int:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=sort_keys))
    return 0


def _build_config(args: argparse.Namespace) -> ResearchConfig:
    return ResearchConfig.build(
        market=args.market,
        cycle=getattr(args, "cycle", "5m"),
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
    if args.research_command == "list-runs":
        return _print_payload(
            list_training_runs(
                _build_config(args),
                model_family=args.model_family,
                target=args.target,
                prefix=args.prefix,
            ),
            sort_keys=False,
        )
    if args.research_command == "list-bundles":
        return _print_payload(
            list_model_bundles(
                _build_config(args),
                profile=args.profile,
                target=args.target,
                prefix=args.prefix,
            ),
            sort_keys=False,
        )
    if args.research_command == "show-active-bundle":
        return _print_payload(
            get_active_bundle_selection(
                _build_config(args),
                profile=args.profile,
                target=args.target,
            )
        )
    if args.research_command == "activate-bundle":
        return _print_payload(
            activate_model_bundle(
                _build_config(args),
                profile=args.profile,
                target=args.target,
                bundle_label=args.bundle_label,
                notes=args.notes,
            )
        )
    deps = build_pm15min_research_deps(
        research_config_type=ResearchConfig,
        describe_runtime_fn=describe_research_runtime,
    )
    return run_pm15min_research_command(args, deps=deps)
