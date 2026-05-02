#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pm15min.research.automation.quick_screen import prewarm_profitable_offset_pool_cache
from pm15min.research.config import ResearchConfig


def _parse_markets(raw: str) -> list[str]:
    return [item.strip().lower() for item in str(raw or "").split(",") if item.strip()]


def _parse_offsets(raw: str) -> tuple[int, ...]:
    values: list[int] = []
    for item in str(raw or "").split(","):
        token = item.strip()
        if not token:
            continue
        values.append(int(token))
    return tuple(values or (7, 8, 9))


def main() -> int:
    parser = argparse.ArgumentParser(description="Prewarm profitable offset pool caches for dense autoresearch.")
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--markets", default="btc,eth,sol,xrp")
    parser.add_argument("--cycle", default="15m")
    parser.add_argument("--profile", default="deep_otm_baseline")
    parser.add_argument("--feature-set", default="bs_q_replace_direction")
    parser.add_argument("--label-set", default="truth")
    parser.add_argument("--target", default="direction")
    parser.add_argument("--model-family", default="deep_otm")
    parser.add_argument("--decision-start", default="2026-04-01")
    parser.add_argument("--decision-end", default="2026-04-30")
    parser.add_argument("--stake-label", default="2usd")
    parser.add_argument("--offsets", default="7,8,9")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    markets = _parse_markets(args.markets)
    offsets = _parse_offsets(args.offsets)

    results: list[dict[str, object]] = []
    for market in markets:
        cfg = ResearchConfig.build(
            market=market,
            cycle=args.cycle,
            profile=args.profile,
            source_surface="backtest",
            feature_set=args.feature_set,
            label_set=args.label_set,
            target=args.target,
            model_family=args.model_family,
            root=root,
        )
        results.append(
            prewarm_profitable_offset_pool_cache(
                cfg=cfg,
                profile=args.profile,
                decision_start=args.decision_start,
                decision_end=args.decision_end,
                stake_label=args.stake_label,
                offsets=offsets,
            )
        )

    payload = {
        "markets": markets,
        "cycle": args.cycle,
        "profile": args.profile,
        "feature_set": args.feature_set,
        "label_set": args.label_set,
        "target": args.target,
        "model_family": args.model_family,
        "decision_start": args.decision_start,
        "decision_end": args.decision_end,
        "stake_label": args.stake_label,
        "offsets": list(offsets),
        "results": results,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
