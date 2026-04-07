#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pm15min.research.automation.focus_feature_search import (
    available_unused_baseline_features,
    build_market_focus_feature_sets,
    load_baseline_direction_summary_payloads,
    rank_focus_features,
    rank_focus_features_by_offset,
)
from pm15min.research.features.registry import feature_set_columns


def _custom_feature_sets_path() -> Path:
    return ROOT / "research" / "experiments" / "custom_feature_sets.json"


def _suite_spec_path(name: str) -> Path:
    return ROOT / "research" / "experiments" / "suite_specs" / f"{name}.json"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate focus feature search custom feature sets and suite specs")
    parser.add_argument("--markets", nargs="+", default=["btc", "eth", "sol", "xrp"])
    parser.add_argument("--widths", nargs="+", type=int, default=[12, 18, 24, 32])
    parser.add_argument("--decision-start", default="2026-03-28")
    parser.add_argument("--decision-end", default="2026-04-03")
    parser.add_argument("--window-start", default="2025-10-27")
    parser.add_argument("--window-end", default="2026-03-27")
    parser.add_argument("--stamp", default=datetime.now(timezone.utc).strftime("%Y%m%d"))
    parser.add_argument("--version", default="v1")
    args = parser.parse_args()

    all_payloads = []
    by_market: dict[str, list[dict[str, object]]] = {}
    for market in [str(item).strip().lower() for item in args.markets]:
        payloads = load_baseline_direction_summary_payloads(market)
        by_market[market] = payloads
        all_payloads.extend(payloads)

    global_ranked = rank_focus_features(all_payloads)
    global_offset_ranked = rank_focus_features_by_offset(all_payloads)
    custom_registry_path = _custom_feature_sets_path()
    custom_registry_path.parent.mkdir(parents=True, exist_ok=True)
    existing_payload: dict[str, object] = {}
    if custom_registry_path.exists():
        existing_payload = json.loads(custom_registry_path.read_text(encoding="utf-8"))
        if not isinstance(existing_payload, dict):
            raise TypeError(f"Expected mapping at {custom_registry_path}, got: {existing_payload!r}")

    for market, payloads in by_market.items():
        market_ranked = rank_focus_features(payloads)
        market_offset_ranked = rank_focus_features_by_offset(payloads)
        fill_candidates = list(feature_set_columns("bs_q_replace_direction")) + available_unused_baseline_features()
        feature_sets = build_market_focus_feature_sets(
            market=market,
            global_ranked_features=global_ranked,
            market_ranked_features=market_ranked,
            market_offset_ranked_features=market_offset_ranked,
            global_offset_ranked_features=global_offset_ranked,
            widths=tuple(int(width) for width in args.widths),
            fill_candidates=fill_candidates,
            version=str(args.version),
        )
        existing_payload.update(feature_sets)
        suite_name = f"baseline_focus_feature_search_{market}_{args.stamp}"
        suite_payload = {
            "suite_name": suite_name,
            "cycle": "15m",
            "profile": "deep_otm_baseline",
            "model_family": "deep_otm",
            "feature_set": "bs_q_replace_direction",
            "label_set": "truth",
            "target": "direction",
            "offsets": [7, 8, 9],
            "window": {
                "start": args.window_start,
                "end": args.window_end,
            },
            "decision_start": args.decision_start,
            "decision_end": args.decision_end,
            "backtest_spec": "baseline_truth",
            "parity": {
                "disable_ret_30m_direction_guard": True,
            },
            "stakes": [1.0],
            "max_notional_usd": 3.0,
            "max_trades_per_market_values": [3],
            "runtime_policy": {
                "completed_cases": "resume",
                "failed_cases": "rerun",
                "parallel_case_workers": 2,
            },
            "tags": [
                "factor_search",
                "focus_feature_search",
                "latest7d",
                "orderbook_locked",
                "noret30",
            ],
            "markets": {
                market: {
                    "groups": {
                        "focus_search": {
                            "runs": [
                                {
                                    "run_name": "focus_search",
                                    "feature_set_variants": [
                                        {
                                            "label": name.rsplit("_", 2)[1],
                                            "feature_set": name,
                                            "notes": payload.get("notes"),
                                        }
                                        for name, payload in feature_sets.items()
                                    ],
                                }
                            ]
                        }
                    }
                }
            },
        }
        path = _suite_spec_path(suite_name)
        path.write_text(json.dumps(suite_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"suite_spec={path}")

    custom_registry_path.write_text(json.dumps(existing_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"custom_feature_sets={custom_registry_path}")
    print("unused_feature_candidates=" + ",".join(available_unused_baseline_features()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
