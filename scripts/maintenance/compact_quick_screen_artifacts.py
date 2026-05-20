#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pm15min.research.automation.quick_screen import compact_quick_screen_artifacts
from pm15min.research.config import ResearchConfig
from pm15min.research.experiments.specs import load_suite_definition
from pm15min.research.layout import ResearchLayout


def main() -> int:
    parser = argparse.ArgumentParser(description="Compact retained artifacts from sparse quick-screen runs")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--market", action="append", default=[])
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    root = args.root.resolve()
    layout = ResearchLayout.discover(root)
    allowed_markets = {str(item).strip().lower() for item in args.market if str(item).strip()}
    rows: list[dict[str, object]] = []
    processed_paths: set[str] = set()

    for leaderboard_path in sorted(layout.experiment_runs_root.glob("suite=*/run=*/quick_screen_leaderboard.csv")):
        suite_name = _token_value(leaderboard_path.parents[1].name, prefix="suite=")
        suite = _load_suite(layout=layout, suite_name=suite_name)
        if suite is None:
            continue
        spec_by_market = {
            str(market_spec.market).lower(): market_spec
            for market_spec in suite.markets
        }
        frame = pd.read_csv(leaderboard_path)
        if frame.empty:
            continue
        for row in frame.to_dict(orient="records"):
            market = str(row.get("market") or "").strip().lower()
            if allowed_markets and market not in allowed_markets:
                continue
            market_spec = spec_by_market.get(market)
            if market_spec is None:
                market_spec = _fallback_market_spec(row=row, suite=suite)
            cfg = ResearchConfig.build(
                market=market or getattr(market_spec, "market", ""),
                cycle=suite.cycle,
                profile=market_spec.profile,
                source_surface="backtest",
                feature_set=market_spec.feature_set,
                label_set=market_spec.label_set,
                target=market_spec.target,
                model_family=market_spec.model_family,
                root=root,
            )
            train_result = {"run_dir": row.get("training_run_dir")}
            bundle_result = {"bundle_dir": row.get("bundle_dir")}
            quick_summary = {
                "trade_rows": row.get("trade_rows"),
                "profitable_pool_capture_rows": row.get("profitable_pool_capture_rows"),
            }
            cleanup = compact_quick_screen_artifacts(
                cfg=cfg,
                market_spec=market_spec,
                train_result=train_result,
                bundle_result=bundle_result,
                quick_summary=quick_summary,
                apply=bool(args.apply),
            )
            if cleanup.get("artifacts_retained"):
                continue
            paths = [str(path) for path in (cleanup.get("removed_paths") or cleanup.get("would_remove_paths") or [])]
            new_paths = [path for path in paths if path not in processed_paths]
            if not new_paths:
                continue
            processed_paths.update(new_paths)
            rows.append(
                {
                    "mode": "apply" if args.apply else "dry_run",
                    "suite": suite_name,
                    "run": _token_value(leaderboard_path.parent.name, prefix="run="),
                    "market": market,
                    "feature_set": row.get("feature_set"),
                    "trade_rows": row.get("trade_rows"),
                    "reason": cleanup.get("retention_reason"),
                    "path_count": len(new_paths),
                    "paths": new_paths,
                }
            )
            if args.limit and len(rows) >= int(args.limit):
                print(json.dumps(_summary(rows), indent=2, ensure_ascii=False))
                return 0

    print(json.dumps(_summary(rows), indent=2, ensure_ascii=False))
    return 0


def _summary(rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "rows": len(rows),
        "path_count": int(sum(int(row.get("path_count") or 0) for row in rows)),
        "items": rows,
    }


def _load_suite(*, layout: ResearchLayout, suite_name: str):
    suite_path = layout.suite_spec_path(suite_name)
    if not suite_path.exists():
        suite_path = layout.experiment_runs_root / f"suite={suite_name}" / "suite.json"
    if not suite_path.exists():
        return None
    return load_suite_definition(suite_path)


def _fallback_market_spec(*, row: dict[str, object], suite) -> SimpleNamespace:
    sample = suite.markets[0]
    return SimpleNamespace(
        market=str(row.get("market") or getattr(sample, "market", "")),
        profile=getattr(sample, "profile", ""),
        feature_set=str(row.get("feature_set") or getattr(sample, "feature_set", "")),
        label_set=getattr(sample, "label_set", "truth"),
        target=str(row.get("target") or getattr(sample, "target", "direction")),
        model_family=getattr(sample, "model_family", "deep_otm"),
        window=getattr(sample, "window", SimpleNamespace(label="")),
        offsets=tuple(getattr(sample, "offsets", ()) or ()),
    )


def _token_value(value: str, *, prefix: str) -> str:
    text = str(value)
    return text[len(prefix):] if text.startswith(prefix) else text


if __name__ == "__main__":
    raise SystemExit(main())
