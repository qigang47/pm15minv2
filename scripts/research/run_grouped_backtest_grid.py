from __future__ import annotations

import argparse
import json
from pathlib import Path

from pm15min.research.backtests.grouped_grid import load_grouped_backtest_grid_spec, run_grouped_backtest_grid


def main() -> None:
    parser = argparse.ArgumentParser(description="Run grouped backtest grid with per-group runtime reuse.")
    parser.add_argument("--spec", required=True, help="Path to grouped backtest grid JSON spec.")
    parser.add_argument("--workers", type=int, default=6, help="Number of concurrent group worker processes.")
    parser.add_argument("--root", default=".", help="Project root path.")
    args = parser.parse_args()

    spec = load_grouped_backtest_grid_spec(Path(args.spec))
    summary = run_grouped_backtest_grid(
        spec=spec,
        root=Path(args.root).resolve(),
        group_workers=max(1, int(args.workers)),
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
