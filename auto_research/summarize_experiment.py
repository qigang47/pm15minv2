#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pm15min.research.automation import summarize_experiment_run


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize a completed experiment run directory")
    parser.add_argument("--run-dir", required=True, help="Absolute or repo-relative experiment run directory")
    parser.add_argument("--output", help="Optional output JSON path")
    args = parser.parse_args()

    payload = summarize_experiment_run(Path(args.run_dir))
    rendered = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True)
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
