#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pm15min.research.automation import record_session_update


def main() -> int:
    parser = argparse.ArgumentParser(description="Append one automation cycle update into a research session")
    parser.add_argument("--session-dir", required=True, help="Session directory")
    parser.add_argument("--cycle", required=True, help="Cycle identifier, for example 007")
    parser.add_argument("--team", required=True, help="Team label for results.tsv")
    parser.add_argument("--metric", required=True, help="Metric text for results.tsv")
    parser.add_argument("--status", required=True, help="Status text for results.tsv")
    parser.add_argument("--description", required=True, help="Description text for results.tsv")
    parser.add_argument("--files-changed", action="append", default=[], help="Repeatable changed file entry")
    parser.add_argument("--timestamp", help="Optional explicit timestamp")
    parser.add_argument("--cycle-eval-file", help="Optional markdown file to copy into cycles/<NNN>/eval-results.md")
    parser.add_argument("--cycle-note", action="append", default=[], help="Repeatable cycle note")
    parser.add_argument("--tried-line", action="append", default=[], help="Repeatable What's been tried line")
    parser.add_argument("--open-issue-line", action="append", default=[], help="Repeatable Open issues line")
    args = parser.parse_args()

    cycle_eval_md = None
    if args.cycle_eval_file:
        cycle_eval_md = Path(args.cycle_eval_file).read_text(encoding="utf-8")

    outputs = record_session_update(
        session_dir=Path(args.session_dir),
        cycle=args.cycle,
        team=args.team,
        metric=args.metric,
        status=args.status,
        description=args.description,
        files_changed=args.files_changed,
        timestamp=args.timestamp,
        cycle_eval_md=cycle_eval_md,
        cycle_notes=args.cycle_note,
        tried_lines=args.tried_line,
        open_issue_lines=args.open_issue_line,
    )
    for key, value in outputs.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
