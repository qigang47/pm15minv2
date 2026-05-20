#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pm15min.research.automation import (  # noqa: E402
    build_factor_scout_prompt,
    factor_scout_backlog_path,
    summarize_factor_scout_backlog,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare factor-scout prompts and inspect candidate backlog.")
    parser.add_argument("--root", default=str(ROOT), help="Repository root.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prompt = subparsers.add_parser("prompt", help="Print a public-source factor-scout prompt.")
    prompt.add_argument("--target", default="reversal")
    prompt.add_argument("--markets", default="btc,eth,sol,xrp")
    prompt.add_argument("--max-candidates", type=int, default=6)
    prompt.add_argument("--output", help="Optional path to write the prompt.")

    summary = subparsers.add_parser("summary", help="Print the current factor-scout backlog summary.")
    summary.add_argument("--limit", type=int, default=8)

    init = subparsers.add_parser("init", help="Create the factor-scout backlog file if missing.")
    init.add_argument("--force", action="store_true", help="Overwrite an existing empty scaffold.")

    args = parser.parse_args()
    root = Path(args.root).resolve()

    if args.command == "prompt":
        markets = [item.strip() for item in str(args.markets).split(",") if item.strip()]
        rendered = build_factor_scout_prompt(
            project_root=root,
            target=args.target,
            markets=markets,
            max_candidates=args.max_candidates,
        )
        if args.output:
            Path(args.output).write_text(rendered, encoding="utf-8")
        print(rendered)
        return 0

    if args.command == "summary":
        payload = summarize_factor_scout_backlog(root, limit=args.limit)
        print(f"path: {payload['path']}")
        print(f"candidates: {payload['candidate_count']}")
        for line in payload["lines"]:
            print(line)
        return 0

    if args.command == "init":
        backlog = factor_scout_backlog_path(root)
        if backlog.exists() and not args.force:
            print(f"exists: {backlog}")
            return 0
        backlog.parent.mkdir(parents=True, exist_ok=True)
        backlog.write_text(
            "\n".join(
                [
                    "# Deep OTM Baseline Factor Scout Backlog",
                    "",
                    "This file stores public-source candidate factor ideas found by the factor scout.",
                    "Candidates here are research leads only; they are not approved experiment config changes.",
                    "",
                    "## Intake Rules",
                    "",
                    "- Use public sources only.",
                    "- Do not paste long copyrighted excerpts.",
                    "- Do not add private, leaked, or unclear-provenance material.",
                    "- Every candidate must include a source URL and a test hypothesis.",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        print(f"created: {backlog}")
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
