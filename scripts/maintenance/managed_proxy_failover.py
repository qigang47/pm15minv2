#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from pm15min.live.proxy_failover import (
    DEFAULT_HEALTHCHECK_URLS,
    build_proxy_candidates_from_ports,
    run_managed_proxy_failover,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Probe managed proxy candidates, keep the current healthy endpoint when possible, and write a managed proxy env file."
    )
    parser.add_argument(
        "--config-path",
        default=str(Path.home() / "_v2raya-lite" / "config.json"),
        help="Path to the v2raya-lite v2ray config.json.",
    )
    parser.add_argument(
        "--state-dir",
        default=str(Path.home() / ".local" / "state" / "pm15min-managed-proxy"),
        help="Directory for managed proxy state.json and active_proxy.env.",
    )
    parser.add_argument(
        "--candidate-port",
        action="append",
        dest="candidate_ports",
        type=int,
        help="Explicit managed proxy port to probe. Repeatable. When omitted, candidates are loaded from config-path.",
    )
    parser.add_argument(
        "--proxy-host",
        default="127.0.0.1",
        help="Proxy host for explicit or config-derived candidates.",
    )
    parser.add_argument(
        "--proxy-scheme",
        default="socks5h",
        help="Proxy scheme written to env and used for requests-based probing, such as socks5h or http.",
    )
    parser.add_argument(
        "--healthcheck-url",
        action="append",
        dest="healthcheck_urls",
        help="Healthcheck URL to probe through each proxy. Repeatable.",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=5.0,
        help="Per-request timeout for each healthcheck URL.",
    )
    parser.add_argument(
        "--refresh-command",
        default="",
        help="Optional shell command to refresh proxy subscriptions or node config if all candidates are unhealthy.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    candidates = None
    if args.candidate_ports:
        candidates = build_proxy_candidates_from_ports(args.candidate_ports)

    payload = run_managed_proxy_failover(
        config_path=Path(args.config_path),
        state_dir=Path(args.state_dir),
        candidates=candidates,
        proxy_host=str(args.proxy_host or "127.0.0.1"),
        proxy_scheme=str(args.proxy_scheme or "socks5h"),
        healthcheck_urls=tuple(args.healthcheck_urls or DEFAULT_HEALTHCHECK_URLS),
        timeout_sec=float(args.timeout_sec),
        refresh_command=str(args.refresh_command or "").strip() or None,
    )
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
