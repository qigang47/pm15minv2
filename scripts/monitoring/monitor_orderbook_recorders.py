from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(text: str | None) -> datetime | None:
    if not text:
        return None
    try:
        return datetime.strptime(str(text), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _csv_tokens(raw: str) -> list[str]:
    return [token.strip().lower() for token in str(raw).split(",") if token.strip()]


@dataclass(frozen=True)
class MonitorConfig:
    root: Path
    cycles: tuple[str, ...]
    assets: tuple[str, ...]
    max_pending_batches: int
    max_persist_lag: int
    max_stale_seconds: float
    interval_sec: float
    log_ok: bool
    loop: bool


def _state_path(root: Path, *, cycle: str, asset: str) -> Path:
    return root / "var" / "live" / "state" / "orderbooks" / f"cycle={cycle}" / f"asset={asset}" / "state.json"


def _emit(level: str, message: str, **fields: object) -> None:
    payload: dict[str, object] = {
        "ts": _utc_now_iso(),
        "level": level,
        "message": message,
    }
    payload.update(fields)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)


def _load_state(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _check_once(cfg: MonitorConfig) -> int:
    issue_count = 0
    now = _utc_now()
    for cycle in cfg.cycles:
        for asset in cfg.assets:
            path = _state_path(cfg.root, cycle=cycle, asset=asset)
            if not path.exists():
                issue_count += 1
                _emit(
                    "error",
                    "orderbook_state_missing",
                    cycle=cycle,
                    asset=asset,
                    path=str(path),
                )
                continue

            state = _load_state(path)
            if state is None:
                issue_count += 1
                _emit(
                    "error",
                    "orderbook_state_unreadable",
                    cycle=cycle,
                    asset=asset,
                    path=str(path),
                )
                continue

            status = str(state.get("status") or "")
            pending_batches = int(state.get("pending_batches") or 0)
            write_errors = int(state.get("write_errors") or 0)
            completed_iterations = int(state.get("completed_iterations") or 0)
            persisted_iterations = int(state.get("persisted_iterations") or 0)
            persist_lag = max(0, completed_iterations - persisted_iterations)
            last_completed_at = _parse_utc(state.get("last_completed_at"))
            stale_seconds = None if last_completed_at is None else max(0.0, (now - last_completed_at).total_seconds())

            issues: list[str] = []
            if status != "running":
                issues.append(f"status={status or 'missing'}")
            if write_errors > 0:
                issues.append(f"write_errors={write_errors}")
            if pending_batches > cfg.max_pending_batches:
                issues.append(f"pending_batches={pending_batches}")
            if persist_lag > cfg.max_persist_lag:
                issues.append(f"persist_lag={persist_lag}")
            if stale_seconds is None:
                issues.append("last_completed_at=missing")
            elif stale_seconds > cfg.max_stale_seconds:
                issues.append(f"stale_seconds={round(stale_seconds, 1)}")

            payload = {
                "cycle": cycle,
                "asset": asset,
                "status": status,
                "persistence_mode": state.get("persistence_mode"),
                "pending_batches": pending_batches,
                "persist_lag": persist_lag,
                "write_errors": write_errors,
                "completed_iterations": completed_iterations,
                "persisted_iterations": persisted_iterations,
                "stale_seconds": None if stale_seconds is None else round(stale_seconds, 3),
                "path": str(path),
            }
            if issues:
                issue_count += 1
                _emit("error", "orderbook_recorder_unhealthy", issues=issues, **payload)
            elif cfg.log_ok:
                _emit("ok", "orderbook_recorder_healthy", **payload)
    return issue_count


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor live orderbook recorder state files.")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--cycles", default="5m,15m")
    parser.add_argument("--assets", default="btc,eth,sol,xrp")
    parser.add_argument("--max-pending-batches", type=int, default=20)
    parser.add_argument("--max-persist-lag", type=int, default=20)
    parser.add_argument("--max-stale-seconds", type=float, default=60.0)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--log-ok", action="store_true")
    args = parser.parse_args()

    cfg = MonitorConfig(
        root=Path(args.root),
        cycles=tuple(_csv_tokens(args.cycles)),
        assets=tuple(_csv_tokens(args.assets)),
        max_pending_batches=max(0, int(args.max_pending_batches)),
        max_persist_lag=max(0, int(args.max_persist_lag)),
        max_stale_seconds=max(1.0, float(args.max_stale_seconds)),
        interval_sec=max(1.0, float(args.interval_sec)),
        log_ok=bool(args.log_ok),
        loop=bool(args.loop),
    )
    if not cfg.cycles or not cfg.assets:
        print("cycles/assets must not be empty", file=sys.stderr)
        return 2

    while True:
        issue_count = _check_once(cfg)
        if not cfg.loop:
            return 1 if issue_count > 0 else 0
        time.sleep(cfg.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
