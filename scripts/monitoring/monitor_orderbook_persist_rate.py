from __future__ import annotations

import argparse
import json
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


def _csv_tokens(raw: str) -> tuple[str, ...]:
    return tuple(token.strip().lower() for token in str(raw).split(",") if token.strip())


@dataclass(frozen=True)
class MonitorConfig:
    root: Path
    cycles: tuple[str, ...]
    assets: tuple[str, ...]
    interval_sec: float
    max_stale_seconds: float
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


def _run_once(
    cfg: MonitorConfig,
    *,
    previous_counts: dict[tuple[str, str], int],
    previous_ts: float | None,
) -> tuple[dict[tuple[str, str], int], float]:
    now_monotonic = time.monotonic()
    elapsed = None if previous_ts is None else max(1e-9, now_monotonic - previous_ts)
    current_counts: dict[tuple[str, str], int] = {}
    now_utc = _utc_now()

    for cycle in cfg.cycles:
        for asset in cfg.assets:
            path = _state_path(cfg.root, cycle=cycle, asset=asset)
            state = _load_state(path)
            if state is None:
                _emit("error", "persist_rate_state_unreadable", cycle=cycle, asset=asset, path=str(path))
                continue

            persisted_iterations = int(state.get("persisted_iterations") or 0)
            current_counts[(cycle, asset)] = persisted_iterations
            previous_count = previous_counts.get((cycle, asset))
            persisted_delta = None if previous_count is None else max(0, persisted_iterations - previous_count)
            writes_per_sec = None if persisted_delta is None or elapsed is None else round(persisted_delta / elapsed, 6)

            last_persisted_at = _parse_utc(state.get("last_persisted_at"))
            stale_seconds = None if last_persisted_at is None else max(0.0, (now_utc - last_persisted_at).total_seconds())
            level = "warn" if stale_seconds is not None and stale_seconds > cfg.max_stale_seconds else "info"
            message = "persist_rate_sample" if level == "info" else "persist_rate_stale"

            _emit(
                level,
                message,
                cycle=cycle,
                asset=asset,
                persistence_mode=state.get("persistence_mode"),
                pending_batches=int(state.get("pending_batches") or 0),
                write_errors=int(state.get("write_errors") or 0),
                persisted_iterations=persisted_iterations,
                persisted_delta=persisted_delta,
                writes_per_sec=writes_per_sec,
                last_persisted_at=state.get("last_persisted_at"),
                stale_seconds=None if stale_seconds is None else round(stale_seconds, 3),
                path=str(path),
            )

    return current_counts, now_monotonic


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor orderbook recorder persist frequency.")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--cycles", default="5m,15m")
    parser.add_argument("--assets", default="btc,eth,sol,xrp")
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--max-stale-seconds", type=float, default=60.0)
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()

    cfg = MonitorConfig(
        root=Path(args.root),
        cycles=_csv_tokens(args.cycles),
        assets=_csv_tokens(args.assets),
        interval_sec=max(1.0, float(args.interval_sec)),
        max_stale_seconds=max(1.0, float(args.max_stale_seconds)),
        loop=bool(args.loop),
    )

    previous_counts: dict[tuple[str, str], int] = {}
    previous_ts: float | None = None
    while True:
        previous_counts, previous_ts = _run_once(cfg, previous_counts=previous_counts, previous_ts=previous_ts)
        if not cfg.loop:
            return 0
        time.sleep(cfg.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
