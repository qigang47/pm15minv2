from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from pm15min.data.io.ndjson_zst import iter_ndjson_zst


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_ms() -> int:
    return int(_utc_now().timestamp() * 1000)


def _csv_tokens(raw: str) -> tuple[str, ...]:
    return tuple(token.strip().lower() for token in str(raw).split(",") if token.strip())


def _state_path(root: Path, *, cycle: str, asset: str) -> Path:
    return root / "var" / "live" / "state" / "orderbooks" / f"cycle={cycle}" / f"asset={asset}" / "state.json"


def _latest_full_path(root: Path, *, cycle: str, asset: str) -> Path:
    return root / "var" / "live" / "state" / "orderbooks" / f"cycle={cycle}" / f"asset={asset}" / "latest_full_depth.json"


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _int_or_none(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _parse_depth_path(state: dict[str, Any] | None) -> Path | None:
    if not isinstance(state, dict):
        return None
    summary = state.get("last_persisted_summary")
    if not isinstance(summary, dict):
        return None
    raw = summary.get("depth_path")
    if not isinstance(raw, str) or not raw.strip():
        return None
    return Path(raw)


def _latency_ms(*, captured_ts_ms: int | None, source_ts_ms: int | None) -> int | None:
    if captured_ts_ms is None or source_ts_ms is None:
        return None
    return int(captured_ts_ms) - int(source_ts_ms)


def _summary_stats(values: list[int]) -> dict[str, int | None]:
    if not values:
        return {"min": None, "p50": None, "p95": None, "max": None}
    ordered = sorted(int(value) for value in values)
    idx_95 = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * 0.95))))
    return {
        "min": int(ordered[0]),
        "p50": int(round(median(ordered))),
        "p95": int(ordered[idx_95]),
        "max": int(ordered[-1]),
    }


def _latest_batch_summary(*, latest_full: dict[str, Any] | None, now_ms: int) -> dict[str, Any]:
    if not isinstance(latest_full, dict):
        return {
            "selected_markets": None,
            "expected_rows": None,
            "actual_rows": 0,
            "batch_ok": False,
            "captured_ts_ms": None,
            "staleness_ms": None,
            "capture_latency_ms": {"min": None, "p50": None, "p95": None, "max": None},
            "end_to_end_latency_ms": {"min": None, "p50": None, "p95": None, "max": None},
            "negative_capture_latency_rows": 0,
        }
    records = [item for item in list(latest_full.get("records") or []) if isinstance(item, dict)]
    selected_markets = _int_or_none(latest_full.get("selected_markets")) or 0
    expected_rows = max(0, selected_markets * 2)
    actual_rows = len(records)
    captured_ts_ms = _int_or_none(latest_full.get("captured_ts_ms"))
    capture_latencies: list[int] = []
    end_to_end_latencies: list[int] = []
    negative_capture_latency_rows = 0
    for row in records:
        row_captured = _int_or_none(row.get("captured_ts_ms")) or captured_ts_ms
        row_source = _int_or_none(row.get("source_ts_ms"))
        capture_latency = _latency_ms(captured_ts_ms=row_captured, source_ts_ms=row_source)
        if capture_latency is not None:
            capture_latencies.append(max(0, int(capture_latency)))
            if capture_latency < 0:
                negative_capture_latency_rows += 1
        if row_source is not None:
            end_to_end_latencies.append(int(now_ms - int(row_source)))
    return {
        "selected_markets": int(selected_markets),
        "expected_rows": int(expected_rows),
        "actual_rows": int(actual_rows),
        "batch_ok": bool(expected_rows > 0 and actual_rows == expected_rows),
        "captured_ts_ms": captured_ts_ms,
        "staleness_ms": None if captured_ts_ms is None else int(now_ms - captured_ts_ms),
        "capture_latency_ms": _summary_stats(capture_latencies),
        "end_to_end_latency_ms": _summary_stats(end_to_end_latencies),
        "negative_capture_latency_rows": int(negative_capture_latency_rows),
    }


def _lookback_summary(
    *,
    depth_path: Path | None,
    expected_rows_per_batch: int,
    cutoff_ts_ms: int,
) -> dict[str, Any]:
    if depth_path is None or not depth_path.exists():
        return {
            "depth_path": None if depth_path is None else str(depth_path),
            "lookback_rows": 0,
            "batches": 0,
            "full_batches": 0,
            "partial_batches": 0,
            "full_batch_rate": None,
            "avg_rows_per_batch": None,
            "capture_latency_ms": {"min": None, "p50": None, "p95": None, "max": None},
            "negative_capture_latency_rows": 0,
        }

    rows_by_batch: dict[int, int] = defaultdict(int)
    capture_latencies: list[int] = []
    negative_capture_latency_rows = 0
    lookback_rows = 0

    for row in iter_ndjson_zst(depth_path):
        if not isinstance(row, dict):
            continue
        captured_ts_ms = _int_or_none(row.get("captured_ts_ms"))
        if captured_ts_ms is None or captured_ts_ms < int(cutoff_ts_ms):
            continue
        lookback_rows += 1
        rows_by_batch[int(captured_ts_ms)] += 1
        source_ts_ms = _int_or_none(row.get("source_ts_ms"))
        capture_latency = _latency_ms(captured_ts_ms=captured_ts_ms, source_ts_ms=source_ts_ms)
        if capture_latency is not None:
            capture_latencies.append(max(0, int(capture_latency)))
            if capture_latency < 0:
                negative_capture_latency_rows += 1

    batches = len(rows_by_batch)
    if expected_rows_per_batch > 0:
        full_batches = sum(1 for count in rows_by_batch.values() if int(count) >= int(expected_rows_per_batch))
    else:
        full_batches = 0
    partial_batches = sum(1 for count in rows_by_batch.values() if 0 < int(count) < int(expected_rows_per_batch))
    full_batch_rate = None if batches <= 0 else round(float(full_batches) / float(batches), 6)
    avg_rows_per_batch = None if batches <= 0 else round(float(lookback_rows) / float(batches), 3)

    return {
        "depth_path": str(depth_path),
        "lookback_rows": int(lookback_rows),
        "batches": int(batches),
        "full_batches": int(full_batches),
        "partial_batches": int(partial_batches),
        "full_batch_rate": full_batch_rate,
        "avg_rows_per_batch": avg_rows_per_batch,
        "capture_latency_ms": _summary_stats(capture_latencies),
        "negative_capture_latency_rows": int(negative_capture_latency_rows),
    }


@dataclass(frozen=True)
class ReportConfig:
    root: Path
    cycle: str
    assets: tuple[str, ...]
    lookback_minutes: int


def _build_asset_report(*, cfg: ReportConfig, asset: str) -> dict[str, Any]:
    now_ms = _utc_now_ms()
    cutoff_ts_ms = now_ms - int(cfg.lookback_minutes) * 60_000
    state_path = _state_path(cfg.root, cycle=cfg.cycle, asset=asset)
    latest_full_path = _latest_full_path(cfg.root, cycle=cfg.cycle, asset=asset)
    state = _load_json(state_path)
    latest_full = _load_json(latest_full_path)

    latest_batch = _latest_batch_summary(latest_full=latest_full, now_ms=now_ms)
    expected_rows = _int_or_none(latest_batch.get("expected_rows")) or 0
    lookback = _lookback_summary(
        depth_path=_parse_depth_path(state),
        expected_rows_per_batch=expected_rows,
        cutoff_ts_ms=cutoff_ts_ms,
    )

    return {
        "asset": asset,
        "cycle": cfg.cycle,
        "lookback_minutes": int(cfg.lookback_minutes),
        "state_path": str(state_path),
        "latest_full_path": str(latest_full_path),
        "recorder_state": {
            "status": None if state is None else state.get("status"),
            "completed_iterations": None if state is None else _int_or_none(state.get("completed_iterations")),
            "persisted_iterations": None if state is None else _int_or_none(state.get("persisted_iterations")),
            "write_errors": None if state is None else _int_or_none(state.get("write_errors")),
            "pending_batches": None if state is None else _int_or_none(state.get("pending_batches")),
            "last_completed_at": None if state is None else state.get("last_completed_at"),
            "last_persisted_at": None if state is None else state.get("last_persisted_at"),
        },
        "latest_batch": latest_batch,
        "lookback": lookback,
    }


def _render_text(report: dict[str, Any]) -> str:
    lines: list[str] = []
    for row in list(report.get("assets") or []):
        asset = row.get("asset")
        state = row.get("recorder_state") or {}
        latest = row.get("latest_batch") or {}
        lookback = row.get("lookback") or {}
        lines.append(f"[{asset}] status={state.get('status')} stale_ms={latest.get('staleness_ms')} full_rate={lookback.get('full_batch_rate')}")
        lines.append(
            "  "
            f"latest_rows={latest.get('actual_rows')}/{latest.get('expected_rows')} "
            f"capture_p50={((latest.get('capture_latency_ms') or {}).get('p50'))}ms "
            f"capture_p95={((latest.get('capture_latency_ms') or {}).get('p95'))}ms "
            f"e2e_p50={((latest.get('end_to_end_latency_ms') or {}).get('p50'))}ms"
        )
        lines.append(
            "  "
            f"lookback_batches={lookback.get('batches')} "
            f"full={lookback.get('full_batches')} "
            f"partial={lookback.get('partial_batches')} "
            f"rows_per_batch={lookback.get('avg_rows_per_batch')}"
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Report orderbook recorder success rate and latency.")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--cycle", default="15m")
    parser.add_argument("--assets", default="btc,eth,sol,xrp")
    parser.add_argument("--lookback-minutes", type=int, default=15)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    cfg = ReportConfig(
        root=Path(args.root),
        cycle=str(args.cycle).strip(),
        assets=_csv_tokens(args.assets),
        lookback_minutes=max(1, int(args.lookback_minutes)),
    )
    payload = {
        "ts": _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "root": str(cfg.root),
        "cycle": cfg.cycle,
        "lookback_minutes": int(cfg.lookback_minutes),
        "assets": [_build_asset_report(cfg=cfg, asset=asset) for asset in cfg.assets],
    }
    if bool(args.json):
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(_render_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
