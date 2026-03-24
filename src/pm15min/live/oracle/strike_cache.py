from __future__ import annotations

import csv
import threading
import time
from dataclasses import dataclass
from pathlib import Path


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True)
class StrikeCacheRecord:
    cycle_start_ts: int
    strike_price: float
    observed_ts_ms: int
    source: str


class StrikeCache:
    """Append-only local strike cache keyed by cycle start seconds."""

    def __init__(self, *, path: str | Path, asset_slug: str) -> None:
        self.path = Path(path)
        self.asset_slug = str(asset_slug or "").strip().lower()
        self._lock = threading.Lock()
        self._loaded = False
        self._by_cycle_start_ts: dict[int, StrikeCacheRecord] = {}

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        if not self.path.exists():
            return
        try:
            with self.path.open("r", newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    if not row:
                        continue
                    asset = str(row.get("asset") or "").strip().lower()
                    if asset and asset != self.asset_slug:
                        continue
                    try:
                        cycle_start_ts = int(float(str(row.get("cycle_start_ts") or "")))
                        strike_price = float(str(row.get("strike_price") or ""))
                        observed_ts_ms = int(float(str(row.get("observed_ts_ms") or cycle_start_ts * 1000)))
                        source = str(row.get("source") or "").strip() or "unknown"
                    except Exception:
                        continue
                    if cycle_start_ts <= 0 or not (strike_price > 0.0):
                        continue
                    self._by_cycle_start_ts[cycle_start_ts] = StrikeCacheRecord(
                        cycle_start_ts=cycle_start_ts,
                        strike_price=strike_price,
                        observed_ts_ms=observed_ts_ms,
                        source=source,
                    )
        except Exception:
            return

    def get(self, cycle_start_ts: int) -> StrikeCacheRecord | None:
        self._ensure_loaded()
        try:
            return self._by_cycle_start_ts.get(int(cycle_start_ts))
        except Exception:
            return None

    def put(self, record: StrikeCacheRecord) -> None:
        if not self.asset_slug:
            return
        self._ensure_loaded()
        if record.cycle_start_ts <= 0 or not (record.strike_price > 0.0):
            return
        with self._lock:
            previous = self._by_cycle_start_ts.get(int(record.cycle_start_ts))
            if previous is not None and float(previous.strike_price) == float(record.strike_price):
                return
            self._by_cycle_start_ts[int(record.cycle_start_ts)] = record
            self.path.parent.mkdir(parents=True, exist_ok=True)
            new_file = not self.path.exists()
            with self.path.open("a", newline="", encoding="utf-8") as fh:
                fieldnames = [
                    "asset",
                    "cycle_start_ts",
                    "strike_price",
                    "observed_ts_ms",
                    "source",
                    "logged_at_ms",
                ]
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                if new_file:
                    writer.writeheader()
                writer.writerow(
                    {
                        "asset": self.asset_slug,
                        "cycle_start_ts": int(record.cycle_start_ts),
                        "strike_price": float(record.strike_price),
                        "observed_ts_ms": int(record.observed_ts_ms),
                        "source": str(record.source),
                        "logged_at_ms": _now_ms(),
                    }
                )
