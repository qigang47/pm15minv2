from __future__ import annotations

from datetime import datetime, timezone


_CYCLE_ALIASES = {
    "5": "5m",
    "5m": "5m",
    "15": "15m",
    "15m": "15m",
    5: "5m",
    15: "15m",
}
_CYCLE_SECONDS = {"5m": 300, "15m": 900}
_SURFACE_ALIASES = {
    "live": "live",
    "backtest": "backtest",
}


def normalize_cycle(cycle: str | int) -> str:
    key = cycle if isinstance(cycle, int) else str(cycle).strip().lower()
    try:
        return _CYCLE_ALIASES[key]
    except KeyError as exc:
        raise ValueError(f"Unsupported cycle {cycle!r}. Expected one of: 5m, 15m") from exc


def cycle_seconds(cycle: str | int) -> int:
    return _CYCLE_SECONDS[normalize_cycle(cycle)]


def normalize_surface(surface: str) -> str:
    key = str(surface or "backtest").strip().lower()
    try:
        return _SURFACE_ALIASES[key]
    except KeyError as exc:
        raise ValueError(f"Unsupported surface {surface!r}. Expected one of: live, backtest") from exc


def utc_snapshot_label(now: datetime | None = None) -> str:
    ts = datetime.now(timezone.utc) if now is None else now.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H-%M-%SZ")
