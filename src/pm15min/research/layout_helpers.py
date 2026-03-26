from __future__ import annotations

from datetime import date, datetime, timezone


_CYCLE_ALIASES = {
    "5": "5m",
    "5m": "5m",
    "15": "15m",
    "15m": "15m",
    5: "5m",
    15: "15m",
}
_TARGET_ALIASES = {
    "direction": "direction",
    "dir": "direction",
    "reversal": "reversal",
    "rev": "reversal",
}
_SOURCE_SURFACE_ALIASES = {
    "live": "live",
    "backtest": "backtest",
}


def normalize_cycle(cycle: str | int) -> str:
    key = cycle if isinstance(cycle, int) else str(cycle).strip().lower()
    try:
        return _CYCLE_ALIASES[key]
    except KeyError as exc:
        raise ValueError(f"Unsupported cycle {cycle!r}. Expected one of: 5m, 15m") from exc


def normalize_target(target: str) -> str:
    key = str(target or "direction").strip().lower()
    try:
        return _TARGET_ALIASES[key]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported target {target!r}. Expected one of: direction, reversal"
        ) from exc


def normalize_source_surface(surface: str) -> str:
    key = str(surface or "backtest").strip().lower()
    try:
        return _SOURCE_SURFACE_ALIASES[key]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported source surface {surface!r}. Expected one of: live, backtest"
        ) from exc


def slug_token(value: str | None, *, default: str = "default") -> str:
    text = str(value or default).strip().lower()
    cleaned = []
    for ch in text:
        if ch.isalnum() or ch in {"-", "_", "."}:
            cleaned.append(ch)
        else:
            cleaned.append("_")
    token = "".join(cleaned).strip("._-")
    return token or default


def utc_run_label(now: datetime | None = None) -> str:
    ts = datetime.now(timezone.utc) if now is None else now.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H-%M-%SZ")


def date_text(value: str | date | datetime) -> str:
    return normalize_window_bound(value)


def normalize_window_bound(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        return _format_window_datetime(_coerce_utc_datetime(value))
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        raise ValueError("date value cannot be empty")
    if _looks_like_date_only(text):
        return date.fromisoformat(text[:10]).isoformat()
    return _format_window_datetime(_parse_window_datetime_text(text))


def window_bound_is_date_only(value: str | date | datetime) -> bool:
    normalized = normalize_window_bound(value)
    return _looks_like_date_only(normalized)


def window_label(start: str | date | datetime, end: str | date | datetime) -> str:
    return f"{_window_bound_label(start)}_{_window_bound_label(end)}"


def _window_bound_label(value: str | date | datetime) -> str:
    normalized = normalize_window_bound(value)
    if _looks_like_date_only(normalized):
        return normalized
    return normalized.replace(":", "-")


def _looks_like_date_only(text: str) -> bool:
    return len(text) == 10 and text[4] == "-" and text[7] == "-"


def _parse_window_datetime_text(text: str) -> datetime:
    normalized = text.replace(" ", "T")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    dt = datetime.fromisoformat(normalized)
    return _coerce_utc_datetime(dt)


def _coerce_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_window_datetime(value: datetime) -> str:
    return _coerce_utc_datetime(value).strftime("%Y-%m-%dT%H:%M:%SZ")
