from __future__ import annotations

from functools import lru_cache
import hashlib
import json
from pathlib import Path

from pm15min.research.backtests.runtime_cache import snapshot_source_mtimes


_ROOT = Path(__file__).resolve().parents[3]
_SIGNATURE_PATHS = (
    _ROOT / "pm15min" / "research" / "backtests" / "data_surface_fallback.py",
    _ROOT / "pm15min" / "research" / "backtests" / "decision_engine_parity.py",
    _ROOT / "pm15min" / "research" / "backtests" / "decision_quote_surface.py",
    _ROOT / "pm15min" / "research" / "backtests" / "engine.py",
    _ROOT / "pm15min" / "research" / "backtests" / "fills.py",
    _ROOT / "pm15min" / "research" / "backtests" / "grouped_grid.py",
    _ROOT / "pm15min" / "research" / "backtests" / "guard_parity.py",
    _ROOT / "pm15min" / "research" / "backtests" / "orderbook_surface.py",
    _ROOT / "pm15min" / "research" / "backtests" / "regime_parity.py",
    _ROOT / "pm15min" / "research" / "backtests" / "reports.py",
    _ROOT / "pm15min" / "live" / "guards" / "__init__.py",
    _ROOT / "pm15min" / "live" / "guards" / "quote.py",
    _ROOT / "pm15min" / "live" / "guards" / "regime.py",
    _ROOT / "pm15min" / "live" / "profiles" / "catalog.py",
)


@lru_cache(maxsize=1)
def backtest_build_signature() -> str:
    payload = snapshot_source_mtimes(list(_SIGNATURE_PATHS))
    digest = hashlib.sha1(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return digest[:12]


__all__ = ["backtest_build_signature"]
