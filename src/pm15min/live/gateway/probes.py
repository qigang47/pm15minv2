from __future__ import annotations

import importlib.util

from pm15min.core.config import LiveConfig
from ..trading.gateway import LiveTradingGateway


def build_live_trading_smoke_runs(cfg: LiveConfig, *, adapter: str) -> list[dict[str, str]]:
    base = "PYTHONPATH=v2/src python -m pm15min"
    market = cfg.asset.slug
    profile = cfg.profile
    adapter_flag = f" --adapter {adapter}" if adapter else ""
    return [
        {
            "name": "gateway_check",
            "command": f"{base} live check-trading-gateway --market {market} --profile {profile}{adapter_flag}",
        },
        {
            "name": "probe_open_orders",
            "command": (
                f"{base} live check-trading-gateway --market {market} --profile {profile}"
                f"{adapter_flag} --probe-open-orders"
            ),
        },
        {
            "name": "probe_positions",
            "command": (
                f"{base} live check-trading-gateway --market {market} --profile {profile}"
                f"{adapter_flag} --probe-positions"
            ),
        },
        {
            "name": "runner_dry_run",
            "command": (
                f"{base} live runner-once --market {market} --profile {profile}"
                f"{adapter_flag} --dry-run-side-effects"
            ),
        },
    ]


def probe_gateway_call(
    *,
    gateway: LiveTradingGateway | None,
    enabled: bool,
    fn_name: str,
) -> dict[str, object]:
    if not enabled:
        return {"enabled": False, "ok": True, "status": "skipped", "reason": "probe_disabled"}
    if gateway is None:
        return {"enabled": True, "ok": False, "status": "error", "reason": "gateway_unavailable"}
    try:
        rows = list(getattr(gateway, fn_name)())
    except Exception as exc:
        return {
            "enabled": True,
            "ok": False,
            "status": "error",
            "reason": f"{type(exc).__name__}: {exc}",
        }
    return {
        "enabled": True,
        "ok": True,
        "status": "ok",
        "row_count": len(rows),
    }


def module_available(name: str) -> bool:
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:
        return False
