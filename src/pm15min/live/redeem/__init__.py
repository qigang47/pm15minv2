from __future__ import annotations

from pm15min.data.io.json_files import append_jsonl
from pm15min.data.layout import utc_snapshot_label
from ..actions import apply_redeem_policy
from .runtime import run_live_redeem_loop as _run_live_redeem_loop_impl
from ..trading.gateway import LiveTradingGateway
from ..trading.service import build_live_trading_gateway_from_env
from ..persistence import write_live_payload_pair


def run_live_redeem_loop(
    cfg,
    *,
    iterations: int = 1,
    loop: bool = False,
    sleep_sec: float = 60.0,
    persist: bool = True,
    refresh_account_state: bool = True,
    dry_run: bool = False,
    max_conditions: int | None = None,
    adapter: str | None = None,
    gateway: LiveTradingGateway | None = None,
) -> dict[str, object]:
    resolved_gateway = gateway if gateway is not None else (
        None if adapter is None else build_live_trading_gateway_from_env(adapter_override=adapter)
    )
    return _run_live_redeem_loop_impl(
        cfg,
        iterations=iterations,
        loop=loop,
        sleep_sec=sleep_sec,
        persist=persist,
        refresh_account_state=refresh_account_state,
        dry_run=dry_run,
        max_conditions=max_conditions,
        gateway=resolved_gateway,
        apply_redeem_policy_fn=apply_redeem_policy,
        append_jsonl_fn=append_jsonl,
        write_live_payload_pair_fn=write_live_payload_pair,
        utc_now_iso_fn=utc_snapshot_label,
    )
