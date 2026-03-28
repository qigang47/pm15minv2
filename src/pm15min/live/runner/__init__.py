from __future__ import annotations

from pathlib import Path

from pm15min.core.config import LiveConfig
from pm15min.data.io.json_files import append_jsonl, write_json_atomic
from pm15min.data.layout import utc_snapshot_label
from pm15min.data.pipelines.foundation_runtime import run_live_data_foundation
from pm15min.data.sources.orderbook_provider import build_orderbook_provider_from_env
from ..account import build_account_state_snapshot
from ..actions import apply_cancel_policy, apply_redeem_policy, submit_execution_payload
from ..execution import build_execution_snapshot, persist_execution_snapshot
from ..layout import LiveStateLayout
from ..liquidity import build_liquidity_state_snapshot
from ..persistence import write_live_payload_pair
from .diagnostics import (
    account_state_status,
    build_runner_health_summary,
    build_runner_risk_alerts,
    build_runner_risk_summary,
    summarize_runner_risk_alerts,
)
from .service import (
    build_runner_iteration as _build_runner_iteration_impl,
    run_live_runner as _run_live_runner_impl,
)
from .utils import (
    build_account_state_error_payload,
    build_side_effect_error_payload,
    utc_now_iso,
)
from ..service import decide_live_latest
from ..service import prewarm_live_signal_cache
from ..trading.gateway import LiveTradingGateway


def build_runner_iteration(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist_decision: bool = True,
    persist_execution: bool = True,
    run_foundation: bool = True,
    foundation_include_direct_oracle: bool = True,
    foundation_include_orderbooks: bool = True,
    apply_side_effects: bool = True,
    side_effect_dry_run: bool = False,
    gateway: LiveTradingGateway | None = None,
    session_state: dict[str, object] | None = None,
    orderbook_provider=None,
) -> dict[str, object]:
    return _build_runner_iteration_impl(
        cfg,
        target=target,
        feature_set=feature_set,
        persist_decision=persist_decision,
        persist_execution=persist_execution,
        run_foundation=run_foundation,
        foundation_include_direct_oracle=foundation_include_direct_oracle,
        foundation_include_orderbooks=foundation_include_orderbooks,
        apply_side_effects=apply_side_effects,
        side_effect_dry_run=side_effect_dry_run,
        gateway=gateway,
        session_state=session_state,
        orderbook_provider=orderbook_provider,
        run_live_data_foundation_fn=run_live_data_foundation,
        build_liquidity_state_snapshot_fn=build_liquidity_state_snapshot,
        decide_live_latest_fn=decide_live_latest,
        prewarm_live_signal_cache_fn=prewarm_live_signal_cache,
        build_execution_snapshot_fn=build_execution_snapshot,
        persist_execution_snapshot_fn=persist_execution_snapshot,
        submit_execution_payload_fn=submit_execution_payload,
        build_account_state_snapshot_fn=build_account_state_snapshot,
        apply_cancel_policy_fn=apply_cancel_policy,
        apply_redeem_policy_fn=apply_redeem_policy,
        utc_snapshot_label_fn=utc_snapshot_label,
        build_runner_risk_summary_fn=lambda **kwargs: build_runner_risk_summary(
            **kwargs,
            account_state_status_fn=account_state_status,
        ),
        build_runner_health_summary_fn=build_runner_health_summary,
        build_runner_risk_alerts_fn=build_runner_risk_alerts,
        summarize_runner_risk_alerts_fn=summarize_runner_risk_alerts,
        build_side_effect_error_payload_fn=build_side_effect_error_payload,
        build_account_state_error_payload_fn=build_account_state_error_payload,
    )


def persist_runner_iteration(*, rewrite_root: Path, payload: dict[str, object]) -> dict[str, Path]:
    layout = LiveStateLayout.discover(root=rewrite_root)
    latest_path = layout.latest_runner_path(
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        profile=str(payload["profile"]),
        target=str(payload["target"]),
    )
    snapshot_path = layout.runner_snapshot_path(
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        profile=str(payload["profile"]),
        target=str(payload["target"]),
        snapshot_ts=str(payload["snapshot_ts"]),
    )
    return write_live_payload_pair(
        payload=payload,
        latest_path=latest_path,
        snapshot_path=snapshot_path,
        write_snapshot_history=False,
    )


def run_live_runner(
    cfg: LiveConfig,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    iterations: int = 1,
    loop: bool = False,
    sleep_sec: float = 1.0,
    persist: bool = True,
    run_foundation: bool = True,
    foundation_include_direct_oracle: bool = True,
    foundation_include_orderbooks: bool = True,
    apply_side_effects: bool = True,
    side_effect_dry_run: bool = False,
    gateway: LiveTradingGateway | None = None,
) -> dict[str, object]:
    orderbook_provider = build_orderbook_provider_from_env(
        source_name=f"v2-live-runner:{cfg.asset.slug}:{int(cfg.cycle_minutes)}m",
        subscribe_on_read=True,
    )
    return _run_live_runner_impl(
        cfg,
        target=target,
        feature_set=feature_set,
        iterations=iterations,
        loop=loop,
        sleep_sec=sleep_sec,
        persist=persist,
        run_foundation=run_foundation,
        foundation_include_direct_oracle=foundation_include_direct_oracle,
        foundation_include_orderbooks=foundation_include_orderbooks,
        apply_side_effects=apply_side_effects,
        side_effect_dry_run=side_effect_dry_run,
        gateway=gateway,
        build_runner_iteration_fn=lambda *args, **kwargs: build_runner_iteration(
            *args,
            **kwargs,
            orderbook_provider=orderbook_provider,
        ),
        persist_runner_iteration_fn=persist_runner_iteration,
        append_jsonl_fn=append_jsonl,
        write_json_atomic_fn=write_json_atomic,
        utc_now_iso_fn=utc_now_iso,
    )
