from __future__ import annotations


def sync_live_account_state(
    cfg,
    *,
    persist: bool = True,
    adapter: str | None = None,
    gateway=None,
    build_live_trading_gateway_from_env_fn,
    build_account_state_snapshot_fn,
) -> dict[str, object]:
    resolved_gateway = gateway if gateway is not None else (
        None if adapter is None else build_live_trading_gateway_from_env_fn(adapter_override=adapter)
    )
    return build_account_state_snapshot_fn(cfg, persist=persist, gateway=resolved_gateway)


def sync_live_liquidity_state(
    cfg,
    *,
    persist: bool = True,
    force_refresh: bool = False,
    build_liquidity_state_snapshot_fn,
) -> dict[str, object]:
    return build_liquidity_state_snapshot_fn(
        cfg,
        persist=persist,
        force_refresh=force_refresh,
    )


def execute_live_cancel_policy(
    cfg,
    *,
    persist: bool = True,
    refresh_account_state: bool = True,
    dry_run: bool = False,
    adapter: str | None = None,
    gateway=None,
    build_live_trading_gateway_from_env_fn,
    apply_cancel_policy_fn,
) -> dict[str, object]:
    resolved_gateway = gateway if gateway is not None else (
        None if adapter is None else build_live_trading_gateway_from_env_fn(adapter_override=adapter)
    )
    return apply_cancel_policy_fn(
        cfg,
        persist=persist,
        refresh_account_state=refresh_account_state,
        dry_run=dry_run,
        gateway=resolved_gateway,
    )


def execute_live_redeem_policy(
    cfg,
    *,
    persist: bool = True,
    refresh_account_state: bool = True,
    dry_run: bool = False,
    max_conditions: int | None = None,
    adapter: str | None = None,
    gateway=None,
    build_live_trading_gateway_from_env_fn,
    apply_redeem_policy_fn,
) -> dict[str, object]:
    resolved_gateway = gateway if gateway is not None else (
        None if adapter is None else build_live_trading_gateway_from_env_fn(adapter_override=adapter)
    )
    return apply_redeem_policy_fn(
        cfg,
        persist=persist,
        refresh_account_state=refresh_account_state,
        dry_run=dry_run,
        max_conditions=max_conditions,
        gateway=resolved_gateway,
    )


def simulate_live_execution(
    cfg,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist: bool = True,
    decide_live_latest_fn,
    build_execution_snapshot_fn,
    persist_execution_snapshot_fn,
) -> dict[str, object]:
    decision = decide_live_latest_fn(
        cfg,
        target=target,
        feature_set=feature_set,
        persist=persist,
    )
    payload = build_execution_snapshot_fn(cfg, decision)
    if persist:
        paths = persist_execution_snapshot_fn(rewrite_root=cfg.layout.rewrite.root, payload=payload)
        payload["latest_execution_path"] = str(paths["latest"])
        payload["execution_snapshot_path"] = str(paths["snapshot"])
    return payload


def execute_live_latest(
    cfg,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist: bool = True,
    dry_run: bool = False,
    refresh_account_state: bool = True,
    adapter: str | None = None,
    gateway=None,
    build_live_trading_gateway_from_env_fn,
    simulate_live_execution_fn,
    submit_execution_payload_fn,
) -> dict[str, object]:
    resolved_gateway = gateway if gateway is not None else (
        None if adapter is None else build_live_trading_gateway_from_env_fn(adapter_override=adapter)
    )
    execution_payload = simulate_live_execution_fn(
        cfg,
        target=target,
        feature_set=feature_set,
        persist=persist,
    )
    return submit_execution_payload_fn(
        cfg,
        execution_payload=execution_payload,
        persist=persist,
        dry_run=dry_run,
        refresh_account_state=refresh_account_state,
        gateway=resolved_gateway,
    )
