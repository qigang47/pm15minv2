from __future__ import annotations

from collections.abc import Callable

from pm15min.core.config import LiveConfig
from pm15min.data.layout import utc_snapshot_label
from ..trading.gateway import LiveTradingGateway


def check_live_trading_gateway(
    cfg: LiveConfig,
    *,
    gateway: LiveTradingGateway | None = None,
    adapter: str | None = None,
    probe_open_orders: bool = False,
    probe_positions: bool = False,
    load_env_configs_fn: Callable[[], tuple[object, object, object]],
    build_gateway_from_env_fn: Callable[..., LiveTradingGateway],
    describe_gateway_fn: Callable[..., dict[str, object]],
    normalize_adapter_fn: Callable[[str | None], str],
    module_available_fn: Callable[[str], bool],
    build_trading_capabilities_fn,
    build_live_trading_smoke_runs_fn,
    probe_gateway_call_fn,
) -> dict[str, object]:
    auth_config, data_api_config, redeem_config = load_env_configs_fn()
    legacy_runtime_available = module_available_fn("live_trading.infra.polymarket_client")
    py_clob_client_available = module_available_fn("py_clob_client.client")
    py_builder_relayer_available = module_available_fn("py_builder_relayer_client.client")
    py_builder_signing_sdk_available = module_available_fn("py_builder_signing_sdk.config")
    web3_available = module_available_fn("web3")

    gateway_error = None
    resolved_gateway = gateway
    selected_adapter = None if adapter is None else normalize_adapter_fn(adapter)
    if resolved_gateway is None:
        try:
            resolved_gateway = build_gateway_from_env_fn(adapter_override=selected_adapter)
        except Exception as exc:
            gateway_error = f"{type(exc).__name__}: {exc}"
    gateway_meta = describe_gateway_fn(
        resolved_gateway if gateway_error is None else gateway,
        adapter=selected_adapter,
        source="env_gateway" if gateway is None else "injected_gateway",
    )
    runtime_adapter = str(gateway_meta.get("adapter") or "unknown")
    capability_adapter = selected_adapter or runtime_adapter
    capabilities = build_trading_capabilities_fn(
        adapter=capability_adapter,
        auth_config=auth_config,
        data_api_config=data_api_config,
        redeem_config=redeem_config,
        legacy_runtime_available=legacy_runtime_available,
        py_clob_client_available=py_clob_client_available,
        py_builder_relayer_available=py_builder_relayer_available,
        py_builder_signing_sdk_available=py_builder_signing_sdk_available,
        web3_available=web3_available,
    )

    checks = [
        {
            "name": "gateway_buildable",
            "ok": gateway_error is None,
            "detail": None if gateway_error is None else gateway_error,
        },
        {
            "name": "adapter_dependency_available",
            "ok": bool(
                capabilities["list_open_orders"]["requirements_status"].get("adapter_dependency_present", True)
            ),
            "detail": {
                "adapter": capability_adapter,
                "legacy_runtime_available": legacy_runtime_available,
                "py_clob_client_available": py_clob_client_available,
            },
        },
        {
            "name": "auth_config_present",
            "ok": bool(auth_config.is_configured),
            "detail": {
                "host": auth_config.host,
                "chain_id": auth_config.chain_id,
                "signature_type": auth_config.signature_type,
                "funder_address_present": bool(auth_config.funder_address),
            },
        },
        {
            "name": "data_api_config_present",
            "ok": bool(data_api_config.is_configured),
            "detail": {
                "base_url": data_api_config.base_url,
                "user_address_present": bool(data_api_config.user_address),
            },
        },
        {
            "name": "redeem_relay_config_present",
            "ok": bool(redeem_config.is_configured),
            "detail": {
                "rpc_url_count": len(redeem_config.rpc_urls),
                "relayer_url": redeem_config.relayer_url,
                "builder_api_key_present": bool(redeem_config.builder_api_key),
                "builder_secret_present": bool(redeem_config.builder_secret),
                "builder_passphrase_present": bool(redeem_config.builder_passphrase),
                "py_builder_relayer_available": py_builder_relayer_available,
                "py_builder_signing_sdk_available": py_builder_signing_sdk_available,
                "web3_available": web3_available,
            },
        },
        {
            "name": "open_orders_ready",
            "ok": bool(capabilities["list_open_orders"]["ready"]),
            "detail": capabilities["list_open_orders"],
        },
        {
            "name": "positions_ready",
            "ok": bool(capabilities["list_positions"]["ready"]),
            "detail": capabilities["list_positions"],
        },
        {
            "name": "place_order_ready",
            "ok": bool(capabilities["place_order"]["ready"]),
            "detail": capabilities["place_order"],
        },
        {
            "name": "cancel_order_ready",
            "ok": bool(capabilities["cancel_order"]["ready"]),
            "detail": capabilities["cancel_order"],
        },
        {
            "name": "redeem_ready",
            "ok": bool(capabilities["redeem_positions"]["ready"]),
            "detail": capabilities["redeem_positions"],
        },
    ]

    probes: dict[str, object] = {}
    probes["open_orders"] = probe_gateway_call_fn(
        gateway=resolved_gateway,
        enabled=probe_open_orders,
        fn_name="list_open_orders",
    )
    probes["positions"] = probe_gateway_call_fn(
        gateway=resolved_gateway,
        enabled=probe_positions,
        fn_name="list_positions",
    )

    overall_ok = all(bool(item.get("ok", False)) for item in checks)
    if probe_open_orders:
        overall_ok = overall_ok and bool((probes["open_orders"] or {}).get("ok", False))
    if probe_positions:
        overall_ok = overall_ok and bool((probes["positions"] or {}).get("ok", False))

    return {
        "domain": "live",
        "dataset": "live_trading_gateway_check",
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": f"{int(cfg.cycle_minutes)}m",
        "snapshot_ts": utc_snapshot_label(),
        "ok": overall_ok,
        "trading_gateway": gateway_meta,
        "adapter_override": selected_adapter,
        "auth_config": {
            "host": auth_config.host,
            "chain_id": auth_config.chain_id,
            "signature_type": auth_config.signature_type,
            "private_key_present": bool(auth_config.private_key),
            "funder_address_present": bool(auth_config.funder_address),
        },
        "data_api_config": {
            "base_url": data_api_config.base_url,
            "user_address_present": bool(data_api_config.user_address),
        },
        "redeem_relay_config": {
            "rpc_url_count": len(redeem_config.rpc_urls),
            "relayer_url": redeem_config.relayer_url,
            "builder_api_key_present": bool(redeem_config.builder_api_key),
            "builder_secret_present": bool(redeem_config.builder_secret),
            "builder_passphrase_present": bool(redeem_config.builder_passphrase),
        },
        "dependencies": {
            "legacy_runtime_available": legacy_runtime_available,
            "py_clob_client_available": py_clob_client_available,
            "py_builder_relayer_available": py_builder_relayer_available,
            "py_builder_signing_sdk_available": py_builder_signing_sdk_available,
            "web3_available": web3_available,
        },
        "capabilities": capabilities,
        "recommended_smoke_runs": build_live_trading_smoke_runs_fn(cfg, adapter=capability_adapter),
        "checks": checks,
        "probes": probes,
    }
