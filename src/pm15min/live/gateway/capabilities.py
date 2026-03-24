from __future__ import annotations


def build_trading_capabilities(
    *,
    adapter: str,
    auth_config,
    data_api_config,
    redeem_config,
    legacy_runtime_available: bool,
    py_clob_client_available: bool,
    py_builder_relayer_available: bool,
    py_builder_signing_sdk_available: bool,
    web3_available: bool,
) -> dict[str, dict[str, object]]:
    normalized_adapter = str(adapter or "unknown").strip().lower()
    adapter_dependency_name = None
    adapter_dependency_available = True
    if normalized_adapter == "direct":
        adapter_dependency_name = "py_clob_client"
        adapter_dependency_available = bool(py_clob_client_available)
    elif normalized_adapter == "legacy":
        adapter_dependency_name = "legacy_runtime"
        adapter_dependency_available = bool(legacy_runtime_available)

    def _capability(
        *,
        name: str,
        requires_auth: bool = False,
        requires_data_api: bool = False,
        requires_redeem_config: bool = False,
        requires_adapter_dependency: bool = False,
        requires_builder_relayer: bool = False,
        requires_builder_signing_sdk: bool = False,
        requires_web3: bool = False,
        probe_flag: str | None = None,
    ) -> dict[str, object]:
        blocked_by: list[str] = []
        requirements_status = {
            "auth_config_present": bool(auth_config.is_configured),
            "data_api_config_present": bool(data_api_config.is_configured),
            "redeem_relay_config_present": bool(redeem_config.is_configured),
            "adapter_dependency_present": bool(adapter_dependency_available),
            "py_builder_relayer_present": bool(py_builder_relayer_available),
            "py_builder_signing_sdk_present": bool(py_builder_signing_sdk_available),
            "web3_present": bool(web3_available),
        }
        required_inputs: list[str] = []
        if requires_auth:
            required_inputs.append("auth_config")
            if not auth_config.is_configured:
                blocked_by.append("missing_auth_config")
        if requires_data_api:
            required_inputs.append("data_api_config")
            if not data_api_config.is_configured:
                blocked_by.append("missing_data_api_config")
        if requires_redeem_config:
            required_inputs.append("redeem_relay_config")
            if not redeem_config.is_configured:
                blocked_by.append("missing_redeem_relay_config")
        if requires_adapter_dependency:
            required_inputs.append(str(adapter_dependency_name or "adapter_dependency"))
            if not adapter_dependency_available:
                blocked_by.append(f"missing_{adapter_dependency_name}")
        if requires_builder_relayer:
            required_inputs.append("py_builder_relayer")
            if not py_builder_relayer_available:
                blocked_by.append("missing_py_builder_relayer")
        if requires_builder_signing_sdk:
            required_inputs.append("py_builder_signing_sdk")
            if not py_builder_signing_sdk_available:
                blocked_by.append("missing_py_builder_signing_sdk")
        if requires_web3:
            required_inputs.append("web3")
            if not web3_available:
                blocked_by.append("missing_web3")
        return {
            "name": name,
            "adapter": normalized_adapter,
            "ready": len(blocked_by) == 0,
            "required_inputs": required_inputs,
            "requirements_status": requirements_status,
            "blocked_by": blocked_by,
            "probe_flag": probe_flag,
        }

    return {
        "list_open_orders": _capability(
            name="list_open_orders",
            requires_auth=True,
            requires_adapter_dependency=True,
            probe_flag="probe_open_orders",
        ),
        "list_positions": _capability(
            name="list_positions",
            requires_data_api=True,
            requires_adapter_dependency=normalized_adapter == "legacy",
            probe_flag="probe_positions",
        ),
        "place_order": _capability(
            name="place_order",
            requires_auth=True,
            requires_adapter_dependency=True,
        ),
        "cancel_order": _capability(
            name="cancel_order",
            requires_auth=True,
            requires_adapter_dependency=True,
        ),
        "redeem_positions": _capability(
            name="redeem_positions",
            requires_auth=True,
            requires_redeem_config=True,
            requires_builder_relayer=normalized_adapter == "direct",
            requires_builder_signing_sdk=normalized_adapter == "direct",
            requires_web3=normalized_adapter == "direct",
        ),
    }
