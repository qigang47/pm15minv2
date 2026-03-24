from __future__ import annotations

from typing import Any

from web3 import Web3

from py_builder_relayer_client.client import RelayClient
from py_builder_relayer_client.models import OperationType, SafeTransaction
from py_builder_signing_sdk.config import BuilderConfig
from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds

from .contracts import RedeemRelayConfig, RedeemRequest, RedeemResult, TradingAuthConfig


USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_ADDRESS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
PARENT_COLLECTION_ID = "0x" + "00" * 32
CTF_ABI_FRAGMENT = [
    {
        "type": "function",
        "stateMutability": "nonpayable",
        "name": "redeemPositions",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "outputs": [],
    }
]


def redeem_positions_via_relayer(
    *,
    auth_config: TradingAuthConfig,
    relay_config: RedeemRelayConfig,
    request: RedeemRequest,
) -> RedeemResult:
    if not auth_config.is_configured:
        raise ValueError("missing_polymarket_private_key")
    if not relay_config.is_configured:
        raise ValueError("missing_redeem_relay_config")
    _validate_condition_id(request.condition_id)
    if not request.index_sets or not all(isinstance(x, int) and x > 0 for x in request.index_sets):
        raise ValueError("invalid_index_sets")

    w3, selected_rpc = _connect_web3(
        rpc_urls=list(relay_config.rpc_urls),
        chain_id=int(auth_config.chain_id),
    )
    calldata = _build_redeem_calldata(
        w3=w3,
        condition_id=request.condition_id,
        index_sets=list(request.index_sets),
    )
    metadata = f"Redeem CTF condition {request.condition_id[:10]}..., indexSets={list(request.index_sets)}"

    builder_config = BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(
            key=str(relay_config.builder_api_key),
            secret=str(relay_config.builder_secret),
            passphrase=str(relay_config.builder_passphrase),
        )
    )
    client = RelayClient(
        relayer_url=str(relay_config.relayer_url),
        chain_id=int(auth_config.chain_id),
        private_key=str(auth_config.private_key),
        builder_config=builder_config,
    )
    safe_tx = SafeTransaction(
        to=str(Web3.to_checksum_address(CTF_ADDRESS)),
        value="0",
        data=calldata,
        operation=OperationType.Call,
    )
    response = client.execute(transactions=[safe_tx], metadata=metadata)
    result = response.wait()
    if not isinstance(result, dict) or not result:
        raise RuntimeError("redeem_result_missing")

    tx_hash = _first_string(result, "transactionHash", "txHash")
    state = _first_string(result, "state", "status")
    proxy = _first_string(result, "proxyAddress", "proxy")
    if not tx_hash:
        raise RuntimeError(f"redeem_result_invalid:{result}")
    return RedeemResult(
        success=True,
        status=state or "confirmed",
        tx_hash=tx_hash,
        state=state,
        message=None,
        raw={
            **result,
            "metadata": metadata,
            "selected_rpc": selected_rpc,
            "proxy": proxy,
        },
    )


def _connect_web3(*, rpc_urls: list[str], chain_id: int) -> tuple[Web3, str]:
    failures: list[str] = []
    for rpc_url in rpc_urls:
        try:
            provider = Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 8})
            w3 = Web3(provider)
            if not w3.is_connected():
                raise RuntimeError("is_connected=False")
            actual_chain_id = int(w3.eth.chain_id)
            if actual_chain_id != int(chain_id):
                raise RuntimeError(f"unexpected_chain_id={actual_chain_id}")
            return w3, rpc_url
        except Exception as exc:
            failures.append(f"{rpc_url}: {exc}")
    raise RuntimeError("all_rpc_unavailable:" + " | ".join(failures[:5]))


def _build_redeem_calldata(*, w3: Web3, condition_id: str, index_sets: list[int]) -> str:
    ctf_contract = w3.eth.contract(
        address=Web3.to_checksum_address(CTF_ADDRESS),
        abi=CTF_ABI_FRAGMENT,
    )
    return ctf_contract.functions.redeemPositions(
        Web3.to_checksum_address(USDC_ADDRESS),
        PARENT_COLLECTION_ID,
        condition_id,
        index_sets,
    )._encode_transaction_data()


def _validate_condition_id(condition_id: str) -> None:
    if not isinstance(condition_id, str) or not condition_id.startswith("0x") or len(condition_id) != 66:
        raise ValueError("invalid_condition_id")


def _first_string(raw: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = raw.get(key)
        if value in (None, ""):
            continue
        out = str(value).strip()
        if out:
            return out
    return None
