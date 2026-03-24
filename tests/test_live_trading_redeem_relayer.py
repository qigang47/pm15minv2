from __future__ import annotations

from types import SimpleNamespace

from pm15min.live.trading.contracts import RedeemRelayConfig, RedeemRequest, TradingAuthConfig
from pm15min.live.trading.redeem_relayer import redeem_positions_via_relayer


def test_redeem_positions_via_relayer_builds_and_executes_safe_tx(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeEncodedCall:
        def _encode_transaction_data(self):
            return "0xcalldata"

    class FakeFunctions:
        def redeemPositions(self, usdc, parent_collection_id, condition_id, index_sets):
            captured["redeem_args"] = {
                "usdc": usdc,
                "parent_collection_id": parent_collection_id,
                "condition_id": condition_id,
                "index_sets": index_sets,
            }
            return FakeEncodedCall()

    class FakeContract:
        def __init__(self):
            self.functions = FakeFunctions()

    class FakeEth:
        chain_id = 137

        def contract(self, *, address, abi):
            captured["contract_address"] = address
            captured["contract_abi"] = abi
            return FakeContract()

    class FakeProvider:
        def __init__(self, url, request_kwargs=None):
            self.url = url
            self.request_kwargs = request_kwargs or {}

    class FakeWeb3:
        HTTPProvider = FakeProvider

        def __init__(self, provider):
            self.provider = provider
            self.eth = FakeEth()

        def is_connected(self):
            return True

        @staticmethod
        def to_checksum_address(value):
            return f"chk:{value}"

    class FakeBuilderApiKeyCreds:
        def __init__(self, *, key, secret, passphrase):
            captured["builder_creds"] = {
                "key": key,
                "secret": secret,
                "passphrase": passphrase,
            }

    class FakeBuilderConfig:
        def __init__(self, *, local_builder_creds):
            captured["builder_config"] = local_builder_creds

    class FakeSafeTransaction:
        def __init__(self, *, to, value, data, operation):
            captured["safe_tx"] = {
                "to": to,
                "value": value,
                "data": data,
                "operation": operation,
            }

    class FakeRelayResponse:
        def wait(self):
            return {
                "transactionHash": "0xtx",
                "state": "confirmed",
                "proxyAddress": "0xproxy",
            }

    class FakeRelayClient:
        def __init__(self, *, relayer_url, chain_id, private_key, builder_config):
            captured["relay_client"] = {
                "relayer_url": relayer_url,
                "chain_id": chain_id,
                "private_key": private_key,
                "builder_config": builder_config,
            }

        def execute(self, *, transactions, metadata):
            captured["relay_execute"] = {
                "transactions": transactions,
                "metadata": metadata,
            }
            return FakeRelayResponse()

    monkeypatch.setattr("pm15min.live.trading.redeem_relayer.Web3", FakeWeb3)
    monkeypatch.setattr("pm15min.live.trading.redeem_relayer.BuilderApiKeyCreds", FakeBuilderApiKeyCreds)
    monkeypatch.setattr("pm15min.live.trading.redeem_relayer.BuilderConfig", FakeBuilderConfig)
    monkeypatch.setattr("pm15min.live.trading.redeem_relayer.SafeTransaction", FakeSafeTransaction)
    monkeypatch.setattr("pm15min.live.trading.redeem_relayer.RelayClient", FakeRelayClient)
    monkeypatch.setattr("pm15min.live.trading.redeem_relayer.OperationType", SimpleNamespace(Call="CALL"))

    result = redeem_positions_via_relayer(
        auth_config=TradingAuthConfig(private_key="pk", chain_id=137),
        relay_config=RedeemRelayConfig(
            rpc_urls=("https://rpc-1",),
            relayer_url="https://relayer",
            builder_api_key="builder-key",
            builder_secret="builder-secret",
            builder_passphrase="builder-pass",
        ),
        request=RedeemRequest(condition_id="0x" + "11" * 32, index_sets=[1, 2]),
    )

    assert result.success is True
    assert result.tx_hash == "0xtx"
    assert result.state == "confirmed"
    assert captured["redeem_args"]["index_sets"] == [1, 2]
    assert captured["relay_client"]["relayer_url"] == "https://relayer"
    assert captured["relay_client"]["private_key"] == "pk"
    assert captured["safe_tx"]["data"] == "0xcalldata"
    assert captured["safe_tx"]["operation"] == "CALL"
