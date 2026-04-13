from __future__ import annotations

from eth_abi.exceptions import InsufficientDataBytes

from pm15min.data.sources import chainlink_rpc
from pm15min.data.sources.chainlink_rpc import ChainlinkRpcSource, DEFAULT_FEEDS, TRANSMIT_SELECTOR


class _FakeRpc:
    def call(self, method: str, params: list[object], retries: int = 5):
        assert method == "eth_getTransactionByHash"
        tx_hash = str(params[0])
        if tx_hash == "0xbad":
            return {
                "blockNumber": "0x65",
                "input": TRANSMIT_SELECTOR + "00",
            }
        if tx_hash == "0xgood":
            return {
                "blockNumber": "0x66",
                "input": TRANSMIT_SELECTOR + "11",
            }
        raise AssertionError(f"unexpected tx hash: {tx_hash}")

    def eth_block_timestamp(self, block_number: int, cache: dict[int, int]) -> int:
        cache[int(block_number)] = 1700000000
        return 1700000000


def test_decode_streams_from_logs_skips_malformed_transmit_payloads(monkeypatch) -> None:
    def fake_decode_transmit_rows(input_hex: str):
        if input_hex.endswith("00"):
            raise InsufficientDataBytes("malformed transmit payload")
        return [
            {
                "perform_idx": 0,
                "value_idx": 0,
                "extra_code": 1,
                "extra_ts": 1700000900,
                "report_feed_id": DEFAULT_FEEDS["btc"].lower(),
                "valid_from_ts": 1700000000,
                "observation_ts": 1700000000,
                "expires_at_ts": 1700000900,
                "benchmark_price_raw": 101,
                "bid_raw": 100,
                "ask_raw": 102,
            }
        ]

    monkeypatch.setattr(chainlink_rpc, "_decode_transmit_rows", fake_decode_transmit_rows)
    source = ChainlinkRpcSource(rpc=_FakeRpc())

    rows = source.decode_streams_from_logs(
        asset="btc",
        logs=[
            {
                "tx_hash": "0xbad",
                "requester": "req-bad",
                "feed_id_log": DEFAULT_FEEDS["btc"].lower(),
            },
            {
                "tx_hash": "0xgood",
                "requester": "req-good",
                "feed_id_log": DEFAULT_FEEDS["btc"].lower(),
            },
        ],
        include_block_timestamp=True,
    )

    assert len(rows) == 1
    assert rows[0]["tx_hash"] == "0xgood"
    assert rows[0]["requester"] == "req-good"
    assert rows[0]["benchmark_price_raw"] == 101
