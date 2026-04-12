from __future__ import annotations

import time
from typing import Any

from eth_abi import decode
from eth_utils import keccak

from .polygon_rpc import PolygonRpcClient


DEFAULT_FEEDS: dict[str, str] = {
    "btc": "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8",
    "eth": "0x000362205e10b3a147d02792eccee483dca6c7b44ecce7012cb8c6e0b68b3ae9",
    "sol": "0x0003b778d3f6b2ac4991302b89cb313f99a42467d6c9c5f96f57c29c0d2bc24f",
    "xrp": "0x0003c16c6aed42294f5cb4741f6e59ba2d728f0eae2eb9e6d3f555808c59fc45",
}
FEED_TO_ASSET = {feed.lower(): asset for asset, feed in DEFAULT_FEEDS.items()}

DEFAULT_DATAFEEDS: dict[str, dict[str, str]] = {
    "btc": {
        "name": "BTC / USD",
        "proxy": "0xc907E116054Ad103354f2D350FD2514433D57F6f",
        "aggregator": "0x014497a2AEF847C7021b17BFF70A68221D22AA63",
    },
    "eth": {
        "name": "ETH / USD",
        "proxy": "0xF9680D99D6C9589e2a93a78A04A279e509205945",
        "aggregator": "0x63db7e86391F5d31BAB58808Bcf75eDB272F4F5C",
    },
    "sol": {
        "name": "SOL / USD",
        "proxy": "0x10C8264C0935b3B9870013e057f330Ff3e9C56dC",
        "aggregator": "0x35B19A67A41282E39C32650b863F714EB95Dacf5",
    },
    "xrp": {
        "name": "XRP / USD",
        "proxy": "0x785ba89291f676b5386652eB12b30cF361020694",
        "aggregator": "0x8d5E29FF3B3f55D58AbB165EA9Ce3886C0A43Fc7",
    },
}

DEFAULT_REPORT_VERIFIED_ADDRESS = "0x2e6621e5e3f916d5e512124dd79e06b55e258054"
REPORT_VERIFIED_TOPIC0 = "0x" + keccak(text="ReportVerified(bytes32,address)").hex()
TRANSMIT_SELECTOR = "0xb1dc65a4"
VERIFY_SELECTOR = "0xf7e83aee"
ANSWER_UPDATED_TOPIC0 = "0x" + keccak(text="AnswerUpdated(int256,uint256,uint256)").hex()
DECIMALS_SELECTOR = "0x313ce567"

DEFAULT_CTF_ADDRESS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
CONDITION_RESOLUTION_TOPIC0 = (
    "0x" + keccak(text="ConditionResolution(bytes32,address,bytes32,uint256,uint256[])").hex()
)


def _to_hex32(v: bytes | None) -> str:
    return "" if not v else "0x" + bytes(v).hex()


def _decode_signed_report_payload(payload: bytes) -> dict[str, Any]:
    ctx0, ctx1, ctx2, report_blob, rs, ss, raw_vs = decode(
        ["bytes32", "bytes32", "bytes32", "bytes", "bytes32[]", "bytes32[]", "bytes32"],
        payload,
    )
    out: dict[str, Any] = {
        "context0_hex": _to_hex32(ctx0),
        "context1_hex": _to_hex32(ctx1),
        "context2_hex": _to_hex32(ctx2),
        "context1_u256": int.from_bytes(ctx1, "big"),
        "context2_u256": int.from_bytes(ctx2, "big"),
        "raw_vs_hex": _to_hex32(raw_vs),
        "rs_count": len(rs),
        "ss_count": len(ss),
        "report_blob_len": len(report_blob or b""),
        "report_feed_id": "",
        "valid_from_ts": None,
        "observation_ts": None,
        "expires_at_ts": None,
        "benchmark_price_raw": None,
        "bid_raw": None,
        "ask_raw": None,
    }
    if len(report_blob or b"") == 288:
        fields = decode(
            [
                "bytes32",
                "uint32",
                "uint32",
                "uint192",
                "uint192",
                "uint32",
                "int192",
                "int192",
                "int192",
            ],
            report_blob,
        )
        out["report_feed_id"] = _to_hex32(fields[0]).lower()
        out["valid_from_ts"] = int(fields[1])
        out["observation_ts"] = int(fields[2])
        out["expires_at_ts"] = int(fields[5])
        out["benchmark_price_raw"] = int(fields[6])
        out["bid_raw"] = int(fields[7])
        out["ask_raw"] = int(fields[8])
    return out


def _decode_transmit_rows(input_hex: str) -> list[dict[str, Any]]:
    body = bytes.fromhex(input_hex[10:])
    _ctx, raw_report, _rs, _ss, _raw_vs = decode(
        ["bytes32[3]", "bytes", "bytes32[]", "bytes32[]", "bytes32"],
        body,
    )
    (report,) = decode(["(uint256,uint256,uint256[],uint256[],bytes[],bytes[])"], raw_report)
    _fast_gas, _link_native, _upkeep_ids, _gas_limits, _triggers, perform_datas = report

    rows: list[dict[str, Any]] = []
    for perform_idx, perform_data in enumerate(perform_datas):
        values, extra = decode(["bytes[]", "bytes"], perform_data)
        extra_code = None
        extra_ts = None
        if len(extra) >= 64:
            extra_code = int.from_bytes(extra[:32], "big")
            extra_ts = int.from_bytes(extra[32:64], "big")
        for value_idx, value in enumerate(values):
            row = {
                "perform_idx": perform_idx,
                "value_idx": value_idx,
                "extra_code": extra_code,
                "extra_ts": extra_ts,
            }
            row.update(_decode_signed_report_payload(value))
            rows.append(row)
    return rows


def _winner_index_from_payouts(payouts: list[int]) -> int | None:
    if not payouts:
        return None
    best = max(payouts)
    if best <= 0:
        return None
    winners = [idx for idx, value in enumerate(payouts) if int(value) == int(best)]
    return int(winners[0]) if len(winners) == 1 else None


def _winner_side_binary(winner_idx: int | None) -> str:
    if winner_idx == 0:
        return "UP"
    if winner_idx == 1:
        return "DOWN"
    return ""


def _hex_to_int_signed_256(h: str) -> int:
    value = int(str(h), 16)
    if value >= (1 << 255):
        value -= 1 << 256
    return value


def _timestamp_iso(ts: int | None) -> str:
    if ts is None:
        return ""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(ts)))


class ChainlinkRpcSource:
    def __init__(self, rpc: PolygonRpcClient | None = None) -> None:
        self.rpc = rpc or PolygonRpcClient()

    def load_datafeed_metadata(self, *, asset: str) -> dict[str, str]:
        key = str(asset).strip().lower()
        if key not in DEFAULT_DATAFEEDS:
            raise ValueError(f"unsupported_datafeed_asset:{asset}")
        meta = DEFAULT_DATAFEEDS[key]
        return {
            "asset": key,
            "feed_name": str(meta["name"]),
            "proxy_address": str(meta["proxy"]).lower(),
            "aggregator_address": str(meta["aggregator"]).lower(),
        }

    def _call_decimals(self, proxy_address: str) -> int:
        result = self.rpc.call(
            "eth_call",
            [{"to": str(proxy_address).lower(), "data": DECIMALS_SELECTOR}, "latest"],
            retries=5,
        )
        if not result:
            raise RuntimeError(f"decimals() empty for {proxy_address}")
        return int(result, 16)

    def scan_report_verified_logs(
        self,
        *,
        asset: str,
        from_block: int,
        to_block: int,
        chunk_blocks: int = 1000,
        sleep_sec: float = 0.02,
        report_verified_address: str = DEFAULT_REPORT_VERIFIED_ADDRESS,
    ) -> list[dict[str, Any]]:
        feed_id = DEFAULT_FEEDS[asset].lower()
        entries: list[dict[str, Any]] = []
        chunk = max(200, int(chunk_blocks))
        block = int(from_block)
        while block <= int(to_block):
            end = min(int(to_block), block + chunk - 1)
            params = [
                {
                    "address": report_verified_address.lower(),
                    "fromBlock": hex(block),
                    "toBlock": hex(end),
                    "topics": [REPORT_VERIFIED_TOPIC0, [feed_id]],
                }
            ]
            logs = self.rpc.call("eth_getLogs", params, retries=5) or []
            for log in logs:
                topics = log.get("topics") or []
                if len(topics) < 2:
                    continue
                data_hex = (log.get("data") or "").lower()
                requester = ""
                if len(data_hex) >= 2 + 64:
                    requester = "0x" + data_hex[-40:]
                entries.append(
                    {
                        "tx_hash": str(log.get("transactionHash") or "").lower(),
                        "block_number": int(str(log.get("blockNumber") or "0x0"), 16),
                        "log_index": int(str(log.get("logIndex") or "0x0"), 16),
                        "feed_id_log": str(topics[1] or "").lower(),
                        "requester": requester.lower(),
                    }
                )
            block = end + 1
            if sleep_sec > 0:
                time.sleep(float(sleep_sec))
        dedup: dict[tuple[str, int], dict[str, Any]] = {}
        for row in entries:
            dedup[(row["tx_hash"], row["log_index"])] = row
        return sorted(dedup.values(), key=lambda row: (row["block_number"], row["log_index"]))

    def scan_datafeeds_answer_updated_logs(
        self,
        *,
        asset: str,
        from_block: int,
        to_block: int,
        chunk_blocks: int = 5000,
        sleep_sec: float = 0.02,
    ) -> list[dict[str, Any]]:
        meta = self.load_datafeed_metadata(asset=asset)
        decimals = self._call_decimals(meta["proxy_address"])
        entries: list[dict[str, Any]] = []
        chunk = max(200, min(10000, int(chunk_blocks)))
        block = int(from_block)
        while block <= int(to_block):
            end = min(int(to_block), block + chunk - 1)
            params = [
                {
                    "address": meta["aggregator_address"],
                    "fromBlock": hex(block),
                    "toBlock": hex(end),
                    "topics": [ANSWER_UPDATED_TOPIC0],
                }
            ]
            logs = self.rpc.call("eth_getLogs", params, retries=6) or []
            for log in logs:
                topics = log.get("topics") or []
                if len(topics) < 3:
                    continue
                answer_raw = _hex_to_int_signed_256(str(topics[1]))
                updated_at = int(str(log.get("data") or "0x0"), 16)
                entries.append(
                    {
                        "asset": meta["asset"],
                        "feed_name": meta["feed_name"],
                        "proxy_address": meta["proxy_address"],
                        "aggregator_address": meta["aggregator_address"],
                        "decimals": int(decimals),
                        "block_number": int(str(log.get("blockNumber") or "0x0"), 16),
                        "tx_hash": str(log.get("transactionHash") or "").lower(),
                        "log_index": int(str(log.get("logIndex") or "0x0"), 16),
                        "round_id": int(str(topics[2]), 16),
                        "updated_at": int(updated_at),
                        "updated_at_iso": _timestamp_iso(updated_at),
                        "answer_raw": int(answer_raw),
                        "answer": float(answer_raw) / float(10**int(decimals)),
                    }
                )
            block = end + 1
            if sleep_sec > 0:
                time.sleep(float(sleep_sec))
        dedup: dict[tuple[str, int], dict[str, Any]] = {}
        for row in entries:
            dedup[(str(row["tx_hash"]), int(row["log_index"]))] = row
        return sorted(dedup.values(), key=lambda row: (int(row["block_number"]), int(row["log_index"])))

    def decode_streams_from_logs(
        self,
        *,
        asset: str,
        logs: list[dict[str, Any]],
        include_block_timestamp: bool = False,
    ) -> list[dict[str, Any]]:
        tx_meta_by_hash: dict[str, dict[str, Any]] = {}
        for row in logs:
            txh = str(row.get("tx_hash") or "").lower()
            if txh and txh not in tx_meta_by_hash:
                tx_meta_by_hash[txh] = {
                    "requester": str(row.get("requester") or "").lower(),
                    "feed_id_log": str(row.get("feed_id_log") or "").lower(),
                }

        block_ts_cache: dict[int, int] = {}
        decoded_rows: list[dict[str, Any]] = []
        for tx_hash in sorted(tx_meta_by_hash.keys()):
            tx = self.rpc.call("eth_getTransactionByHash", [tx_hash], retries=5)
            if not tx:
                continue
            input_hex = (tx.get("input") or "").lower()
            selector = input_hex[:10] if input_hex else ""
            if selector not in {TRANSMIT_SELECTOR, VERIFY_SELECTOR}:
                continue
            block_number = int(tx.get("blockNumber"), 16) if tx.get("blockNumber") else None
            block_ts = None
            if include_block_timestamp and block_number is not None:
                block_ts = self.rpc.eth_block_timestamp(block_number, block_ts_cache)
            meta = tx_meta_by_hash[tx_hash]

            try:
                if selector == TRANSMIT_SELECTOR:
                    rows = _decode_transmit_rows(input_hex)
                    for row in rows:
                        report_feed_id = str(row.get("report_feed_id") or "").lower()
                        if report_feed_id and report_feed_id != DEFAULT_FEEDS[asset].lower():
                            continue
                        decoded_rows.append(
                            {
                                "asset": asset,
                                "tx_hash": tx_hash,
                                "block_number": block_number,
                                "block_timestamp": block_ts,
                                "requester": meta["requester"],
                                "feed_id_log": meta["feed_id_log"],
                                "report_feed_id": report_feed_id,
                                "perform_idx": row.get("perform_idx"),
                                "value_idx": row.get("value_idx"),
                                "extra_code": row.get("extra_code"),
                                "extra_ts": row.get("extra_ts"),
                                "valid_from_ts": row.get("valid_from_ts"),
                                "observation_ts": row.get("observation_ts"),
                                "expires_at_ts": row.get("expires_at_ts"),
                                "benchmark_price_raw": row.get("benchmark_price_raw"),
                                "bid_raw": row.get("bid_raw"),
                                "ask_raw": row.get("ask_raw"),
                                "path": "keeper_transmit",
                            }
                        )
                else:
                    payload, _parameter_payload = decode(["bytes", "bytes"], bytes.fromhex(input_hex[10:]))
                    row = _decode_signed_report_payload(payload)
                    report_feed_id = str(row.get("report_feed_id") or "").lower()
                    if report_feed_id and report_feed_id != DEFAULT_FEEDS[asset].lower():
                        continue
                    decoded_rows.append(
                        {
                            "asset": asset,
                            "tx_hash": tx_hash,
                            "block_number": block_number,
                            "block_timestamp": block_ts,
                            "requester": meta["requester"],
                            "feed_id_log": meta["feed_id_log"],
                            "report_feed_id": report_feed_id,
                            "perform_idx": 0,
                            "value_idx": 0,
                            "extra_code": None,
                            "extra_ts": None,
                            "valid_from_ts": row.get("valid_from_ts"),
                            "observation_ts": row.get("observation_ts"),
                            "expires_at_ts": row.get("expires_at_ts"),
                            "benchmark_price_raw": row.get("benchmark_price_raw"),
                            "bid_raw": row.get("bid_raw"),
                            "ask_raw": row.get("ask_raw"),
                            "path": "direct_verify",
                        }
                    )
            except Exception:
                continue
        return decoded_rows

    def scan_condition_resolutions(
        self,
        *,
        from_block: int,
        to_block: int,
        chunk_blocks: int = 3000,
        sleep_sec: float = 0.01,
        ctf_address: str = DEFAULT_CTF_ADDRESS,
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        chunk = max(200, int(chunk_blocks))
        block = int(from_block)
        while block <= int(to_block):
            end = min(int(to_block), block + chunk - 1)
            params = [
                {
                    "address": ctf_address.lower(),
                    "fromBlock": hex(block),
                    "toBlock": hex(end),
                    "topics": [CONDITION_RESOLUTION_TOPIC0],
                }
            ]
            logs = self.rpc.call("eth_getLogs", params, retries=5) or []
            for log in logs:
                topics = log.get("topics") or []
                if len(topics) < 4:
                    continue
                condition_id = str(topics[1] or "").lower()
                oracle_topic = str(topics[2] or "")
                oracle = "0x" + oracle_topic[-40:].lower() if len(oracle_topic) >= 42 else ""
                question_id = str(topics[3] or "").lower()
                data_hex = str(log.get("data") or "")
                if data_hex.startswith("0x"):
                    data = bytes.fromhex(data_hex[2:])
                else:
                    data = bytes.fromhex(data_hex)
                decoded = decode(["uint256", "uint256[]"], data)
                outcome_slot_count = int(decoded[0])
                payout_nums = [int(x) for x in decoded[1]]
                winner_index = _winner_index_from_payouts(payout_nums)
                rec = {
                    "condition_id": condition_id,
                    "resolve_tx_hash": str(log.get("transactionHash") or "").lower(),
                    "resolve_block_number": int(str(log.get("blockNumber") or "0x0"), 16),
                    "resolve_log_index": int(str(log.get("logIndex") or "0x0"), 16),
                    "oracle_address": oracle,
                    "question_id_topic": question_id,
                    "outcome_slot_count": outcome_slot_count,
                    "payout_numerators_json": str(payout_nums),
                    "winner_index": winner_index,
                    "winner_side": _winner_side_binary(winner_index),
                }
                prev = out.get(condition_id)
                if prev is None or (
                    rec["resolve_block_number"],
                    rec["resolve_log_index"],
                ) > (
                    prev["resolve_block_number"],
                    prev["resolve_log_index"],
                ):
                    out[condition_id] = rec
            block = end + 1
            if sleep_sec > 0:
                time.sleep(float(sleep_sec))
        return out
