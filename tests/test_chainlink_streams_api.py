from __future__ import annotations

import hashlib
import hmac

from eth_abi import encode

from pm15min.data.sources.chainlink_streams_api import ChainlinkDataStreamsApiClient


class _FakeResponse:
    def __init__(self, *, status_code: int = 200, payload: dict[str, object] | None = None, text: str = "") -> None:
        self.status_code = int(status_code)
        self._payload = payload if payload is not None else {}
        self.text = text

    def json(self) -> dict[str, object]:
        return dict(self._payload)


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse] | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self._responses = list(responses or [])

    def get(self, url: str, *, headers: dict[str, str], timeout: float):
        self.calls.append({"url": url, "headers": dict(headers), "timeout": timeout})
        return self._responses.pop(0)


def _full_report_hex(*, feed_id_hex: str, valid_from_ts: int, observation_ts: int, expires_at_ts: int, benchmark_price_raw: int) -> str:
    report_blob = encode(
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
        [
            bytes.fromhex(feed_id_hex.removeprefix("0x")),
            int(valid_from_ts),
            int(observation_ts),
            0,
            0,
            int(expires_at_ts),
            int(benchmark_price_raw),
            0,
            0,
        ],
    )
    payload = encode(
        ["bytes32", "bytes32", "bytes32", "bytes", "bytes32[]", "bytes32[]", "bytes32"],
        [b"\x00" * 32, b"\x00" * 32, b"\x00" * 32, report_blob, [], [], b"\x00" * 32],
    )
    return "0x" + payload.hex()


def test_fetch_report_signs_request_and_decodes_price() -> None:
    api_key = "b3f75771-5cc5-436d-a639-2f566d19f203"
    api_secret = "test-secret"
    fixed_timestamp_ms = 1716211845123
    feed_id = "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8"
    report_timestamp = 1700000900
    session = _FakeSession(
        responses=[
            _FakeResponse(
                payload={
                    "report": {
                        "feedID": feed_id,
                        "validFromTimestamp": 1700000800,
                        "observationsTimestamp": report_timestamp,
                        "fullReport": _full_report_hex(
                            feed_id_hex=feed_id,
                            valid_from_ts=1700000800,
                            observation_ts=report_timestamp,
                            expires_at_ts=1700000950,
                            benchmark_price_raw=1234560000000000000000,
                        ),
                    }
                }
            )
        ]
    )
    client = ChainlinkDataStreamsApiClient(
        session=session,
        api_key=api_key,
        api_secret=api_secret,
        timeout_sec=12.0,
        now_ms=lambda: fixed_timestamp_ms,
    )

    payload = client.fetch_report(feed_id=feed_id, timestamp=report_timestamp)

    assert payload["feed_id"] == feed_id
    assert payload["observation_ts"] == report_timestamp
    assert payload["price"] == 1234.56
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"] == f"https://api.dataengine.chain.link/api/v1/reports?feedID={feed_id}&timestamp={report_timestamp}"
    assert call["headers"]["Authorization"] == api_key
    assert call["headers"]["X-Authorization-Timestamp"] == str(fixed_timestamp_ms)
    body_hash = hashlib.sha256(b"").hexdigest()
    string_to_sign = f"GET /api/v1/reports?feedID={feed_id}&timestamp={report_timestamp} {body_hash} {api_key} {fixed_timestamp_ms}"
    expected_signature = hmac.new(api_secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    assert call["headers"]["X-Authorization-Signature-SHA256"] == expected_signature


def test_fetch_report_returns_none_for_not_found() -> None:
    session = _FakeSession(responses=[_FakeResponse(status_code=404, payload={}, text="not found")])
    client = ChainlinkDataStreamsApiClient(
        session=session,
        api_key="b3f75771-5cc5-436d-a639-2f566d19f203",
        api_secret="test-secret",
        now_ms=lambda: 1716211845123,
    )

    payload = client.fetch_report(
        feed_id="0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8",
        timestamp=1700000900,
    )

    assert payload is None
