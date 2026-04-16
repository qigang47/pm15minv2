from __future__ import annotations

from pathlib import Path

import requests

from pm15min.live.proxy_failover import (
    ProxyCandidate,
    ProxyProbeResult,
    load_v2ray_proxy_candidates,
    run_managed_proxy_failover,
    render_proxy_env,
    select_best_candidate,
)


def test_load_v2ray_proxy_candidates_maps_inbounds_to_outbounds() -> None:
    payload = {
        "inbounds": [
            {"tag": "inbound41271", "protocol": "socks", "port": 41271},
            {"tag": "api-in", "protocol": "dokodemo-door", "port": 46273},
            {"tag": "inbound36897", "protocol": "socks", "port": 36897},
        ],
        "outbounds": [
            {"tag": "outbound41271"},
            {"tag": "outbound36897"},
        ],
        "routing": {
            "rules": [
                {"type": "field", "outboundTag": "outbound41271", "inboundTag": ["inbound41271"]},
                {"type": "field", "outboundTag": "outbound36897", "inboundTag": ["inbound36897"]},
            ]
        },
    }

    out = load_v2ray_proxy_candidates(payload)

    assert out == [
        ProxyCandidate(inbound_tag="inbound41271", outbound_tag="outbound41271", port=41271),
        ProxyCandidate(inbound_tag="inbound36897", outbound_tag="outbound36897", port=36897),
    ]


def test_select_best_candidate_prefers_current_port_when_still_healthy() -> None:
    results = [
        ProxyProbeResult(
            candidate=ProxyCandidate(inbound_tag="inbound41271", outbound_tag="outbound41271", port=41271),
            success_count=1,
            total_count=2,
            avg_latency_sec=1.25,
            errors=["timeout"],
            status_codes=[200, None],
        ),
        ProxyProbeResult(
            candidate=ProxyCandidate(inbound_tag="inbound36897", outbound_tag="outbound36897", port=36897),
            success_count=2,
            total_count=2,
            avg_latency_sec=0.42,
            errors=[],
            status_codes=[200, 204],
        ),
    ]

    selected = select_best_candidate(results, current_port=41271)

    assert selected is results[0]


def test_select_best_candidate_falls_forward_when_current_port_is_unhealthy() -> None:
    results = [
        ProxyProbeResult(
            candidate=ProxyCandidate(inbound_tag="inbound41271", outbound_tag="outbound41271", port=41271),
            success_count=0,
            total_count=2,
            avg_latency_sec=None,
            errors=["timeout", "timeout"],
            status_codes=[None, None],
        ),
        ProxyProbeResult(
            candidate=ProxyCandidate(inbound_tag="inbound36897", outbound_tag="outbound36897", port=36897),
            success_count=1,
            total_count=2,
            avg_latency_sec=0.66,
            errors=["timeout"],
            status_codes=[200, None],
        ),
    ]

    selected = select_best_candidate(results, current_port=41271)

    assert selected is results[1]


def test_render_proxy_env_uses_socks5h_urls() -> None:
    env_map = render_proxy_env(
        ProxyCandidate(inbound_tag="inbound36897", outbound_tag="outbound36897", port=36897)
    )

    assert env_map["HTTP_PROXY"] == "socks5h://127.0.0.1:36897"
    assert env_map["HTTPS_PROXY"] == "socks5h://127.0.0.1:36897"
    assert env_map["ALL_PROXY"] == "socks5h://127.0.0.1:36897"
    assert env_map["http_proxy"] == "socks5h://127.0.0.1:36897"
    assert env_map["https_proxy"] == "socks5h://127.0.0.1:36897"
    assert env_map["all_proxy"] == "socks5h://127.0.0.1:36897"
    assert env_map["PM15MIN_MANAGED_PROXY_ACTIVE_PORT"] == "36897"
    assert env_map["NO_PROXY"] == "127.0.0.1,localhost"
    assert env_map["no_proxy"] == "127.0.0.1,localhost"


def test_run_managed_proxy_failover_writes_env_and_state(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    state_dir = tmp_path / "state"
    config_path.write_text(
        """
        {
          "inbounds": [
            {"tag": "inbound41271", "protocol": "socks", "port": 41271},
            {"tag": "inbound36897", "protocol": "socks", "port": 36897}
          ],
          "routing": {
            "rules": [
              {"type": "field", "outboundTag": "outbound41271", "inboundTag": ["inbound41271"]},
              {"type": "field", "outboundTag": "outbound36897", "inboundTag": ["inbound36897"]}
            ]
          }
        }
        """,
        encoding="utf-8",
    )

    class _FakeResponse:
        def __init__(self, status_code: int) -> None:
            self.status_code = status_code

    class _FakeSession:
        def get(self, url: str, *, proxies: dict[str, str], timeout: float):
            del url, timeout
            if proxies["http"].endswith(":41271"):
                return _FakeResponse(204)
            raise requests.exceptions.ReadTimeout("bad proxy")

    payload = run_managed_proxy_failover(
        config_path=config_path,
        state_dir=state_dir,
        healthcheck_urls=("https://example.com/health",),
        timeout_sec=5.0,
        session_factory=_FakeSession,
    )

    env_text = (state_dir / "active_proxy.env").read_text(encoding="utf-8")
    state_text = (state_dir / "state.json").read_text(encoding="utf-8")
    assert payload["selected_port"] == 41271
    assert "HTTP_PROXY='socks5h://127.0.0.1:41271'" in env_text
    assert '"selected_port": 41271' in state_text


def test_run_managed_proxy_failover_can_fallback_to_curl_when_requests_lacks_socks(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    state_dir = tmp_path / "state"
    config_path.write_text(
        """
        {
          "inbounds": [
            {"tag": "inbound41271", "protocol": "socks", "port": 41271}
          ],
          "routing": {
            "rules": [
              {"type": "field", "outboundTag": "outbound41271", "inboundTag": ["inbound41271"]}
            ]
          }
        }
        """,
        encoding="utf-8",
    )

    class _FakeSession:
        def get(self, url: str, *, proxies: dict[str, str], timeout: float):
            del url, proxies, timeout
            raise requests.exceptions.InvalidSchema("Missing dependencies for SOCKS support.")

    def _fake_curl_probe(url: str, *, proxy_url: str, timeout_sec: float):
        del url, timeout_sec
        if proxy_url.endswith(":41271"):
            return 204, 0.25
        raise AssertionError("unexpected proxy")

    payload = run_managed_proxy_failover(
        config_path=config_path,
        state_dir=state_dir,
        healthcheck_urls=("https://example.com/health",),
        timeout_sec=5.0,
        session_factory=_FakeSession,
        curl_probe_fn=_fake_curl_probe,
    )

    assert payload["selected_port"] == 41271
    assert "PM15MIN_MANAGED_PROXY_ACTIVE_PORT='41271'" in (state_dir / "active_proxy.env").read_text(encoding="utf-8")


def test_run_managed_proxy_failover_can_fallback_to_socket_probe_when_curl_is_missing(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    state_dir = tmp_path / "state"
    config_path.write_text(
        """
        {
          "inbounds": [
            {"tag": "inbound41271", "protocol": "socks", "port": 41271}
          ],
          "routing": {
            "rules": [
              {"type": "field", "outboundTag": "outbound41271", "inboundTag": ["inbound41271"]}
            ]
          }
        }
        """,
        encoding="utf-8",
    )

    class _FakeSession:
        def get(self, url: str, *, proxies: dict[str, str], timeout: float):
            del url, proxies, timeout
            raise requests.exceptions.InvalidSchema("Missing dependencies for SOCKS support.")

    def _missing_curl_probe(url: str, *, proxy_url: str, timeout_sec: float):
        del url, proxy_url, timeout_sec
        raise FileNotFoundError("curl")

    def _fake_socket_probe(url: str, *, proxy_url: str, timeout_sec: float):
        del url, timeout_sec
        if proxy_url.endswith(":41271"):
            return 204, 0.12
        raise AssertionError("unexpected proxy")

    payload = run_managed_proxy_failover(
        config_path=config_path,
        state_dir=state_dir,
        healthcheck_urls=("https://example.com/health",),
        timeout_sec=5.0,
        session_factory=_FakeSession,
        curl_probe_fn=_missing_curl_probe,
        raw_probe_fn=_fake_socket_probe,
    )

    assert payload["selected_port"] == 41271
    assert "PM15MIN_MANAGED_PROXY_ACTIVE_PORT='41271'" in (state_dir / "active_proxy.env").read_text(encoding="utf-8")


def test_run_managed_proxy_failover_can_use_explicit_http_candidate(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"

    class _FakeResponse:
        def __init__(self, status_code: int) -> None:
            self.status_code = status_code

    class _FakeSession:
        def get(self, url: str, *, proxies: dict[str, str], timeout: float):
            del url, timeout
            assert proxies["http"] == "http://127.0.0.1:20171"
            assert proxies["https"] == "http://127.0.0.1:20171"
            return _FakeResponse(200)

    payload = run_managed_proxy_failover(
        config_path=tmp_path / "unused.json",
        state_dir=state_dir,
        candidates=[ProxyCandidate(inbound_tag="managed20171", outbound_tag="managed", port=20171)],
        proxy_scheme="http",
        healthcheck_urls=("https://gamma-api.polymarket.com/markets?limit=1",),
        timeout_sec=5.0,
        session_factory=_FakeSession,
    )

    assert payload["selected_port"] == 20171
    assert "HTTP_PROXY='http://127.0.0.1:20171'" in (state_dir / "active_proxy.env").read_text(encoding="utf-8")
