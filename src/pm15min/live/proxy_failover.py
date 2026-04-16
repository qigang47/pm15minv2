from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from urllib.parse import urlsplit
import subprocess
import socket
import ssl
from typing import Any, Iterable, Sequence

import requests


DEFAULT_HEALTHCHECK_URLS = (
    "https://api.exchange.coinbase.com/time",
    "https://www.google.com/generate_204",
)


ProbeFn = Callable[..., tuple[int | None, float | None]]


@dataclass(frozen=True)
class ProxyCandidate:
    inbound_tag: str
    outbound_tag: str | None
    port: int


@dataclass(frozen=True)
class ProxyProbeResult:
    candidate: ProxyCandidate
    success_count: int
    total_count: int
    avg_latency_sec: float | None
    errors: list[str]
    status_codes: list[int | None]

    @property
    def healthy(self) -> bool:
        return int(self.success_count) > 0


def load_v2ray_proxy_candidates(payload: dict[str, Any]) -> list[ProxyCandidate]:
    inbound_ports: dict[str, int] = {}
    for row in payload.get("inbounds", []):
        if not isinstance(row, dict):
            continue
        tag = str(row.get("tag") or "").strip()
        protocol = str(row.get("protocol") or "").strip().lower()
        port = row.get("port")
        if not tag.startswith("inbound") or protocol != "socks":
            continue
        try:
            inbound_ports[tag] = int(port)
        except Exception:
            continue

    inbound_to_outbound: dict[str, str | None] = {tag: None for tag in inbound_ports}
    routing = payload.get("routing") if isinstance(payload.get("routing"), dict) else {}
    for rule in routing.get("rules", []):
        if not isinstance(rule, dict):
            continue
        outbound_tag = str(rule.get("outboundTag") or "").strip() or None
        for inbound_tag in rule.get("inboundTag", []) or []:
            inbound_tag = str(inbound_tag or "").strip()
            if inbound_tag in inbound_to_outbound:
                inbound_to_outbound[inbound_tag] = outbound_tag

    out: list[ProxyCandidate] = []
    for inbound_tag, port in inbound_ports.items():
        out.append(
            ProxyCandidate(
                inbound_tag=inbound_tag,
                outbound_tag=inbound_to_outbound.get(inbound_tag),
                port=int(port),
            )
        )
    return out


def load_v2ray_proxy_candidates_from_path(path: Path) -> list[ProxyCandidate]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid v2ray config payload: {path}")
    return load_v2ray_proxy_candidates(payload)


def build_proxy_candidates_from_ports(
    ports: Iterable[int],
    *,
    inbound_prefix: str = "managed",
    outbound_tag: str | None = "managed",
) -> list[ProxyCandidate]:
    out: list[ProxyCandidate] = []
    for raw_port in ports:
        port = int(raw_port)
        out.append(
            ProxyCandidate(
                inbound_tag=f"{inbound_prefix}{port}",
                outbound_tag=outbound_tag,
                port=port,
            )
        )
    return out


def select_best_candidate(
    results: Sequence[ProxyProbeResult],
    *,
    current_port: int | None = None,
) -> ProxyProbeResult | None:
    healthy = [item for item in results if item.healthy]
    if not healthy:
        return None
    if current_port is not None:
        for item in healthy:
            if int(item.candidate.port) == int(current_port):
                return item
    return min(
        healthy,
        key=lambda item: (
            -int(item.success_count),
            float("inf") if item.avg_latency_sec is None else float(item.avg_latency_sec),
            int(item.candidate.port),
        ),
    )


def render_proxy_env(
    candidate: ProxyCandidate,
    *,
    host: str = "127.0.0.1",
    scheme: str = "socks5h",
) -> dict[str, str]:
    proxy_url = f"{scheme}://{host}:{int(candidate.port)}"
    return {
        "HTTP_PROXY": proxy_url,
        "HTTPS_PROXY": proxy_url,
        "ALL_PROXY": proxy_url,
        "http_proxy": proxy_url,
        "https_proxy": proxy_url,
        "all_proxy": proxy_url,
        "NO_PROXY": "127.0.0.1,localhost",
        "no_proxy": "127.0.0.1,localhost",
        "PM15MIN_MANAGED_PROXY_ACTIVE_PORT": str(int(candidate.port)),
        "PM15MIN_MANAGED_PROXY_ACTIVE_URL": proxy_url,
        "PM15MIN_MANAGED_PROXY_INBOUND_TAG": str(candidate.inbound_tag),
        "PM15MIN_MANAGED_PROXY_OUTBOUND_TAG": "" if candidate.outbound_tag is None else str(candidate.outbound_tag),
    }


def render_proxy_env_text(env_map: dict[str, str]) -> str:
    lines = [f"export {key}='{value}'" for key, value in sorted(env_map.items())]
    return "\n".join(lines) + "\n"


def write_proxy_env(path: Path, env_map: dict[str, str]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_proxy_env_text(env_map), encoding="utf-8")
    return path


def write_proxy_state(
    path: Path,
    *,
    selected: ProxyProbeResult | None,
    results: Sequence[ProxyProbeResult],
    refreshed: bool = False,
    refresh_returncode: int | None = None,
) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "selected_port": None if selected is None else int(selected.candidate.port),
        "selected_inbound_tag": None if selected is None else str(selected.candidate.inbound_tag),
        "selected_outbound_tag": None if selected is None else selected.candidate.outbound_tag,
        "refreshed": bool(refreshed),
        "refresh_returncode": refresh_returncode,
        "results": [
            {
                **asdict(item.candidate),
                "success_count": int(item.success_count),
                "total_count": int(item.total_count),
                "avg_latency_sec": item.avg_latency_sec,
                "errors": list(item.errors),
                "status_codes": list(item.status_codes),
                "healthy": bool(item.healthy),
            }
            for item in results
        ],
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return path


def load_selected_port_from_state(path: Path) -> int | None:
    path = Path(path)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        value = payload.get("selected_port")
        return None if value in (None, "") else int(value)
    except Exception:
        return None


def _supports_curl_probe_fallback(exc: requests.RequestException) -> bool:
    return isinstance(exc, requests.exceptions.InvalidSchema) and "Missing dependencies for SOCKS support" in str(exc)


def _probe_url_via_curl(
    url: str,
    *,
    proxy_url: str,
    timeout_sec: float,
) -> tuple[int | None, float | None]:
    completed = subprocess.run(
        [
            "curl",
            "--silent",
            "--show-error",
            "--output",
            "/dev/null",
            "--write-out",
            "%{http_code} %{time_total}",
            "--proxy",
            str(proxy_url),
            "--connect-timeout",
            str(float(timeout_sec)),
            "--max-time",
            str(float(timeout_sec)),
            str(url),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(stderr or f"curl probe failed with exit code {completed.returncode}")
    parts = (completed.stdout or "").strip().split()
    if len(parts) != 2:
        raise RuntimeError(f"Unexpected curl probe output: {completed.stdout!r}")
    status_code = None if parts[0] in ("", "000") else int(parts[0])
    latency_sec = None if parts[1] == "" else float(parts[1])
    return status_code, latency_sec


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = int(size)
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise RuntimeError("unexpected EOF while reading SOCKS response")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _discard_socks_address(sock: socket.socket, atyp: int) -> None:
    if atyp == 0x01:
        addr_len = 4
    elif atyp == 0x03:
        addr_len = _recv_exact(sock, 1)[0]
    elif atyp == 0x04:
        addr_len = 16
    else:
        raise RuntimeError(f"unsupported SOCKS address type: {atyp}")
    _recv_exact(sock, addr_len + 2)


def _probe_url_via_socks_socket(
    url: str,
    *,
    proxy_url: str,
    timeout_sec: float,
) -> tuple[int | None, float | None]:
    started = datetime.now(timezone.utc).timestamp()
    proxy = urlsplit(str(proxy_url))
    target = urlsplit(str(url))
    proxy_host = str(proxy.hostname or "").strip()
    proxy_port = int(proxy.port or 0)
    target_host = str(target.hostname or "").strip()
    if not proxy_host or proxy_port <= 0:
        raise RuntimeError(f"invalid proxy url: {proxy_url}")
    if not target_host:
        raise RuntimeError(f"invalid target url: {url}")
    target_port = int(target.port or (443 if target.scheme == "https" else 80))
    path = target.path or "/"
    if target.query:
        path = f"{path}?{target.query}"

    with socket.create_connection((proxy_host, proxy_port), timeout=float(timeout_sec)) as raw_sock:
        raw_sock.settimeout(float(timeout_sec))
        raw_sock.sendall(b"\x05\x01\x00")
        greeting = _recv_exact(raw_sock, 2)
        if greeting != b"\x05\x00":
            raise RuntimeError(f"SOCKS auth negotiation failed: {greeting!r}")

        host_bytes = target_host.encode("idna")
        if len(host_bytes) > 255:
            raise RuntimeError(f"target host too long for SOCKS5: {target_host}")
        request = b"".join(
            (
                b"\x05\x01\x00\x03",
                bytes([len(host_bytes)]),
                host_bytes,
                int(target_port).to_bytes(2, "big"),
            )
        )
        raw_sock.sendall(request)
        response = _recv_exact(raw_sock, 4)
        if response[0] != 0x05:
            raise RuntimeError(f"unexpected SOCKS version in connect response: {response[0]}")
        if response[1] != 0x00:
            raise RuntimeError(f"SOCKS connect failed with code {response[1]}")
        _discard_socks_address(raw_sock, response[3])

        stream: socket.socket
        if target.scheme == "https":
            context = ssl.create_default_context()
            stream = context.wrap_socket(raw_sock, server_hostname=target_host)
        else:
            stream = raw_sock

        request_bytes = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {target_host}\r\n"
            "User-Agent: pm15min-managed-proxy/1\r\n"
            "Accept: */*\r\n"
            "Connection: close\r\n\r\n"
        ).encode("ascii")
        stream.sendall(request_bytes)
        response_line = b""
        while not response_line.endswith(b"\r\n"):
            chunk = stream.recv(1)
            if not chunk:
                break
            response_line += chunk
            if len(response_line) > 4096:
                raise RuntimeError("HTTP status line too long")
        text = response_line.decode("iso-8859-1").strip()
        parts = text.split()
        if len(parts) < 2 or not parts[1].isdigit():
            raise RuntimeError(f"invalid HTTP response line: {text!r}")
        elapsed = datetime.now(timezone.utc).timestamp() - started
        return int(parts[1]), float(elapsed)


def _run_probe_fallbacks(
    probe_fns: Sequence[ProbeFn | None],
    *,
    url: str,
    proxy_url: str,
    timeout_sec: float,
) -> tuple[int | None, float | None]:
    probe_errors: list[str] = []
    for probe_fn in probe_fns:
        if probe_fn is None:
            continue
        try:
            return probe_fn(
                str(url),
                proxy_url=str(proxy_url),
                timeout_sec=float(timeout_sec),
            )
        except Exception as exc:
            probe_errors.append(f"{type(exc).__name__}: {exc}")
    joined = "; ".join(probe_errors)
    raise RuntimeError(joined or "no probe fallback available")


def probe_candidate(
    candidate: ProxyCandidate,
    *,
    healthcheck_urls: Sequence[str],
    timeout_sec: float,
    session_factory=requests.Session,
    proxy_host: str = "127.0.0.1",
    proxy_scheme: str = "socks5h",
    curl_probe_fn: ProbeFn | None = _probe_url_via_curl,
    raw_probe_fn: ProbeFn | None = _probe_url_via_socks_socket,
) -> ProxyProbeResult:
    proxy_url = render_proxy_env(candidate, host=proxy_host, scheme=proxy_scheme)["HTTP_PROXY"]
    proxies = {"http": proxy_url, "https": proxy_url}
    success_count = 0
    latencies: list[float] = []
    errors: list[str] = []
    status_codes: list[int | None] = []

    session = session_factory()
    for url in healthcheck_urls:
        try:
            started = datetime.now(timezone.utc).timestamp()
            response = session.get(str(url), proxies=proxies, timeout=float(timeout_sec))
            elapsed = datetime.now(timezone.utc).timestamp() - started
            status_codes.append(int(response.status_code))
            if int(response.status_code) < 500:
                success_count += 1
                latencies.append(float(elapsed))
            else:
                errors.append(f"HTTP {int(response.status_code)} {url}")
        except requests.RequestException as exc:
            if _supports_curl_probe_fallback(exc):
                try:
                    status_code, elapsed = _run_probe_fallbacks(
                        (curl_probe_fn, raw_probe_fn),
                        url=str(url),
                        proxy_url=proxy_url,
                        timeout_sec=float(timeout_sec),
                    )
                    status_codes.append(None if status_code is None else int(status_code))
                    if status_code is not None and int(status_code) < 500:
                        success_count += 1
                        if elapsed is not None:
                            latencies.append(float(elapsed))
                    else:
                        errors.append(
                            f"HTTP {int(status_code)} {url}" if status_code is not None else f"curl probe returned no status {url}"
                        )
                except Exception as curl_exc:
                    status_codes.append(None)
                    errors.append(f"{type(curl_exc).__name__}: {curl_exc}")
                continue
            status_codes.append(None)
            errors.append(f"{type(exc).__name__}: {exc}")

    avg_latency = None if not latencies else round(sum(latencies) / len(latencies), 6)
    return ProxyProbeResult(
        candidate=candidate,
        success_count=success_count,
        total_count=len(list(healthcheck_urls)),
        avg_latency_sec=avg_latency,
        errors=errors,
        status_codes=status_codes,
    )


def probe_candidates(
    candidates: Sequence[ProxyCandidate],
    *,
    healthcheck_urls: Sequence[str] = DEFAULT_HEALTHCHECK_URLS,
    timeout_sec: float = 5.0,
    session_factory=requests.Session,
    proxy_host: str = "127.0.0.1",
    proxy_scheme: str = "socks5h",
    curl_probe_fn: ProbeFn | None = _probe_url_via_curl,
    raw_probe_fn: ProbeFn | None = _probe_url_via_socks_socket,
) -> list[ProxyProbeResult]:
    return [
        probe_candidate(
            item,
            healthcheck_urls=healthcheck_urls,
            timeout_sec=timeout_sec,
            session_factory=session_factory,
            proxy_host=proxy_host,
            proxy_scheme=proxy_scheme,
            curl_probe_fn=curl_probe_fn,
            raw_probe_fn=raw_probe_fn,
        )
        for item in candidates
    ]


def run_refresh_command(command: str, *, cwd: Path | None = None) -> int:
    completed = subprocess.run(
        command,
        shell=True,
        executable="/bin/bash",
        cwd=None if cwd is None else str(cwd),
        check=False,
    )
    return int(completed.returncode)


def run_managed_proxy_failover(
    *,
    config_path: Path,
    state_dir: Path,
    candidates: Sequence[ProxyCandidate] | None = None,
    proxy_host: str = "127.0.0.1",
    proxy_scheme: str = "socks5h",
    healthcheck_urls: Sequence[str] = DEFAULT_HEALTHCHECK_URLS,
    timeout_sec: float = 5.0,
    refresh_command: str | None = None,
    session_factory=requests.Session,
    curl_probe_fn: ProbeFn | None = _probe_url_via_curl,
    raw_probe_fn: ProbeFn | None = _probe_url_via_socks_socket,
) -> dict[str, Any]:
    resolved_candidates = list(candidates) if candidates is not None else load_v2ray_proxy_candidates_from_path(config_path)
    state_path = Path(state_dir) / "state.json"
    env_path = Path(state_dir) / "active_proxy.env"
    current_port = load_selected_port_from_state(state_path)

    results = probe_candidates(
        resolved_candidates,
        healthcheck_urls=healthcheck_urls,
        timeout_sec=timeout_sec,
        session_factory=session_factory,
        proxy_host=proxy_host,
        proxy_scheme=proxy_scheme,
        curl_probe_fn=curl_probe_fn,
        raw_probe_fn=raw_probe_fn,
    )
    refreshed = False
    refresh_returncode: int | None = None
    selected = select_best_candidate(results, current_port=current_port)

    if selected is None and refresh_command:
        refreshed = True
        refresh_returncode = run_refresh_command(refresh_command, cwd=Path(config_path).parent)
        results = probe_candidates(
            resolved_candidates,
            healthcheck_urls=healthcheck_urls,
            timeout_sec=timeout_sec,
            session_factory=session_factory,
            proxy_host=proxy_host,
            proxy_scheme=proxy_scheme,
            curl_probe_fn=curl_probe_fn,
            raw_probe_fn=raw_probe_fn,
        )
        selected = select_best_candidate(results, current_port=current_port)

    if selected is not None:
        write_proxy_env(env_path, render_proxy_env(selected.candidate, host=proxy_host, scheme=proxy_scheme))
    write_proxy_state(
        state_path,
        selected=selected,
        results=results,
        refreshed=refreshed,
        refresh_returncode=refresh_returncode,
    )
    return {
        "selected_port": None if selected is None else int(selected.candidate.port),
        "selected_env_path": str(env_path),
        "state_path": str(state_path),
        "refreshed": bool(refreshed),
        "refresh_returncode": refresh_returncode,
        "healthy_ports": [int(item.candidate.port) for item in results if item.healthy],
    }
