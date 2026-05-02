#!/usr/bin/env python3
from __future__ import annotations

import selectors
import socket
import ssl
import threading
import time
from urllib.parse import urlsplit

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 20171
UPSTREAM_SOCKS_PORTS = [36897, 45233, 39477, 34785, 41271, 41221]
BUFFER = 65536
CONNECT_TIMEOUT_SEC = 5.0
TLS_PROBE_TIMEOUT_SEC = 6.0
HEALTH_CACHE_TTL_SEC = 120.0
FAIL_COOLDOWN_SEC = 300.0

_health_cache: dict[tuple[int, str, int], tuple[bool, float]] = {}
_cache_lock = threading.Lock()


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = int(size)
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise OSError("unexpected EOF while reading SOCKS response")
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
        raise OSError(f"unsupported SOCKS address type: {atyp}")
    _recv_exact(sock, addr_len + 2)


def connect_via_socks_port(socks_port: int, host: str, port: int) -> socket.socket:
    sock = socket.socket()
    sock.settimeout(CONNECT_TIMEOUT_SEC)
    try:
        sock.connect(("127.0.0.1", int(socks_port)))
        sock.sendall(b"\x05\x01\x00")
        resp = _recv_exact(sock, 2)
        if resp != b"\x05\x00":
            raise OSError(f"greeting_failed:{resp!r}")
        host_b = host.encode("idna")
        if len(host_b) > 255:
            raise OSError(f"target host too long: {host}")
        req = b"\x05\x01\x00\x03" + bytes([len(host_b)]) + host_b + int(port).to_bytes(2, "big")
        sock.sendall(req)
        resp = _recv_exact(sock, 4)
        if resp[0] != 0x05:
            raise OSError(f"unexpected_socks_version:{resp!r}")
        if resp[1] != 0x00:
            raise OSError(f"connect_failed:{resp!r}")
        _discard_socks_address(sock, resp[3])
        sock.settimeout(None)
        return sock
    except Exception:
        try:
            sock.close()
        except Exception:
            pass
        raise


def _cache_key(socks_port: int, host: str, port: int) -> tuple[int, str, int]:
    return int(socks_port), str(host).lower(), int(port)


def _cached_health(socks_port: int, host: str, port: int) -> bool | None:
    key = _cache_key(socks_port, host, port)
    now = time.monotonic()
    with _cache_lock:
        item = _health_cache.get(key)
        if item is None:
            return None
        healthy, expires_at = item
        if now >= expires_at:
            _health_cache.pop(key, None)
            return None
        return healthy


def _store_health(socks_port: int, host: str, port: int, healthy: bool) -> None:
    ttl = HEALTH_CACHE_TTL_SEC if healthy else FAIL_COOLDOWN_SEC
    with _cache_lock:
        _health_cache[_cache_key(socks_port, host, port)] = (bool(healthy), time.monotonic() + ttl)


def tls_probe_via_socks(socks_port: int, host: str, port: int) -> bool:
    cached = _cached_health(socks_port, host, port)
    if cached is not None:
        return bool(cached)
    sock = None
    try:
        sock = connect_via_socks_port(socks_port, host, port)
        sock.settimeout(TLS_PROBE_TIMEOUT_SEC)
        context = ssl.create_default_context()
        with context.wrap_socket(sock, server_hostname=host):
            sock = None
        _store_health(socks_port, host, port, True)
        return True
    except Exception:
        _store_health(socks_port, host, port, False)
        return False
    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass


def connect_via_socks(host: str, port: int) -> socket.socket:
    last: Exception | None = None
    for socks_port in UPSTREAM_SOCKS_PORTS:
        try:
            if int(port) == 443 and not tls_probe_via_socks(socks_port, host, port):
                continue
            return connect_via_socks_port(socks_port, host, port)
        except Exception as exc:
            last = exc
    raise last or OSError("no_healthy_upstream_socks_available")


def relay(left: socket.socket, right: socket.socket) -> None:
    sel = selectors.DefaultSelector()
    sel.register(left, selectors.EVENT_READ, right)
    sel.register(right, selectors.EVENT_READ, left)
    try:
        while True:
            for key, _ in sel.select():
                src = key.fileobj
                dst = key.data
                data = src.recv(BUFFER)
                if not data:
                    return
                dst.sendall(data)
    finally:
        try:
            sel.close()
        except Exception:
            pass
        for sock in (left, right):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                sock.close()
            except Exception:
                pass


def read_headers(conn: socket.socket) -> bytes:
    data = b""
    while b"\r\n\r\n" not in data and len(data) < 65536:
        chunk = conn.recv(BUFFER)
        if not chunk:
            break
        data += chunk
    return data


def parse_target(first_line: str, headers: list[str]) -> tuple[str, str, int]:
    parts = first_line.split()
    if len(parts) < 2:
        raise ValueError("bad_request_line")
    method = parts[0].upper()
    target = parts[1]
    if method == "CONNECT":
        host, port = target.rsplit(":", 1)
        return method, host, int(port)
    parsed = urlsplit(target)
    if parsed.scheme and parsed.hostname:
        return method, parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80)
    host = None
    for line in headers:
        if line.lower().startswith("host:"):
            host = line.split(":", 1)[1].strip()
            break
    if not host:
        raise ValueError("missing_host")
    if ":" in host:
        h, p = host.rsplit(":", 1)
        return method, h, int(p)
    return method, host, 80


def handle_client(conn: socket.socket, _addr: tuple[str, int]) -> None:
    upstream = None
    try:
        raw = read_headers(conn)
        if not raw:
            return
        text = raw.decode("latin1", errors="ignore")
        lines = text.split("\r\n")
        method, host, port = parse_target(lines[0], lines[1:])
        upstream = connect_via_socks(host, port)
        if method == "CONNECT":
            conn.sendall(b"HTTP/1.1 200 Connection Established\r\nProxy-Agent: pm15min-bridge\r\n\r\n")
        else:
            upstream.sendall(raw)
        relay(conn, upstream)
    except Exception:
        try:
            conn.sendall(b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n")
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        if upstream is not None:
            try:
                upstream.close()
            except Exception:
                pass


def main() -> None:
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((LISTEN_HOST, LISTEN_PORT))
    srv.listen(200)
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()
