from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .app import ConsoleHttpHandlers, ConsoleHttpResponse, route_console_http_request


def build_console_http_server(
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    handlers: ConsoleHttpHandlers | None = None,
    server_class: type[ThreadingHTTPServer] = ThreadingHTTPServer,
) -> ThreadingHTTPServer:
    resolved_handlers = handlers or ConsoleHttpHandlers.build_default()

    class ConsoleHttpRequestHandler(BaseHTTPRequestHandler):
        server_version = "pm15min-console/0.1"
        sys_version = ""

        def do_GET(self) -> None:  # noqa: N802
            self._write_response(
                route_console_http_request(
                    method="GET",
                    target=self.path,
                    handlers=resolved_handlers,
                    body=None,
                )
            )

        def do_POST(self) -> None:  # noqa: N802
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(content_length) if content_length > 0 else b""
            self._write_response(
                route_console_http_request(
                    method="POST",
                    target=self.path,
                    handlers=resolved_handlers,
                    body=body,
                )
            )

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return None

        def _write_response(self, response: ConsoleHttpResponse) -> None:
            body = response.body_bytes()
            self.send_response(int(response.status_code))
            self.send_header("Content-Type", response.content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return server_class((str(host), int(port)), ConsoleHttpRequestHandler)


def serve_console_http(
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    handlers: ConsoleHttpHandlers | None = None,
    poll_interval: float = 0.5,
) -> None:
    server = build_console_http_server(
        host=host,
        port=port,
        handlers=handlers,
    )
    try:
        server.serve_forever(poll_interval=float(poll_interval))
    finally:
        server.server_close()
