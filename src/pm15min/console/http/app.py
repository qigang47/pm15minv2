from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlsplit

from pm15min.console.service import load_console_query
from pm15min.console.web import (
    CONSOLE_CSS_PATH,
    CONSOLE_JS_PATH,
    build_console_css,
    build_console_js,
    build_console_shell_page,
)
from .action_routes import execute_console_action_payload
from .routes import build_default_section_handlers, dispatch_console_section_route


JsonMapping = Mapping[str, str]
HealthHandler = Callable[[], object]
ConsoleHandler = Callable[[JsonMapping], object]
SectionHandlers = Mapping[str, Callable[[JsonMapping], object]]
ActionExecuteHandler = Callable[[Mapping[str, object]], object]


@dataclass(frozen=True)
class ConsoleHttpHandlers:
    health_handler: HealthHandler
    console_handler: ConsoleHandler
    section_handlers: SectionHandlers = field(default_factory=dict)
    action_execute_handler: ActionExecuteHandler | None = None

    @classmethod
    def build_default(cls) -> "ConsoleHttpHandlers":
        return cls(
            health_handler=_default_health_handler,
            console_handler=_default_console_handler,
            section_handlers=build_default_section_handlers(),
            action_execute_handler=_default_action_execute_handler,
        )


@dataclass(frozen=True)
class ConsoleHttpResponse:
    status_code: int
    payload: dict[str, object] | None = None
    content_type: str = "application/json; charset=utf-8"
    text: str | None = None

    def body_bytes(self) -> bytes:
        if self.text is not None:
            return self.text.encode("utf-8")
        return json.dumps(self.payload or {}, indent=2, ensure_ascii=False, sort_keys=True).encode("utf-8")


def route_console_http_request(
    *,
    method: str,
    target: str,
    handlers: ConsoleHttpHandlers | None = None,
    body: bytes | None = None,
) -> ConsoleHttpResponse:
    resolved_handlers = handlers or ConsoleHttpHandlers.build_default()
    normalized_method = str(method or "").strip().upper()
    request = parse_console_http_target(target)

    if request.path in {"/", "/console", "/console/"}:
        if normalized_method != "GET":
            return _method_not_allowed(path=request.path, method=normalized_method)
        return ConsoleHttpResponse(
            status_code=200,
            content_type="text/html; charset=utf-8",
            text=build_console_shell_page(
                active_section=str(request.query.get("section") or "home"),
                api_base="/api/console",
            ),
        )

    if request.path == CONSOLE_CSS_PATH:
        if normalized_method != "GET":
            return _method_not_allowed(path=request.path, method=normalized_method)
        return ConsoleHttpResponse(
            status_code=200,
            content_type="text/css; charset=utf-8",
            text=build_console_css(),
        )

    if request.path == CONSOLE_JS_PATH:
        if normalized_method != "GET":
            return _method_not_allowed(path=request.path, method=normalized_method)
        return ConsoleHttpResponse(
            status_code=200,
            content_type="application/javascript; charset=utf-8",
            text=build_console_js(),
        )

    if request.path == "/api/console/actions/execute":
        if normalized_method != "POST":
            return _method_not_allowed(path=request.path, method=normalized_method)
        try:
            request_payload = parse_console_http_json_body(body)
        except ValueError as exc:
            return _error_response(
                status_code=400,
                code="invalid_json",
                message=str(exc),
                path=request.path,
                method=normalized_method,
            )
        try:
            handler = resolved_handlers.action_execute_handler or _default_action_execute_handler
            payload = handler(request_payload)
        except ValueError as exc:
            return _error_response(
                status_code=400,
                code="invalid_request",
                message=str(exc),
                path=request.path,
                method=normalized_method,
            )
        except Exception as exc:
            return _error_response(
                status_code=500,
                code="action_execute_exception",
                message=str(exc),
                path=request.path,
                method=normalized_method,
            )
        return ConsoleHttpResponse(
            status_code=200,
            payload=_json_mapping(payload, fallback={"dataset": "console_action_execution"}),
        )

    if request.path == "/health":
        if normalized_method != "GET":
            return _method_not_allowed(path=request.path, method=normalized_method)
        try:
            payload = resolved_handlers.health_handler()
        except Exception as exc:
            return _error_response(
                status_code=500,
                code="health_handler_exception",
                message=str(exc),
                path=request.path,
                method=normalized_method,
            )
        return ConsoleHttpResponse(
            status_code=200,
            payload=_json_mapping(
                payload,
                fallback={
                    "domain": "console_http",
                    "dataset": "health",
                    "status": "ok",
                },
            ),
        )

    if request.path == "/api/console":
        if normalized_method != "GET":
            return _method_not_allowed(path=request.path, method=normalized_method)
        try:
            payload = resolved_handlers.console_handler(request.query)
        except Exception as exc:
            return _error_response(
                status_code=500,
                code="console_handler_exception",
                message=str(exc),
                path=request.path,
                method=normalized_method,
                query=request.query,
            )
        return ConsoleHttpResponse(
            status_code=200,
            payload=_json_mapping(
                payload,
                fallback={
                    "domain": "console_http",
                    "dataset": "console_home",
                    "query": dict(request.query),
                },
            ),
        )

    try:
        if normalized_method != "GET":
            return _method_not_allowed(path=request.path, method=normalized_method)
        dispatched = dispatch_console_section_route(
            request.path,
            request.query,
            section_handlers=resolved_handlers.section_handlers,
        )
    except ValueError as exc:
        return _error_response(
            status_code=400,
            code="invalid_request",
            message=str(exc),
            path=request.path,
            method=normalized_method,
            query=request.query,
        )
    except Exception as exc:
        return _error_response(
            status_code=500,
            code="console_route_exception",
            message=str(exc),
            path=request.path,
            method=normalized_method,
            query=request.query,
        )
    if dispatched is not None:
        _route, payload = dispatched
        return ConsoleHttpResponse(status_code=200, payload=payload)

    return _error_response(
        status_code=404,
        code="route_not_found",
        message="Unknown console HTTP route.",
        path=request.path,
        method=normalized_method,
        query=request.query,
    )


@dataclass(frozen=True)
class ParsedConsoleHttpTarget:
    path: str
    query: dict[str, str]


def parse_console_http_target(target: str) -> ParsedConsoleHttpTarget:
    split = urlsplit(str(target or "/"))
    query_pairs = parse_qs(split.query, keep_blank_values=False)
    flattened = {
        str(key): str(values[-1])
        for key, values in query_pairs.items()
        if values and str(key).strip()
    }
    return ParsedConsoleHttpTarget(
        path=str(split.path or "/"),
        query=flattened,
    )


def parse_console_http_json_body(body: bytes | None) -> dict[str, object]:
    if body is None or body == b"":
        return {}
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception as exc:
        raise ValueError(f"Invalid JSON body: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object")
    return {str(key): value for key, value in payload.items()}


def _default_health_handler() -> dict[str, object]:
    return {
        "domain": "console_http",
        "dataset": "health",
        "status": "ok",
    }


def _default_console_handler(query: JsonMapping) -> dict[str, object]:
    payload = load_console_query(query)
    if isinstance(payload, Mapping):
        return dict(payload)
    return {
        "domain": "console_http",
        "dataset": "console_list",
        "rows": list(payload),
        "row_count": len(payload),
    }


def _default_action_execute_handler(payload: Mapping[str, object]) -> dict[str, object]:
    return execute_console_action_payload(payload)


def _error_response(
    *,
    status_code: int,
    code: str,
    message: str,
    path: str,
    method: str,
    query: Mapping[str, str] | None = None,
) -> ConsoleHttpResponse:
    payload: dict[str, object] = {
        "domain": "console_http",
        "dataset": "error",
        "status_code": int(status_code),
        "error": {
            "code": str(code),
            "message": str(message),
        },
        "request": {
            "method": str(method),
            "path": str(path),
            "query": dict(query or {}),
        },
    }
    return ConsoleHttpResponse(status_code=int(status_code), payload=payload)


def _method_not_allowed(*, path: str, method: str) -> ConsoleHttpResponse:
    return _error_response(
        status_code=405,
        code="method_not_allowed",
        message="Method not allowed for this console HTTP route.",
        path=path,
        method=method,
    )


def _json_mapping(value: object, *, fallback: dict[str, object]) -> dict[str, object]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    return dict(fallback)
