from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from . import service as console_service
from .compat import execute_console_action, submit_console_action_task
from .read_models import describe_console_backtest_stake_sweep, describe_console_experiment_matrix


JsonMapping = Mapping[str, str]
HealthHandler = Callable[[], object]
ConsoleHandler = Callable[[JsonMapping], object]
SectionHandler = Callable[[JsonMapping], object]
SectionHandlers = Mapping[str, SectionHandler]
ActionExecuteHandler = Callable[[Mapping[str, object]], object]

CONSOLE_CSS_PATH = "/static/console.css"
CONSOLE_JS_PATH = "/static/console.js"

_SECTION_PATHS: dict[str, str] = {
    "home": "/api/console/home",
    "runtime_state": "/api/console/runtime-state",
    "runtime_history": "/api/console/runtime-history",
    "data_overview": "/api/console/data-overview",
    "training_runs": "/api/console/training-runs",
    "bundles": "/api/console/bundles",
    "backtests": "/api/console/backtests",
    "backtest_stake_sweep": "/api/console/backtests/stake-sweep",
    "experiments": "/api/console/experiments",
    "experiment_matrix": "/api/console/experiments/matrix",
    "actions": "/api/console/actions",
    "tasks": "/api/console/tasks",
}


@dataclass(frozen=True)
class ConsoleHttpHandlers:
    health_handler: HealthHandler
    console_handler: ConsoleHandler
    section_handlers: SectionHandlers = field(default_factory=dict)
    action_execute_handler: ActionExecuteHandler | None = None

    @classmethod
    def build_default(cls) -> "ConsoleHttpHandlers":
        return build_pm5min_console_http_handlers()


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


@dataclass(frozen=True)
class ParsedConsoleHttpTarget:
    path: str
    query: dict[str, str]


def build_pm5min_console_http_handlers() -> ConsoleHttpHandlers:
    return ConsoleHttpHandlers(
        health_handler=_health_handler,
        console_handler=_handle_console_query,
        section_handlers=_build_section_handlers(),
        action_execute_handler=_default_action_execute_handler,
    )


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
            text=_build_console_shell_page(
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
            text=_build_console_css(),
        )

    if request.path == CONSOLE_JS_PATH:
        if normalized_method != "GET":
            return _method_not_allowed(path=request.path, method=normalized_method)
        return ConsoleHttpResponse(
            status_code=200,
            content_type="application/javascript; charset=utf-8",
            text=_build_console_js(),
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

    if normalized_method != "GET":
        return _method_not_allowed(path=request.path, method=normalized_method)

    handler = resolved_handlers.section_handlers.get(request.path)
    if handler is None:
        return _error_response(
            status_code=404,
            code="route_not_found",
            message="Unknown console HTTP route.",
            path=request.path,
            method=normalized_method,
            query=request.query,
        )
    try:
        payload = _as_http_payload(
            result=handler(request.query),
            section_id=_section_id_for_path(request.path),
            path=request.path,
            query=request.query,
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
    return ConsoleHttpResponse(status_code=200, payload=payload)


def build_console_http_server(
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    handlers: ConsoleHttpHandlers | None = None,
    server_class: type[ThreadingHTTPServer] = ThreadingHTTPServer,
) -> ThreadingHTTPServer:
    resolved_handlers = handlers or ConsoleHttpHandlers.build_default()

    class ConsoleHttpRequestHandler(BaseHTTPRequestHandler):
        server_version = "pm5min-console/0.1"
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


def _build_section_handlers() -> dict[str, SectionHandler]:
    return {
        "/api/console/home": _handle_console_home,
        "/api/console/runtime-state": _handle_runtime_state,
        "/api/console/runtime-history": _handle_runtime_history,
        "/api/console/data-overview": _handle_data_overview,
        "/api/console/training-runs": _handle_training_runs,
        "/api/console/bundles": _handle_bundles,
        "/api/console/backtests": _handle_backtests,
        "/api/console/backtests/stake-sweep": _handle_backtest_stake_sweep,
        "/api/console/experiments": _handle_experiments,
        "/api/console/experiments/matrix": _handle_experiment_matrix,
        "/api/console/actions": _handle_actions,
        "/api/console/tasks": _handle_tasks,
    }


def _health_handler() -> dict[str, object]:
    return {
        "domain": "console_http",
        "dataset": "health",
        "status": "ok",
    }


def _handle_console_query(query: JsonMapping) -> dict[str, object]:
    section = _string_value(query.get("section")) or "home"
    path = _SECTION_PATHS.get(section)
    if path is None:
        raise ValueError(f"Unsupported console section: {section}")
    handler = _build_section_handlers()[path]
    handler_query = {
        str(key): value
        for key, value in query.items()
        if str(key) != "section"
    }
    return _as_http_payload(
        result=handler(handler_query),
        section_id=section,
        path=path,
        query=handler_query,
    )


def _default_action_execute_handler(payload: Mapping[str, object]) -> dict[str, object]:
    return _execute_console_action_payload(
        payload,
        executor=execute_console_action,
        task_submitter=submit_console_action_task,
    )


def _handle_console_home(query: JsonMapping) -> dict[str, object]:
    return console_service.load_console_home(root=_optional_path(query.get("root")))


def _handle_runtime_state(query: JsonMapping) -> dict[str, object]:
    return console_service.load_console_runtime_state(root=_optional_path(query.get("root")))


def _handle_runtime_history(query: JsonMapping) -> dict[str, object]:
    return console_service.load_console_runtime_history(root=_optional_path(query.get("root")))


def _handle_data_overview(query: JsonMapping) -> dict[str, object]:
    return console_service.load_console_data_overview(
        market=_string_value(query.get("market")) or "sol",
        cycle=_string_value(query.get("cycle")) or "5m",
        surface=_string_value(query.get("surface")) or "backtest",
        root=_optional_path(query.get("root")),
    )


def _handle_training_runs(query: JsonMapping) -> object:
    common = {
        "market": _string_value(query.get("market")) or "sol",
        "cycle": _string_value(query.get("cycle")) or "5m",
        "root": _optional_path(query.get("root")),
    }
    if _string_value(query.get("run_label")) or _string_value(query.get("run_dir")):
        return console_service.load_console_training_run(
            **common,
            model_family=_string_value(query.get("model_family")) or "deep_otm",
            target=_string_value(query.get("target")) or "direction",
            run_label=_string_value(query.get("run_label")),
            run_dir=_string_value(query.get("run_dir")),
        )
    return console_service.list_console_training_runs(
        **common,
        model_family=_string_value(query.get("model_family")),
        target=_string_value(query.get("target")),
        prefix=_string_value(query.get("prefix")),
    )


def _handle_bundles(query: JsonMapping) -> object:
    common = {
        "market": _string_value(query.get("market")) or "sol",
        "cycle": _string_value(query.get("cycle")) or "5m",
        "root": _optional_path(query.get("root")),
    }
    if _string_value(query.get("bundle_label")) or _string_value(query.get("bundle_dir")):
        return console_service.load_console_bundle(
            **common,
            profile=_string_value(query.get("profile")) or "deep_otm_5m",
            target=_string_value(query.get("target")) or "direction",
            bundle_label=_string_value(query.get("bundle_label")),
            bundle_dir=_string_value(query.get("bundle_dir")),
        )
    return console_service.list_console_bundles(
        **common,
        profile=_string_value(query.get("profile")),
        target=_string_value(query.get("target")),
        prefix=_string_value(query.get("prefix")),
    )


def _handle_backtests(query: JsonMapping) -> object:
    common = {
        "market": _string_value(query.get("market")) or "sol",
        "cycle": _string_value(query.get("cycle")) or "5m",
        "root": _optional_path(query.get("root")),
    }
    if _string_value(query.get("run_label")):
        return console_service.load_console_backtest(
            **common,
            profile=_string_value(query.get("profile")) or "deep_otm_5m",
            spec_name=_string_value(query.get("spec")) or "baseline_truth",
            run_label=_required_string(query, "run_label"),
        )
    return console_service.list_console_backtests(
        **common,
        profile=_string_value(query.get("profile")),
        spec_name=_string_value(query.get("spec")),
        prefix=_string_value(query.get("prefix")),
    )


def _handle_backtest_stake_sweep(query: JsonMapping) -> object:
    return _load_console_backtest_stake_sweep(
        market=_string_value(query.get("market")) or "sol",
        cycle=_string_value(query.get("cycle")) or "5m",
        profile=_string_value(query.get("profile")) or "deep_otm_5m",
        spec_name=_string_value(query.get("spec")) or "baseline_truth",
        run_label=_required_string(query, "run_label"),
        root=_optional_path(query.get("root")),
    )


def _handle_experiments(query: JsonMapping) -> object:
    root = _optional_path(query.get("root"))
    if _string_value(query.get("run_label")):
        return console_service.load_console_experiment(
            suite_name=_required_string(query, "suite"),
            run_label=_required_string(query, "run_label"),
            root=root,
        )
    return console_service.list_console_experiments(
        suite_name=_string_value(query.get("suite")),
        prefix=_string_value(query.get("prefix")),
        root=root,
    )


def _handle_experiment_matrix(query: JsonMapping) -> object:
    return _load_console_experiment_matrix(
        suite_name=_required_string(query, "suite"),
        run_label=_required_string(query, "run_label"),
        root=_optional_path(query.get("root")),
    )


def _handle_actions(query: JsonMapping) -> object:
    if _string_value(query.get("action_id")):
        request = {
            key: value
            for key, value in query.items()
            if key not in {"action_id", "for_section", "shell_enabled"}
        }
        return console_service.build_console_action_request(
            action_id=_required_string(query, "action_id"),
            request=request,
        )
    return console_service.load_console_action_catalog(
        for_section=_string_value(query.get("for_section")),
        shell_enabled=_optional_bool(query.get("shell_enabled")),
    )


def _handle_tasks(query: JsonMapping) -> object:
    if _string_value(query.get("task_id")):
        return console_service.load_console_task(
            task_id=_required_string(query, "task_id"),
            root=_optional_path(query.get("root")),
        )
    return console_service.list_console_tasks(
        action_id=_string_value(query.get("action_id")),
        action_ids=_optional_csv_strings(query.get("action_ids")),
        status=_string_value(query.get("status")),
        status_group=_string_value(query.get("status_group")),
        marker=_string_value(query.get("marker")),
        group_by=_string_value(query.get("group_by")),
        limit=_optional_int(query.get("limit"), default=20),
        root=_optional_path(query.get("root")),
    )


def _load_console_backtest_stake_sweep(
    *,
    market: str,
    cycle: str,
    profile: str,
    spec_name: str,
    run_label: str,
    root: Path | None,
) -> dict[str, object]:
    loader = getattr(console_service, "load_console_backtest_stake_sweep", None)
    if callable(loader):
        return loader(
            market=market,
            cycle=cycle,
            profile=profile,
            spec_name=spec_name,
            run_label=run_label,
            root=root,
        )
    return describe_console_backtest_stake_sweep(
        market=market,
        cycle=cycle,
        profile=profile,
        spec_name=spec_name,
        run_label=run_label,
        root=root,
    )


def _load_console_experiment_matrix(
    *,
    suite_name: str,
    run_label: str,
    root: Path | None,
) -> dict[str, object]:
    loader = getattr(console_service, "load_console_experiment_matrix", None)
    if callable(loader):
        return loader(
            suite_name=suite_name,
            run_label=run_label,
            root=root,
        )
    return describe_console_experiment_matrix(
        suite_name=suite_name,
        run_label=run_label,
        root=root,
    )


def _as_http_payload(
    *,
    result: object,
    section_id: str,
    path: str,
    query: JsonMapping,
) -> dict[str, object]:
    if isinstance(result, Mapping):
        payload = {str(key): value for key, value in result.items()}
        payload.setdefault("section", section_id)
        payload.setdefault("request_path", path)
        return payload
    rows = list(result if isinstance(result, list) else list(result))
    return {
        "domain": "console_http",
        "dataset": "console_section_rows",
        "section": section_id,
        "request_path": path,
        "query": dict(query),
        "rows": rows,
        "row_count": len(rows),
    }


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


def _execute_console_action_payload(
    body: Mapping[str, object],
    *,
    executor: ActionExecuteHandler = execute_console_action,
    task_submitter: ActionExecuteHandler = submit_console_action_task,
) -> dict[str, object]:
    action_id = _required_payload_string(body, "action_id")
    request = body.get("request")
    if request is not None and not isinstance(request, Mapping):
        raise ValueError("request must be a mapping when provided")
    execution_mode = _execution_mode(body.get("execution_mode"))
    root = body.get("root")
    if root is not None and not str(root).strip():
        root = None
    runner = task_submitter if execution_mode == "async" else executor
    kwargs: dict[str, object] = {
        "action_id": action_id,
        "request": None if request is None else dict(request),
    }
    if execution_mode == "async":
        kwargs["root"] = None if root is None else str(root)
    payload = dict(runner(**kwargs))
    payload.setdefault("section", "actions")
    payload.setdefault("execution_mode", execution_mode)
    return payload


def _section_id_for_path(path: str) -> str:
    normalized = str(path or "/").rstrip("/") or "/"
    if normalized == "/api/console/backtests/stake-sweep":
        return "backtest_stake_sweep"
    if normalized == "/api/console/experiments/matrix":
        return "experiment_matrix"
    return normalized.split("/")[-1].replace("-", "_")


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


def _required_payload_string(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if value is None or not str(value).strip():
        raise ValueError(f"Missing required action field: {key}")
    return str(value).strip()


def _string_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_string(query: JsonMapping, key: str) -> str:
    value = _string_value(query.get(key))
    if value is None:
        raise ValueError(f"Missing required query parameter: {key}")
    return value


def _optional_path(value: object) -> Path | None:
    text = _string_value(value)
    return None if text is None else Path(text)


def _optional_bool(value: object) -> bool | None:
    text = _string_value(value)
    if text is None:
        return None
    token = text.lower()
    if token in {"1", "true", "yes", "on"}:
        return True
    if token in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean token: {value!r}")


def _optional_int(value: object, *, default: int) -> int:
    text = _string_value(value)
    if text is None:
        return int(default)
    return int(text)


def _optional_csv_strings(value: object) -> tuple[str, ...]:
    text = _string_value(value)
    if text is None:
        return ()
    return tuple(token.strip() for token in text.split(",") if token.strip())


def _execution_mode(value: object) -> str:
    text = _string_value(value)
    if text is None:
        return "sync"
    token = text.lower()
    if token not in {"sync", "async"}:
        raise ValueError(f"Invalid execution_mode: {value!r}")
    return token


def _build_console_shell_page(*, active_section: str, api_base: str) -> str:
    sections = (
        ("home", "Home"),
        ("runtime_state", "Runtime"),
        ("data_overview", "Data"),
        ("training_runs", "Training"),
        ("bundles", "Bundles"),
        ("backtests", "Backtests"),
        ("experiments", "Experiments"),
        ("tasks", "Tasks"),
    )
    section_ids = {section_id for section_id, _title in sections}
    resolved_section = active_section if active_section in section_ids else "home"
    nav = "\n".join(
        f'<a class="console-nav-link{" is-active" if section_id == resolved_section else ""}" '
        f'href="?section={section_id}" data-section-id="{section_id}">{title}</a>'
        for section_id, title in sections
    )
    bootstrap = json.dumps(
        {
            "active_section": resolved_section,
            "api_base": str(api_base),
            "defaults": {
                "market": "sol",
                "cycle": "5m",
                "surface": "backtest",
                "profile": "deep_otm_5m",
                "target": "direction",
                "model_family": "deep_otm",
                "spec": "baseline_truth",
            },
        },
        ensure_ascii=False,
    )
    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '  <meta charset="utf-8">',
            '  <meta name="viewport" content="width=device-width, initial-scale=1">',
            "  <title>pm5min console</title>",
            f'  <link rel="stylesheet" href="{CONSOLE_CSS_PATH}">',
            "</head>",
            f'  <body data-console-shell data-api-base="{api_base}" data-active-section="{resolved_section}">',
            '    <div class="console-shell">',
            '      <header class="console-hero">',
            "        <p class=\"console-kicker\">Local read-only console</p>",
            "        <h1 class=\"console-title\">pm5min console</h1>",
            "        <p class=\"console-subtitle\">Browse 5m data, runs, bundles, backtests, experiments, and task state from one local surface.</p>",
            "      </header>",
            '      <div class="console-layout">',
            f'        <nav class="console-nav">{nav}</nav>',
            '        <main class="console-panel">',
            '          <p class="console-panel-copy">The panel below fetches the selected 5m section through the local console HTTP routes.</p>',
            '          <pre class="console-json" data-console-json></pre>',
            "        </main>",
            "      </div>",
            "    </div>",
            f'    <script id="console-bootstrap" type="application/json">{bootstrap}</script>',
            f'    <script src="{CONSOLE_JS_PATH}"></script>',
            "  </body>",
            "</html>",
        ]
    )


def _build_console_css() -> str:
    return """
:root {
  --bg: #f5efe4;
  --panel: #fffaf2;
  --border: #d8ccb3;
  --ink: #1d1a16;
  --muted: #675f53;
  --accent: #8a3d16;
  --accent-soft: #efe0c8;
  --shadow: rgba(69, 48, 22, 0.10);
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  min-height: 100vh;
  padding: 24px;
  background:
    radial-gradient(circle at top right, rgba(138, 61, 22, 0.12), transparent 28rem),
    linear-gradient(180deg, #faf5ea 0%, var(--bg) 55%, #f0e8d9 100%);
  color: var(--ink);
  font-family: Georgia, "Times New Roman", serif;
}

.console-shell {
  max-width: 1180px;
  margin: 0 auto;
}

.console-hero,
.console-panel,
.console-nav {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: 18px;
  box-shadow: 0 18px 45px var(--shadow);
}

.console-hero {
  padding: 28px 30px;
  margin-bottom: 20px;
}

.console-kicker {
  margin: 0 0 8px;
  font-size: 12px;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--accent);
}

.console-title {
  margin: 0;
  font-size: clamp(28px, 5vw, 48px);
}

.console-subtitle,
.console-panel-copy {
  color: var(--muted);
  line-height: 1.6;
}

.console-layout {
  display: grid;
  grid-template-columns: 240px 1fr;
  gap: 20px;
}

.console-nav {
  padding: 12px;
  display: grid;
  gap: 10px;
  align-content: start;
}

.console-nav-link {
  display: block;
  padding: 12px 14px;
  border-radius: 12px;
  color: var(--ink);
  text-decoration: none;
  background: rgba(255, 255, 255, 0.55);
}

.console-nav-link.is-active {
  background: var(--accent-soft);
  border: 1px solid var(--accent);
}

.console-panel {
  padding: 22px 24px;
}

.console-json {
  margin: 0;
  padding: 16px;
  min-height: 420px;
  overflow: auto;
  border-radius: 14px;
  border: 1px solid var(--border);
  background: #fffdf8;
  font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
  font-size: 13px;
  line-height: 1.5;
}

@media (max-width: 900px) {
  body {
    padding: 16px;
  }

  .console-layout {
    grid-template-columns: 1fr;
  }
}
""".strip()


def _build_console_js() -> str:
    return """
const bootstrapNode = document.getElementById("console-bootstrap");
const bootstrap = bootstrapNode ? JSON.parse(bootstrapNode.textContent || "{}") : {};
const output = document.querySelector("[data-console-json]");
const navLinks = Array.from(document.querySelectorAll("[data-section-id]"));

function buildUrl(sectionId) {
  const url = new URL(String(bootstrap.api_base || "/api/console"), window.location.origin);
  url.searchParams.set("section", sectionId);
  const defaults = bootstrap.defaults || {};
  for (const [key, value] of Object.entries(defaults)) {
    if (value !== undefined && value !== null && String(value) !== "") {
      url.searchParams.set(key, String(value));
    }
  }
  return url.toString();
}

function setActive(sectionId) {
  for (const link of navLinks) {
    link.classList.toggle("is-active", link.dataset.sectionId === sectionId);
  }
  document.body.dataset.activeSection = sectionId;
}

async function loadSection(sectionId) {
  const resolved = sectionId || String(bootstrap.active_section || "home");
  setActive(resolved);
  if (output) {
    output.textContent = "Loading...";
  }
  try {
    const response = await fetch(buildUrl(resolved));
    const payload = await response.json();
    if (output) {
      output.textContent = JSON.stringify(payload, null, 2);
    }
  } catch (error) {
    if (output) {
      output.textContent = JSON.stringify({ error: String(error) }, null, 2);
    }
  }
}

for (const link of navLinks) {
  link.addEventListener("click", (event) => {
    event.preventDefault();
    const sectionId = link.dataset.sectionId || "home";
    const url = new URL(window.location.href);
    url.searchParams.set("section", sectionId);
    window.history.replaceState({}, "", url);
    loadSection(sectionId);
  });
}

loadSection(String(bootstrap.active_section || "home"));
""".strip()


__all__ = [
    "ConsoleHttpHandlers",
    "ConsoleHttpResponse",
    "build_console_http_server",
    "build_pm5min_console_http_handlers",
    "route_console_http_request",
    "serve_console_http",
]
