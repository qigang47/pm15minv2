from __future__ import annotations

import json
import threading
from http.client import HTTPConnection

from pm15min.console.http import (
    ConsoleHttpHandlers,
    build_console_http_server,
    route_console_http_request,
)


def test_route_console_http_request_handles_health_console_and_errors() -> None:
    handlers = ConsoleHttpHandlers(
        health_handler=lambda: {"status": "ok", "source": "test"},
        console_handler=lambda query: {"dataset": "console_home", "query": dict(query)},
        action_execute_handler=lambda payload: {"dataset": "console_action_execution", "action_id": payload.get("action_id"), "status": "ok"},
    )

    health = route_console_http_request(method="GET", target="/health", handlers=handlers)
    console = route_console_http_request(method="GET", target="/api/console?market=sol", handlers=handlers)
    not_found = route_console_http_request(method="GET", target="/missing", handlers=handlers)
    invalid_method = route_console_http_request(method="POST", target="/health", handlers=handlers)
    action = route_console_http_request(
        method="POST",
        target="/api/console/actions/execute",
        handlers=handlers,
        body=json.dumps({"action_id": "data_refresh_summary"}).encode("utf-8"),
    )
    task_list = route_console_http_request(
        method="GET",
        target="/api/console/tasks",
        handlers=ConsoleHttpHandlers(
            health_handler=handlers.health_handler,
            console_handler=handlers.console_handler,
            section_handlers={
                "/api/console/tasks": lambda query: {"dataset": "console_task_list", "row_count": 0, "rows": []},
            },
            action_execute_handler=handlers.action_execute_handler,
        ),
    )

    assert health.status_code == 200
    assert health.payload["source"] == "test"

    assert console.status_code == 200
    assert console.payload["query"]["market"] == "sol"

    assert not_found.status_code == 404
    assert not_found.payload["error"]["code"] == "route_not_found"

    assert invalid_method.status_code == 405
    assert invalid_method.payload["error"]["code"] == "method_not_allowed"
    assert action.status_code == 200
    assert action.payload["dataset"] == "console_action_execution"
    assert action.payload["action_id"] == "data_refresh_summary"
    assert task_list.status_code == 200
    assert task_list.payload["dataset"] == "console_task_list"


def test_build_console_http_server_serves_json_endpoints() -> None:
    handlers = ConsoleHttpHandlers(
        health_handler=lambda: {"status": "ok", "source": "server_test"},
        console_handler=lambda query: {"dataset": "console_home", "query": dict(query), "section_count": 1},
        action_execute_handler=lambda payload: {"dataset": "console_action_execution", "action_id": payload.get("action_id"), "status": "ok"},
    )
    server = build_console_http_server(host="127.0.0.1", port=0, handlers=handlers)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        conn = HTTPConnection(host, port, timeout=5)
        conn.request("GET", "/health")
        health_resp = conn.getresponse()
        health_payload = json.loads(health_resp.read().decode("utf-8"))
        assert health_resp.status == 200
        assert health_payload["source"] == "server_test"

        conn.request("GET", "/api/console?suite=demo")
        console_resp = conn.getresponse()
        console_payload = json.loads(console_resp.read().decode("utf-8"))
        assert console_resp.status == 200
        assert console_payload["query"]["suite"] == "demo"

        conn.request("POST", "/api/console")
        post_resp = conn.getresponse()
        post_payload = json.loads(post_resp.read().decode("utf-8"))
        assert post_resp.status == 405
        assert post_payload["error"]["code"] == "method_not_allowed"

        conn.request(
            "POST",
            "/api/console/actions/execute",
            body=json.dumps({"action_id": "research_activate_bundle"}),
            headers={"Content-Type": "application/json"},
        )
        exec_resp = conn.getresponse()
        exec_payload = json.loads(exec_resp.read().decode("utf-8"))
        assert exec_resp.status == 200
        assert exec_payload["dataset"] == "console_action_execution"
        assert exec_payload["action_id"] == "research_activate_bundle"
        conn.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_default_console_http_handler_routes_to_service_query(monkeypatch) -> None:
    from pm15min.console.http.app import route_console_http_request

    monkeypatch.setattr(
        "pm15min.console.http.app.load_console_query",
        lambda query: {"dataset": "console_data_overview", "section": query.get("section"), "market": query.get("market")},
    )
    response = route_console_http_request(
        method="GET",
        target="/api/console?section=data_overview&market=sol",
    )
    assert response.status_code == 200
    assert response.payload["dataset"] == "console_data_overview"
    assert response.payload["market"] == "sol"


def test_console_http_routes_html_and_static_assets() -> None:
    html = route_console_http_request(method="GET", target="/console?section=bundles")
    css = route_console_http_request(method="GET", target="/static/console.css")
    js = route_console_http_request(method="GET", target="/static/console.js")

    assert html.status_code == 200
    assert html.content_type.startswith("text/html")
    assert "pm15min 控制台" in (html.text or "")
    assert 'data-active-section="bundles"' in (html.text or "")

    assert css.status_code == 200
    assert css.content_type.startswith("text/css")
    assert ".console-shell" in (css.text or "")

    assert js.status_code == 200
    assert js.content_type.startswith("application/javascript")
    assert "fetch(buildUrl(resolved))" in (js.text or "")


def test_console_http_rejects_invalid_json_for_action_execute() -> None:
    response = route_console_http_request(
        method="POST",
        target="/api/console/actions/execute",
        body=b"{bad json",
    )
    assert response.status_code == 400
    assert response.payload["error"]["code"] == "invalid_json"
