from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from pm15min.console.action_runner import execute_console_action
from pm15min.console.service import (
    build_console_action_request,
    load_console_action_catalog,
    submit_console_action_task,
)


ActionExecutor = Callable[..., dict[str, object]]


def load_console_action_catalog_payload(query: Mapping[str, str] | None = None) -> dict[str, object]:
    request_query = {} if query is None else {str(key): value for key, value in query.items()}
    payload = dict(
        load_console_action_catalog(
            for_section=request_query.get("for_section"),
            shell_enabled=_optional_bool(request_query.get("shell_enabled")),
        )
    )
    payload.setdefault("section", "actions")
    return payload


def load_console_action_plan_payload(query: Mapping[str, str]) -> dict[str, object]:
    action_id = _required_string(query, "action_id")
    request = {
        str(key): value
        for key, value in query.items()
        if str(key) not in {"action_id", "section", "for_section", "shell_enabled"}
    }
    payload = dict(
        build_console_action_request(
            action_id=action_id,
            request=request,
        )
    )
    payload.setdefault("section", "actions")
    payload.setdefault("dataset", "console_action_plan")
    return payload


def execute_console_action_payload(
    body: Mapping[str, object],
    *,
    executor: ActionExecutor = execute_console_action,
    task_submitter: ActionExecutor = submit_console_action_task,
) -> dict[str, object]:
    action_id = _required_string(body, "action_id")
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
    payload = dict(
        runner(
            **kwargs,
        )
    )
    payload.setdefault("section", "actions")
    payload.setdefault("execution_mode", execution_mode)
    return payload


def _required_string(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if value is None or not str(value).strip():
        raise ValueError(f"Missing required action field: {key}")
    return str(value).strip()


def _optional_bool(value: object) -> bool | None:
    if value is None or not str(value).strip():
        return None
    token = str(value).strip().lower()
    if token in {"1", "true", "yes", "on"}:
        return True
    if token in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Invalid boolean token: {value!r}")


def _execution_mode(value: object) -> str:
    if value is None or not str(value).strip():
        return "sync"
    token = str(value).strip().lower()
    if token not in {"sync", "async"}:
        raise ValueError(f"Invalid execution_mode: {value!r}")
    return token


__all__ = [
    "execute_console_action_payload",
    "load_console_action_catalog_payload",
    "load_console_action_plan_payload",
]
