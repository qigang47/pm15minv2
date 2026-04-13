from __future__ import annotations

from collections.abc import Mapping
import importlib
from pathlib import Path


def _load_console_action_runner_module() -> object:
    return importlib.import_module("pm5min.console.action_runner")


def _load_console_tasks_module() -> object:
    return importlib.import_module("pm15min.console.tasks")


def _load_console_actions_module() -> object:
    return importlib.import_module("pm5min.console.actions")


def _load_console_http_module() -> object:
    return importlib.import_module("pm5min.console.http")


def execute_console_action(
    *,
    action_id: str,
    request: Mapping[str, object] | None = None,
) -> dict[str, object]:
    module = _load_console_action_runner_module()
    return getattr(module, "execute_console_action")(action_id=action_id, request=request)


def submit_console_action_task(
    *,
    action_id: str,
    request: Mapping[str, object] | None = None,
    root: str | Path | None = None,
) -> dict[str, object]:
    tasks_module = _load_console_tasks_module()
    actions_module = _load_console_actions_module()
    return getattr(tasks_module, "submit_console_task")(
        action_id=action_id,
        request=request,
        root=root,
        planner=getattr(actions_module, "build_console_action_request"),
        executor=lambda context: execute_console_action(
            action_id=context.action_id,
            request=context.request,
        ),
    )


def serve_console_http(*, host: str, port: int, poll_interval: float) -> None:
    module = _load_console_http_module()
    return getattr(module, "serve_console_http")(host=host, port=port, poll_interval=poll_interval)


__all__ = [
    "execute_console_action",
    "serve_console_http",
    "submit_console_action_task",
]
