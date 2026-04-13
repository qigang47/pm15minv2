from __future__ import annotations

from collections.abc import Callable, Mapping
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import asdict, dataclass
from io import StringIO
import json

from pm5min.console.actions import build_console_action_request


@dataclass(frozen=True)
class ConsoleActionExecutionResult:
    action_id: str
    normalized_request: dict[str, object]
    pm15min_args: tuple[str, ...]
    command_preview: str
    return_code: int
    succeeded: bool
    stdout: str
    stderr: str
    parsed_stdout: object | None
    execution_summary: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["pm15min_args"] = list(self.pm15min_args)
        payload["dataset"] = "console_action_execution"
        payload["status"] = "ok" if self.succeeded else "error"
        return payload


def execute_console_action(
    *,
    action_id: str,
    request: Mapping[str, object] | None = None,
    main_fn: Callable[[list[str] | None], int] | None = None,
) -> dict[str, object]:
    plan = build_console_action_request(action_id, request)
    resolved_main_fn = main_fn or _default_main_fn
    stdout_buffer = StringIO()
    stderr_buffer = StringIO()
    return_code = 1
    try:
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            return_code = int(resolved_main_fn(list(plan["pm15min_args"])))
    except Exception as exc:
        stderr_buffer.write(f"{exc.__class__.__name__}: {exc}")
        return_code = 1
    result = ConsoleActionExecutionResult(
        action_id=str(plan["action_id"]),
        normalized_request=dict(plan["normalized_request"]),
        pm15min_args=tuple(str(item) for item in plan["pm15min_args"]),
        command_preview=str(plan["command_preview"]),
        return_code=int(return_code),
        succeeded=int(return_code) == 0,
        stdout=stdout_buffer.getvalue(),
        stderr=stderr_buffer.getvalue(),
        parsed_stdout=_parse_stdout_payload(stdout_buffer.getvalue()),
        execution_summary=_build_execution_summary(
            action_id=str(plan["action_id"]),
            return_code=int(return_code),
            stdout=stdout_buffer.getvalue(),
            stderr=stderr_buffer.getvalue(),
        ),
    )
    return result.to_dict()


def _default_main_fn(argv: list[str] | None) -> int:
    from pm5min.cli import main as pm5min_main

    return int(pm5min_main(argv))


def _parse_stdout_payload(stdout: str) -> object | None:
    text = str(stdout or "").strip()
    if not text:
        return None
    lines = [line for line in text.splitlines() if line.strip()]
    candidates = [text]
    if lines:
        candidates.append(lines[-1])
    for candidate in candidates:
        try:
            return json.loads(candidate)
        except Exception:
            continue
    return None


def _build_execution_summary(
    *,
    action_id: str,
    return_code: int,
    stdout: str,
    stderr: str,
) -> dict[str, object]:
    parsed_stdout = _parse_stdout_payload(stdout)
    stdout_lines = [line for line in str(stdout or "").splitlines() if line.strip()]
    stderr_lines = [line for line in str(stderr or "").splitlines() if line.strip()]
    return {
        "action_id": str(action_id),
        "return_code": int(return_code),
        "status": "ok" if int(return_code) == 0 else "error",
        "stdout_line_count": len(stdout_lines),
        "stderr_line_count": len(stderr_lines),
        "has_parsed_stdout": parsed_stdout is not None,
        "parsed_stdout_type": None if parsed_stdout is None else type(parsed_stdout).__name__,
        "last_stdout_line": None if not stdout_lines else stdout_lines[-1],
        "last_stderr_line": None if not stderr_lines else stderr_lines[-1],
    }


__all__ = ["execute_console_action"]
