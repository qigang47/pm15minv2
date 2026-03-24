from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pm15min.console.service import (
    list_console_tasks,
    load_console_task,
)


def load_console_tasks_payload(query: Mapping[str, str]) -> dict[str, object]:
    task_id = _optional_string(query, "task_id")
    if task_id is not None:
        payload = dict(
            load_console_task(
                task_id=task_id,
                root=_optional_string(query, "root"),
            )
        )
        payload.setdefault("section", "tasks")
        return payload
    payload = dict(
        list_console_tasks(
            action_id=_optional_string(query, "action_id"),
            action_ids=_optional_csv_strings(query.get("action_ids")),
            status=_optional_string(query, "status"),
            status_group=_optional_string(query, "status_group"),
            marker=_optional_string(query, "marker"),
            group_by=_optional_string(query, "group_by"),
            limit=_optional_int(query.get("limit"), default=20),
            root=_optional_string(query, "root"),
        )
    )
    payload.setdefault("section", "tasks")
    return payload


def _optional_string(query: Mapping[str, Any], key: str) -> str | None:
    value = query.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: object, *, default: int) -> int:
    if value is None or not str(value).strip():
        return int(default)
    return int(str(value).strip())


def _optional_csv_strings(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    text = str(value).strip()
    if not text:
        return ()
    return tuple(token.strip() for token in text.split(",") if token.strip())


__all__ = ["load_console_tasks_payload"]
