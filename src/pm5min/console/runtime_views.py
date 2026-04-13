from __future__ import annotations

from collections.abc import Mapping, Sequence


_STATUS_LABELS = {
    "queued": "排队中",
    "running": "运行中",
    "succeeded": "已完成",
    "completed": "已完成",
    "ok": "成功",
    "failed": "失败",
    "error": "错误",
    "unknown": "未知",
}


def build_runtime_state_payload(summary_record: Mapping[str, object]) -> dict[str, object]:
    payload = dict(summary_record)
    recent_tasks = [_task_row_summary(item) for item in _task_rows_payload(payload.get("recent_tasks"))]
    recent_active_tasks = [_task_row_summary(item) for item in _task_rows_payload(payload.get("recent_active_tasks"))]
    recent_terminal_tasks = [_task_row_summary(item) for item in _task_rows_payload(payload.get("recent_terminal_tasks"))]
    recent_failed_tasks = [_task_row_summary(item) for item in _task_rows_payload(payload.get("recent_failed_tasks"))]
    latest_markers = _mapping_or_none(payload.get("latest_markers")) or {}
    payload["recent_tasks"] = recent_tasks
    payload["recent_active_tasks"] = recent_active_tasks
    payload["recent_terminal_tasks"] = recent_terminal_tasks
    payload["recent_failed_tasks"] = recent_failed_tasks
    payload["recent_task_briefs"] = _task_briefs(recent_tasks)
    payload["recent_active_task_briefs"] = _task_briefs(recent_active_tasks)
    payload["recent_terminal_task_briefs"] = _task_briefs(recent_terminal_tasks)
    payload["recent_failed_task_briefs"] = _task_briefs(recent_failed_tasks)
    payload["latest_task_briefs"] = {
        "latest": _task_brief(_mapping_or_none(latest_markers.get("latest"))) or _first_or_none(payload["recent_task_briefs"]),
        "active": _task_brief(_mapping_or_none(latest_markers.get("active"))) or _first_or_none(payload["recent_active_task_briefs"]),
        "terminal": _task_brief(_mapping_or_none(latest_markers.get("terminal")))
        or _first_or_none(payload["recent_terminal_task_briefs"]),
        "failed": _task_brief(_mapping_or_none(latest_markers.get("failed"))) or _first_or_none(payload["recent_failed_task_briefs"]),
    }
    payload["runtime_board"] = _runtime_board_payload(payload)
    payload["operator_summary"] = {
        "has_active_tasks": bool(payload["recent_active_task_briefs"]),
        "has_failed_tasks": bool(payload["recent_failed_task_briefs"]),
        "active_task_count": len(payload["recent_active_task_briefs"]),
        "failed_task_count": len(payload["recent_failed_task_briefs"]),
        "invalid_task_file_count": _runtime_history_scan_count(payload.get("history_scan")),
        "invalid_task_files": _runtime_invalid_task_files(payload.get("history_scan")),
        "history_retention": _mapping_or_none(payload.get("history_retention")) or {},
        "summary_source": _string_value(payload.get("summary_source")),
        "warnings": _runtime_board_warnings(payload),
        "latest_headline": _brief_field(payload["latest_task_briefs"]["latest"], "headline"),
        "latest_failed_summary": _brief_field(payload["latest_task_briefs"]["failed"], "summary"),
    }
    return payload


def build_runtime_history_payload(history_record: Mapping[str, object]) -> dict[str, object]:
    payload = dict(history_record)
    rows = [_task_row_summary(item) for item in _task_rows_payload(payload.get("rows"))]
    groups_payload = _mapping_or_none(payload.get("groups")) or {}
    payload["rows"] = rows
    payload["task_briefs"] = _task_briefs(rows)
    payload["group_task_briefs"] = {
        group: _task_briefs(_task_rows_payload((_mapping_or_none(groups_payload.get(group)) or {}).get("rows")))
        for group in ("active", "terminal", "failed")
    }
    payload["operator_summary"] = {
        "task_count": _optional_int(payload.get("task_count"), default=len(rows)),
        "row_count": _optional_int(payload.get("row_count"), default=len(rows)),
        "invalid_task_file_count": _runtime_history_scan_count(payload.get("history_scan")),
        "invalid_task_files": _runtime_invalid_task_files(payload.get("history_scan")),
        "retention": _mapping_or_none(payload.get("retention")) or {},
        "history_source": _string_value(payload.get("history_source")),
        "updated_at": _string_value(payload.get("updated_at")),
        "latest_headline": _brief_field(_first_or_none(payload["task_briefs"]), "headline"),
    }
    return payload


def build_task_list_payload(
    *,
    rows_source: object,
    runtime_state: Mapping[str, object],
    action_id: str | None = None,
    action_ids: Sequence[str] | None = None,
    status: str | None = None,
    status_group: str | None = None,
    marker: str | None = None,
    group_by: str | None = None,
    limit: int = 20,
) -> dict[str, object]:
    normalized_action_ids = _task_action_filters(action_id=action_id, action_ids=action_ids)
    normalized_marker = _normalized_task_history_marker(marker)
    normalized_group_by = _normalized_task_history_group_by(group_by)
    rows = [_task_row_summary(item) for item in _task_rows_payload(rows_source)]
    task_briefs = _task_briefs(rows)
    history_markers = _task_history_markers(rows)
    history_groups = _task_history_groups(rows)
    return {
        "domain": "console",
        "dataset": "console_task_list",
        "object_type": "console_task_list",
        "action_id_filter": normalized_action_ids[0] if len(normalized_action_ids) == 1 else None,
        "action_ids_filter": list(normalized_action_ids),
        "status_filter": status,
        "status_group_filter": status_group,
        "marker_filter": normalized_marker,
        "group_by": normalized_group_by,
        "filters": {
            "action_id": normalized_action_ids[0] if len(normalized_action_ids) == 1 else None,
            "action_ids": list(normalized_action_ids),
            "status": status,
            "status_group": status_group,
            "marker": normalized_marker,
            "group_by": normalized_group_by,
            "limit": int(limit),
        },
        "row_count": len(rows),
        "status_counts": _task_status_counts(rows),
        "status_group_counts": _task_status_group_counts(rows),
        "action_counts": _task_action_counts(rows),
        "marker_options": ["latest", "active", "terminal", "failed"],
        "group_by_options": ["action_id", "status", "status_group"],
        "history_markers": history_markers,
        "selected_marker": history_markers.get(normalized_marker) if normalized_marker is not None else None,
        "history_groups": history_groups,
        "selected_group_rows": history_groups.get(normalized_group_by) if normalized_group_by is not None else None,
        "history_scan": runtime_state.get("history_scan"),
        "summary_recovery": runtime_state.get("summary_recovery"),
        "history_retention": runtime_state.get("history_retention"),
        "operator_summary": runtime_state.get("operator_summary"),
        "runtime_board": runtime_state.get("runtime_board"),
        "task_briefs": task_briefs,
        "latest_task_brief": task_briefs[0] if task_briefs else None,
        "rows": rows,
    }


def build_task_detail_payload(record: Mapping[str, object]) -> dict[str, object]:
    return _task_detail_payload(dict(record))


def _task_action_filters(
    *,
    action_id: str | None = None,
    action_ids: Sequence[str] | None = None,
) -> tuple[str, ...]:
    values: list[str] = []
    primary = _string_value(action_id)
    if primary is not None:
        values.append(primary)
    if action_ids is not None:
        for value in action_ids:
            token = _string_value(value)
            if token is not None and token not in values:
                values.append(token)
    return tuple(values)


def _normalized_task_history_marker(value: object) -> str | None:
    token = _string_value(value)
    if token is None:
        return None
    if token not in {"latest", "active", "terminal", "failed"}:
        raise ValueError(f"Unsupported console task marker: {value!r}")
    return token


def _normalized_task_history_group_by(value: object) -> str | None:
    token = _string_value(value)
    if token is None:
        return None
    if token not in {"action_id", "status", "status_group"}:
        raise ValueError(f"Unsupported console task group_by: {value!r}")
    return token


def _task_rows_payload(source: object) -> list[dict[str, object]]:
    if isinstance(source, Mapping):
        rows = source.get("rows")
        if isinstance(rows, list):
            return [dict(item) for item in rows if isinstance(item, Mapping)]
        return []
    if isinstance(source, list):
        return [dict(item) for item in source if isinstance(item, Mapping)]
    return []


def _task_detail_payload(record: Mapping[str, object]) -> dict[str, object]:
    payload = _task_row_summary(record)
    result_paths = list(payload.get("result_paths") or [])
    primary_output = result_paths[0] if result_paths else {"label": None, "path": None}
    detail_payload = {
        "domain": "console",
        "dataset": "console_task",
        "object_type": "console_task",
        **payload,
        "result_path_briefs": [{"label": item.get("label"), "path": item.get("path")} for item in result_paths],
        "primary_output_label": primary_output.get("label"),
        "primary_output_path": primary_output.get("path"),
        "task_brief": _task_brief(payload),
    }
    return detail_payload


def _task_row_summary(record: Mapping[str, object]) -> dict[str, object]:
    payload = {str(key): value for key, value in record.items()}
    status = _string_value(payload.get("status"))
    request = _mapping_or_none(payload.get("request")) or {}
    progress = _mapping_or_none(payload.get("progress")) or {}
    result_paths = _task_result_paths(payload.get("result"))
    primary_output = result_paths[0] if result_paths else {"label": None, "path": None}
    row_payload = {
        **payload,
        "object_type": "console_task",
        "status_label": _task_status_label(payload.get("status_label") or status),
        "status_group": _string_value(payload.get("status_group")) or _task_status_group(status),
        "subject_summary": _string_value(payload.get("subject_summary")) or _task_subject_summary(payload),
        "progress_summary": _string_value(payload.get("progress_summary")) or _task_progress_summary(progress),
        "request_summary": _task_request_summary(request),
        "result_summary": _string_value(payload.get("result_summary")) or _task_result_summary(payload.get("result")),
        "error_summary": _string_value(payload.get("error_summary")) or _task_error_summary(payload.get("error")),
        "primary_output_label": payload.get("primary_output_label") or primary_output.get("label"),
        "primary_output_path": payload.get("primary_output_path") or primary_output.get("path"),
        "result_paths": result_paths,
        "linked_objects": list(payload.get("linked_objects") or []),
        "is_terminal": _task_is_terminal(status),
        "action_context": {
            "task_id": payload.get("task_id"),
            "action_id": payload.get("action_id"),
            "status": status,
        },
    }
    row_payload["task_brief"] = _task_brief(row_payload)
    return row_payload


def _task_status_label(status: object) -> str:
    token = _string_value(status) or "unknown"
    return _STATUS_LABELS.get(token, token.replace("_", " ").title())


def _task_status_group(status: object) -> str:
    token = _string_value(status) or ""
    if token in {"queued", "running"}:
        return "active"
    if token in {"failed", "error"}:
        return "failed"
    if token in {"succeeded", "completed", "ok"}:
        return "terminal"
    return "unknown"


def _task_is_terminal(status: object) -> bool:
    return _task_status_group(status) in {"terminal", "failed"}


def _task_progress_summary(progress: Mapping[str, object] | None) -> str | None:
    if not isinstance(progress, Mapping):
        return None
    parts = [
        part
        for part in (
            _string_value(progress.get("summary")),
            _string_value(progress.get("current_stage")),
        )
        if part
    ]
    pct = progress.get("progress_pct")
    if pct not in (None, ""):
        parts.append(f"{int(pct)}%")
    return " · ".join(parts) if parts else None


def _task_request_summary(request: Mapping[str, object]) -> dict[str, object]:
    summary: dict[str, object] = {}
    for key in (
        "market",
        "cycle",
        "surface",
        "profile",
        "target",
        "model_family",
        "bundle_label",
        "run_label",
        "suite_name",
        "suite",
        "spec_name",
        "spec",
        "sync_command",
        "build_command",
    ):
        value = request.get(key)
        if value in (None, "", []):
            continue
        summary[key] = value
    return summary


def _task_subject_summary(record: Mapping[str, object]) -> str | None:
    action_id = _string_value(record.get("action_id"))
    request_summary = _mapping_or_none(record.get("request_summary")) or _task_request_summary(
        _mapping_or_none(record.get("request")) or {}
    )
    if action_id is None:
        return None
    parts = [action_id]
    for key in ("market", "profile", "target", "bundle_label", "run_label", "suite_name", "suite", "spec_name", "spec"):
        value = _string_value(request_summary.get(key))
        if value:
            parts.append(value)
    return " · ".join(parts)


def _task_result_summary(result: object) -> str | None:
    if result is None:
        return None
    if isinstance(result, Mapping):
        for key in ("summary", "message", "status"):
            value = _string_value(result.get(key))
            if value:
                return value
        paths = _task_result_paths(result)
        if paths:
            return _string_value(paths[0].get("path"))
    if isinstance(result, Sequence) and not isinstance(result, (str, bytes, bytearray)):
        return f"{len(result)} result item(s)"
    return _string_value(result)


def _task_error_summary(error: object) -> str | None:
    if error is None:
        return None
    if isinstance(error, Mapping):
        for key in ("message", "error", "detail", "type"):
            value = _string_value(error.get(key))
            if value:
                return value
    return _string_value(error)


def _task_result_paths(result: object) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    _collect_result_paths(result, rows, label=None)
    deduped: list[dict[str, object]] = []
    seen: set[tuple[str | None, str]] = set()
    for row in rows:
        path = _string_value(row.get("path"))
        if path is None:
            continue
        key = (_string_value(row.get("label")), path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append({"label": key[0], "path": path})
    return deduped


def _collect_result_paths(value: object, rows: list[dict[str, object]], *, label: str | None) -> None:
    if isinstance(value, Mapping):
        path_value = _string_value(value.get("path")) or _string_value(value.get("root"))
        if path_value is not None:
            rows.append({"label": label, "path": path_value})
        for key, item in value.items():
            next_label = str(key) if label is None else f"{label}.{key}"
            _collect_result_paths(item, rows, label=next_label)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            _collect_result_paths(item, rows, label=label)


def _task_status_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = _string_value(row.get("status")) or "unknown"
        counts[status] = counts.get(status, 0) + 1
    return counts


def _task_status_group_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counts = {"active": 0, "terminal": 0, "failed": 0}
    for row in rows:
        group = _task_status_group(row.get("status"))
        if group in counts:
            counts[group] += 1
    return counts


def _task_action_counts(rows: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        action_id = _string_value(row.get("action_id")) or "unknown"
        counts[action_id] = counts.get(action_id, 0) + 1
    return counts


def _task_history_markers(rows: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    return {
        "latest": _task_marker(_first_row(rows)),
        "active": _first_task_marker(rows, status_group="active"),
        "terminal": _first_task_marker(rows, status_group="terminal"),
        "failed": _first_task_marker(rows, status_group="failed"),
    }


def _task_history_groups(rows: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    return {
        "action_id": _task_groups_by_action(rows),
        "status": _task_groups_by_status(rows),
        "status_group": _task_groups_by_status_group(rows),
    }


def _task_groups_by_action(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        action_id = _string_value(row.get("action_id")) or "unknown"
        grouped.setdefault(action_id, []).append(row)
    result = [
        {
            "group": action_id,
            "action_id": action_id,
            "count": len(members),
            "status_counts": _task_status_counts(members),
            "status_group_counts": _task_status_group_counts(members),
            "latest_marker": _task_marker(members[0]),
        }
        for action_id, members in grouped.items()
    ]
    result.sort(key=lambda item: (_marker_updated_at(item.get("latest_marker")), str(item.get("action_id") or "")), reverse=True)
    return result


def _task_groups_by_status(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        status = _string_value(row.get("status")) or "unknown"
        grouped.setdefault(status, []).append(row)
    result = [
        {
            "group": status,
            "status": status,
            "count": len(members),
            "latest_marker": _task_marker(members[0]),
        }
        for status, members in grouped.items()
    ]
    result.sort(key=lambda item: (_marker_updated_at(item.get("latest_marker")), str(item.get("status") or "")), reverse=True)
    return result


def _task_groups_by_status_group(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    markers = _task_history_markers(rows)
    counts = _task_status_group_counts(rows)
    return [
        {
            "group": group,
            "status_group": group,
            "count": int(counts.get(group, 0)),
            "latest_marker": markers.get(group) or {},
        }
        for group in ("active", "terminal", "failed")
    ]


def _first_task_marker(rows: list[dict[str, object]], *, status_group: str) -> dict[str, object]:
    for row in rows:
        if _task_status_group(row.get("status")) == status_group:
            return _task_marker(row)
    return {}


def _task_marker(record: Mapping[str, object] | None) -> dict[str, object]:
    if not isinstance(record, Mapping):
        return {}
    return {
        "task_id": record.get("task_id"),
        "action_id": record.get("action_id"),
        "status": record.get("status"),
        "status_label": record.get("status_label"),
        "status_group": record.get("status_group"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "subject_summary": record.get("subject_summary"),
        "progress_summary": record.get("progress_summary"),
        "result_summary": record.get("result_summary"),
        "error_summary": record.get("error_summary"),
        "primary_output_path": record.get("primary_output_path"),
    }


def _task_briefs(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    briefs: list[dict[str, object]] = []
    for row in rows:
        brief = _task_brief(row)
        if brief is not None:
            briefs.append(brief)
    return briefs


def _task_brief(record: Mapping[str, object] | None) -> dict[str, object] | None:
    if not isinstance(record, Mapping):
        return None
    task_id = _string_value(record.get("task_id"))
    action_id = _string_value(record.get("action_id"))
    status = _string_value(record.get("status"))
    status_group = _string_value(record.get("status_group")) or _task_status_group(status)
    status_label = _task_status_label(record.get("status_label") or status)
    subject_summary = _string_value(record.get("subject_summary")) or _task_subject_summary(record)
    progress_summary = _string_value(record.get("progress_summary"))
    result_summary = _string_value(record.get("result_summary"))
    error_summary = _string_value(record.get("error_summary"))
    primary_output_path = _string_value(record.get("primary_output_path"))
    if all(value is None for value in (task_id, action_id, status, subject_summary, progress_summary, result_summary, error_summary)):
        return None
    summary = error_summary or progress_summary or result_summary
    headline = " · ".join(part for part in (status_label, subject_summary, task_id) if part)
    return {
        "task_id": task_id,
        "action_id": action_id,
        "status": status,
        "status_group": status_group,
        "headline": headline or None,
        "summary": summary,
        "updated_at": _string_value(record.get("updated_at")),
        "primary_output_path": primary_output_path,
    }


def _runtime_board_payload(summary: Mapping[str, object]) -> dict[str, object]:
    history_groups = _mapping_or_none(summary.get("history_groups")) or {}
    latest_task_briefs = _mapping_or_none(summary.get("latest_task_briefs")) or {}
    return {
        "summary_source": _string_value(summary.get("summary_source")),
        "recovery": _mapping_or_none(summary.get("summary_recovery")) or {},
        "history_scan": _mapping_or_none(summary.get("history_scan")) or {},
        "invalid_task_files": _runtime_invalid_task_files(summary.get("history_scan")),
        "retention": _mapping_or_none(summary.get("history_retention")) or {},
        "latest": _mapping_or_none(latest_task_briefs.get("latest")),
        "active": _mapping_or_none(latest_task_briefs.get("active")),
        "terminal": _mapping_or_none(latest_task_briefs.get("terminal")),
        "failed": _mapping_or_none(latest_task_briefs.get("failed")),
        "status_groups": list(history_groups.get("status_group") or []),
        "action_groups": list(history_groups.get("action_id") or []),
        "warnings": _runtime_board_warnings(summary),
    }


def _runtime_board_warnings(summary: Mapping[str, object]) -> list[str]:
    warnings: list[str] = []
    status_group_counts = _mapping_or_none(summary.get("status_group_counts")) or {}
    if int(status_group_counts.get("failed", 0)) > 0:
        warnings.append("failed_tasks")
    if _runtime_history_scan_count(summary.get("history_scan")) > 0:
        warnings.append("invalid_task_files")
    return warnings


def _runtime_history_scan_count(history_scan: object) -> int:
    if not isinstance(history_scan, Mapping):
        return 0
    return _optional_int(history_scan.get("invalid_task_file_count"), default=0)


def _runtime_invalid_task_files(history_scan: object) -> list[dict[str, object]]:
    if not isinstance(history_scan, Mapping):
        return []
    invalid = history_scan.get("invalid_task_files")
    if not isinstance(invalid, list):
        return []
    return [dict(item) for item in invalid if isinstance(item, Mapping)]


def _marker_updated_at(marker: object) -> str:
    if not isinstance(marker, Mapping):
        return ""
    return _string_value(marker.get("updated_at")) or ""


def _brief_field(brief: object, key: str) -> str | None:
    if not isinstance(brief, Mapping):
        return None
    return _string_value(brief.get(key))


def _mapping_or_none(value: object) -> dict[str, object] | None:
    return {str(key): item for key, item in value.items()} if isinstance(value, Mapping) else None


def _string_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: object, *, default: int) -> int:
    if value in (None, ""):
        return int(default)
    return int(value)


def _first_row(rows: list[dict[str, object]]) -> dict[str, object] | None:
    return rows[0] if rows else None


def _first_or_none(rows: list[dict[str, object]]) -> dict[str, object] | None:
    return rows[0] if rows else None


__all__ = [
    "build_runtime_history_payload",
    "build_runtime_state_payload",
    "build_task_detail_payload",
    "build_task_list_payload",
]
