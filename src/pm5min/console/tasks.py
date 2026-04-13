from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import tempfile
from typing import Any

from pm5min.core.layout import rewrite_root


TASK_STATUS_QUEUED = "queued"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_SUCCEEDED = "succeeded"
TASK_STATUS_FAILED = "failed"
CONSOLE_RUNTIME_SUMMARY_RECENT_LIMIT = 12
CONSOLE_RUNTIME_SUMMARY_GROUP_LIMIT = 6
CONSOLE_RUNTIME_HISTORY_LIMIT = 50
CONSOLE_RUNTIME_HISTORY_GROUP_LIMIT = 12


def default_console_tasks_root() -> Path:
    return rewrite_root() / "var" / "console" / "tasks"


def default_console_runtime_state_root() -> Path:
    return rewrite_root() / "var" / "console" / "state"


@dataclass(frozen=True)
class ConsoleTaskProgress:
    summary: str | None = None
    current: int | None = None
    total: int | None = None
    current_stage: str | None = None
    progress_pct: int | None = None
    heartbeat: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, object] | None) -> "ConsoleTaskProgress":
        data = payload or {}
        return cls(
            summary=_optional_text(data.get("summary")),
            current=_optional_int(data.get("current")),
            total=_optional_int(data.get("total")),
            current_stage=_optional_text(data.get("current_stage")),
            progress_pct=_optional_progress_pct(data.get("progress_pct")),
            heartbeat=_optional_text(data.get("heartbeat")),
        )


@dataclass(frozen=True)
class ConsoleTaskRecord:
    task_id: str
    action_id: str
    status: str
    created_at: str
    updated_at: str
    started_at: str | None
    finished_at: str | None
    request: dict[str, object]
    command_preview: str
    result: object | None = None
    error: object | None = None
    progress: ConsoleTaskProgress = ConsoleTaskProgress()

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "action_id": self.action_id,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "request": _mapping_payload(self.request),
            "command_preview": self.command_preview,
            "result": _json_safe(self.result),
            "error": _json_safe(self.error),
            "progress": self.progress.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ConsoleTaskRecord":
        status = str(payload["status"])
        updated_at = str(payload["updated_at"])
        return cls(
            task_id=str(payload["task_id"]),
            action_id=str(payload["action_id"]),
            status=status,
            created_at=str(payload["created_at"]),
            updated_at=updated_at,
            started_at=_optional_text(payload.get("started_at")),
            finished_at=_optional_text(payload.get("finished_at")),
            request=_mapping_payload(_mapping_or_none(payload.get("request"))),
            command_preview=str(payload.get("command_preview") or ""),
            result=_json_safe(payload.get("result")),
            error=_json_safe(payload.get("error")),
            progress=_normalize_loaded_progress(
                ConsoleTaskProgress.from_dict(_mapping_or_none(payload.get("progress"))),
                status=status,
                updated_at=updated_at,
            ),
        )


@dataclass(frozen=True)
class ConsoleTaskScanIssue:
    path: str
    error_type: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class ConsoleTaskHistoryScan:
    records: tuple[ConsoleTaskRecord, ...] = ()
    task_file_count: int = 0
    invalid_files: tuple[ConsoleTaskScanIssue, ...] = ()
    latest_task_mtime: float | None = None

    def invalid_file_payloads(self, *, limit: int = 5) -> list[dict[str, str]]:
        rows = [issue.to_dict() for issue in self.invalid_files]
        return rows[: max(int(limit), 0)]


def get_console_task(
    task_id: str,
    *,
    root: str | Path | None = None,
) -> dict[str, object] | None:
    record = _read_record(_resolve_tasks_root(root), str(task_id))
    return None if record is None else record.to_dict()


def load_console_task(
    *,
    task_id: str,
    root: str | Path | None = None,
) -> dict[str, object]:
    payload = get_console_task(task_id, root=root)
    if payload is None:
        raise FileNotFoundError(f"Unknown console task_id {task_id!r}")
    return payload


def list_console_tasks(
    *,
    root: str | Path | None = None,
    action_id: str | None = None,
    action_ids: Sequence[str] | None = None,
    status: str | None = None,
    status_group: str | None = None,
    limit: int | None = None,
) -> list[dict[str, object]]:
    if limit is not None and int(limit) < 0:
        raise ValueError("limit must be >= 0")
    filters = _normalized_action_filters(action_id=action_id, action_ids=action_ids)
    resolved_status_group = _normalized_status_group(status_group)
    records = list(_scan_task_history(_resolve_tasks_root(root)).records)
    rows: list[ConsoleTaskRecord] = []
    for record in records:
        if filters is not None and record.action_id not in filters:
            continue
        if status is not None and record.status != str(status):
            continue
        if resolved_status_group is not None and not _status_matches_group(record.status, resolved_status_group):
            continue
        rows.append(record)
    rows.sort(key=lambda item: (item.created_at, item.task_id), reverse=True)
    if limit is not None:
        rows = rows[: int(limit)]
    return [record.to_dict() for record in rows]


def load_console_runtime_summary(
    *,
    root: str | Path | None = None,
) -> dict[str, object]:
    tasks_root = _resolve_tasks_root(root)
    summary_path = _runtime_summary_path(tasks_root)
    scan = _scan_task_history(tasks_root)
    persisted = _load_runtime_payload(summary_path, dataset="console_runtime_summary")
    if persisted is not None and not _runtime_state_needs_rebuild(
        persisted,
        state_path=summary_path,
        scan=scan,
    ):
        payload = dict(persisted)
        payload["summary_source"] = "persisted"
        payload["summary_recovery"] = _runtime_recovery_payload(
            source="persisted",
            recovered=False,
            path=summary_path,
            path_key="summary_path",
            scan=scan,
        )
        return payload
    payload = _build_runtime_summary_payload(scan.records, tasks_root=tasks_root, scan=scan)
    if scan.task_file_count or scan.invalid_files:
        _write_json_atomically(summary_path, payload)
        _write_json_atomically(
            _runtime_history_path(tasks_root),
            _build_runtime_history_payload(scan.records, tasks_root=tasks_root, scan=scan),
        )
        payload["summary_source"] = "recovered_from_tasks"
    else:
        payload["summary_source"] = "empty"
    payload["summary_recovery"] = _runtime_recovery_payload(
        source=str(payload["summary_source"]),
        recovered=payload["summary_source"] == "recovered_from_tasks",
        path=summary_path,
        path_key="summary_path",
        scan=scan,
    )
    return payload


def load_console_runtime_history(
    *,
    root: str | Path | None = None,
) -> dict[str, object]:
    tasks_root = _resolve_tasks_root(root)
    history_path = _runtime_history_path(tasks_root)
    scan = _scan_task_history(tasks_root)
    persisted = _load_runtime_payload(history_path, dataset="console_runtime_history")
    if persisted is not None and not _runtime_state_needs_rebuild(
        persisted,
        state_path=history_path,
        scan=scan,
    ):
        payload = dict(persisted)
        payload["history_source"] = "persisted"
        payload["history_recovery"] = _runtime_recovery_payload(
            source="persisted",
            recovered=False,
            path=history_path,
            path_key="history_path",
            scan=scan,
        )
        return payload
    payload = _build_runtime_history_payload(scan.records, tasks_root=tasks_root, scan=scan)
    if scan.task_file_count or scan.invalid_files:
        _write_json_atomically(history_path, payload)
        _write_json_atomically(
            _runtime_summary_path(tasks_root),
            _build_runtime_summary_payload(scan.records, tasks_root=tasks_root, scan=scan),
        )
        payload["history_source"] = "recovered_from_tasks"
    else:
        payload["history_source"] = "empty"
    payload["history_recovery"] = _runtime_recovery_payload(
        source=str(payload["history_source"]),
        recovered=payload["history_source"] == "recovered_from_tasks",
        path=history_path,
        path_key="history_path",
        scan=scan,
    )
    return payload


def _resolve_tasks_root(root: str | Path | None) -> Path:
    if root is None:
        return default_console_tasks_root()
    path = Path(root)
    if path.name == "tasks":
        return path
    return path / "var" / "console" / "tasks"


def _runtime_state_root(tasks_root: Path) -> Path:
    if tasks_root.name == "tasks":
        return tasks_root.parent / "state"
    return tasks_root / "state"


def _runtime_summary_path(tasks_root: Path) -> Path:
    return _runtime_state_root(tasks_root) / "runtime_summary.json"


def _runtime_history_path(tasks_root: Path) -> Path:
    return _runtime_state_root(tasks_root) / "runtime_history.json"


def _task_record_path(tasks_root: Path, task_id: str) -> Path:
    return tasks_root / f"{task_id}.json"


def _read_record(tasks_root: Path, task_id: str) -> ConsoleTaskRecord | None:
    path = _task_record_path(tasks_root, task_id)
    record, _ = _load_task_record_path(path)
    return record


def _scan_task_history(tasks_root: Path) -> ConsoleTaskHistoryScan:
    if not tasks_root.exists():
        return ConsoleTaskHistoryScan()
    records: list[ConsoleTaskRecord] = []
    invalid_files: list[ConsoleTaskScanIssue] = []
    task_file_count = 0
    latest_task_mtime: float | None = None
    for path in sorted(tasks_root.glob("*.json")):
        if not path.is_file():
            continue
        task_file_count += 1
        try:
            mtime = path.stat().st_mtime
            latest_task_mtime = mtime if latest_task_mtime is None else max(latest_task_mtime, mtime)
        except OSError:
            pass
        record, issue = _load_task_record_path(path)
        if record is not None:
            records.append(record)
        elif issue is not None:
            invalid_files.append(issue)
    records.sort(key=lambda item: (item.created_at, item.task_id), reverse=True)
    return ConsoleTaskHistoryScan(
        records=tuple(records),
        task_file_count=task_file_count,
        invalid_files=tuple(invalid_files),
        latest_task_mtime=latest_task_mtime,
    )


def _load_task_record_path(path: Path) -> tuple[ConsoleTaskRecord | None, ConsoleTaskScanIssue | None]:
    if not path.exists() or not path.is_file():
        return None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("Task payload must be a JSON object.")
        return ConsoleTaskRecord.from_dict(payload), None
    except Exception as exc:
        return None, ConsoleTaskScanIssue(
            path=str(path),
            error_type=exc.__class__.__name__,
            message=str(exc),
        )


def _load_runtime_payload(path: Path, *, dataset: str) -> dict[str, object] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, Mapping):
        return None
    resolved = _mapping_payload(payload)
    if _optional_text(resolved.get("dataset")) != dataset:
        return None
    return resolved


def _runtime_state_needs_rebuild(
    payload: Mapping[str, object],
    *,
    state_path: Path,
    scan: ConsoleTaskHistoryScan,
) -> bool:
    if scan.invalid_files:
        return True
    if (_optional_int(payload.get("task_count")) or 0) != len(scan.records):
        return True
    if scan.latest_task_mtime is None:
        return False
    try:
        state_mtime = state_path.stat().st_mtime
    except OSError:
        return True
    return state_mtime + 1e-9 < scan.latest_task_mtime


def _runtime_recovery_payload(
    *,
    source: str,
    recovered: bool,
    path: Path,
    path_key: str,
    scan: ConsoleTaskHistoryScan,
) -> dict[str, object]:
    return {
        "source": source,
        "recovered": bool(recovered),
        path_key: str(path),
        f"{path_key}_exists": path.exists(),
        "invalid_task_file_count": len(scan.invalid_files),
    }


def _normalized_action_filters(
    *,
    action_id: str | None = None,
    action_ids: Sequence[str] | None = None,
) -> set[str] | None:
    values: list[str] = []
    primary = _optional_text(action_id)
    if primary is not None:
        values.append(primary)
    if action_ids is not None:
        values.extend(str(item).strip() for item in action_ids if str(item).strip())
    if not values:
        return None
    return {value for value in values if value}


def _normalized_status_group(value: str | None) -> str | None:
    token = _optional_text(value)
    if token is None:
        return None
    token = token.lower()
    if token not in {"active", "terminal", "failed"}:
        raise ValueError("Unsupported status_group. Expected one of: active, terminal, failed")
    return token


def _status_matches_group(status: str, status_group: str) -> bool:
    token = str(status or "").strip().lower()
    if status_group == "active":
        return token in {TASK_STATUS_QUEUED, TASK_STATUS_RUNNING}
    if status_group == "terminal":
        return token in {TASK_STATUS_SUCCEEDED, TASK_STATUS_FAILED, "ok", "error"}
    if status_group == "failed":
        return token in {TASK_STATUS_FAILED, "error"}
    return False


def _build_runtime_summary_payload(
    records: Sequence[ConsoleTaskRecord],
    *,
    tasks_root: Path,
    scan: ConsoleTaskHistoryScan,
) -> dict[str, object]:
    ordered = sorted(records, key=lambda item: (item.created_at, item.task_id), reverse=True)
    status_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    for record in ordered:
        status_counts[record.status] = status_counts.get(record.status, 0) + 1
        action_counts[record.action_id] = action_counts.get(record.action_id, 0) + 1
    all_rows = [_runtime_recent_task_row(record, tasks_root=tasks_root) for record in ordered]
    recent_tasks = all_rows[:CONSOLE_RUNTIME_SUMMARY_RECENT_LIMIT]
    recent_active_tasks = _filter_runtime_rows_by_group(
        all_rows,
        status_group="active",
        limit=CONSOLE_RUNTIME_SUMMARY_GROUP_LIMIT,
    )
    recent_terminal_tasks = _filter_runtime_rows_by_group(
        all_rows,
        status_group="terminal",
        limit=CONSOLE_RUNTIME_SUMMARY_GROUP_LIMIT,
    )
    recent_failed_tasks = _filter_runtime_rows_by_group(
        all_rows,
        status_group="failed",
        limit=CONSOLE_RUNTIME_SUMMARY_GROUP_LIMIT,
    )
    latest_markers = {
        "latest": _runtime_marker_from_rows(all_rows),
        "active": _runtime_marker_from_rows(recent_active_tasks),
        "terminal": _runtime_marker_from_rows(recent_terminal_tasks),
        "failed": _runtime_marker_from_rows(recent_failed_tasks),
    }
    status_group_counts = {
        "active": sum(1 for record in ordered if _status_matches_group(record.status, "active")),
        "terminal": sum(1 for record in ordered if _status_matches_group(record.status, "terminal")),
        "failed": sum(1 for record in ordered if _status_matches_group(record.status, "failed")),
    }
    return {
        "domain": "console",
        "dataset": "console_runtime_summary",
        "tasks_root": str(tasks_root),
        "state_root": str(_runtime_state_root(tasks_root)),
        "runtime_summary_path": str(_runtime_summary_path(tasks_root)),
        "runtime_history_path": str(_runtime_history_path(tasks_root)),
        "task_count": len(ordered),
        "latest_task_id": latest_markers["latest"].get("task_id"),
        "updated_at": latest_markers["latest"].get("updated_at"),
        "status_counts": status_counts,
        "status_group_counts": status_group_counts,
        "action_counts": action_counts,
        "history_scan": _runtime_history_scan_payload(scan),
        "history_retention": _runtime_retention_payload(
            total_task_count=len(ordered),
            row_limit=CONSOLE_RUNTIME_HISTORY_LIMIT,
            group_row_limit=CONSOLE_RUNTIME_HISTORY_GROUP_LIMIT,
        ),
        "history_groups": _runtime_history_groups_payload(
            action_counts=action_counts,
            status_group_counts=status_group_counts,
            rows=all_rows,
            latest_markers=latest_markers,
        ),
        "latest_active_task_id": latest_markers["active"].get("task_id"),
        "latest_terminal_task_id": latest_markers["terminal"].get("task_id"),
        "latest_failed_task_id": latest_markers["failed"].get("task_id"),
        "latest_markers": latest_markers,
        "recent_tasks": recent_tasks,
        "recent_task_count": len(recent_tasks),
        "recent_active_tasks": recent_active_tasks,
        "recent_terminal_tasks": recent_terminal_tasks,
        "recent_failed_tasks": recent_failed_tasks,
    }


def _build_runtime_history_payload(
    records: Sequence[ConsoleTaskRecord],
    *,
    tasks_root: Path,
    scan: ConsoleTaskHistoryScan,
    limit: int = CONSOLE_RUNTIME_HISTORY_LIMIT,
) -> dict[str, object]:
    ordered = sorted(records, key=lambda item: (item.created_at, item.task_id), reverse=True)
    resolved_limit = max(int(limit), 0)
    all_rows = [_runtime_recent_task_row(record, tasks_root=tasks_root) for record in ordered]
    rows = all_rows[:resolved_limit]
    groups = {
        status_group: {
            "task_count": sum(1 for record in ordered if _status_matches_group(record.status, status_group)),
            "latest": _runtime_marker_from_rows(
                _filter_runtime_rows_by_group(all_rows, status_group=status_group, limit=1)
            ),
            "rows": _filter_runtime_rows_by_group(
                all_rows,
                status_group=status_group,
                limit=CONSOLE_RUNTIME_HISTORY_GROUP_LIMIT,
            ),
        }
        for status_group in ("active", "terminal", "failed")
    }
    return {
        "domain": "console",
        "dataset": "console_runtime_history",
        "tasks_root": str(tasks_root),
        "state_root": str(_runtime_state_root(tasks_root)),
        "runtime_summary_path": str(_runtime_summary_path(tasks_root)),
        "runtime_history_path": str(_runtime_history_path(tasks_root)),
        "retention": _runtime_retention_payload(
            total_task_count=len(ordered),
            row_limit=resolved_limit,
            group_row_limit=CONSOLE_RUNTIME_HISTORY_GROUP_LIMIT,
        ),
        "task_count": len(ordered),
        "row_count": len(rows),
        "history_limit": resolved_limit,
        "updated_at": rows[0]["updated_at"] if rows else None,
        "rows": rows,
        "groups": groups,
        "history_scan": _runtime_history_scan_payload(scan),
    }


def _runtime_recent_task_row(
    record: ConsoleTaskRecord,
    *,
    tasks_root: Path,
) -> dict[str, object]:
    request_summary = _runtime_request_summary(record.request)
    result_summary = _runtime_result_summary(record.result)
    primary_output = _runtime_primary_output(record.result)
    result_paths = _runtime_result_paths(record.result)
    return {
        "task_id": record.task_id,
        "task_path": str(_task_record_path(tasks_root, record.task_id)),
        "action_id": record.action_id,
        "status": record.status,
        "status_label": _runtime_status_label(record.status),
        "status_group": _runtime_status_group(record.status),
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "started_at": record.started_at,
        "finished_at": record.finished_at,
        "progress": record.progress.to_dict(),
        "progress_summary": _runtime_progress_summary(record.progress),
        "request": _mapping_payload(record.request),
        "request_summary": request_summary,
        "subject_summary": _runtime_subject_summary(record.action_id, request_summary),
        "result_summary": result_summary,
        "error_summary": _runtime_error_summary(record.error, result=record.result),
        "error_detail": _runtime_error_detail(record.error, result=record.result),
        "primary_output_label": primary_output.get("label"),
        "primary_output_path": primary_output.get("path"),
        "result_paths": result_paths,
        "linked_objects": _runtime_linked_objects(record.action_id, result_paths),
    }


def _filter_runtime_rows_by_group(
    rows: Sequence[Mapping[str, object]],
    *,
    status_group: str,
    limit: int,
) -> list[dict[str, object]]:
    matched: list[dict[str, object]] = []
    for row in rows:
        if not _status_matches_group(str(row.get("status") or ""), status_group):
            continue
        matched.append(dict(row))
        if len(matched) >= limit:
            break
    return matched


def _runtime_marker_from_rows(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    if not rows:
        return {}
    return _runtime_row_marker(rows[0])


def _runtime_row_marker(row: Mapping[str, object]) -> dict[str, object]:
    return {
        "task_id": row.get("task_id"),
        "task_path": row.get("task_path"),
        "action_id": row.get("action_id"),
        "status": row.get("status"),
        "status_label": row.get("status_label"),
        "status_group": row.get("status_group"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
        "subject_summary": row.get("subject_summary"),
        "progress_summary": row.get("progress_summary"),
        "result_summary": row.get("result_summary"),
        "error_summary": row.get("error_summary"),
        "error_detail": row.get("error_detail"),
        "primary_output_path": row.get("primary_output_path"),
        "linked_objects": row.get("linked_objects"),
    }


def _runtime_status_label(status: object) -> str:
    token = _optional_text(status) or "unknown"
    return token.replace("_", " ").title()


def _runtime_status_group(status: object) -> str:
    token = _optional_text(status) or ""
    if _status_matches_group(token, "active"):
        return "active"
    if _status_matches_group(token, "failed"):
        return "failed"
    if _status_matches_group(token, "terminal"):
        return "terminal"
    return "unknown"


def _runtime_progress_summary(progress: ConsoleTaskProgress) -> str | None:
    parts = [item for item in (_optional_text(progress.summary), _optional_text(progress.current_stage)) if item]
    if progress.progress_pct is not None:
        parts.append(f"{int(progress.progress_pct)}%")
    return " · ".join(parts) if parts else None


def _runtime_request_summary(request: Mapping[str, object] | None) -> dict[str, object]:
    if request is None:
        return {}
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
        "suite",
        "spec",
        "sync_command",
        "build_command",
    ):
        value = request.get(key)
        if value in (None, "", []):
            continue
        summary[key] = _json_safe(value)
    return summary


def _runtime_subject_summary(action_id: str, request: Mapping[str, object]) -> str | None:
    market = _optional_text(request.get("market"))
    if action_id == "data_sync":
        primary = _optional_text(request.get("sync_command"))
    elif action_id == "data_build":
        primary = _optional_text(request.get("build_command"))
    elif action_id in {"research_train_run", "research_backtest_run", "research_experiment_run_suite"}:
        primary = _optional_text(request.get("run_label"))
    elif action_id in {"research_bundle_build", "research_activate_bundle"}:
        primary = _optional_text(request.get("bundle_label"))
    else:
        primary = None
    values = [item for item in (primary, market) if item]
    return " | ".join(values) if values else None


def _runtime_result_summary(result: object) -> str | None:
    if not isinstance(result, Mapping):
        return _optional_text(result)
    dataset = _optional_text(result.get("dataset")) or _optional_text(result.get("object_type"))
    label = (
        _optional_text(result.get("bundle_label"))
        or _optional_text(result.get("run_label"))
        or _optional_text(result.get("suite_name"))
    )
    if dataset and label:
        return f"{dataset}: {label}"
    if dataset:
        return dataset
    if _optional_text(result.get("selection_path")):
        return "bundle activation updated"
    if _optional_text(result.get("summary_path")):
        return "artifact summary written"
    if _optional_text(result.get("report_path")):
        return "report written"
    status = _optional_text(result.get("status"))
    if status:
        return f"status: {status}"
    return None


def _runtime_primary_output(result: object) -> dict[str, str]:
    if not isinstance(result, Mapping):
        return {"label": "", "path": ""}
    for label in ("bundle_dir", "selection_path", "run_dir", "summary_path", "report_path", "manifest_path"):
        value = _optional_text(result.get(label))
        if value is not None:
            return {"label": label, "path": value}
    return {"label": "", "path": ""}


def _runtime_result_paths(result: object) -> list[dict[str, str]]:
    if not isinstance(result, Mapping):
        return []
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for label in ("bundle_dir", "selection_path", "run_dir", "summary_path", "report_path", "manifest_path"):
        value = _optional_text(result.get(label))
        if value is None or value in seen:
            continue
        seen.add(value)
        rows.append({"label": label, "path": value})
    return rows


def _runtime_linked_objects(
    action_id: str,
    result_paths: Sequence[Mapping[str, object]],
) -> list[dict[str, str]]:
    lookup = {
        str(item.get("label")): str(item.get("path"))
        for item in result_paths
        if _optional_text(item.get("label")) is not None and _optional_text(item.get("path")) is not None
    }
    rows: list[dict[str, str]] = []

    def append(object_type: str, *, path_label: str, title: str) -> None:
        path = _optional_text(lookup.get(path_label))
        if path is None:
            return
        rows.append({"object_type": object_type, "title": title, "path": path})

    if action_id == "research_train_run":
        append("training_run", path_label="run_dir", title="Training Run")
    elif action_id == "research_bundle_build":
        append("model_bundle", path_label="bundle_dir", title="Model Bundle")
    elif action_id == "research_activate_bundle":
        append("active_bundle_selection", path_label="selection_path", title="Active Bundle Selection")
    elif action_id == "research_backtest_run":
        append("backtest_run", path_label="run_dir", title="Backtest Run")
    elif action_id == "research_experiment_run_suite":
        append("experiment_run", path_label="run_dir", title="Experiment Run")
    return rows


def _runtime_error_summary(error: object, *, result: object | None = None) -> str | None:
    if isinstance(error, Mapping):
        error_type = _optional_text(error.get("type"))
        message = _optional_text(error.get("message"))
        if error_type and message:
            return f"{error_type}: {message}"
        if error_type or message:
            return error_type or message
    else:
        message = _optional_text(error)
        if message is not None:
            return message
    if isinstance(result, Mapping):
        stderr_text = _optional_text(result.get("stderr"))
        if stderr_text:
            lines = [line.strip() for line in stderr_text.splitlines() if line.strip()]
            if lines:
                return lines[-1]
    return None


def _runtime_error_detail(error: object, *, result: object | None = None) -> dict[str, object]:
    rows: dict[str, object] = {}
    if isinstance(error, Mapping):
        if _optional_text(error.get("type")) is not None:
            rows["type"] = _optional_text(error.get("type"))
        if _optional_text(error.get("message")) is not None:
            rows["message"] = _optional_text(error.get("message"))
    elif _optional_text(error) is not None:
        rows["message"] = _optional_text(error)
    if isinstance(result, Mapping):
        if _optional_text(result.get("status")) is not None:
            rows["result_status"] = _optional_text(result.get("status"))
        if result.get("return_code") is not None:
            rows["return_code"] = result.get("return_code")
    return rows


def _runtime_history_scan_payload(scan: ConsoleTaskHistoryScan) -> dict[str, object]:
    return {
        "task_file_count": int(scan.task_file_count),
        "valid_task_count": len(scan.records),
        "invalid_task_file_count": len(scan.invalid_files),
        "invalid_task_files": scan.invalid_file_payloads(),
        "has_invalid_task_files": bool(scan.invalid_files),
    }


def _runtime_retention_payload(
    *,
    total_task_count: int,
    row_limit: int,
    group_row_limit: int,
) -> dict[str, object]:
    retained_task_count = min(max(int(total_task_count), 0), max(int(row_limit), 0))
    dropped_task_count = max(int(total_task_count) - retained_task_count, 0)
    return {
        "total_task_count": max(int(total_task_count), 0),
        "retained_task_count": retained_task_count,
        "dropped_task_count": dropped_task_count,
        "is_truncated": dropped_task_count > 0,
        "row_limit": max(int(row_limit), 0),
        "group_row_limit": max(int(group_row_limit), 0),
    }


def _runtime_history_groups_payload(
    *,
    action_counts: Mapping[str, int],
    status_group_counts: Mapping[str, int],
    rows: Sequence[Mapping[str, object]],
    latest_markers: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    latest_by_action: dict[str, dict[str, object]] = {}
    for row in rows:
        action_id = _optional_text(row.get("action_id"))
        if action_id is None or action_id in latest_by_action:
            continue
        latest_by_action[action_id] = _runtime_row_marker(row)
    return {
        "group_keys": ["status_group", "action_id"],
        "status_group": [
            {
                "group": group,
                "count": int(status_group_counts.get(group, 0)),
                "latest_marker": dict(latest_markers.get(group) or {}),
            }
            for group in ("active", "terminal", "failed")
        ],
        "action_id": [
            {
                "group": action_id,
                "action_id": action_id,
                "count": int(count),
                "latest_marker": latest_by_action.get(action_id, {}),
            }
            for action_id, count in sorted(action_counts.items(), key=lambda item: item[0])
        ],
    }


def _normalize_loaded_progress(
    progress: ConsoleTaskProgress,
    *,
    status: str,
    updated_at: str,
) -> ConsoleTaskProgress:
    current_stage = progress.current_stage
    if current_stage is None:
        if status == TASK_STATUS_QUEUED:
            current_stage = "queued"
        elif status == TASK_STATUS_RUNNING:
            current_stage = "running"
        else:
            current_stage = "finished"
    return ConsoleTaskProgress(
        summary=progress.summary,
        current=progress.current,
        total=progress.total,
        current_stage=current_stage,
        progress_pct=progress.progress_pct,
        heartbeat=progress.heartbeat or updated_at,
    )


def _write_json_atomically(target: Path, payload: Mapping[str, object]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{target.stem}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
        Path(temp_name).replace(target)
    finally:
        temp_path = Path(temp_name)
        if temp_path.exists():
            temp_path.unlink()


def _mapping_or_none(value: object) -> Mapping[str, object] | None:
    return value if isinstance(value, Mapping) else None


def _mapping_payload(payload: Mapping[str, object] | None) -> dict[str, object]:
    if payload is None:
        return {}
    return {str(key): _json_safe(item) for key, item in payload.items()}


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_safe(item) for item in value]
    return str(value)


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _optional_progress_pct(value: object) -> int | None:
    parsed = _optional_int(value)
    if parsed is None:
        return None
    return int(max(0, min(100, parsed)))


__all__ = [
    "ConsoleTaskProgress",
    "ConsoleTaskRecord",
    "TASK_STATUS_FAILED",
    "TASK_STATUS_QUEUED",
    "TASK_STATUS_RUNNING",
    "TASK_STATUS_SUCCEEDED",
    "default_console_runtime_state_root",
    "default_console_tasks_root",
    "get_console_task",
    "list_console_tasks",
    "load_console_runtime_history",
    "load_console_runtime_summary",
    "load_console_task",
]
