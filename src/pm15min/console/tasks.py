from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
import threading
import uuid

from pm15min.console.action_runner import execute_console_action
from pm15min.console.actions import build_console_action_request
from pm15min.core.layout import rewrite_root


TASK_STATUS_QUEUED = "queued"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_SUCCEEDED = "succeeded"
TASK_STATUS_FAILED = "failed"
CONSOLE_TASK_STALE_HEARTBEAT_SEC = 30.0
CONSOLE_RUNTIME_SUMMARY_RECENT_LIMIT = 12
CONSOLE_RUNTIME_SUMMARY_GROUP_LIMIT = 6
CONSOLE_RUNTIME_HISTORY_LIMIT = 50
CONSOLE_RUNTIME_HISTORY_GROUP_LIMIT = 12

ConsoleTaskPlanner = Callable[[str, Mapping[str, object] | None], Mapping[str, object]]
ConsoleTaskExecutor = Callable[["ConsoleTaskRunContext"], object]


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
        return {
            "summary": self.summary,
            "current": self.current,
            "total": self.total,
            "current_stage": self.current_stage,
            "progress_pct": self.progress_pct,
            "heartbeat": self.heartbeat,
        }

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
            "request": _json_safe(self.request),
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
            request=_mapping_payload(payload.get("request")),
            command_preview=str(payload.get("command_preview") or ""),
            result=payload.get("result"),
            error=payload.get("error"),
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
        return {
            "path": self.path,
            "error_type": self.error_type,
            "message": self.message,
        }


@dataclass(frozen=True)
class ConsoleTaskHistoryScan:
    records: tuple[ConsoleTaskRecord, ...] = ()
    task_file_count: int = 0
    invalid_files: tuple[ConsoleTaskScanIssue, ...] = ()
    latest_task_mtime: float | None = None

    def invalid_file_payloads(self, *, limit: int = 5) -> list[dict[str, str]]:
        rows = [issue.to_dict() for issue in self.invalid_files]
        return rows[: max(int(limit), 0)]


class ConsoleTaskRunContext:
    def __init__(
        self,
        *,
        task_id: str,
        action_id: str,
        request: Mapping[str, object],
        command_preview: str,
        rewrite_root: Path,
        reporter: Callable[..., None],
    ) -> None:
        self.task_id = str(task_id)
        self.action_id = str(action_id)
        self.request = _mapping_payload(request)
        self.command_preview = str(command_preview)
        self.rewrite_root = Path(rewrite_root)
        self._reporter = reporter

    def report_progress(
        self,
        summary: str,
        *,
        current: int | None = None,
        total: int | None = None,
        current_stage: str | None = None,
        progress_pct: int | None = None,
        heartbeat: str | None = None,
    ) -> None:
        self._reporter(
            summary=summary,
            current=current,
            total=total,
            current_stage=current_stage,
            progress_pct=progress_pct,
            heartbeat=heartbeat,
        )


class ConsoleTaskManager:
    def __init__(
        self,
        *,
        root: str | Path | None = None,
        planner: ConsoleTaskPlanner | None = None,
        executor: ConsoleTaskExecutor | None = None,
        heartbeat_interval_sec: float = 5.0,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self.root = _resolve_tasks_root(root)
        self._planner = planner or _default_task_planner
        self._executor = executor or _default_task_executor
        self._heartbeat_interval_sec = max(float(heartbeat_interval_sec), 0.05)
        self._now_fn = now_fn or _utc_now
        self._threads: dict[str, threading.Thread] = {}
        self._lock = threading.Lock()

    def submit(
        self,
        action_id: str,
        request: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        plan = self._planner(str(action_id), request)
        submitted_at = self._now_fn()
        created_at = _utc_timestamp(submitted_at)
        task_id = _new_task_id(submitted_at)
        normalized_request = _planned_request(plan, request)
        record = ConsoleTaskRecord(
            task_id=task_id,
            action_id=str(plan.get("action_id") or action_id),
            status=TASK_STATUS_QUEUED,
            created_at=created_at,
            updated_at=created_at,
            started_at=None,
            finished_at=None,
            request=normalized_request,
            command_preview=str(plan.get("command_preview") or ""),
            result=None,
            error=None,
            progress=ConsoleTaskProgress(
                summary="Queued",
                current_stage="queued",
                progress_pct=0,
                heartbeat=created_at,
            ),
        )
        self._write_record(record)
        thread = threading.Thread(
            target=self._run_task,
            args=(record.task_id,),
            name=f"console-task-{record.task_id}",
            daemon=True,
        )
        self._threads[record.task_id] = thread
        thread.start()
        return record.to_dict()

    def get(self, task_id: str) -> dict[str, object] | None:
        record = self._read_record(str(task_id))
        return None if record is None else record.to_dict()

    def list(
        self,
        *,
        action_id: str | None = None,
        action_ids: Sequence[str] | None = None,
        status: str | None = None,
        status_group: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, object]]:
        if limit is not None and int(limit) < 0:
            raise ValueError("limit must be >= 0")
        action_filters = _normalized_action_filters(
            action_id=action_id,
            action_ids=action_ids,
        )
        resolved_status_group = _normalized_status_group(status_group)
        records: list[ConsoleTaskRecord] = []
        for record in _scan_task_history(self.root).records:
            if action_filters is not None and record.action_id not in action_filters:
                continue
            if status is not None and record.status != str(status):
                continue
            if resolved_status_group is not None and not _status_matches_group(record.status, resolved_status_group):
                continue
            records.append(record)
        records.sort(key=lambda item: (item.created_at, item.task_id), reverse=True)
        if limit is not None:
            records = records[: int(limit)]
        return [record.to_dict() for record in records]

    def _run_task(self, task_id: str) -> None:
        try:
            queued = self._require_record(task_id)
            started_at = _utc_timestamp(self._now_fn())
            self._update_record(
                task_id,
                status=TASK_STATUS_RUNNING,
                started_at=queued.started_at or started_at,
                updated_at=started_at,
                progress=_progress_snapshot(
                    queued.progress,
                    summary="Running",
                    current_stage=TASK_STATUS_RUNNING,
                    heartbeat=started_at,
                ),
            )
            running = self._require_record(task_id)
            heartbeat_stop = threading.Event()
            heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop,
                args=(task_id, heartbeat_stop),
                name=f"console-task-heartbeat-{task_id}",
                daemon=True,
            )
            heartbeat_thread.start()
            context = ConsoleTaskRunContext(
                task_id=running.task_id,
                action_id=running.action_id,
                request=running.request,
                command_preview=running.command_preview,
                rewrite_root=_rewrite_root_for_tasks_dir(self.root),
                reporter=lambda *, summary, current=None, total=None, current_stage=None, progress_pct=None, heartbeat=None: self._report_progress(
                    task_id,
                    summary=summary,
                    current=current,
                    total=total,
                    current_stage=current_stage,
                    progress_pct=progress_pct,
                    heartbeat=heartbeat,
                ),
            )
            result = self._executor(context)
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=0.2)
        except Exception as exc:
            if "heartbeat_stop" in locals():
                heartbeat_stop.set()
            if "heartbeat_thread" in locals():
                heartbeat_thread.join(timeout=0.2)
            self._finish_failed(task_id, exc=exc)
            self._threads.pop(task_id, None)
            return

        if _executor_succeeded(result):
            self._finish_succeeded(task_id, result=result)
        else:
            self._finish_failed(task_id, result=result, error=_error_from_result(result))
        self._threads.pop(task_id, None)

    def _finish_succeeded(self, task_id: str, *, result: object) -> None:
        record = self._require_record(task_id)
        finished_at = _utc_timestamp(self._now_fn())
        progress = _complete_progress(
            record.progress,
            summary="Completed",
            heartbeat=finished_at,
            succeeded=True,
        )
        command_preview = record.command_preview
        if not command_preview and isinstance(result, Mapping):
            command_preview = str(result.get("command_preview") or "")
        self._write_record(
            replace(
                record,
                status=TASK_STATUS_SUCCEEDED,
                updated_at=finished_at,
                finished_at=finished_at,
                result=_json_safe(result),
                error=None,
                command_preview=command_preview,
                progress=progress,
            )
        )

    def _finish_failed(
        self,
        task_id: str,
        *,
        exc: Exception | None = None,
        result: object | None = None,
        error: object | None = None,
    ) -> None:
        record = self._require_record(task_id)
        resolved_error = error
        if exc is not None:
            resolved_error = {
                "type": exc.__class__.__name__,
                "message": str(exc),
            }
        elif resolved_error is None:
            resolved_error = {"message": "Task execution failed"}
        finished_at = _utc_timestamp(self._now_fn())
        progress = _complete_progress(
            record.progress,
            summary="Failed",
            heartbeat=finished_at,
            succeeded=False,
        )
        command_preview = record.command_preview
        if not command_preview and isinstance(result, Mapping):
            command_preview = str(result.get("command_preview") or "")
        self._write_record(
            replace(
                record,
                status=TASK_STATUS_FAILED,
                updated_at=finished_at,
                finished_at=finished_at,
                result=_json_safe(result),
                error=_json_safe(resolved_error),
                command_preview=command_preview,
                progress=progress,
            )
        )

    def _report_progress(
        self,
        task_id: str,
        *,
        summary: str,
        current: int | None = None,
        total: int | None = None,
        current_stage: str | None = None,
        progress_pct: int | None = None,
        heartbeat: str | None = None,
    ) -> None:
        record = self._require_record(task_id)
        progress = record.progress
        next_heartbeat = _optional_text(heartbeat) or _utc_timestamp(self._now_fn())
        next_progress = _progress_snapshot(
            progress,
            summary=summary,
            current=current,
            total=total,
            current_stage=current_stage or progress.current_stage or _default_progress_stage(record.status),
            progress_pct=progress_pct,
            heartbeat=next_heartbeat,
        )
        self._write_record(
            replace(
                record,
                updated_at=next_heartbeat,
                progress=next_progress,
            )
        )

    def _heartbeat_loop(self, task_id: str, stop_event: threading.Event) -> None:
        while not stop_event.wait(timeout=self._heartbeat_interval_sec):
            record = self._read_record(task_id)
            if record is None or record.status != TASK_STATUS_RUNNING:
                return
            heartbeat = _utc_timestamp(self._now_fn())
            self._write_record(
                replace(
                    record,
                    updated_at=heartbeat,
                    progress=_progress_snapshot(
                        record.progress,
                        heartbeat=heartbeat,
                    ),
                )
            )

    def _update_record(
        self,
        task_id: str,
        *,
        status: str | None = None,
        progress: ConsoleTaskProgress | None = None,
        started_at: str | None = None,
        finished_at: str | None = None,
        updated_at: str | None = None,
    ) -> None:
        record = self._require_record(task_id)
        next_updated_at = updated_at or _utc_timestamp(self._now_fn())
        self._write_record(
            replace(
                record,
                status=record.status if status is None else str(status),
                updated_at=next_updated_at,
                started_at=record.started_at if started_at is None else str(started_at),
                finished_at=record.finished_at if finished_at is None else str(finished_at),
                progress=record.progress if progress is None else progress,
            )
        )

    def _task_path(self, task_id: str) -> Path:
        return self.root / f"{task_id}.json"

    def _read_record(self, task_id: str) -> ConsoleTaskRecord | None:
        path = self._task_path(task_id)
        return self._read_path(path)

    def _read_path(self, path: Path) -> ConsoleTaskRecord | None:
        record, _ = _load_task_record_path(path)
        return record

    def _require_record(self, task_id: str) -> ConsoleTaskRecord:
        record = self._read_record(task_id)
        if record is None:
            raise FileNotFoundError(f"Unknown console task_id {task_id!r}")
        return record

    def _write_record(self, record: ConsoleTaskRecord) -> None:
        with self._lock:
            self.root.mkdir(parents=True, exist_ok=True)
            target = self._task_path(record.task_id)
            payload = json.dumps(record.to_dict(), indent=2, ensure_ascii=False, sort_keys=True)
            fd, temp_name = tempfile.mkstemp(
                prefix=f".{target.stem}.",
                suffix=".tmp",
                dir=str(self.root),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(payload)
                Path(temp_name).replace(target)
            finally:
                temp_path = Path(temp_name)
                if temp_path.exists():
                    temp_path.unlink()
            self._write_runtime_summary()

    def _write_runtime_summary(self) -> None:
        scan = _scan_task_history(self.root)
        summary_payload = _build_runtime_summary_payload(
            scan.records,
            tasks_root=self.root,
            scan=scan,
        )
        history_payload = _build_runtime_history_payload(
            scan.records,
            tasks_root=self.root,
            scan=scan,
        )
        _write_json_atomically(_runtime_summary_path(self.root), summary_payload)
        _write_json_atomically(_runtime_history_path(self.root), history_payload)


def submit_console_task(
    *,
    action_id: str,
    request: Mapping[str, object] | None = None,
    root: str | Path | None = None,
    planner: ConsoleTaskPlanner | None = None,
    executor: ConsoleTaskExecutor | None = None,
    heartbeat_interval_sec: float = 5.0,
    now_fn: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    manager = ConsoleTaskManager(
        root=root,
        planner=planner,
        executor=executor,
        heartbeat_interval_sec=heartbeat_interval_sec,
        now_fn=now_fn,
    )
    return manager.submit(action_id=action_id, request=request)


def get_console_task(
    task_id: str,
    *,
    root: str | Path | None = None,
) -> dict[str, object] | None:
    return ConsoleTaskManager(root=root).get(task_id)


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
    return ConsoleTaskManager(root=root).list(
        action_id=action_id,
        action_ids=action_ids,
        status=status,
        status_group=status_group,
        limit=limit,
    )


def load_console_runtime_summary(
    *,
    root: str | Path | None = None,
) -> dict[str, object]:
    tasks_root = _resolve_tasks_root(root)
    scan = _scan_task_history(tasks_root)
    summary_path = _runtime_summary_path(tasks_root)
    if summary_path.exists():
        payload, issue = _load_runtime_summary_payload(summary_path)
        if payload is not None:
            normalized = _normalize_runtime_summary_payload(
                payload,
                tasks_root=tasks_root,
                scan=scan,
            )
            rebuild_reason = _runtime_summary_rebuild_reason(
                normalized,
                tasks_root=tasks_root,
                scan=scan,
                summary_path=summary_path,
            )
            if rebuild_reason is None:
                normalized["summary_source"] = "persisted"
                normalized["summary_recovery"] = _runtime_summary_recovery_payload(
                    source="persisted",
                    recovered=False,
                    reason=None,
                    summary_path=summary_path,
                    scan=scan,
                )
                return normalized
        else:
            rebuild_reason = _scan_issue_label(issue)
        rebuilt = _build_runtime_summary_payload(
            scan.records,
            tasks_root=tasks_root,
            scan=scan,
        )
        _write_json_atomically(summary_path, rebuilt)
        _write_json_atomically(
            _runtime_history_path(tasks_root),
            _build_runtime_history_payload(
                scan.records,
                tasks_root=tasks_root,
                scan=scan,
            ),
        )
        rebuilt["summary_source"] = "recovered_from_tasks" if scan.task_file_count else "recovered_empty"
        rebuilt["summary_recovery"] = _runtime_summary_recovery_payload(
            source=str(rebuilt["summary_source"]),
            recovered=True,
            reason=rebuild_reason,
            summary_path=summary_path,
            scan=scan,
        )
        return rebuilt
    rebuilt = _build_runtime_summary_payload(
        scan.records,
        tasks_root=tasks_root,
        scan=scan,
    )
    if scan.task_file_count or scan.invalid_files:
        _write_json_atomically(summary_path, rebuilt)
        _write_json_atomically(
            _runtime_history_path(tasks_root),
            _build_runtime_history_payload(
                scan.records,
                tasks_root=tasks_root,
                scan=scan,
            ),
        )
    rebuilt["summary_source"] = "recovered_from_tasks" if scan.task_file_count else "empty"
    rebuilt["summary_recovery"] = _runtime_summary_recovery_payload(
        source=str(rebuilt["summary_source"]),
        recovered=bool(scan.task_file_count or scan.invalid_files),
        reason="runtime_summary_missing" if scan.task_file_count or scan.invalid_files else None,
        summary_path=summary_path,
        scan=scan,
    )
    return rebuilt


def load_console_runtime_history(
    *,
    root: str | Path | None = None,
) -> dict[str, object]:
    tasks_root = _resolve_tasks_root(root)
    scan = _scan_task_history(tasks_root)
    history_path = _runtime_history_path(tasks_root)
    if history_path.exists():
        payload, issue = _load_runtime_summary_payload(history_path)
        if payload is not None:
            normalized = _normalize_runtime_history_payload(
                payload,
                tasks_root=tasks_root,
                scan=scan,
            )
            rebuild_reason = _runtime_history_rebuild_reason(
                normalized,
                tasks_root=tasks_root,
                scan=scan,
                history_path=history_path,
            )
            if rebuild_reason is None:
                normalized["history_source"] = "persisted"
                normalized["history_recovery"] = _runtime_history_recovery_payload(
                    source="persisted",
                    recovered=False,
                    reason=None,
                    history_path=history_path,
                    scan=scan,
                )
                return normalized
        else:
            rebuild_reason = _scan_issue_label(issue)
        rebuilt = _build_runtime_history_payload(
            scan.records,
            tasks_root=tasks_root,
            scan=scan,
        )
        _write_json_atomically(history_path, rebuilt)
        _write_json_atomically(
            _runtime_summary_path(tasks_root),
            _build_runtime_summary_payload(
                scan.records,
                tasks_root=tasks_root,
                scan=scan,
            ),
        )
        rebuilt["history_source"] = "recovered_from_tasks" if scan.task_file_count else "recovered_empty"
        rebuilt["history_recovery"] = _runtime_history_recovery_payload(
            source=str(rebuilt["history_source"]),
            recovered=True,
            reason=rebuild_reason,
            history_path=history_path,
            scan=scan,
        )
        return rebuilt
    rebuilt = _build_runtime_history_payload(
        scan.records,
        tasks_root=tasks_root,
        scan=scan,
    )
    if scan.task_file_count or scan.invalid_files:
        _write_json_atomically(history_path, rebuilt)
        _write_json_atomically(
            _runtime_summary_path(tasks_root),
            _build_runtime_summary_payload(
                scan.records,
                tasks_root=tasks_root,
                scan=scan,
            ),
        )
    rebuilt["history_source"] = "recovered_from_tasks" if scan.task_file_count else "empty"
    rebuilt["history_recovery"] = _runtime_history_recovery_payload(
        source=str(rebuilt["history_source"]),
        recovered=bool(scan.task_file_count or scan.invalid_files),
        reason="runtime_history_missing" if scan.task_file_count or scan.invalid_files else None,
        history_path=history_path,
        scan=scan,
    )
    return rebuilt


def submit_console_action_task(
    *,
    action_id: str,
    request: Mapping[str, object] | None = None,
    root: str | Path | None = None,
) -> dict[str, object]:
    return submit_console_task(
        action_id=action_id,
        request=request,
        root=root,
    )


def _default_task_planner(
    action_id: str,
    request: Mapping[str, object] | None = None,
) -> Mapping[str, object]:
    return build_console_action_request(action_id, request)


def _default_task_executor(context: ConsoleTaskRunContext) -> object:
    direct_result = _execute_direct_research_action(context)
    if direct_result is not None:
        return direct_result
    context.report_progress(
        "Running",
        current_stage=TASK_STATUS_RUNNING,
    )
    return execute_console_action(
        action_id=context.action_id,
        request=context.request,
    )


def _planned_request(
    plan: Mapping[str, object],
    fallback: Mapping[str, object] | None,
) -> dict[str, object]:
    normalized = _mapping_or_none(plan.get("normalized_request"))
    if normalized is not None:
        return _mapping_payload(normalized)
    return _mapping_payload(fallback)


def _complete_progress(
    progress: ConsoleTaskProgress,
    *,
    summary: str,
    heartbeat: str,
    succeeded: bool,
) -> ConsoleTaskProgress:
    current = progress.current
    if succeeded and progress.total is not None:
        current = progress.total
    return _progress_snapshot(
        progress,
        summary=summary,
        current=current,
        total=progress.total,
        current_stage="finished",
        progress_pct=100,
        heartbeat=heartbeat,
    )


def _executor_succeeded(result: object) -> bool:
    if isinstance(result, Mapping):
        if "status" in result:
            status = str(result.get("status") or "").strip().lower()
            if status in {"ok", "success", "succeeded", "completed"}:
                return True
            if status:
                return False
        if "succeeded" in result:
            return bool(result.get("succeeded"))
        if "return_code" in result:
            try:
                return int(result.get("return_code")) == 0
            except Exception:
                return False
    return True


def _error_from_result(result: object) -> object:
    if isinstance(result, Mapping):
        if result.get("error") not in (None, ""):
            return result.get("error")
        stderr_text = _optional_text(result.get("stderr"))
        if stderr_text:
            lines = [line.strip() for line in stderr_text.splitlines() if line.strip()]
            return {"message": lines[-1] if lines else stderr_text}
        summary = _mapping_or_none(result.get("execution_summary"))
        if summary is not None:
            last_stderr_line = _optional_text(summary.get("last_stderr_line"))
            if last_stderr_line:
                return {"message": last_stderr_line}
        if "status" in result:
            return {"message": f"Executor returned status {result.get('status')!r}"}
        if "return_code" in result:
            return {"message": f"Executor returned code {result.get('return_code')!r}"}
    return {"message": "Task execution failed"}


def _mapping_payload(payload: Mapping[str, object] | None) -> dict[str, object]:
    if payload is None:
        return {}
    return {str(key): _json_safe(value) for key, value in payload.items()}


def _mapping_or_none(payload: object) -> Mapping[str, object] | None:
    return payload if isinstance(payload, Mapping) else None


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return _utc_timestamp(value)
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, set):
        return [_json_safe(item) for item in sorted(value, key=str)]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_safe(item) for item in value]
    return str(value)


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _optional_progress_pct(value: object) -> int | None:
    parsed = _optional_int(value)
    if parsed is None:
        return None
    return int(max(0, min(100, parsed)))


def _parse_utc_timestamp(value: object) -> datetime | None:
    text = _optional_text(value)
    if text is None:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _recover_stale_active_record(
    record: ConsoleTaskRecord,
    *,
    now: datetime,
) -> ConsoleTaskRecord:
    if record.status not in {TASK_STATUS_QUEUED, TASK_STATUS_RUNNING}:
        return record
    last_heartbeat = _parse_utc_timestamp(record.progress.heartbeat or record.updated_at or record.created_at)
    if last_heartbeat is None:
        return record
    age_seconds = (now.astimezone(timezone.utc) - last_heartbeat).total_seconds()
    if age_seconds < CONSOLE_TASK_STALE_HEARTBEAT_SEC:
        return record
    finished_at = _utc_timestamp(now)
    existing_error = _mapping_or_none(record.error) or {}
    stale_error = {
        **existing_error,
        "type": str(existing_error.get("type") or "StaleTaskHeartbeat"),
        "message": str(
            existing_error.get("message")
            or "Task heartbeat expired; the task was likely interrupted before it could finish."
        ),
        "last_heartbeat": record.progress.heartbeat or record.updated_at or record.created_at,
        "stale_after_sec": int(CONSOLE_TASK_STALE_HEARTBEAT_SEC),
        "recovered": True,
    }
    progress = _complete_progress(
        record.progress,
        summary="Failed",
        heartbeat=finished_at,
        succeeded=False,
    )
    return replace(
        record,
        status=TASK_STATUS_FAILED,
        updated_at=finished_at,
        finished_at=record.finished_at or finished_at,
        error=_json_safe(stale_error),
        progress=progress,
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_task_id(now: datetime) -> str:
    ts = now.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"task_{ts}_{uuid.uuid4().hex[:8]}"


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


def _load_task_record_path(path: Path) -> tuple[ConsoleTaskRecord | None, ConsoleTaskScanIssue | None]:
    if not path.exists() or not path.is_file():
        return None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        record = ConsoleTaskRecord.from_dict(payload)
        recovered = _recover_stale_active_record(record, now=_utc_now())
        if recovered != record:
            _write_json_atomically(path, recovered.to_dict())
        return recovered, None
    except Exception as exc:
        return None, _scan_issue(path, exc)


def _scan_task_history(tasks_root: Path) -> ConsoleTaskHistoryScan:
    if not tasks_root.exists():
        return ConsoleTaskHistoryScan()
    records: list[ConsoleTaskRecord] = []
    invalid_files: list[ConsoleTaskScanIssue] = []
    task_file_count = 0
    latest_task_mtime: float | None = None
    for path in tasks_root.glob("*.json"):
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
            continue
        if issue is not None:
            invalid_files.append(issue)
    records.sort(key=lambda item: (item.created_at, item.task_id), reverse=True)
    return ConsoleTaskHistoryScan(
        records=tuple(records),
        task_file_count=task_file_count,
        invalid_files=tuple(invalid_files),
        latest_task_mtime=latest_task_mtime,
    )


def _load_runtime_summary_payload(path: Path) -> tuple[dict[str, object] | None, ConsoleTaskScanIssue | None]:
    if not path.exists() or not path.is_file():
        return None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return None, _scan_issue(path, exc)
    if not isinstance(payload, Mapping):
        return None, ConsoleTaskScanIssue(
            path=str(path),
            error_type="InvalidRuntimeSummary",
            message="Runtime summary payload must be a JSON object.",
        )
    return _mapping_payload(payload), None


def _runtime_summary_rebuild_reason(
    payload: Mapping[str, object],
    *,
    tasks_root: Path,
    scan: ConsoleTaskHistoryScan,
    summary_path: Path,
) -> str | None:
    if _optional_text(payload.get("dataset")) != "console_runtime_summary":
        return "runtime_summary_invalid_dataset"
    persisted_tasks_root = _optional_text(payload.get("tasks_root"))
    if persisted_tasks_root is not None and persisted_tasks_root != str(tasks_root):
        return "runtime_summary_root_mismatch"
    if scan.task_file_count <= 0:
        return None
    if _runtime_summary_missing_fields(payload):
        return "runtime_summary_schema_outdated"
    try:
        summary_mtime = summary_path.stat().st_mtime
    except OSError:
        return "runtime_summary_mtime_unavailable"
    if scan.latest_task_mtime is not None and summary_mtime + 1e-9 < scan.latest_task_mtime:
        return "runtime_summary_stale_vs_tasks"
    return None


def _runtime_summary_missing_fields(payload: Mapping[str, object]) -> bool:
    if not isinstance(payload.get("status_group_counts"), Mapping):
        return True
    if not isinstance(payload.get("latest_markers"), Mapping):
        return True
    if not isinstance(payload.get("history_groups"), Mapping):
        return True
    if not isinstance(payload.get("history_scan"), Mapping):
        return True
    if not isinstance(payload.get("history_retention"), Mapping):
        return True
    if _optional_text(payload.get("runtime_history_path")) is None:
        return True
    if _runtime_rows_missing_fields(_runtime_rows_payload(payload.get("recent_tasks"))):
        return True
    return False


def _normalize_runtime_summary_payload(
    payload: Mapping[str, object],
    *,
    tasks_root: Path,
    scan: ConsoleTaskHistoryScan,
) -> dict[str, object]:
    normalized = _mapping_payload(payload)
    recent_tasks = _runtime_rows_payload(normalized.get("recent_tasks"))
    recent_active_tasks = _runtime_rows_payload(normalized.get("recent_active_tasks"))
    recent_terminal_tasks = _runtime_rows_payload(normalized.get("recent_terminal_tasks"))
    recent_failed_tasks = _runtime_rows_payload(normalized.get("recent_failed_tasks"))
    if not recent_active_tasks:
        recent_active_tasks = _filter_runtime_rows_by_group(recent_tasks, status_group="active")
    if not recent_terminal_tasks:
        recent_terminal_tasks = _filter_runtime_rows_by_group(recent_tasks, status_group="terminal")
    if not recent_failed_tasks:
        recent_failed_tasks = _filter_runtime_rows_by_group(recent_tasks, status_group="failed")
    latest_markers = _runtime_summary_markers_from_rows(
        recent_tasks,
        active_rows=recent_active_tasks,
        terminal_rows=recent_terminal_tasks,
        failed_rows=recent_failed_tasks,
        existing=_mapping_or_none(normalized.get("latest_markers")),
    )
    status_counts = _int_mapping_payload(normalized.get("status_counts"))
    action_counts = _int_mapping_payload(normalized.get("action_counts"))
    status_group_counts = _status_group_counts_payload(
        normalized.get("status_group_counts"),
        recent_tasks=recent_tasks,
    )
    normalized.update(
        {
            "domain": "console",
            "dataset": "console_runtime_summary",
            "tasks_root": str(tasks_root),
            "state_root": str(_runtime_state_root(tasks_root)),
            "runtime_summary_path": str(_runtime_summary_path(tasks_root)),
            "runtime_history_path": str(_runtime_history_path(tasks_root)),
            "history_retention": _runtime_retention_mapping(
                normalized.get("history_retention"),
                total_task_count=_optional_int(normalized.get("task_count")) or len(scan.records) or len(recent_tasks),
            ),
            "task_count": _optional_int(normalized.get("task_count")) or len(scan.records) or len(recent_tasks),
            "latest_task_id": _optional_text(normalized.get("latest_task_id")) or latest_markers["latest"].get("task_id"),
            "updated_at": _optional_text(normalized.get("updated_at")) or latest_markers["latest"].get("updated_at"),
            "status_counts": status_counts,
            "status_group_counts": status_group_counts,
            "action_counts": action_counts,
            "latest_active_task_id": _optional_text(normalized.get("latest_active_task_id")) or latest_markers["active"].get("task_id"),
            "latest_active_updated_at": _optional_text(normalized.get("latest_active_updated_at")) or latest_markers["active"].get("updated_at"),
            "latest_terminal_task_id": _optional_text(normalized.get("latest_terminal_task_id")) or latest_markers["terminal"].get("task_id"),
            "latest_terminal_updated_at": _optional_text(normalized.get("latest_terminal_updated_at")) or latest_markers["terminal"].get("updated_at"),
            "latest_failed_task_id": _optional_text(normalized.get("latest_failed_task_id")) or latest_markers["failed"].get("task_id"),
            "latest_failed_updated_at": _optional_text(normalized.get("latest_failed_updated_at")) or latest_markers["failed"].get("updated_at"),
            "latest_markers": latest_markers,
            "recent_tasks": recent_tasks,
            "recent_task_count": _optional_int(normalized.get("recent_task_count")) or len(recent_tasks),
            "recent_active_tasks": recent_active_tasks,
            "recent_terminal_tasks": recent_terminal_tasks,
            "recent_failed_tasks": recent_failed_tasks,
        }
    )
    normalized["history_scan"] = _runtime_history_scan_payload(scan)
    normalized["history_groups"] = _runtime_history_groups_payload(
        action_counts=action_counts,
        status_group_counts=status_group_counts,
        rows=recent_tasks,
        latest_markers=latest_markers,
    )
    return normalized


def _runtime_summary_recovery_payload(
    *,
    source: str,
    recovered: bool,
    reason: str | None,
    summary_path: Path,
    scan: ConsoleTaskHistoryScan,
) -> dict[str, object]:
    return {
        "source": source,
        "recovered": bool(recovered),
        "reason": reason,
        "summary_path": str(summary_path),
        "summary_path_exists": summary_path.exists(),
        "invalid_task_file_count": len(scan.invalid_files),
    }


def _runtime_history_rebuild_reason(
    payload: Mapping[str, object],
    *,
    tasks_root: Path,
    scan: ConsoleTaskHistoryScan,
    history_path: Path,
) -> str | None:
    if _optional_text(payload.get("dataset")) != "console_runtime_history":
        return "runtime_history_invalid_dataset"
    persisted_tasks_root = _optional_text(payload.get("tasks_root"))
    if persisted_tasks_root is not None and persisted_tasks_root != str(tasks_root):
        return "runtime_history_root_mismatch"
    if scan.task_file_count <= 0:
        return None
    if _runtime_history_missing_fields(payload):
        return "runtime_history_schema_outdated"
    try:
        history_mtime = history_path.stat().st_mtime
    except OSError:
        return "runtime_history_mtime_unavailable"
    if scan.latest_task_mtime is not None and history_mtime + 1e-9 < scan.latest_task_mtime:
        return "runtime_history_stale_vs_tasks"
    return None


def _runtime_history_missing_fields(payload: Mapping[str, object]) -> bool:
    if not isinstance(payload.get("groups"), Mapping):
        return True
    if not isinstance(payload.get("history_scan"), Mapping):
        return True
    if not isinstance(payload.get("retention"), Mapping):
        return True
    if _optional_text(payload.get("runtime_history_path")) is None:
        return True
    return _runtime_rows_missing_fields(_runtime_rows_payload(payload.get("rows")))


def _normalize_runtime_history_payload(
    payload: Mapping[str, object],
    *,
    tasks_root: Path,
    scan: ConsoleTaskHistoryScan,
) -> dict[str, object]:
    normalized = _mapping_payload(payload)
    rows = _runtime_rows_payload(normalized.get("rows"))
    groups_payload = _mapping_or_none(normalized.get("groups")) or {}
    groups: dict[str, dict[str, object]] = {}
    for status_group in ("active", "terminal", "failed"):
        group_payload = _mapping_or_none(groups_payload.get(status_group)) or {}
        group_rows = _runtime_rows_payload(group_payload.get("rows"))
        if not group_rows:
            group_rows = _filter_runtime_rows_by_group(
                rows,
                status_group=status_group,
                limit=CONSOLE_RUNTIME_HISTORY_GROUP_LIMIT,
            )
        latest = _runtime_marker_from_rows(group_rows, existing=_mapping_or_none(group_payload.get("latest")))
        task_count = _optional_int(group_payload.get("task_count"))
        if task_count is None:
            task_count = sum(1 for record in scan.records if _status_matches_group(record.status, status_group))
        groups[status_group] = {
            "task_count": task_count,
            "latest": latest,
            "rows": group_rows,
        }
    normalized.update(
        {
            "domain": "console",
            "dataset": "console_runtime_history",
            "tasks_root": str(tasks_root),
            "state_root": str(_runtime_state_root(tasks_root)),
            "runtime_summary_path": str(_runtime_summary_path(tasks_root)),
            "runtime_history_path": str(_runtime_history_path(tasks_root)),
            "retention": _runtime_retention_mapping(
                normalized.get("retention"),
                total_task_count=_optional_int(normalized.get("task_count")) or len(scan.records),
                row_limit=_optional_int(normalized.get("history_limit")) or CONSOLE_RUNTIME_HISTORY_LIMIT,
            ),
            "task_count": _optional_int(normalized.get("task_count")) or len(scan.records),
            "row_count": _optional_int(normalized.get("row_count")) or len(rows),
            "history_limit": _optional_int(normalized.get("history_limit")) or max(len(rows), CONSOLE_RUNTIME_HISTORY_LIMIT),
            "updated_at": _optional_text(normalized.get("updated_at")) or _runtime_marker_from_rows(rows).get("updated_at"),
            "rows": rows,
            "groups": groups,
            "history_scan": _runtime_history_scan_payload(scan),
        }
    )
    return normalized


def _runtime_history_recovery_payload(
    *,
    source: str,
    recovered: bool,
    reason: str | None,
    history_path: Path,
    scan: ConsoleTaskHistoryScan,
) -> dict[str, object]:
    return {
        "source": source,
        "recovered": bool(recovered),
        "reason": reason,
        "history_path": str(history_path),
        "history_path_exists": history_path.exists(),
        "invalid_task_file_count": len(scan.invalid_files),
    }


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


def _scan_issue(path: Path, exc: Exception) -> ConsoleTaskScanIssue:
    return ConsoleTaskScanIssue(
        path=str(path),
        error_type=exc.__class__.__name__,
        message=str(exc),
    )


def _scan_issue_label(issue: ConsoleTaskScanIssue | None) -> str | None:
    if issue is None:
        return None
    return f"{issue.error_type}: {issue.message}"


def _rewrite_root_for_tasks_dir(tasks_root: Path) -> Path:
    if tasks_root.name == "tasks" and tasks_root.parent.name == "console" and tasks_root.parent.parent.name == "var":
        return tasks_root.parents[2]
    return tasks_root


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
        for value in action_ids:
            token = _optional_text(value)
            if token is not None:
                values.append(token)
    if not values:
        return None
    return set(values)


def _normalized_status_group(value: object) -> str | None:
    token = _optional_text(value)
    if token is None:
        return None
    if token not in {"active", "terminal", "failed"}:
        raise ValueError(f"Unsupported status_group {value!r}. Expected one of: active, terminal, failed")
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
    scan: ConsoleTaskHistoryScan | None = None,
) -> dict[str, object]:
    ordered = sorted(records, key=lambda item: (item.created_at, item.task_id), reverse=True)
    status_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    for record in ordered:
        status_counts[record.status] = status_counts.get(record.status, 0) + 1
        action_counts[record.action_id] = action_counts.get(record.action_id, 0) + 1
    status_group_counts = {
        "active": sum(1 for record in ordered if _status_matches_group(record.status, "active")),
        "terminal": sum(1 for record in ordered if _status_matches_group(record.status, "terminal")),
        "failed": sum(1 for record in ordered if _status_matches_group(record.status, "failed")),
    }
    recent_tasks = [
        _runtime_recent_task_row(record, tasks_root=tasks_root)
        for record in ordered[:CONSOLE_RUNTIME_SUMMARY_RECENT_LIMIT]
    ]
    recent_active_tasks = _runtime_recent_group_rows(
        ordered,
        status_group="active",
        limit=CONSOLE_RUNTIME_SUMMARY_GROUP_LIMIT,
        tasks_root=tasks_root,
    )
    recent_terminal_tasks = _runtime_recent_group_rows(
        ordered,
        status_group="terminal",
        limit=CONSOLE_RUNTIME_SUMMARY_GROUP_LIMIT,
        tasks_root=tasks_root,
    )
    recent_failed_tasks = _runtime_recent_group_rows(
        ordered,
        status_group="failed",
        limit=CONSOLE_RUNTIME_SUMMARY_GROUP_LIMIT,
        tasks_root=tasks_root,
    )
    latest = _runtime_latest_group_marker(ordered, tasks_root=tasks_root)
    latest_active = _runtime_latest_group_marker(ordered, status_group="active", tasks_root=tasks_root)
    latest_terminal = _runtime_latest_group_marker(ordered, status_group="terminal", tasks_root=tasks_root)
    latest_failed = _runtime_latest_group_marker(ordered, status_group="failed", tasks_root=tasks_root)
    latest_updated_at = ordered[0].updated_at if ordered else None
    latest_task_id = ordered[0].task_id if ordered else None
    return {
        "domain": "console",
        "dataset": "console_runtime_summary",
        "tasks_root": str(tasks_root),
        "state_root": str(_runtime_state_root(tasks_root)),
        "runtime_summary_path": str(_runtime_summary_path(tasks_root)),
        "runtime_history_path": str(_runtime_history_path(tasks_root)),
        "history_retention": _runtime_retention_payload(
            total_task_count=len(ordered),
            row_limit=CONSOLE_RUNTIME_HISTORY_LIMIT,
            group_row_limit=CONSOLE_RUNTIME_HISTORY_GROUP_LIMIT,
        ),
        "task_count": len(ordered),
        "latest_task_id": latest_task_id,
        "updated_at": latest_updated_at,
        "status_counts": status_counts,
        "status_group_counts": status_group_counts,
        "action_counts": action_counts,
        "history_scan": _runtime_history_scan_payload(scan),
        "history_groups": _runtime_history_groups_payload(
            action_counts=action_counts,
            status_group_counts=status_group_counts,
            rows=recent_tasks,
            latest_markers={
                "latest": latest,
                "active": latest_active,
                "terminal": latest_terminal,
                "failed": latest_failed,
            },
        ),
        "latest_active_task_id": latest_active.get("task_id"),
        "latest_active_updated_at": latest_active.get("updated_at"),
        "latest_terminal_task_id": latest_terminal.get("task_id"),
        "latest_terminal_updated_at": latest_terminal.get("updated_at"),
        "latest_failed_task_id": latest_failed.get("task_id"),
        "latest_failed_updated_at": latest_failed.get("updated_at"),
        "latest_markers": {
            "latest": latest,
            "active": latest_active,
            "terminal": latest_terminal,
            "failed": latest_failed,
        },
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
    scan: ConsoleTaskHistoryScan | None = None,
    limit: int = CONSOLE_RUNTIME_HISTORY_LIMIT,
) -> dict[str, object]:
    ordered = sorted(records, key=lambda item: (item.created_at, item.task_id), reverse=True)
    resolved_limit = max(int(limit), 0)
    rows = [_runtime_recent_task_row(record, tasks_root=tasks_root) for record in ordered[:resolved_limit]]
    groups = {
        status_group: {
            "task_count": sum(1 for record in ordered if _status_matches_group(record.status, status_group)),
            "latest": _runtime_latest_group_marker(ordered, status_group=status_group, tasks_root=tasks_root),
            "rows": _runtime_recent_group_rows(
                ordered,
                status_group=status_group,
                limit=CONSOLE_RUNTIME_HISTORY_GROUP_LIMIT,
                tasks_root=tasks_root,
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
    error_summary = _runtime_error_summary(record.error, result=record.result)
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
        "error_summary": error_summary,
        "error_detail": _runtime_error_detail(record.error, result=record.result),
        "primary_output_label": primary_output.get("label"),
        "primary_output_path": primary_output.get("path"),
        "result_paths": result_paths,
        "linked_objects": _runtime_linked_objects(record.action_id, result_paths),
    }


def _runtime_recent_group_rows(
    records: Sequence[ConsoleTaskRecord],
    *,
    status_group: str,
    limit: int = 6,
    tasks_root: Path,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for record in records:
        if not _status_matches_group(record.status, status_group):
            continue
        rows.append(_runtime_recent_task_row(record, tasks_root=tasks_root))
        if len(rows) >= limit:
            break
    return rows


def _runtime_latest_group_marker(
    records: Sequence[ConsoleTaskRecord],
    *,
    status_group: str | None = None,
    tasks_root: Path,
) -> dict[str, object]:
    for record in records:
        if status_group is not None and not _status_matches_group(record.status, status_group):
            continue
        return _runtime_task_marker(record, tasks_root=tasks_root)
    return {}


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
    parts = [
        part
        for part in (
            _optional_text(progress.summary),
            _optional_text(progress.current_stage).replace("_", " ") if _optional_text(progress.current_stage) else None,
        )
        if part
    ]
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
        "source_training_run",
        "offsets",
        "suite_mode",
        "suite_spec_path",
        "window_start",
        "window_end",
        "markets",
        "feature_set",
        "label_set",
        "feature_set_variants",
        "stakes_usd",
        "max_notional_usd",
        "runtime_policy",
        "compare_policy",
    ):
        value = request.get(key)
        if value in (None, "", []):
            continue
        summary[key] = _json_safe(value)
    return summary


def _runtime_subject_summary(action_id: str, request: Mapping[str, object]) -> str | None:
    market = _optional_text(request.get("market"))
    cycle = _optional_text(request.get("cycle"))
    market_scope = " / ".join(part for part in (market, cycle) if part)
    if action_id == "data_sync":
        return _join_runtime_subject(_optional_text(request.get("sync_command")), market_scope)
    if action_id == "data_build":
        return _join_runtime_subject(_optional_text(request.get("build_command")), market_scope)
    if action_id == "research_train_run":
        return _join_runtime_subject(_optional_text(request.get("run_label")), market_scope)
    if action_id == "research_bundle_build":
        bundle_label = _optional_text(request.get("bundle_label"))
        source_training_run = _optional_text(request.get("source_training_run"))
        primary = bundle_label if source_training_run is None else f"{bundle_label} from {source_training_run}"
        return _join_runtime_subject(primary, market_scope)
    if action_id == "research_activate_bundle":
        return _join_runtime_subject(_optional_text(request.get("bundle_label")), market_scope)
    if action_id == "research_backtest_run":
        primary = " / ".join(
            part for part in (_optional_text(request.get("spec")), _optional_text(request.get("run_label"))) if part
        )
        return _join_runtime_subject(primary or None, market_scope)
    if action_id == "research_experiment_run_suite":
        primary = " / ".join(
            part for part in (_optional_text(request.get("suite")), _optional_text(request.get("run_label"))) if part
        )
        if _optional_text(request.get("suite_mode")) == "inline" and primary:
            primary = f"{primary} [inline]"
        return _join_runtime_subject(primary or None, market_scope)
    return market_scope or None


def _join_runtime_subject(primary: str | None, scope: str | None) -> str | None:
    values = [part for part in (primary, scope) if part]
    if not values:
        return None
    return " | ".join(values)


def _runtime_result_summary(result: object) -> str | None:
    if not isinstance(result, Mapping):
        return _optional_text(result)
    dataset = _optional_text(result.get("dataset")) or _optional_text(result.get("object_type"))
    label = (
        _optional_text(result.get("bundle_label"))
        or _optional_text(result.get("run_label"))
        or _optional_text(result.get("suite_name"))
        or _optional_text(result.get("task_id"))
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
        summary = result.get("execution_summary")
        if isinstance(summary, Mapping):
            last_stderr_line = _optional_text(summary.get("last_stderr_line"))
            if last_stderr_line is not None:
                return last_stderr_line
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
    for label in ("bundle_dir", "selection_path", "run_dir", "suite_spec_path", "summary_path", "report_path", "manifest_path"):
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
    path_lookup = {
        str(item.get("label")): str(item.get("path"))
        for item in result_paths
        if _optional_text(item.get("label")) is not None and _optional_text(item.get("path")) is not None
    }
    rows: list[dict[str, str]] = []

    def _append(object_type: str, *, path_label: str, title: str) -> None:
        path = _optional_text(path_lookup.get(path_label))
        if path is None:
            return
        row = {
            "object_type": object_type,
            "title": title,
            "path": path,
        }
        for extra_label in ("summary_path", "report_path", "manifest_path"):
            extra_path = _optional_text(path_lookup.get(extra_label))
            if extra_path is not None:
                row[extra_label] = extra_path
        rows.append(row)

    if action_id == "research_train_run":
        _append("training_run", path_label="run_dir", title="Training Run")
    elif action_id == "research_bundle_build":
        _append("model_bundle", path_label="bundle_dir", title="Model Bundle")
    elif action_id == "research_activate_bundle":
        _append("active_bundle_selection", path_label="selection_path", title="Active Bundle Selection")
        _append("model_bundle", path_label="bundle_dir", title="Model Bundle")
    elif action_id == "research_backtest_run":
        _append("backtest_run", path_label="run_dir", title="Backtest Run")
    elif action_id == "research_experiment_run_suite":
        _append("experiment_run", path_label="run_dir", title="Experiment Run")
    return rows


def _runtime_error_detail(
    error: object,
    *,
    result: object | None = None,
) -> dict[str, object]:
    rows: dict[str, object] = {}
    if isinstance(error, Mapping):
        error_type = _optional_text(error.get("type"))
        error_message = _optional_text(error.get("message"))
        if error_type is not None:
            rows["type"] = error_type
        if error_message is not None:
            rows["message"] = error_message
    else:
        error_message = _optional_text(error)
        if error_message is not None:
            rows["message"] = error_message
    if not isinstance(result, Mapping):
        return rows
    status = _optional_text(result.get("status"))
    if status is not None:
        rows["result_status"] = status
    return_code = result.get("return_code")
    if return_code is not None:
        rows["return_code"] = return_code
    stderr_text = _optional_text(result.get("stderr"))
    if stderr_text is not None:
        stderr_lines = [line.strip() for line in stderr_text.splitlines() if line.strip()]
        if stderr_lines:
            rows["stderr_excerpt"] = stderr_lines[-3:]
    execution_summary = result.get("execution_summary")
    if isinstance(execution_summary, Mapping):
        last_stderr_line = _optional_text(execution_summary.get("last_stderr_line"))
        if last_stderr_line is not None:
            rows["last_stderr_line"] = last_stderr_line
    return rows


def _runtime_task_marker(
    record: ConsoleTaskRecord,
    *,
    tasks_root: Path,
) -> dict[str, object]:
    return _runtime_row_marker(_runtime_recent_task_row(record, tasks_root=tasks_root))


def _runtime_history_scan_payload(scan: ConsoleTaskHistoryScan | None) -> dict[str, object]:
    if scan is None:
        return {
            "task_file_count": 0,
            "valid_task_count": 0,
            "invalid_task_file_count": 0,
            "invalid_task_files": [],
            "has_invalid_task_files": False,
        }
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
    resolved_total = max(int(total_task_count), 0)
    resolved_row_limit = max(int(row_limit), 0)
    resolved_group_row_limit = max(int(group_row_limit), 0)
    retained_task_count = min(resolved_total, resolved_row_limit)
    dropped_task_count = max(resolved_total - retained_task_count, 0)
    return {
        "total_task_count": resolved_total,
        "retained_task_count": retained_task_count,
        "dropped_task_count": dropped_task_count,
        "is_truncated": dropped_task_count > 0,
        "row_limit": resolved_row_limit,
        "group_row_limit": resolved_group_row_limit,
    }


def _runtime_retention_mapping(
    value: object,
    *,
    total_task_count: int,
    row_limit: int = CONSOLE_RUNTIME_HISTORY_LIMIT,
    group_row_limit: int = CONSOLE_RUNTIME_HISTORY_GROUP_LIMIT,
) -> dict[str, object]:
    payload = _mapping_or_none(value)
    if payload is None:
        return _runtime_retention_payload(
            total_task_count=total_task_count,
            row_limit=row_limit,
            group_row_limit=group_row_limit,
        )
    resolved_total = _optional_int(payload.get("total_task_count"))
    resolved_row_limit = _optional_int(payload.get("row_limit"))
    resolved_group_row_limit = _optional_int(payload.get("group_row_limit"))
    retained_task_count = _optional_int(payload.get("retained_task_count"))
    dropped_task_count = _optional_int(payload.get("dropped_task_count"))
    is_truncated = payload.get("is_truncated")
    computed = _runtime_retention_payload(
        total_task_count=resolved_total if resolved_total is not None else total_task_count,
        row_limit=resolved_row_limit if resolved_row_limit is not None else row_limit,
        group_row_limit=resolved_group_row_limit if resolved_group_row_limit is not None else group_row_limit,
    )
    if retained_task_count is not None:
        computed["retained_task_count"] = max(int(retained_task_count), 0)
    if dropped_task_count is not None:
        computed["dropped_task_count"] = max(int(dropped_task_count), 0)
    if is_truncated is not None:
        computed["is_truncated"] = bool(is_truncated)
    return computed


def _runtime_history_groups_payload(
    *,
    action_counts: Mapping[str, int],
    status_group_counts: Mapping[str, int],
    rows: Sequence[Mapping[str, object]],
    latest_markers: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    action_markers: dict[str, dict[str, object]] = {}
    for row in rows:
        action_id = _optional_text(row.get("action_id"))
        if action_id is None or action_id in action_markers:
            continue
        action_markers[action_id] = _runtime_row_marker(row)
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
                "latest_marker": action_markers.get(action_id, {}),
            }
            for action_id, count in sorted(
                action_counts.items(),
                key=lambda item: (
                    _optional_text(action_markers.get(item[0], {}).get("updated_at")) or "",
                    item[0],
                ),
                reverse=True,
            )
        ],
    }


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


def _runtime_rows_payload(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _runtime_rows_missing_fields(rows: Sequence[Mapping[str, object]]) -> bool:
    if not rows:
        return False
    required_keys = {
        "task_path",
        "status_group",
        "error_detail",
        "result_paths",
        "linked_objects",
    }
    first = rows[0]
    return any(key not in first for key in required_keys)


def _filter_runtime_rows_by_group(
    rows: Sequence[Mapping[str, object]],
    *,
    status_group: str,
    limit: int = 6,
) -> list[dict[str, object]]:
    matched: list[dict[str, object]] = []
    for row in rows:
        status = _optional_text(row.get("status")) or ""
        if not _status_matches_group(status, status_group):
            continue
        matched.append(dict(row))
        if len(matched) >= limit:
            break
    return matched


def _runtime_summary_markers_from_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    active_rows: Sequence[Mapping[str, object]],
    terminal_rows: Sequence[Mapping[str, object]],
    failed_rows: Sequence[Mapping[str, object]],
    existing: Mapping[str, object] | None = None,
) -> dict[str, dict[str, object]]:
    existing_mapping = existing or {}
    markers = {
        "latest": _runtime_row_marker(rows[0]) if rows else {},
        "active": _runtime_row_marker(active_rows[0]) if active_rows else {},
        "terminal": _runtime_row_marker(terminal_rows[0]) if terminal_rows else {},
        "failed": _runtime_row_marker(failed_rows[0]) if failed_rows else {},
    }
    for key in ("latest", "active", "terminal", "failed"):
        persisted = _mapping_or_none(existing_mapping.get(key))
        if persisted is not None and not markers[key]:
            markers[key] = {str(name): value for name, value in persisted.items()}
    return markers


def _runtime_marker_from_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    existing: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if rows:
        return _runtime_row_marker(rows[0])
    if existing is None:
        return {}
    return {str(name): value for name, value in existing.items()}


def _int_mapping_payload(value: object) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    rows: dict[str, int] = {}
    for key, item in value.items():
        try:
            rows[str(key)] = int(item)
        except Exception:
            continue
    return rows


def _status_group_counts_payload(
    value: object,
    *,
    recent_tasks: Sequence[Mapping[str, object]],
) -> dict[str, int]:
    rows = _int_mapping_payload(value)
    if rows:
        return {
            "active": int(rows.get("active", 0)),
            "terminal": int(rows.get("terminal", 0)),
            "failed": int(rows.get("failed", 0)),
        }
    return {
        "active": sum(1 for row in recent_tasks if _runtime_status_group(row.get("status")) == "active"),
        "terminal": sum(1 for row in recent_tasks if _runtime_status_group(row.get("status")) == "terminal"),
        "failed": sum(1 for row in recent_tasks if _runtime_status_group(row.get("status")) == "failed"),
    }


def _execute_direct_research_action(context: ConsoleTaskRunContext) -> object | None:
    if context.action_id == "research_train_run":
        return _run_direct_training_task(context)
    if context.action_id == "research_bundle_build":
        return _run_direct_bundle_build_task(context)
    if context.action_id == "research_activate_bundle":
        return _run_direct_bundle_activate_task(context)
    if context.action_id == "research_backtest_run":
        return _run_direct_backtest_task(context)
    if context.action_id == "research_experiment_run_suite":
        return _run_direct_experiment_task(context)
    return None


def _run_direct_training_task(context: ConsoleTaskRunContext) -> dict[str, object]:
    from pm15min.research.config import ResearchConfig
    from pm15min.research.contracts import DateWindow, TrainingRunSpec
    from pm15min.research.training.runner import train_research_run

    request = context.request
    cfg = ResearchConfig.build(
        market=str(request.get("market") or "sol"),
        cycle=str(request.get("cycle") or "15m"),
        profile=str(request.get("profile") or "deep_otm"),
        source_surface="backtest",
        feature_set=str(request.get("feature_set") or "deep_otm_v1"),
        label_set=str(request.get("label_set") or "truth"),
        target=str(request.get("target") or "direction"),
        model_family=str(request.get("model_family") or "deep_otm"),
        root=context.rewrite_root,
    )
    spec = TrainingRunSpec(
        model_family=str(request.get("model_family") or "deep_otm"),
        feature_set=str(request.get("feature_set") or "deep_otm_v1"),
        label_set=str(request.get("label_set") or "truth"),
        target=str(request.get("target") or "direction"),
        window=DateWindow.from_bounds(
            str(request.get("window_start") or ""),
            str(request.get("window_end") or ""),
        ),
        run_label=str(request.get("run_label") or "planned"),
        offsets=_coerce_offsets(request.get("offsets")),
        label_source=_optional_text(request.get("label_source")),
    )
    context.report_progress(
        "Preparing training runtime",
        current_stage="prepare",
        progress_pct=20,
    )
    try:
        return train_research_run(
            cfg,
            spec,
            reporter=context.report_progress,
        )
    except TypeError:
        return train_research_run(cfg, spec)


def _run_direct_backtest_task(context: ConsoleTaskRunContext) -> dict[str, object]:
    from pm15min.research.backtests.engine import run_research_backtest
    from pm15min.research.config import ResearchConfig
    from pm15min.research.contracts import BacktestRunSpec

    request = context.request
    cfg = ResearchConfig.build(
        market=str(request.get("market") or "sol"),
        cycle=str(request.get("cycle") or "15m"),
        profile=str(request.get("profile") or "deep_otm"),
        source_surface="backtest",
        target=str(request.get("target") or "direction"),
        model_family=str(request.get("model_family") or "deep_otm"),
        root=context.rewrite_root,
    )
    spec = BacktestRunSpec(
        profile=str(request.get("profile") or "deep_otm"),
        spec_name=str(request.get("spec") or "baseline_truth"),
        run_label=str(request.get("run_label") or "planned"),
        target=str(request.get("target") or "direction"),
        bundle_label=_optional_text(request.get("bundle_label")),
    )
    context.report_progress(
        "Preparing backtest runtime",
        current_stage="prepare",
        progress_pct=20,
    )
    try:
        return run_research_backtest(
            cfg,
            spec,
            reporter=context.report_progress,
        )
    except TypeError:
        return run_research_backtest(cfg, spec)


def _run_direct_bundle_build_task(context: ConsoleTaskRunContext) -> dict[str, object]:
    from pm15min.research.bundles.builder import build_model_bundle
    from pm15min.research.config import ResearchConfig
    from pm15min.research.contracts import ModelBundleSpec

    request = context.request
    cfg = ResearchConfig.build(
        market=str(request.get("market") or "sol"),
        cycle=str(request.get("cycle") or "15m"),
        profile=str(request.get("profile") or "deep_otm"),
        source_surface="backtest",
        target=str(request.get("target") or "direction"),
        model_family=str(request.get("model_family") or "deep_otm"),
        root=context.rewrite_root,
    )
    spec = ModelBundleSpec(
        profile=str(request.get("profile") or "deep_otm"),
        target=str(request.get("target") or "direction"),
        bundle_label=str(request.get("bundle_label") or "planned"),
        offsets=_coerce_offsets(request.get("offsets")),
        source_training_run=_optional_text(request.get("source_training_run")),
    )
    context.report_progress(
        "Preparing bundle build runtime",
        current_stage="prepare",
        progress_pct=20,
    )
    return build_model_bundle(cfg, spec)


def _run_direct_bundle_activate_task(context: ConsoleTaskRunContext) -> dict[str, object]:
    from pm15min.research.config import ResearchConfig
    from pm15min.research.service import activate_model_bundle

    request = context.request
    cfg = ResearchConfig.build(
        market=str(request.get("market") or "sol"),
        cycle=str(request.get("cycle") or "15m"),
        profile=str(request.get("profile") or "deep_otm"),
        source_surface="backtest",
        target=str(request.get("target") or "direction"),
        model_family=str(request.get("model_family") or "deep_otm"),
        root=context.rewrite_root,
    )
    context.report_progress(
        "Preparing bundle activation runtime",
        current_stage="prepare",
        progress_pct=20,
    )
    return activate_model_bundle(
        cfg,
        profile=str(request.get("profile") or "deep_otm"),
        target=str(request.get("target") or "direction"),
        bundle_label=_optional_text(request.get("bundle_label")),
        notes=_optional_text(request.get("notes")),
    )


def _run_direct_experiment_task(context: ConsoleTaskRunContext) -> dict[str, object]:
    from pm15min.research.config import ResearchConfig
    from pm15min.research.experiments.runner import run_experiment_suite

    request = context.request
    cfg = ResearchConfig.build(
        market=str(request.get("market") or "sol"),
        cycle=str(request.get("cycle") or "15m"),
        profile=str(request.get("profile") or "deep_otm"),
        source_surface="backtest",
        target=str(request.get("target") or "direction"),
        model_family=str(request.get("model_family") or "deep_otm"),
        root=context.rewrite_root,
    )
    suite_name = str(request.get("suite") or "")
    suite_mode = _normalize_experiment_suite_mode(request.get("suite_mode"))
    suite_spec_path: Path | None = None
    runner_suite_name = suite_name
    if suite_mode == "inline":
        inline_suite_payload = _mapping_or_none(request.get("inline_suite_payload"))
        if inline_suite_payload is None:
            raise ValueError("Inline experiment suite 缺少 inline_suite_payload。")
        suite_spec_path = cfg.layout.storage.suite_spec_path(suite_name)
        _write_json_atomically(suite_spec_path, inline_suite_payload)
        runner_suite_name = str(suite_spec_path)
        context.report_progress(
            "Materializing inline suite spec",
            current_stage="prepare",
            progress_pct=10,
        )
    context.report_progress(
        "Preparing experiment runtime",
        current_stage="prepare",
        progress_pct=20,
    )
    try:
        result = run_experiment_suite(
            cfg=cfg,
            suite_name=runner_suite_name,
            run_label=str(request.get("run_label") or "planned"),
            reporter=context.report_progress,
        )
    except TypeError:
        result = run_experiment_suite(
            cfg=cfg,
            suite_name=runner_suite_name,
            run_label=str(request.get("run_label") or "planned"),
        )
    return _attach_experiment_suite_artifacts(
        result,
        suite_mode=suite_mode,
        suite_spec_path=suite_spec_path,
    )


def _normalize_experiment_suite_mode(value: object) -> str:
    token = _optional_text(value) or "existing"
    token = token.strip().lower()
    if token not in {"existing", "inline"}:
        raise ValueError("Experiment suite mode only supports existing or inline.")
    return token


def _attach_experiment_suite_artifacts(
    result: object,
    *,
    suite_mode: str,
    suite_spec_path: Path | None,
) -> dict[str, object]:
    payload = dict(result) if isinstance(result, Mapping) else {"value": result}
    payload.setdefault("suite_mode", suite_mode)
    if suite_spec_path is not None:
        payload["suite_spec_path"] = str(suite_spec_path)
    return payload


def _coerce_offsets(value: object) -> tuple[int, ...]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(int(item) for item in value)
    text = _optional_text(value)
    if text is None:
        return ()
    return tuple(int(item.strip()) for item in text.split(",") if item.strip())


def _progress_snapshot(
    progress: ConsoleTaskProgress,
    *,
    summary: str | None = None,
    current: int | None = None,
    total: int | None = None,
    current_stage: str | None = None,
    progress_pct: int | None = None,
    heartbeat: str | None = None,
) -> ConsoleTaskProgress:
    next_current = progress.current if current is None else int(current)
    next_total = progress.total if total is None else int(total)
    if progress_pct is None:
        if next_current is not None and next_total not in (None, 0):
            progress_pct = int(max(0, min(100, round((float(next_current) / float(next_total)) * 100))))
        else:
            progress_pct = progress.progress_pct
    return ConsoleTaskProgress(
        summary=progress.summary if summary is None else str(summary),
        current=next_current,
        total=next_total,
        current_stage=progress.current_stage if current_stage is None else str(current_stage),
        progress_pct=None if progress_pct is None else int(progress_pct),
        heartbeat=progress.heartbeat if heartbeat is None else str(heartbeat),
    )


def _default_progress_stage(status: str) -> str:
    if status == TASK_STATUS_QUEUED:
        return "queued"
    if status == TASK_STATUS_RUNNING:
        return "running"
    return "finished"


def _normalize_loaded_progress(
    progress: ConsoleTaskProgress,
    *,
    status: str,
    updated_at: str,
) -> ConsoleTaskProgress:
    return _progress_snapshot(
        progress,
        current_stage=progress.current_stage or _default_progress_stage(status),
        heartbeat=progress.heartbeat or updated_at,
    )


__all__ = [
    "ConsoleTaskManager",
    "ConsoleTaskProgress",
    "ConsoleTaskRecord",
    "ConsoleTaskRunContext",
    "TASK_STATUS_FAILED",
    "TASK_STATUS_QUEUED",
    "TASK_STATUS_RUNNING",
    "TASK_STATUS_SUCCEEDED",
    "default_console_runtime_state_root",
    "default_console_tasks_root",
    "get_console_task",
    "load_console_runtime_history",
    "load_console_runtime_summary",
    "load_console_task",
    "list_console_tasks",
    "submit_console_action_task",
    "submit_console_task",
]
