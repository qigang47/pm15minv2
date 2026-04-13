from __future__ import annotations

import json
import os
from pathlib import Path


def _write_console_task(
    root: Path,
    *,
    task_id: str,
    action_id: str,
    status: str,
    request: dict[str, object],
    command_preview: str,
    created_at: str = "2026-01-02T03:04:05Z",
    updated_at: str = "2026-01-02T03:05:06Z",
    started_at: str | None = "2026-01-02T03:04:10Z",
    finished_at: str | None = None,
    result: object | None = None,
    error: object | None = None,
    progress: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = {
        "task_id": task_id,
        "action_id": action_id,
        "status": status,
        "created_at": created_at,
        "updated_at": updated_at,
        "started_at": started_at,
        "finished_at": finished_at,
        "request": request,
        "command_preview": command_preview,
        "result": result,
        "error": error,
        "progress": progress
        or {
            "summary": "Completed" if finished_at else "Running",
            "current": 1,
            "total": 1,
            "current_stage": "finished" if finished_at else "running",
            "progress_pct": 100 if finished_at else 50,
            "heartbeat": finished_at or updated_at,
        },
    }
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{task_id}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def test_action_plan_uses_pm5min_command_preview_and_5m_defaults() -> None:
    from pm5min.console.actions import build_console_action_request

    plan = build_console_action_request(
        "research_bundle_build",
        {
            "market": "SOL",
        },
    )

    assert plan["command_preview"].startswith("PYTHONPATH=src python -m pm5min research bundle build")
    assert "--cycle 5m" in plan["command_preview"]
    assert "--profile deep_otm_5m" in plan["command_preview"]
    assert plan["normalized_request"]["market"] == "sol"
    assert plan["normalized_request"]["cycle"] == "5m"
    assert plan["normalized_request"]["profile"] == "deep_otm_5m"


def test_task_reads_stay_local_and_load_from_task_files(tmp_path: Path) -> None:
    module_path = Path(__file__).resolve().parents[1] / "src" / "pm5min" / "console" / "tasks.py"
    assert module_path.exists()
    assert "pm15min.console.tasks" not in module_path.read_text(encoding="utf-8")

    from pm5min.console.tasks import list_console_tasks, load_console_task

    root = tmp_path / "tasks"
    task = _write_console_task(
        root,
        task_id="task_local_read",
        action_id="research_backtest_run",
        status="running",
        request={
            "market": "sol",
            "cycle": "5m",
            "profile": "deep_otm_5m",
            "run_label": "planned",
            "spec": "baseline_truth",
        },
        command_preview="PYTHONPATH=src python -m pm5min research backtest run --market sol --cycle 5m --profile deep_otm_5m --spec baseline_truth --run-label planned",
        progress={
            "summary": "Dispatch",
            "current": 1,
            "total": 2,
            "current_stage": "dispatch",
            "progress_pct": 50,
            "heartbeat": "2026-01-02T03:05:06Z",
        },
    )

    listed = list_console_tasks(root=root)
    loaded = load_console_task(task_id=task["task_id"], root=root)

    assert [row["task_id"] for row in listed] == [task["task_id"]]
    assert listed[0]["request"]["profile"] == "deep_otm_5m"
    assert loaded["task_id"] == task["task_id"]
    assert loaded["command_preview"] == task["command_preview"]
    assert loaded["progress"]["current_stage"] == "dispatch"


def test_runtime_views_recover_from_task_files(tmp_path: Path) -> None:
    from pm5min.console.tasks import load_console_runtime_history, load_console_runtime_summary

    root = tmp_path / "tasks"
    task = _write_console_task(
        root,
        task_id="task_runtime_recover",
        action_id="research_train_run",
        status="succeeded",
        request={
            "market": "sol",
            "cycle": "5m",
            "profile": "deep_otm_5m",
            "run_label": "planned",
        },
        command_preview="PYTHONPATH=src python -m pm5min research train run --market sol --cycle 5m --profile deep_otm_5m --run-label planned",
        finished_at="2026-01-02T03:05:06Z",
        result={
            "status": "ok",
            "summary_path": "/tmp/run/summary.json",
        },
    )

    summary = load_console_runtime_summary(root=root)
    history = load_console_runtime_history(root=root)

    assert summary["dataset"] == "console_runtime_summary"
    assert summary["summary_source"] == "recovered_from_tasks"
    assert summary["task_count"] == 1
    assert summary["recent_tasks"][0]["task_id"] == task["task_id"]
    assert summary["recent_tasks"][0]["task_path"] == str(root / f"{task['task_id']}.json")
    assert summary["recent_tasks"][0]["status_group"] == "terminal"
    assert summary["recent_tasks"][0]["subject_summary"] == "planned | sol"
    assert summary["recent_tasks"][0]["result_summary"] == "artifact summary written"
    assert summary["recent_tasks"][0]["primary_output_path"] == "/tmp/run/summary.json"
    assert summary["latest_markers"]["terminal"]["task_id"] == task["task_id"]

    assert history["dataset"] == "console_runtime_history"
    assert history["history_source"] in {"persisted", "recovered_from_tasks"}
    assert history["task_count"] == 1
    assert history["rows"][0]["task_id"] == task["task_id"]
    assert history["groups"]["terminal"]["rows"][0]["task_id"] == task["task_id"]
    assert history["retention"]["retained_task_count"] == 1
    assert (tmp_path / "state" / "runtime_summary.json").exists()
    assert (tmp_path / "state" / "runtime_history.json").exists()


def test_runtime_views_keep_recovery_metadata_shape_for_persisted_reads(tmp_path: Path) -> None:
    from pm5min.console.tasks import load_console_runtime_history, load_console_runtime_summary

    root = tmp_path / "tasks"
    _write_console_task(
        root,
        task_id="task_recovery_shape",
        action_id="research_train_run",
        status="succeeded",
        request={"market": "sol", "cycle": "5m", "run_label": "shape"},
        command_preview="preview shape",
        finished_at="2026-01-02T03:05:06Z",
        result={"status": "ok", "summary_path": "/tmp/run/shape.json"},
    )

    recovered_summary = load_console_runtime_summary(root=root)
    recovered_history = load_console_runtime_history(root=root)
    persisted_summary = load_console_runtime_summary(root=root)
    persisted_history = load_console_runtime_history(root=root)

    assert recovered_summary["summary_recovery"]["source"] == "recovered_from_tasks"
    assert recovered_summary["summary_recovery"]["recovered"] is True
    assert "summary_recovery" in persisted_summary
    assert persisted_summary["summary_recovery"]["source"] == "persisted"
    assert persisted_summary["summary_recovery"]["recovered"] is False

    assert recovered_history["history_recovery"]["source"] in {"recovered_from_tasks", "persisted"}
    assert "history_recovery" in persisted_history
    assert persisted_history["history_recovery"]["source"] == "persisted"
    assert persisted_history["history_recovery"]["recovered"] is False


def test_runtime_views_refresh_when_new_task_file_arrives_after_recovery(tmp_path: Path) -> None:
    from pm5min.console.tasks import load_console_runtime_history, load_console_runtime_summary

    root = tmp_path / "tasks"
    first = _write_console_task(
        root,
        task_id="task_first",
        action_id="research_train_run",
        status="succeeded",
        request={"market": "sol", "cycle": "5m", "run_label": "first"},
        command_preview="preview first",
        created_at="2026-01-02T03:04:05Z",
        updated_at="2026-01-02T03:05:06Z",
        finished_at="2026-01-02T03:05:06Z",
        result={"status": "ok", "summary_path": "/tmp/run/first.json"},
    )

    first_summary = load_console_runtime_summary(root=root)
    first_history = load_console_runtime_history(root=root)
    assert first_summary["task_count"] == 1
    assert first_history["task_count"] == 1

    summary_path = tmp_path / "state" / "runtime_summary.json"
    history_path = tmp_path / "state" / "runtime_history.json"
    stale_mtime = max(summary_path.stat().st_mtime, history_path.stat().st_mtime)

    second = _write_console_task(
        root,
        task_id="task_second",
        action_id="research_train_run",
        status="running",
        request={"market": "sol", "cycle": "5m", "run_label": "second"},
        command_preview="preview second",
        created_at="2026-01-02T03:06:07Z",
        updated_at="2026-01-02T03:06:08Z",
    )
    os.utime(root / f"{second['task_id']}.json", (stale_mtime + 10.0, stale_mtime + 10.0))

    refreshed_summary = load_console_runtime_summary(root=root)
    refreshed_history = load_console_runtime_history(root=root)

    assert refreshed_summary["task_count"] == 2
    assert refreshed_summary["latest_task_id"] == second["task_id"]
    assert refreshed_summary["recent_tasks"][0]["task_id"] == second["task_id"]
    assert refreshed_history["task_count"] == 2
    assert refreshed_history["rows"][0]["task_id"] == second["task_id"]
    assert first["task_id"] in {row["task_id"] for row in refreshed_history["rows"]}


def test_runtime_summary_keeps_latest_terminal_marker_outside_recent_window(tmp_path: Path) -> None:
    from pm5min.console.tasks import load_console_runtime_summary

    root = tmp_path / "tasks"
    terminal = _write_console_task(
        root,
        task_id="task_terminal",
        action_id="research_train_run",
        status="succeeded",
        request={"market": "sol", "cycle": "5m", "run_label": "terminal"},
        command_preview="preview terminal",
        created_at="2026-01-02T03:00:00Z",
        updated_at="2026-01-02T03:00:10Z",
        finished_at="2026-01-02T03:00:10Z",
        result={"status": "ok", "summary_path": "/tmp/run/terminal.json"},
    )

    for index in range(12):
        _write_console_task(
            root,
            task_id=f"task_running_{index:02d}",
            action_id="research_train_run",
            status="running",
            request={"market": "sol", "cycle": "5m", "run_label": f"running_{index:02d}"},
            command_preview=f"preview running {index}",
            created_at=f"2026-01-02T03:{index + 1:02d}:00Z",
            updated_at=f"2026-01-02T03:{index + 1:02d}:30Z",
        )

    summary = load_console_runtime_summary(root=root)

    assert summary["recent_task_count"] == 12
    assert summary["latest_terminal_task_id"] == terminal["task_id"]
    assert summary["latest_markers"]["terminal"]["task_id"] == terminal["task_id"]
    assert summary["recent_terminal_tasks"][0]["task_id"] == terminal["task_id"]


def test_action_plan_normalizes_name_like_identifiers() -> None:
    from pm5min.console.actions import build_console_action_request

    bundle_plan = build_console_action_request(
        "research_bundle_build",
        {
            "bundle_label": " My Bundle ",
            "source_training_run": " Run Label ",
            "window_start": "2026-01-01",
            "window_end": "2026-01-02",
        },
    )
    backtest_plan = build_console_action_request(
        "research_backtest_run",
        {
            "bundle_label": " Main Bundle ",
            "secondary_bundle_label": " Hedge Bundle ",
        },
    )

    assert bundle_plan["normalized_request"]["bundle_label"] == "my_bundle"
    assert bundle_plan["normalized_request"]["source_training_run"] == "run_label"
    assert "--source-training-run run_label" in bundle_plan["command_preview"]
    assert backtest_plan["normalized_request"]["bundle_label"] == "main_bundle"
    assert backtest_plan["normalized_request"]["secondary_bundle_label"] == "hedge_bundle"
    assert "--bundle-label main_bundle" in backtest_plan["command_preview"]
    assert "--secondary-bundle-label hedge_bundle" in backtest_plan["command_preview"]
