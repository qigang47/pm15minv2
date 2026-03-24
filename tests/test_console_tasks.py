from __future__ import annotations

import json
from pathlib import Path
import threading
import time
import pytest

from pm15min.console.tasks import (
    ConsoleTaskManager,
    TASK_STATUS_FAILED,
    TASK_STATUS_RUNNING,
    TASK_STATUS_SUCCEEDED,
    default_console_runtime_state_root,
    default_console_tasks_root,
    get_console_task,
    list_console_tasks,
    load_console_runtime_history,
    load_console_runtime_summary,
    submit_console_action_task,
    submit_console_task,
)


def test_console_task_manager_submit_get_and_persist(tmp_path: Path) -> None:
    root = tmp_path / "tasks"
    started = threading.Event()
    release = threading.Event()

    def planner(action_id: str, request=None) -> dict[str, object]:
        assert request == {"market": "SOL"}
        return {
            "action_id": action_id,
            "normalized_request": {"market": "sol", "surface": "backtest"},
            "command_preview": "PYTHONPATH=v2/src python -m pm15min data show-summary --market sol --surface backtest",
        }

    def executor(context) -> dict[str, object]:
        context.report_progress("Collecting inputs", current=1, total=2)
        started.set()
        assert release.wait(timeout=2.0)
        context.report_progress("Finishing", current=2, total=2)
        return {
            "status": "ok",
            "task_output": {"market": context.request["market"]},
            "command_preview": context.command_preview,
        }

    manager = ConsoleTaskManager(root=root, planner=planner, executor=executor)

    submitted = manager.submit("data_refresh_summary", {"market": "SOL"})

    assert submitted["status"] == "queued"
    assert submitted["action_id"] == "data_refresh_summary"
    assert submitted["started_at"] is None
    assert submitted["finished_at"] is None
    assert submitted["request"] == {"market": "sol", "surface": "backtest"}
    assert submitted["command_preview"].endswith("--surface backtest")
    assert submitted["progress"]["current_stage"] == "queued"
    assert submitted["progress"]["progress_pct"] == 0
    assert submitted["progress"]["heartbeat"] == submitted["created_at"]
    assert (root / f"{submitted['task_id']}.json").exists()

    assert started.wait(timeout=2.0)
    running = _wait_for_task(
        submitted["task_id"],
        lambda task_id: manager.get(task_id),
        status=TASK_STATUS_RUNNING,
    )
    assert running["started_at"] is not None
    assert running["finished_at"] is None
    assert running["progress"]["summary"] in {"Running", "Collecting inputs"}
    assert running["progress"]["current_stage"] in {"running", "dispatch"}
    assert running["progress"]["progress_pct"] is not None
    assert running["progress"]["heartbeat"] is not None

    release.set()

    completed = _wait_for_task(
        submitted["task_id"],
        lambda task_id: get_console_task(task_id, root=root),
        status=TASK_STATUS_SUCCEEDED,
    )
    assert completed["result"]["status"] == "ok"
    assert completed["result"]["task_output"] == {"market": "sol"}
    assert completed["error"] is None
    assert completed["finished_at"] is not None
    assert completed["progress"] == {
        "summary": "Completed",
        "current": 2,
        "total": 2,
        "current_stage": "finished",
        "progress_pct": 100,
        "heartbeat": completed["finished_at"],
    }
    assert completed["created_at"]
    assert completed["updated_at"]

    listed = list_console_tasks(root=root)
    assert [row["task_id"] for row in listed] == [submitted["task_id"]]

    persisted = json.loads((root / f"{submitted['task_id']}.json").read_text(encoding="utf-8"))
    assert persisted["status"] == TASK_STATUS_SUCCEEDED
    assert persisted["result"]["task_output"] == {"market": "sol"}


def test_console_task_manager_records_executor_exception(tmp_path: Path) -> None:
    root = tmp_path / "tasks"

    def planner(action_id: str, request=None) -> dict[str, object]:
        return {
            "action_id": action_id,
            "normalized_request": dict(request or {}),
            "command_preview": f"preview {action_id}",
        }

    def executor(context) -> dict[str, object]:
        context.report_progress("Starting", current=1, total=1)
        raise RuntimeError("boom")

    manager = ConsoleTaskManager(root=root, planner=planner, executor=executor)

    submitted = manager.submit("explode_action", {"market": "sol"})
    failed = _wait_for_task(
        submitted["task_id"],
        lambda task_id: manager.get(task_id),
        status=TASK_STATUS_FAILED,
    )

    assert failed["result"] is None
    assert failed["error"] == {"type": "RuntimeError", "message": "boom"}
    assert failed["progress"] == {
        "summary": "Failed",
        "current": 1,
        "total": 1,
        "current_stage": "finished",
        "progress_pct": 100,
        "heartbeat": failed["finished_at"],
    }


def test_console_task_list_filters_and_top_level_submit(tmp_path: Path) -> None:
    root = tmp_path / "tasks"

    def planner(action_id: str, request=None) -> dict[str, object]:
        return {
            "action_id": action_id,
            "normalized_request": {"request_id": (request or {}).get("request_id")},
            "command_preview": f"preview {action_id}",
        }

    def executor(context) -> dict[str, object]:
        if context.action_id == "fail_action":
            context.report_progress("Broken", current=1, total=1)
            return {"status": "error", "stderr": "RuntimeError: broken"}
        return {"status": "ok", "value": context.action_id}

    ok_task = submit_console_task(
        action_id="ok_action",
        request={"request_id": "one"},
        root=root,
        planner=planner,
        executor=executor,
    )
    failed_task = submit_console_task(
        action_id="fail_action",
        request={"request_id": "two"},
        root=root,
        planner=planner,
        executor=executor,
    )

    _wait_for_task(ok_task["task_id"], lambda task_id: get_console_task(task_id, root=root), status=TASK_STATUS_SUCCEEDED)
    failed = _wait_for_task(failed_task["task_id"], lambda task_id: get_console_task(task_id, root=root), status=TASK_STATUS_FAILED)

    assert failed["error"] == {"message": "RuntimeError: broken"}
    assert get_console_task("missing-task", root=root) is None

    all_tasks = list_console_tasks(root=root)
    assert [row["task_id"] for row in all_tasks] == [failed_task["task_id"], ok_task["task_id"]]
    assert [row["task_id"] for row in list_console_tasks(root=root, status=TASK_STATUS_FAILED)] == [failed_task["task_id"]]
    assert [row["task_id"] for row in list_console_tasks(root=root, status_group="failed")] == [failed_task["task_id"]]
    assert [row["task_id"] for row in list_console_tasks(root=root, status_group="terminal")] == [failed_task["task_id"], ok_task["task_id"]]
    assert list_console_tasks(root=root, status_group="active") == []
    assert [row["task_id"] for row in list_console_tasks(root=root, action_id="ok_action")] == [ok_task["task_id"]]
    assert [row["task_id"] for row in list_console_tasks(root=root, action_ids=("fail_action", "ok_action"))] == [
        failed_task["task_id"],
        ok_task["task_id"],
    ]
    assert [row["task_id"] for row in list_console_tasks(root=root, limit=1)] == [failed_task["task_id"]]
    assert [row["task_id"] for row in list_console_tasks(root=root, action_ids=("ok_action", "fail_action"))] == [
        failed_task["task_id"],
        ok_task["task_id"],
    ]


def test_console_task_list_supports_active_status_group(tmp_path: Path) -> None:
    root = tmp_path / "tasks"
    started = threading.Event()
    release = threading.Event()

    manager = ConsoleTaskManager(
        root=root,
        planner=lambda action_id, request=None: {
            "action_id": action_id,
            "normalized_request": dict(request or {}),
            "command_preview": f"preview {action_id}",
        },
        executor=lambda context: (
            started.set(),
            release.wait(timeout=1.0),
            {"status": "ok"},
        )[-1],
        heartbeat_interval_sec=0.05,
    )

    submitted = manager.submit("run_action", {"market": "sol"})
    assert started.wait(timeout=1.0)
    active_rows = list_console_tasks(root=root, status_group="active")
    assert [row["task_id"] for row in active_rows] == [submitted["task_id"]]
    release.set()
    _wait_for_task(submitted["task_id"], lambda task_id: get_console_task(task_id, root=root), status=TASK_STATUS_SUCCEEDED)


def test_console_task_list_rejects_unknown_status_group(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unsupported status_group"):
        list_console_tasks(root=tmp_path / "tasks", status_group="weird")


def test_default_console_tasks_root_uses_v2_var_console_tasks(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("pm15min.console.tasks.rewrite_root", lambda: tmp_path / "v2")

    assert default_console_tasks_root() == tmp_path / "v2" / "var" / "console" / "tasks"
    assert default_console_runtime_state_root() == tmp_path / "v2" / "var" / "console" / "state"


def test_console_task_manager_accepts_rewrite_root_and_normalizes_tasks_dir(tmp_path: Path) -> None:
    manager = ConsoleTaskManager(root=tmp_path / "v2")
    assert manager.root == tmp_path / "v2" / "var" / "console" / "tasks"


def test_console_runtime_summary_persists_active_and_terminal_history(tmp_path: Path) -> None:
    root = tmp_path / "tasks"
    started = threading.Event()
    release = threading.Event()

    manager = ConsoleTaskManager(
        root=root,
        planner=lambda action_id, request=None: {
            "action_id": action_id,
            "normalized_request": {"market": "sol", "run_label": "planned"},
            "command_preview": f"preview {action_id}",
        },
        executor=lambda context: (
            context.report_progress("Dispatch", current=1, total=2),
            started.set(),
            release.wait(timeout=1.0),
            context.report_progress("Finalize", current=2, total=2),
            {"status": "ok", "summary_path": "/tmp/run/summary.json"},
        )[-1],
        heartbeat_interval_sec=0.05,
    )

    submitted = manager.submit("research_train_run", {"market": "sol"})
    assert started.wait(timeout=1.0)

    active_summary = load_console_runtime_summary(root=root)
    assert active_summary["dataset"] == "console_runtime_summary"
    assert active_summary["summary_source"] == "persisted"
    assert active_summary["task_count"] == 1
    assert active_summary["status_group_counts"]["active"] == 1
    assert active_summary["recent_tasks"][0]["task_id"] == submitted["task_id"]
    assert active_summary["recent_tasks"][0]["task_path"] == str(root / f"{submitted['task_id']}.json")
    assert active_summary["recent_tasks"][0]["status_label"] == "Running"
    assert active_summary["recent_tasks"][0]["status_group"] == "active"
    assert active_summary["recent_tasks"][0]["subject_summary"] == "planned | sol"
    assert active_summary["recent_tasks"][0]["progress_summary"] is not None
    assert active_summary["recent_tasks"][0]["error_detail"] == {}
    assert active_summary["recent_tasks"][0]["result_paths"] == []
    assert active_summary["recent_tasks"][0]["linked_objects"] == []
    assert active_summary["latest_markers"]["latest"]["task_id"] == submitted["task_id"]
    assert active_summary["latest_markers"]["latest"]["task_path"] == str(root / f"{submitted['task_id']}.json")
    assert active_summary["latest_active_task_id"] == submitted["task_id"]
    assert active_summary["latest_markers"]["active"]["task_id"] == submitted["task_id"]
    assert active_summary["recent_active_tasks"][0]["task_id"] == submitted["task_id"]
    assert active_summary["history_groups"]["status_group"][0]["group"] == "active"
    assert active_summary["history_groups"]["action_id"][0]["action_id"] == "research_train_run"
    assert active_summary["runtime_summary_path"] == str(tmp_path / "state" / "runtime_summary.json")
    assert active_summary["runtime_history_path"] == str(tmp_path / "state" / "runtime_history.json")
    assert active_summary["history_retention"] == {
        "total_task_count": 1,
        "retained_task_count": 1,
        "dropped_task_count": 0,
        "is_truncated": False,
        "row_limit": 50,
        "group_row_limit": 12,
    }
    assert (tmp_path / "state" / "runtime_summary.json").exists()
    assert (tmp_path / "state" / "runtime_history.json").exists()

    active_history = load_console_runtime_history(root=root)
    assert active_history["dataset"] == "console_runtime_history"
    assert active_history["task_count"] == 1
    assert active_history["row_count"] == 1
    assert active_history["retention"] == {
        "total_task_count": 1,
        "retained_task_count": 1,
        "dropped_task_count": 0,
        "is_truncated": False,
        "row_limit": 50,
        "group_row_limit": 12,
    }
    assert active_history["rows"][0]["task_id"] == submitted["task_id"]
    assert active_history["rows"][0]["task_path"] == str(root / f"{submitted['task_id']}.json")
    assert active_history["groups"]["active"]["rows"][0]["task_id"] == submitted["task_id"]
    assert active_history["history_source"] == "persisted"
    assert active_history["history_recovery"]["recovered"] is False

    release.set()
    _wait_for_task(submitted["task_id"], lambda task_id: get_console_task(task_id, root=root), status=TASK_STATUS_SUCCEEDED)

    terminal_summary = load_console_runtime_summary(root=root)
    assert terminal_summary["status_group_counts"]["active"] == 0
    assert terminal_summary["status_group_counts"]["terminal"] == 1
    assert terminal_summary["status_counts"]["succeeded"] == 1
    assert terminal_summary["recent_tasks"][0]["status"] == TASK_STATUS_SUCCEEDED
    assert terminal_summary["recent_tasks"][0]["status_group"] == "terminal"
    assert terminal_summary["recent_tasks"][0]["result_summary"] == "artifact summary written"
    assert terminal_summary["recent_tasks"][0]["primary_output_path"] == "/tmp/run/summary.json"
    assert terminal_summary["recent_tasks"][0]["result_paths"] == [
        {"label": "summary_path", "path": "/tmp/run/summary.json"}
    ]
    assert terminal_summary["recent_tasks"][0]["linked_objects"] == []
    assert terminal_summary["latest_markers"]["latest"]["status"] == TASK_STATUS_SUCCEEDED
    assert terminal_summary["latest_terminal_task_id"] == submitted["task_id"]
    assert terminal_summary["latest_markers"]["terminal"]["status"] == TASK_STATUS_SUCCEEDED
    assert terminal_summary["recent_terminal_tasks"][0]["task_id"] == submitted["task_id"]
    assert terminal_summary["recent_active_tasks"] == []


def test_console_runtime_summary_tracks_recent_failed_and_terminal_tasks(tmp_path: Path) -> None:
    root = tmp_path / "tasks"

    def planner(action_id: str, request=None) -> dict[str, object]:
        return {
            "action_id": action_id,
            "normalized_request": {"market": "sol", "run_label": str((request or {}).get("run_label") or "")},
            "command_preview": f"preview {action_id}",
        }

    def executor(context) -> dict[str, object]:
        if context.request.get("run_label") == "fail":
            context.report_progress("Broken", current=1, total=1)
            return {"status": "error", "stderr": "RuntimeError: broken"}
        context.report_progress("Done", current=1, total=1)
        return {"status": "ok", "summary_path": "/tmp/run/summary.json"}

    ok_task = submit_console_task(
        action_id="research_train_run",
        request={"run_label": "ok"},
        root=root,
        planner=planner,
        executor=executor,
    )
    failed_task = submit_console_task(
        action_id="research_backtest_run",
        request={"run_label": "fail"},
        root=root,
        planner=planner,
        executor=executor,
    )

    _wait_for_task(ok_task["task_id"], lambda task_id: get_console_task(task_id, root=root), status=TASK_STATUS_SUCCEEDED)
    _wait_for_task(failed_task["task_id"], lambda task_id: get_console_task(task_id, root=root), status=TASK_STATUS_FAILED)

    summary = load_console_runtime_summary(root=root)
    assert summary["status_group_counts"]["terminal"] == 2
    assert summary["status_group_counts"]["failed"] == 1
    assert summary["latest_failed_task_id"] == failed_task["task_id"]
    assert summary["latest_markers"]["failed"]["task_id"] == failed_task["task_id"]
    assert summary["recent_failed_tasks"][0]["task_id"] == failed_task["task_id"]
    assert summary["recent_failed_tasks"][0]["status_label"] == "Failed"
    assert summary["recent_failed_tasks"][0]["subject_summary"] == "fail | sol"
    assert summary["recent_failed_tasks"][0]["error_summary"] == "RuntimeError: broken"
    assert summary["recent_failed_tasks"][0]["error_detail"] == {
        "message": "RuntimeError: broken",
        "result_status": "error",
        "stderr_excerpt": ["RuntimeError: broken"],
    }
    assert summary["latest_markers"]["failed"]["error_detail"] == {
        "message": "RuntimeError: broken",
        "result_status": "error",
        "stderr_excerpt": ["RuntimeError: broken"],
    }
    assert [row["task_id"] for row in summary["recent_terminal_tasks"]] == [
        failed_task["task_id"],
        ok_task["task_id"],
    ]

    history = load_console_runtime_history(root=root)
    assert history["groups"]["failed"]["task_count"] == 1
    assert history["groups"]["failed"]["rows"][0]["task_id"] == failed_task["task_id"]
    assert history["groups"]["failed"]["rows"][0]["error_detail"]["message"] == "RuntimeError: broken"
    assert history["groups"]["terminal"]["rows"][0]["task_id"] == failed_task["task_id"]


def test_console_runtime_history_recovers_from_invalid_snapshot(tmp_path: Path) -> None:
    root = tmp_path / "tasks"

    task = submit_console_task(
        action_id="research_train_run",
        request={"run_label": "ok"},
        root=root,
        planner=lambda action_id, request=None: {
            "action_id": action_id,
            "normalized_request": {"market": "sol", "run_label": "ok"},
            "command_preview": f"preview {action_id}",
        },
        executor=lambda context: {"status": "ok", "summary_path": "/tmp/run/summary.json"},
    )
    _wait_for_task(task["task_id"], lambda task_id: get_console_task(task_id, root=root), status=TASK_STATUS_SUCCEEDED)

    history_path = tmp_path / "state" / "runtime_history.json"
    history_path.write_text("{invalid", encoding="utf-8")

    recovered = load_console_runtime_history(root=root)
    assert recovered["dataset"] == "console_runtime_history"
    assert recovered["history_source"] == "recovered_from_tasks"
    assert recovered["history_recovery"]["recovered"] is True
    assert str(recovered["history_recovery"]["reason"]).startswith("JSONDecodeError:")
    assert recovered["rows"][0]["task_id"] == task["task_id"]


def test_console_runtime_summary_recovers_from_invalid_snapshot_and_skips_bad_task_files(tmp_path: Path) -> None:
    root = tmp_path / "tasks"

    task = submit_console_task(
        action_id="research_train_run",
        request={"run_label": "ok"},
        root=root,
        planner=lambda action_id, request=None: {
            "action_id": action_id,
            "normalized_request": {"market": "sol", "run_label": "ok"},
            "command_preview": f"preview {action_id}",
        },
        executor=lambda context: {"status": "ok", "summary_path": "/tmp/run/summary.json"},
    )
    _wait_for_task(task["task_id"], lambda task_id: get_console_task(task_id, root=root), status=TASK_STATUS_SUCCEEDED)

    summary_path = tmp_path / "state" / "runtime_summary.json"
    summary_path.write_text("{invalid", encoding="utf-8")
    (root / "task_broken.json").write_text("{still invalid", encoding="utf-8")

    recovered = load_console_runtime_summary(root=root)
    assert recovered["dataset"] == "console_runtime_summary"
    assert recovered["summary_source"] == "recovered_from_tasks"
    assert recovered["summary_recovery"]["recovered"] is True
    assert str(recovered["summary_recovery"]["reason"]).startswith("JSONDecodeError:")
    assert recovered["history_scan"]["task_file_count"] == 2
    assert recovered["history_scan"]["valid_task_count"] == 1
    assert recovered["history_scan"]["invalid_task_file_count"] == 1
    assert recovered["history_scan"]["has_invalid_task_files"] is True
    assert recovered["latest_markers"]["latest"]["task_id"] == task["task_id"]
    assert recovered["recent_tasks"][0]["task_id"] == task["task_id"]
    assert (tmp_path / "state" / "runtime_history.json").exists()
    assert [row["task_id"] for row in list_console_tasks(root=root)] == [task["task_id"]]


def test_submit_console_action_task_dispatches_direct_training_runner(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "pm15min.console.tasks.build_console_action_request",
        lambda action_id, request=None: {
            "action_id": action_id,
            "normalized_request": {
                "market": "sol",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "window_start": "2026-03-01",
                "window_end": "2026-03-01",
                "run_label": "planned",
                "offsets": [7, 8],
            },
            "command_preview": "preview",
        },
    )
    monkeypatch.setattr(
        "pm15min.research.training.runner.train_research_run",
        lambda cfg, spec, reporter=None: {
            "dataset": "training_run",
            "market": cfg.asset.slug,
            "run_label": spec.run_label,
            "offsets": list(spec.offsets),
            "reporter_attached": callable(reporter),
        },
    )

    payload = submit_console_action_task(
        action_id="research_train_run",
        root=tmp_path / "v2",
    )
    completed = _wait_for_task(
        payload["task_id"],
        lambda task_id: get_console_task(task_id, root=tmp_path / "v2"),
        status=TASK_STATUS_SUCCEEDED,
    )
    assert completed["result"]["dataset"] == "training_run"
    assert completed["result"]["reporter_attached"] is True


def test_submit_console_action_task_dispatches_direct_bundle_builder(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "pm15min.console.tasks.build_console_action_request",
        lambda action_id, request=None: {
            "action_id": action_id,
            "normalized_request": {
                "market": "sol",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "target": "direction",
                "bundle_label": "bundle_a",
                "offsets": [7, 8],
                "source_training_run": "train_a",
            },
            "command_preview": "preview",
        },
    )
    monkeypatch.setattr(
        "pm15min.research.bundles.builder.build_model_bundle",
        lambda cfg, spec: {
            "dataset": "model_bundle",
            "market": cfg.asset.slug,
            "bundle_label": spec.bundle_label,
            "offsets": list(spec.offsets),
            "source_training_run": spec.source_training_run,
        },
    )

    payload = submit_console_action_task(
        action_id="research_bundle_build",
        root=tmp_path / "v2",
    )
    completed = _wait_for_task(
        payload["task_id"],
        lambda task_id: get_console_task(task_id, root=tmp_path / "v2"),
        status=TASK_STATUS_SUCCEEDED,
    )
    assert completed["result"] == {
        "dataset": "model_bundle",
        "market": "sol",
        "bundle_label": "bundle_a",
        "offsets": [7, 8],
        "source_training_run": "train_a",
    }


def test_submit_console_action_task_dispatches_direct_bundle_activation(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "pm15min.console.tasks.build_console_action_request",
        lambda action_id, request=None: {
            "action_id": action_id,
            "normalized_request": {
                "market": "sol",
                "cycle": "15m",
                "profile": "deep_otm",
                "target": "direction",
                "bundle_label": "bundle_a",
                "notes": "promote",
            },
            "command_preview": "preview",
        },
    )
    monkeypatch.setattr(
        "pm15min.research.service.activate_model_bundle",
        lambda cfg, profile, target, bundle_label=None, notes=None: {
            "dataset": "bundle_activation",
            "market": cfg.asset.slug,
            "profile": profile,
            "target": target,
            "bundle_label": bundle_label,
            "notes": notes,
        },
    )

    payload = submit_console_action_task(
        action_id="research_activate_bundle",
        root=tmp_path / "v2",
    )
    completed = _wait_for_task(
        payload["task_id"],
        lambda task_id: get_console_task(task_id, root=tmp_path / "v2"),
        status=TASK_STATUS_SUCCEEDED,
    )
    assert completed["result"] == {
        "dataset": "bundle_activation",
        "market": "sol",
        "profile": "deep_otm",
        "target": "direction",
        "bundle_label": "bundle_a",
        "notes": "promote",
    }


def test_submit_console_action_task_dispatches_direct_experiment_runner(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "pm15min.console.tasks.build_console_action_request",
        lambda action_id, request=None: {
            "action_id": action_id,
            "normalized_request": {
                "market": "sol",
                "cycle": "15m",
                "profile": "deep_otm",
                "suite": "suite_a",
                "run_label": "planned",
            },
            "command_preview": "preview",
        },
    )
    monkeypatch.setattr(
        "pm15min.research.experiments.runner.run_experiment_suite",
        lambda cfg, suite_name, run_label, reporter=None: {
            "dataset": "experiment_run",
            "suite_name": suite_name,
            "run_label": run_label,
            "reporter_attached": callable(reporter),
        },
    )

    payload = submit_console_action_task(
        action_id="research_experiment_run_suite",
        root=tmp_path / "v2",
    )
    completed = _wait_for_task(
        payload["task_id"],
        lambda task_id: get_console_task(task_id, root=tmp_path / "v2"),
        status=TASK_STATUS_SUCCEEDED,
    )
    assert completed["result"]["dataset"] == "experiment_run"
    assert completed["result"]["reporter_attached"] is True


def test_submit_console_action_task_materializes_inline_suite_spec(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "pm15min.console.tasks.build_console_action_request",
        lambda action_id, request=None: {
            "action_id": action_id,
            "normalized_request": {
                "market": "sol",
                "cycle": "15m",
                "profile": "deep_otm",
                "suite_mode": "inline",
                "suite": "feature_matrix",
                "run_label": "planned",
                "inline_suite_payload": {
                    "suite_name": "feature_matrix",
                    "cycle": "15m",
                    "profile": "deep_otm",
                    "model_family": "deep_otm",
                    "feature_set": "deep_otm_v1",
                    "label_set": "truth",
                    "target": "direction",
                    "offsets": [7, 8, 9],
                    "window": {"start": "2026-03-01", "end": "2026-03-10"},
                    "compare_policy": {"reference_variant_labels": ["default"]},
                    "runtime_policy": {
                        "completed_cases": "resume",
                        "failed_cases": "rerun",
                        "parallel_case_workers": 2,
                    },
                    "markets": [
                        {
                            "market": "sol",
                            "group_name": "main",
                            "run_name": "stake_matrix",
                            "feature_set_variants": [
                                {"label": "baseline", "feature_set": "deep_otm_v1"},
                                {"label": "wide", "feature_set": "deep_otm_v2"},
                            ],
                            "stakes_usd": [1.0, 5.0],
                            "max_notional_usd": 8.0,
                            "backtest_spec": "baseline_truth",
                        }
                    ],
                },
            },
            "command_preview": "preview",
        },
    )

    def _fake_run_experiment_suite(cfg, suite_name, run_label, reporter=None):
        suite_path = Path(suite_name)
        captured["suite_name"] = suite_name
        captured["suite_path"] = suite_path
        captured["payload"] = json.loads(suite_path.read_text(encoding="utf-8"))
        return {
            "dataset": "experiment_run",
            "suite_name": "feature_matrix",
            "run_label": run_label,
            "reporter_attached": callable(reporter),
        }

    monkeypatch.setattr(
        "pm15min.research.experiments.runner.run_experiment_suite",
        _fake_run_experiment_suite,
    )

    payload = submit_console_action_task(
        action_id="research_experiment_run_suite",
        root=tmp_path / "v2",
    )
    completed = _wait_for_task(
        payload["task_id"],
        lambda task_id: get_console_task(task_id, root=tmp_path / "v2"),
        status=TASK_STATUS_SUCCEEDED,
    )
    suite_path = Path(completed["result"]["suite_spec_path"])
    assert suite_path == tmp_path / "v2" / "research" / "experiments" / "suite_specs" / "feature_matrix.json"
    assert suite_path.exists()
    assert Path(captured["suite_name"]) == suite_path
    assert captured["payload"]["runtime_policy"]["parallel_case_workers"] == 2
    assert captured["payload"]["markets"][0]["feature_set_variants"][1]["feature_set"] == "deep_otm_v2"
    assert completed["result"]["suite_mode"] == "inline"
    assert completed["result"]["reporter_attached"] is True


def test_console_task_manager_updates_heartbeat_while_running(tmp_path: Path) -> None:
    root = tmp_path / "tasks"
    started = threading.Event()
    release = threading.Event()

    def executor(context) -> dict[str, object]:
        started.set()
        assert release.wait(timeout=1.0)
        return {"status": "ok"}

    manager = ConsoleTaskManager(
        root=root,
        planner=lambda action_id, request=None: {
            "action_id": action_id,
            "normalized_request": {},
            "command_preview": "preview",
        },
        executor=executor,
        heartbeat_interval_sec=0.05,
    )
    submitted = manager.submit("research_train_run", {})
    assert started.wait(timeout=1.0)
    first = _wait_for_task(
        submitted["task_id"],
        lambda task_id: manager.get(task_id),
        status=TASK_STATUS_RUNNING,
    )
    first_heartbeat = str(first["progress"]["heartbeat"])
    time.sleep(0.12)
    second = manager.get(submitted["task_id"])
    assert second is not None
    assert str(second["progress"]["heartbeat"]) >= first_heartbeat
    release.set()


def _wait_for_task(
    task_id: str,
    fetcher,
    *,
    status: str,
    timeout: float = 2.0,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    last_payload = None
    while time.monotonic() < deadline:
        payload = fetcher(task_id)
        if payload is not None:
            last_payload = payload
        if payload is not None and payload.get("status") == status:
            return payload
        time.sleep(0.01)
    raise AssertionError(f"Timed out waiting for {task_id} to reach {status!r}. Last payload: {last_payload!r}")
