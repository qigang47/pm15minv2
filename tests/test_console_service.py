from __future__ import annotations

from pathlib import Path

from pm15min.console import service


def test_load_console_home_advertises_sections_and_commands() -> None:
    payload = service.load_console_home(root=Path("/tmp/v2"))

    assert payload["dataset"] == "console_home"
    assert payload["read_only"] is True
    assert payload["section_count"] == 5
    section_ids = [row["id"] for row in payload["sections"]]
    assert section_ids == [
        "data_overview",
        "training_runs",
        "bundles",
        "backtests",
        "experiments",
    ]
    assert payload["commands"]["data_overview"]["list_command"] is not None
    assert payload["commands"]["data_overview"]["detail_command"] is None
    assert payload["commands"]["training_runs"]["detail_command"] is not None
    assert payload["action_catalog"]["action_count"] >= 1
    assert payload["runtime_summary"]["dataset"] == "console_runtime_summary"
    assert payload["runtime_board"]["summary_source"] == "empty"
    assert payload["runtime_task_count"] == 0
    assert payload["active_task_count"] == 0
    assert payload["terminal_task_count"] == 0
    assert payload["failed_task_count"] == 0


def test_load_console_runtime_state_returns_runtime_summary() -> None:
    payload = service.load_console_runtime_state(root=Path("/tmp/v2"))

    assert payload["dataset"] == "console_runtime_summary"
    assert payload["runtime_board"]["summary_source"] == "empty"
    assert payload["task_count"] == 0
    assert payload["status_group_counts"] == {"active": 0, "terminal": 0, "failed": 0}
    assert payload["recent_active_task_briefs"] == []
    assert payload["recent_failed_task_briefs"] == []
    assert payload["latest_task_briefs"] == {
        "latest": None,
        "active": None,
        "terminal": None,
        "failed": None,
    }
    assert payload["operator_summary"] == {
        "has_active_tasks": False,
        "has_failed_tasks": False,
        "active_task_count": 0,
        "failed_task_count": 0,
        "invalid_task_file_count": 0,
        "invalid_task_files": [],
        "history_retention": {
            "total_task_count": 0,
            "retained_task_count": 0,
            "dropped_task_count": 0,
            "is_truncated": False,
            "row_limit": 50,
            "group_row_limit": 12,
        },
        "history_truncated": False,
        "history_limit": 50,
        "history_group_limit": 12,
        "retained_task_count": 0,
        "dropped_task_count": 0,
        "summary_source": "empty",
        "recovery_reason": None,
        "warnings": [],
        "latest_headline": None,
        "latest_active_headline": None,
        "latest_terminal_headline": None,
        "latest_failed_headline": None,
        "latest_failed_summary": None,
    }


def test_load_console_runtime_history_returns_operator_payload() -> None:
    payload = service.load_console_runtime_history(root=Path("/tmp/v2"))

    assert payload["dataset"] == "console_runtime_history"
    assert payload["task_briefs"] == []
    assert payload["group_task_briefs"] == {"active": [], "terminal": [], "failed": []}
    assert payload["operator_summary"] == {
        "task_count": 0,
        "row_count": 0,
        "invalid_task_file_count": 0,
        "invalid_task_files": [],
        "retention": {
            "total_task_count": 0,
            "retained_task_count": 0,
            "dropped_task_count": 0,
            "is_truncated": False,
            "row_limit": 50,
            "group_row_limit": 12,
        },
        "history_truncated": False,
        "history_limit": 50,
        "history_group_limit": 12,
        "retained_task_count": 0,
        "dropped_task_count": 0,
        "history_source": "empty",
        "recovery_reason": None,
        "updated_at": None,
        "latest_headline": None,
    }


def test_service_delegates_to_underlying_read_models(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "_load_data_overview",
        lambda **kwargs: {"dataset": "console_data_overview", "market": kwargs["market"]},
    )
    monkeypatch.setattr(
        service,
        "_list_console_training_runs",
        lambda **kwargs: [{"object_type": "training_run", "market": kwargs["market"]}],
    )
    monkeypatch.setattr(
        service,
        "_load_console_training_run",
        lambda **kwargs: {"dataset": "console_training_run", "run_label": kwargs["run_label"]},
    )
    monkeypatch.setattr(
        service,
        "_list_console_model_bundles",
        lambda **kwargs: [{"object_type": "model_bundle", "market": kwargs["market"]}],
    )
    monkeypatch.setattr(
        service,
        "_load_console_model_bundle",
        lambda **kwargs: {"dataset": "console_model_bundle", "bundle_label": kwargs["bundle_label"]},
    )
    monkeypatch.setattr(
        service,
        "_list_console_backtest_runs",
        lambda **kwargs: [{"object_type": "backtest_run", "market": kwargs["market"]}],
    )
    monkeypatch.setattr(
        service,
        "_describe_console_backtest_run",
        lambda **kwargs: {"dataset": "console_backtest_run_detail", "run_label": kwargs["run_label"]},
    )
    monkeypatch.setattr(
        service,
        "_describe_console_backtest_stake_sweep",
        lambda **kwargs: {"dataset": "console_backtest_stake_sweep_detail", "run_label": kwargs["run_label"]},
    )
    monkeypatch.setattr(
        service,
        "_list_console_experiment_runs",
        lambda **kwargs: [{"object_type": "experiment_run", "suite_name": kwargs["suite_name"]}],
    )
    monkeypatch.setattr(
        service,
        "_describe_console_experiment_run",
        lambda **kwargs: {"dataset": "console_experiment_run_detail", "run_label": kwargs["run_label"]},
    )
    monkeypatch.setattr(
        service,
        "_describe_console_experiment_matrix",
        lambda **kwargs: {"dataset": "console_experiment_matrix_detail", "run_label": kwargs["run_label"]},
    )
    monkeypatch.setattr(
        service,
        "_execute_console_action",
        lambda **kwargs: {"dataset": "console_action_execution", "action_id": kwargs["action_id"]},
    )
    monkeypatch.setattr(
        service,
        "_submit_console_action_task",
        lambda **kwargs: {"dataset": "console_task", "task_id": "task_1", "action_id": kwargs["action_id"]},
    )
    monkeypatch.setattr(
        service,
        "_load_console_task",
        lambda **kwargs: {
            "task_id": kwargs["task_id"],
            "action_id": "research_train_run",
            "status": "running",
            "request": {"market": "sol", "cycle": "15m", "run_label": "planned"},
            "progress": {"summary": "Training folds", "current_stage": "dispatch", "progress_pct": 45},
            "result": {"summary_path": "/tmp/training/summary.json"},
            "error": None,
        },
    )
    monkeypatch.setattr(
        service,
        "_list_console_tasks",
        lambda **kwargs: [
            {
                "task_id": "task_1",
                "action_id": (
                    (kwargs.get("action_ids") or [kwargs.get("action_id")] or ["research_train_run"])[0]
                ),
                "status": kwargs.get("status") or "queued",
                "request": {"market": "sol", "cycle": "15m", "run_label": "planned"},
                "progress": {"summary": "Queued", "current_stage": "queued", "progress_pct": 0},
                "result": {"report_path": "/tmp/training/report.md"},
                "error": None,
            }
        ],
    )
    monkeypatch.setattr(
        service,
        "_load_console_runtime_summary",
        lambda **kwargs: {
            "dataset": "console_runtime_summary",
            "history_scan": {"invalid_task_file_count": 0, "invalid_task_files": [], "has_invalid_task_files": False},
            "history_retention": {
                "total_task_count": 1,
                "retained_task_count": 1,
                "dropped_task_count": 0,
                "is_truncated": False,
                "row_limit": 50,
                "group_row_limit": 12,
            },
            "summary_source": "persisted",
            "summary_recovery": {"reason": None},
            "latest_markers": {},
            "recent_tasks": [],
            "recent_active_tasks": [],
            "recent_terminal_tasks": [],
            "recent_failed_tasks": [],
            "history_groups": {"status_group": [], "action_id": []},
        },
    )

    assert service.load_console_data_overview(market="sol")["market"] == "sol"
    assert service.list_console_training_runs(market="sol")[0]["object_type"] == "training_run"
    assert service.load_console_training_run(market="sol", run_label="r1")["run_label"] == "r1"
    assert service.list_console_bundles(market="sol")[0]["object_type"] == "model_bundle"
    assert service.load_console_bundle(market="sol", profile="deep_otm", target="direction", bundle_label="b1")["bundle_label"] == "b1"
    assert service.list_console_backtests(market="sol")[0]["object_type"] == "backtest_run"
    assert service.load_console_backtest(market="sol", profile="deep_otm", spec_name="baseline_truth", run_label="bt1")["run_label"] == "bt1"
    assert service.load_console_backtest_stake_sweep(market="sol", profile="deep_otm", spec_name="baseline_truth", run_label="bt1")["dataset"] == "console_backtest_stake_sweep_detail"
    assert service.list_console_experiments(suite_name="suite1")[0]["suite_name"] == "suite1"
    assert service.load_console_experiment(suite_name="suite1", run_label="exp1")["run_label"] == "exp1"
    assert service.load_console_experiment_matrix(suite_name="suite1", run_label="exp1")["dataset"] == "console_experiment_matrix_detail"
    assert service.execute_console_action(action_id="data_refresh_summary")["dataset"] == "console_action_execution"
    assert service.submit_console_action_task(action_id="research_train_run")["dataset"] == "console_task"
    task_detail = service.load_console_task(task_id="task_1")
    assert task_detail["status_label"] == "运行中"
    assert task_detail["subject_summary"] == "planned | sol / 15m"
    assert task_detail["primary_output_path"] == "/tmp/training/summary.json"
    assert task_detail["result_paths"] == [{"label": "summary_path", "path": "/tmp/training/summary.json"}]
    assert task_detail["task_brief"]["headline"] == "运行中 · planned | sol / 15m · task_1"
    assert task_detail["task_brief"]["summary"] == "Training folds · 调度中 · 45%"
    assert task_detail["result_path_briefs"] == [
        {
            "label": "summary_path",
            "path": "/tmp/training/summary.json",
            "headline": "summary_path @ /tmp/training/summary.json",
        }
    ]
    assert task_detail["error_brief"] is None
    listed = service.list_console_tasks(action_id="research_train_run", status="queued")
    assert listed["dataset"] == "console_task_list"
    assert listed["action_id_filter"] == "research_train_run"
    assert listed["rows"][0]["object_type"] == "console_task"
    assert listed["rows"][0]["subject_summary"] == "planned | sol / 15m"
    assert listed["rows"][0]["primary_output_path"] == "/tmp/training/report.md"
    assert listed["rows"][0]["task_brief"]["headline"] == "排队中 · planned | sol / 15m · task_1"
    assert listed["task_briefs"][0]["headline"] == "排队中 · planned | sol / 15m · task_1"
    assert listed["latest_task_brief"]["summary"] == "排队中 · 排队中 · 0%"
    active = service.list_console_tasks(status_group="active")
    assert active["status_group_filter"] == "active"
    assert active["filters"]["status_group"] == "active"
    multi = service.list_console_tasks(action_ids=["research_train_run", "research_backtest_run"])
    assert multi["action_id_filter"] is None
    assert multi["action_ids_filter"] == ["research_train_run", "research_backtest_run"]
    listed_many = service.list_console_tasks(action_ids=("research_train_run", "research_backtest_run"))
    assert listed_many["action_ids_filter"] == ["research_train_run", "research_backtest_run"]
    multi = service.list_console_tasks(action_ids=["research_train_run", "research_backtest_run"])
    assert multi["action_ids_filter"] == ["research_train_run", "research_backtest_run"]


def test_task_detail_payload_exposes_subject_and_multiple_result_paths(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "_load_console_model_bundle",
        lambda **kwargs: {
            "dataset": "console_model_bundle",
            "bundle_label": "bundle_a",
            "profile": "deep_otm",
            "target": "direction",
            "offset_count": 3,
            "is_active": False,
        },
    )
    monkeypatch.setattr(
        service,
        "_load_console_task",
        lambda **kwargs: {
            "task_id": kwargs["task_id"],
            "action_id": "research_bundle_build",
            "status": "succeeded",
            "request": {
                "market": "sol",
                "cycle": "15m",
                "bundle_label": "bundle_a",
                "source_training_run": "train_a",
            },
            "result": {
                "bundle_dir": "/tmp/bundle",
                "summary_path": "/tmp/bundle/summary.json",
                "report_path": "/tmp/bundle/report.md",
            },
            "error": None,
            "progress": {"summary": "Completed", "current_stage": "finished", "progress_pct": 100},
        },
    )

    payload = service.load_console_task(task_id="task_bundle")
    assert payload["subject_summary"] == "bundle_a 来自 train_a | sol / 15m"
    assert payload["primary_output_label"] == "bundle_dir"
    assert payload["primary_output_path"] == "/tmp/bundle"
    assert payload["result_paths"] == [
        {"label": "bundle_dir", "path": "/tmp/bundle"},
        {"label": "summary_path", "path": "/tmp/bundle/summary.json"},
        {"label": "report_path", "path": "/tmp/bundle/report.md"},
    ]
    assert payload["linked_objects"] == [
        {
            "object_type": "model_bundle",
            "title": "模型包",
            "path": "/tmp/bundle",
            "summary_path": "/tmp/bundle/summary.json",
            "report_path": "/tmp/bundle/report.md",
        }
    ]
    assert payload["linked_object_details"] == [
        {
            "object_type": "model_bundle",
            "title": "模型包",
            "path": "/tmp/bundle",
            "identity": "bundle_a",
            "summary": {
                "profile": "deep_otm",
                "target": "direction",
                "offset_count": 3,
                "is_active": False,
            },
        }
    ]
    assert payload["linked_object_detail_briefs"] == [
        {
            "object_type": "model_bundle",
            "title": "模型包",
            "identity": "bundle_a",
            "path": "/tmp/bundle",
            "headline": "模型包 · bundle_a",
            "summary_items": [
                {"label": "profile", "value": "deep_otm"},
                {"label": "target", "value": "direction"},
                {"label": "offset_count", "value": 3},
                {"label": "is_active", "value": False},
            ],
            "summary_text": "profile=deep_otm · target=direction · offset_count=3 · is_active=false",
        }
    ]
    assert payload["task_brief"]["supporting_text"] == "已写入摘要产物 | 模型包 · bundle_a | /tmp/bundle"


def test_task_detail_payload_exposes_error_detail(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "_load_console_task",
        lambda **kwargs: {
            "task_id": kwargs["task_id"],
            "action_id": "data_sync",
            "status": "failed",
            "request": {"market": "sol", "cycle": "15m", "sync_command": "market-catalog"},
            "result": {
                "status": "error",
                "return_code": 1,
                "stderr": "line one\nline two\nline three\nline four",
                "execution_summary": {"last_stderr_line": "line four"},
            },
            "error": {"type": "RuntimeError", "message": "sync failed"},
            "progress": {"summary": "Failed", "current_stage": "finished", "progress_pct": 100},
        },
    )

    payload = service.load_console_task(task_id="task_error")
    assert payload["error_detail"] == {
        "type": "RuntimeError",
        "message": "sync failed",
        "result_status": "error",
        "return_code": 1,
        "stderr_excerpt": ["line two", "line three", "line four"],
        "last_stderr_line": "line four",
    }
    assert payload["error_brief"] == {
        "headline": "RuntimeError: sync failed",
        "supporting_text": "line four",
        "type": "RuntimeError",
        "message": "sync failed",
        "result_status": "error",
        "return_code": 1,
        "last_stderr_line": "line four",
        "stderr_excerpt": ["line two", "line three", "line four"],
    }


def test_load_console_query_routes_sections(monkeypatch) -> None:
    monkeypatch.setattr(service, "load_console_home", lambda root=None: {"dataset": "console_home"})
    monkeypatch.setattr(service, "load_console_runtime_state", lambda root=None: {"dataset": "console_runtime_summary"})
    monkeypatch.setattr(service, "load_console_runtime_history", lambda root=None: {"dataset": "console_runtime_history"})
    monkeypatch.setattr(service, "load_console_data_overview", lambda **kwargs: {"dataset": "console_data_overview", "market": kwargs["market"]})
    monkeypatch.setattr(service, "list_console_training_runs", lambda **kwargs: [{"object_type": "training_run"}])
    monkeypatch.setattr(service, "load_console_training_run", lambda **kwargs: {"dataset": "console_training_run", "run_label": kwargs["run_label"]})
    monkeypatch.setattr(service, "list_console_bundles", lambda **kwargs: [{"object_type": "model_bundle"}])
    monkeypatch.setattr(service, "load_console_bundle", lambda **kwargs: {"dataset": "console_model_bundle", "bundle_label": kwargs["bundle_label"]})
    monkeypatch.setattr(service, "list_console_backtests", lambda **kwargs: [{"object_type": "backtest_run"}])
    monkeypatch.setattr(service, "load_console_backtest", lambda **kwargs: {"dataset": "console_backtest_run_detail", "run_label": kwargs["run_label"]})
    monkeypatch.setattr(service, "load_console_backtest_stake_sweep", lambda **kwargs: {"dataset": "console_backtest_stake_sweep_detail", "run_label": kwargs["run_label"]})
    monkeypatch.setattr(service, "list_console_experiments", lambda **kwargs: [{"object_type": "experiment_run"}])
    monkeypatch.setattr(service, "load_console_experiment", lambda **kwargs: {"dataset": "console_experiment_run_detail", "run_label": kwargs["run_label"]})
    monkeypatch.setattr(service, "load_console_experiment_matrix", lambda **kwargs: {"dataset": "console_experiment_matrix_detail", "run_label": kwargs["run_label"]})
    monkeypatch.setattr(
        service,
        "_load_console_action_catalog",
        lambda **kwargs: {
            "dataset": "console_action_catalog",
            "action_count": 8,
            "for_section": kwargs.get("for_section"),
            "shell_enabled": kwargs.get("shell_enabled"),
        },
    )
    monkeypatch.setattr(service, "_build_console_action_request", lambda action_id, request=None: {"dataset": "console_action_plan", "action_id": action_id, "request": dict(request or {})})
    monkeypatch.setattr(service, "load_console_task", lambda **kwargs: {"dataset": "console_task", "task_id": kwargs["task_id"]})
    monkeypatch.setattr(
        service,
        "list_console_tasks",
        lambda **kwargs: {
            "dataset": "console_task_list",
            "row_count": 1,
            "action_id_filter": kwargs.get("action_id"),
            "action_ids_filter": list(kwargs.get("action_ids") or ()),
            "status_group_filter": kwargs.get("status_group"),
            "marker_filter": kwargs.get("marker"),
            "group_by": kwargs.get("group_by"),
        },
    )

    assert service.load_console_query({})["dataset"] == "console_home"
    assert service.load_console_query({"section": "runtime_state"})["dataset"] == "console_runtime_summary"
    assert service.load_console_query({"section": "runtime_history"})["dataset"] == "console_runtime_history"
    assert service.load_console_query({"section": "data_overview", "market": "sol"})["market"] == "sol"
    assert service.load_console_query({"section": "training_runs"})[0]["object_type"] == "training_run"
    assert service.load_console_query({"section": "training_runs", "run_label": "r1"})["run_label"] == "r1"
    assert service.load_console_query({"section": "bundles"})[0]["object_type"] == "model_bundle"
    assert service.load_console_query({"section": "bundles", "bundle_label": "b1", "profile": "deep_otm", "target": "direction"})["bundle_label"] == "b1"
    assert service.load_console_query({"section": "backtests"})[0]["object_type"] == "backtest_run"
    assert service.load_console_query({"section": "backtest_stake_sweep", "run_label": "bt1", "profile": "deep_otm", "spec": "baseline_truth"})["dataset"] == "console_backtest_stake_sweep_detail"
    assert service.load_console_query({"section": "backtests", "run_label": "bt1", "profile": "deep_otm", "spec": "baseline_truth"})["run_label"] == "bt1"
    assert service.load_console_query({"section": "experiments"})[0]["object_type"] == "experiment_run"
    assert service.load_console_query({"section": "experiment_matrix", "suite": "s1", "run_label": "exp1"})["dataset"] == "console_experiment_matrix_detail"
    assert service.load_console_query({"section": "experiments", "suite": "s1", "run_label": "exp1"})["run_label"] == "exp1"
    assert service.load_console_query({"section": "actions"})["dataset"] == "console_action_catalog"
    assert service.load_console_query({"section": "actions", "for_section": "bundles", "shell_enabled": "true"})["for_section"] == "bundles"
    assert service.load_console_query({"section": "actions", "action_id": "data_refresh_summary", "market": "sol"})["action_id"] == "data_refresh_summary"
    assert service.load_console_query({"section": "tasks"})["dataset"] == "console_task_list"
    assert service.load_console_query({"section": "tasks", "action_id": "research_train_run"})["action_id_filter"] == "research_train_run"
    assert service.load_console_query({"section": "tasks", "action_ids": "research_train_run,research_backtest_run"})["action_ids_filter"] == [
        "research_train_run",
        "research_backtest_run",
    ]
    assert service.load_console_query({"section": "tasks", "status_group": "failed"})["status_group_filter"] == "failed"
    assert service.load_console_query({"section": "tasks", "marker": "failed", "group_by": "action_id"})["marker_filter"] == "failed"
    assert service.load_console_query({"section": "tasks", "marker": "failed", "group_by": "action_id"})["group_by"] == "action_id"
    assert service.load_console_query({"section": "tasks", "task_id": "task_1"})["task_id"] == "task_1"


def test_console_task_payload_exposes_subject_and_result_paths(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "_load_console_task",
        lambda **kwargs: {
            "task_id": kwargs["task_id"],
            "action_id": "research_bundle_build",
            "status": "succeeded",
            "request": {
                "market": "sol",
                "cycle": "15m",
                "bundle_label": "bundle_a",
                "source_training_run": "train_a",
            },
            "progress": {"summary": "Completed", "current_stage": "finished", "progress_pct": 100},
            "result": {
                "dataset": "model_bundle",
                "bundle_dir": "/tmp/bundle",
                "summary_path": "/tmp/bundle/summary.json",
                "report_path": "/tmp/bundle/report.md",
            },
            "error": None,
        },
    )
    monkeypatch.setattr(
        service,
        "_list_console_tasks",
        lambda **kwargs: [
            {
                "task_id": "task_1",
                "action_id": "research_bundle_build",
                "status": "succeeded",
                "request": {
                    "market": "sol",
                    "cycle": "15m",
                    "bundle_label": "bundle_a",
                    "source_training_run": "train_a",
                },
                "progress": {"summary": "Completed", "current_stage": "finished", "progress_pct": 100},
                "result": {
                    "dataset": "model_bundle",
                    "bundle_dir": "/tmp/bundle",
                    "summary_path": "/tmp/bundle/summary.json",
                },
                "error": None,
            }
        ],
    )

    detail = service.load_console_task(task_id="task_1")
    assert detail["subject_summary"] == "bundle_a 来自 train_a | sol / 15m"
    assert detail["primary_output_label"] == "bundle_dir"
    assert detail["primary_output_path"] == "/tmp/bundle"
    assert detail["result_paths"] == [
        {"label": "bundle_dir", "path": "/tmp/bundle"},
        {"label": "summary_path", "path": "/tmp/bundle/summary.json"},
        {"label": "report_path", "path": "/tmp/bundle/report.md"},
    ]

    listing = service.list_console_tasks(action_id="research_bundle_build")
    assert listing["rows"][0]["subject_summary"] == "bundle_a 来自 train_a | sol / 15m"
    assert listing["rows"][0]["primary_output_label"] == "bundle_dir"
    assert listing["rows"][0]["primary_output_path"] == "/tmp/bundle"


def test_list_console_tasks_exposes_history_markers_and_groups(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "_list_console_tasks",
        lambda **kwargs: [
            {
                "task_id": "task_failed",
                "action_id": "research_backtest_run",
                "status": "failed",
                "updated_at": "2026-03-23T10:05:00Z",
                "request": {"market": "sol", "cycle": "15m", "run_label": "bt_fail", "spec": "baseline"},
                "progress": {"summary": "Failed", "current_stage": "finished", "progress_pct": 100},
                "result": {"stderr": "boom"},
                "error": {"message": "boom"},
            },
            {
                "task_id": "task_active",
                "action_id": "research_train_run",
                "status": "running",
                "updated_at": "2026-03-23T10:00:00Z",
                "request": {"market": "sol", "cycle": "15m", "run_label": "train_a"},
                "progress": {"summary": "OOF", "current_stage": "training_oof", "progress_pct": 50},
                "result": None,
                "error": None,
            },
        ],
    )
    monkeypatch.setattr(
        service,
        "_load_console_runtime_summary",
        lambda **kwargs: {
            "dataset": "console_runtime_summary",
            "history_scan": {
                "invalid_task_file_count": 1,
                "invalid_task_files": [{"path": "/tmp/bad.json", "error_type": "JSONDecodeError", "message": "bad"}],
                "has_invalid_task_files": True,
            },
            "history_retention": {
                "total_task_count": 52,
                "retained_task_count": 50,
                "dropped_task_count": 2,
                "is_truncated": True,
                "row_limit": 50,
                "group_row_limit": 12,
            },
            "summary_source": "persisted",
            "summary_recovery": {"reason": None},
            "latest_markers": {},
            "recent_tasks": [],
            "recent_active_tasks": [],
            "recent_terminal_tasks": [],
            "recent_failed_tasks": [],
            "history_groups": {"status_group": [], "action_id": []},
        },
    )

    payload = service.list_console_tasks(marker="failed", group_by="action_id")
    assert payload["marker_filter"] == "failed"
    assert payload["group_by"] == "action_id"
    assert payload["status_group_counts"] == {"active": 1, "terminal": 0, "failed": 1}
    assert payload["history_markers"]["latest"]["task_id"] == "task_failed"
    assert payload["history_markers"]["failed"]["task_id"] == "task_failed"
    assert payload["history_markers"]["active"]["task_id"] == "task_active"
    assert payload["selected_marker"]["task_id"] == "task_failed"
    assert payload["history_groups"]["action_id"][0]["action_id"] == "research_backtest_run"
    assert payload["history_groups"]["status_group"][0]["group"] == "active"
    assert payload["selected_group_rows"][0]["action_id"] == "research_backtest_run"
    assert payload["history_scan"]["invalid_task_file_count"] == 1
    assert payload["history_retention"]["is_truncated"] is True
    assert payload["operator_summary"]["history_truncated"] is True
    assert payload["operator_summary"]["invalid_task_file_count"] == 1
    assert payload["rows"][0]["status_group"] == "failed"
    assert payload["rows"][0]["task_brief"] == {
        "task_id": "task_failed",
        "action_id": "research_backtest_run",
        "status": "failed",
        "status_label": "失败",
        "status_group": "failed",
        "subject_summary": "baseline / bt_fail | sol / 15m",
        "headline": "失败 · baseline / bt_fail | sol / 15m · task_failed",
        "summary": "boom",
        "supporting_text": "失败 · 已结束 · 100%",
        "progress_summary": "失败 · 已结束 · 100%",
        "result_summary": None,
        "error_summary": "boom",
        "linked_object": None,
        "primary_output_path": None,
        "updated_at": "2026-03-23T10:05:00Z",
        "request_summary": {
            "market": "sol",
            "cycle": "15m",
            "run_label": "bt_fail",
            "spec": "baseline",
        },
    }


def test_load_console_runtime_state_exposes_operator_briefs(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "_load_console_runtime_summary",
        lambda **kwargs: {
            "dataset": "console_runtime_summary",
            "task_count": 2,
            "status_group_counts": {"active": 1, "terminal": 1, "failed": 1},
            "recent_tasks": [
                {
                    "task_id": "task_failed",
                    "action_id": "data_sync",
                    "status": "failed",
                    "status_label": "失败",
                    "subject_summary": "market-catalog | sol / 15m",
                    "progress_summary": "失败 · 已结束 · 100%",
                    "error_summary": "RuntimeError: sync failed",
                    "primary_output_path": None,
                    "request_summary": {"market": "sol", "cycle": "15m", "sync_command": "market-catalog"},
                },
                {
                    "task_id": "task_running",
                    "action_id": "research_train_run",
                    "status": "running",
                    "status_label": "运行中",
                    "subject_summary": "planned | sol / 15m",
                    "progress_summary": "Training offsets · training_offsets · 42%",
                    "result_summary": None,
                    "primary_output_path": "/tmp/training",
                    "request_summary": {"market": "sol", "cycle": "15m", "run_label": "planned"},
                },
            ],
            "recent_active_tasks": [
                {
                    "task_id": "task_running",
                    "action_id": "research_train_run",
                    "status": "running",
                    "status_label": "运行中",
                    "subject_summary": "planned | sol / 15m",
                    "progress_summary": "Training offsets · training_offsets · 42%",
                    "result_summary": None,
                    "primary_output_path": "/tmp/training",
                    "request_summary": {"market": "sol", "cycle": "15m", "run_label": "planned"},
                }
            ],
            "recent_terminal_tasks": [
                {
                    "task_id": "task_failed",
                    "action_id": "data_sync",
                    "status": "failed",
                    "status_label": "失败",
                    "subject_summary": "market-catalog | sol / 15m",
                    "progress_summary": "失败 · 已结束 · 100%",
                    "error_summary": "RuntimeError: sync failed",
                    "request_summary": {"market": "sol", "cycle": "15m", "sync_command": "market-catalog"},
                }
            ],
            "recent_failed_tasks": [
                {
                    "task_id": "task_failed",
                    "action_id": "data_sync",
                    "status": "failed",
                    "status_label": "失败",
                    "subject_summary": "market-catalog | sol / 15m",
                    "progress_summary": "失败 · 已结束 · 100%",
                    "error_summary": "RuntimeError: sync failed",
                    "request_summary": {"market": "sol", "cycle": "15m", "sync_command": "market-catalog"},
                }
            ],
            "latest_markers": {
                "active": {
                    "task_id": "task_running",
                    "action_id": "research_train_run",
                    "status": "running",
                    "status_label": "运行中",
                    "subject_summary": "planned | sol / 15m",
                    "progress_summary": "Training offsets · training_offsets · 42%",
                    "primary_output_path": "/tmp/training",
                    "request_summary": {"market": "sol", "cycle": "15m", "run_label": "planned"},
                },
                "terminal": {
                    "task_id": "task_failed",
                    "action_id": "data_sync",
                    "status": "failed",
                    "status_label": "失败",
                    "subject_summary": "market-catalog | sol / 15m",
                    "error_summary": "RuntimeError: sync failed",
                    "request_summary": {"market": "sol", "cycle": "15m", "sync_command": "market-catalog"},
                },
                "failed": {
                    "task_id": "task_failed",
                    "action_id": "data_sync",
                    "status": "failed",
                    "status_label": "失败",
                    "subject_summary": "market-catalog | sol / 15m",
                    "error_summary": "RuntimeError: sync failed",
                    "request_summary": {"market": "sol", "cycle": "15m", "sync_command": "market-catalog"},
                },
            },
        },
    )

    payload = service.load_console_runtime_state(root=Path("/tmp/v2"))
    assert payload["recent_active_task_briefs"] == [
        {
            "task_id": "task_running",
            "action_id": "research_train_run",
            "status": "running",
            "status_label": "运行中",
            "status_group": "active",
            "subject_summary": "planned | sol / 15m",
            "headline": "运行中 · planned | sol / 15m · task_running",
            "summary": "Training offsets · training_offsets · 42%",
            "supporting_text": "/tmp/training",
            "progress_summary": "Training offsets · training_offsets · 42%",
            "result_summary": None,
            "error_summary": None,
            "linked_object": None,
            "primary_output_path": "/tmp/training",
            "updated_at": None,
            "request_summary": {"market": "sol", "cycle": "15m", "run_label": "planned"},
        }
    ]
    assert payload["recent_failed_task_briefs"][0]["headline"] == "失败 · market-catalog | sol / 15m · task_failed"
    assert payload["recent_failed_task_briefs"][0]["summary"] == "RuntimeError: sync failed"
    assert payload["latest_task_briefs"]["latest"]["headline"] == "失败 · market-catalog | sol / 15m · task_failed"
    assert payload["latest_task_briefs"]["terminal"]["headline"] == "失败 · market-catalog | sol / 15m · task_failed"
    assert payload["latest_task_briefs"]["failed"]["headline"] == "失败 · market-catalog | sol / 15m · task_failed"
    assert payload["operator_summary"] == {
        "has_active_tasks": True,
        "has_failed_tasks": True,
        "active_task_count": 1,
        "failed_task_count": 1,
        "invalid_task_file_count": 0,
        "invalid_task_files": [],
        "history_retention": {
            "total_task_count": 0,
            "retained_task_count": 0,
            "dropped_task_count": 0,
            "is_truncated": False,
            "row_limit": 0,
            "group_row_limit": 0,
        },
        "history_truncated": False,
        "history_limit": 0,
        "history_group_limit": 0,
        "retained_task_count": 0,
        "dropped_task_count": 0,
        "summary_source": None,
        "recovery_reason": None,
        "warnings": [],
        "latest_headline": "失败 · market-catalog | sol / 15m · task_failed",
        "latest_active_headline": "运行中 · planned | sol / 15m · task_running",
        "latest_terminal_headline": "失败 · market-catalog | sol / 15m · task_failed",
        "latest_failed_headline": "失败 · market-catalog | sol / 15m · task_failed",
        "latest_failed_summary": "RuntimeError: sync failed",
    }
