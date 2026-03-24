from __future__ import annotations

from pm15min.console.http.app import ConsoleHttpHandlers, route_console_http_request


def test_section_routes_dispatch_to_clear_console_api_paths(monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.load_console_home",
        lambda **kwargs: {"dataset": "console_home", "home": True},
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.load_console_runtime_state",
        lambda **kwargs: {
            "dataset": "console_runtime_summary",
            "task_count": 2,
            "recent_failed_task_briefs": [{"headline": "失败 · market-catalog | sol / 15m · task_failed"}],
        },
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.load_console_runtime_history",
        lambda **kwargs: {
            "dataset": "console_runtime_history",
            "row_count": 2,
            "operator_summary": {"history_truncated": False},
        },
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.load_console_data_overview",
        lambda **kwargs: {"dataset": "console_data_overview", "market": kwargs["market"]},
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.list_console_training_runs",
        lambda **kwargs: [{"object_type": "training_run", "market": kwargs["market"]}],
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.load_console_training_run",
        lambda **kwargs: {"dataset": "console_training_run", "run_label": kwargs["run_label"]},
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.list_console_bundles",
        lambda **kwargs: [{"object_type": "model_bundle", "market": kwargs["market"]}],
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.load_console_bundle",
        lambda **kwargs: {"dataset": "console_model_bundle", "bundle_label": kwargs["bundle_label"]},
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.list_console_backtests",
        lambda **kwargs: [{"object_type": "backtest_run", "market": kwargs["market"]}],
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.load_console_backtest",
        lambda **kwargs: {"dataset": "console_backtest_run_detail", "run_label": kwargs["run_label"]},
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.load_console_backtest_stake_sweep",
        lambda **kwargs: {"dataset": "console_backtest_stake_sweep_detail", "run_label": kwargs["run_label"]},
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.list_console_experiments",
        lambda **kwargs: [{"object_type": "experiment_run", "suite_name": kwargs["suite_name"]}],
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.load_console_experiment",
        lambda **kwargs: {"dataset": "console_experiment_run_detail", "run_label": kwargs["run_label"]},
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.load_console_experiment_matrix",
        lambda **kwargs: {"dataset": "console_experiment_matrix_detail", "run_label": kwargs["run_label"]},
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.load_console_action_catalog",
        lambda **kwargs: {
            "dataset": "console_action_catalog",
            "action_count": 8,
            "for_section": kwargs.get("for_section"),
            "shell_enabled": kwargs.get("shell_enabled"),
        },
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.build_console_action_request",
        lambda **kwargs: {"dataset": "console_action_plan", "action_id": kwargs["action_id"]},
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.list_console_tasks",
        lambda **kwargs: {
            "dataset": "console_task_list",
            "row_count": 1,
            "rows": [{"task_id": "task_1"}],
            "action_id_filter": kwargs.get("action_id"),
            "action_ids_filter": list(kwargs.get("action_ids") or ()),
            "status_group_filter": kwargs.get("status_group"),
            "marker_filter": kwargs.get("marker"),
            "group_by": kwargs.get("group_by"),
        },
    )
    monkeypatch.setattr(
        "pm15min.console.http.routes.console_service.load_console_task",
        lambda **kwargs: {
            "dataset": "console_task",
            "task_id": kwargs["task_id"],
            "task_brief": {"headline": "运行中 · planned | sol / 15m · task_1"},
            "error_brief": None,
        },
    )

    assert route_console_http_request(method="GET", target="/api/console/home").payload["dataset"] == "console_home"
    runtime_state = route_console_http_request(method="GET", target="/api/console/runtime-state")
    assert runtime_state.payload["dataset"] == "console_runtime_summary"
    assert runtime_state.payload["recent_failed_task_briefs"] == [
        {"headline": "失败 · market-catalog | sol / 15m · task_failed"}
    ]
    runtime_history = route_console_http_request(method="GET", target="/api/console/runtime-history")
    assert runtime_history.payload["dataset"] == "console_runtime_history"
    assert runtime_history.payload["row_count"] == 2
    assert route_console_http_request(method="GET", target="/api/console/data-overview?market=sol").payload["market"] == "sol"

    training_list = route_console_http_request(method="GET", target="/api/console/training-runs?market=sol")
    assert training_list.status_code == 200
    assert training_list.payload["section"] == "training_runs"
    assert training_list.payload["rows"][0]["object_type"] == "training_run"

    training_detail = route_console_http_request(method="GET", target="/api/console/training-runs?run_label=r1")
    assert training_detail.payload["dataset"] == "console_training_run"
    assert training_detail.payload["run_label"] == "r1"

    bundle_list = route_console_http_request(method="GET", target="/api/console/bundles?market=sol")
    assert bundle_list.payload["rows"][0]["object_type"] == "model_bundle"

    bundle_detail = route_console_http_request(
        method="GET",
        target="/api/console/bundles?bundle_label=b1&profile=deep_otm&target=direction",
    )
    assert bundle_detail.payload["dataset"] == "console_model_bundle"
    assert bundle_detail.payload["bundle_label"] == "b1"

    backtest_list = route_console_http_request(method="GET", target="/api/console/backtests?market=sol")
    assert backtest_list.payload["rows"][0]["object_type"] == "backtest_run"

    backtest_detail = route_console_http_request(
        method="GET",
        target="/api/console/backtests?run_label=bt1&profile=deep_otm&spec=baseline_truth",
    )
    assert backtest_detail.payload["dataset"] == "console_backtest_run_detail"
    assert backtest_detail.payload["run_label"] == "bt1"

    backtest_stake_sweep = route_console_http_request(
        method="GET",
        target="/api/console/backtests/stake-sweep?run_label=bt1&profile=deep_otm&spec=baseline_truth",
    )
    assert backtest_stake_sweep.payload["dataset"] == "console_backtest_stake_sweep_detail"
    assert backtest_stake_sweep.payload["section"] == "backtest_stake_sweep"

    experiment_list = route_console_http_request(method="GET", target="/api/console/experiments?suite=suite1")
    assert experiment_list.payload["rows"][0]["object_type"] == "experiment_run"

    experiment_detail = route_console_http_request(
        method="GET",
        target="/api/console/experiments?suite=suite1&run_label=exp1",
    )
    assert experiment_detail.payload["dataset"] == "console_experiment_run_detail"
    assert experiment_detail.payload["run_label"] == "exp1"

    experiment_matrix = route_console_http_request(
        method="GET",
        target="/api/console/experiments/matrix?suite=suite1&run_label=exp1",
    )
    assert experiment_matrix.payload["dataset"] == "console_experiment_matrix_detail"
    assert experiment_matrix.payload["section"] == "experiment_matrix"

    action_catalog = route_console_http_request(
        method="GET",
        target="/api/console/actions?for_section=bundles&shell_enabled=true",
    )
    assert action_catalog.payload["dataset"] == "console_action_catalog"
    assert action_catalog.payload["for_section"] == "bundles"
    assert action_catalog.payload["shell_enabled"] is True

    action_plan = route_console_http_request(
        method="GET",
        target="/api/console/actions?action_id=data_refresh_summary&market=sol",
    )
    assert action_plan.payload["dataset"] == "console_action_plan"
    assert action_plan.payload["action_id"] == "data_refresh_summary"

    task_list = route_console_http_request(method="GET", target="/api/console/tasks?status=ok&limit=5")
    assert task_list.payload["dataset"] == "console_task_list"
    assert task_list.payload["section"] == "tasks"

    task_detail = route_console_http_request(method="GET", target="/api/console/tasks?task_id=task_1")
    assert task_detail.payload["dataset"] == "console_task"
    assert task_detail.payload["task_id"] == "task_1"
    assert task_detail.payload["task_brief"] == {
        "headline": "运行中 · planned | sol / 15m · task_1"
    }

    filtered_task_list = route_console_http_request(
        method="GET",
        target="/api/console/tasks?action_id=research_train_run&status=ok&limit=5",
    )
    assert filtered_task_list.payload["dataset"] == "console_task_list"
    assert filtered_task_list.payload["section"] == "tasks"

    multi_task_list = route_console_http_request(
        method="GET",
        target="/api/console/tasks?action_ids=research_train_run,research_backtest_run&status=ok&limit=5",
    )
    assert multi_task_list.payload["dataset"] == "console_task_list"
    assert multi_task_list.payload["action_ids_filter"] == ["research_train_run", "research_backtest_run"]
    assert filtered_task_list.payload["action_id_filter"] == "research_train_run"

    multi_filtered_task_list = route_console_http_request(
        method="GET",
        target="/api/console/tasks?action_ids=research_train_run,research_backtest_run&status=ok&limit=5",
    )
    assert multi_filtered_task_list.payload["dataset"] == "console_task_list"
    assert multi_filtered_task_list.payload["action_ids_filter"] == [
        "research_train_run",
        "research_backtest_run",
    ]
    group_task_list = route_console_http_request(
        method="GET",
        target="/api/console/tasks?status_group=failed&marker=failed&group_by=action_id&limit=5",
    )
    assert group_task_list.payload["status_group_filter"] == "failed"
    assert group_task_list.payload["marker_filter"] == "failed"
    assert group_task_list.payload["group_by"] == "action_id"


def test_section_route_overrides_and_invalid_request() -> None:
    handlers = ConsoleHttpHandlers(
        health_handler=lambda: {"status": "ok"},
        console_handler=lambda query: {"dataset": "console_home"},
        section_handlers={
            "/api/console/home": lambda query: {"dataset": "custom_home", "query": dict(query)},
        },
    )

    custom = route_console_http_request(method="GET", target="/api/console/home?foo=bar", handlers=handlers)
    assert custom.status_code == 200
    assert custom.payload["dataset"] == "custom_home"
    assert custom.payload["query"]["foo"] == "bar"

    invalid = route_console_http_request(
        method="GET",
        target="/api/console/experiments?run_label=exp1",
    )
    assert invalid.status_code == 400
    assert invalid.payload["error"]["code"] == "invalid_request"

    invalid_backtest_stake_sweep = route_console_http_request(
        method="GET",
        target="/api/console/backtests/stake-sweep?profile=deep_otm&spec=baseline_truth",
    )
    assert invalid_backtest_stake_sweep.status_code == 400
    assert invalid_backtest_stake_sweep.payload["error"]["code"] == "invalid_request"

    invalid_experiment_matrix = route_console_http_request(
        method="GET",
        target="/api/console/experiments/matrix?run_label=exp1",
    )
    assert invalid_experiment_matrix.status_code == 400
    assert invalid_experiment_matrix.payload["error"]["code"] == "invalid_request"

    invalid_bool = route_console_http_request(
        method="GET",
        target="/api/console/actions?shell_enabled=maybe",
    )
    assert invalid_bool.status_code == 400
    assert invalid_bool.payload["error"]["code"] == "invalid_request"

    invalid_limit = route_console_http_request(
        method="GET",
        target="/api/console/tasks?limit=bad",
    )
    assert invalid_limit.status_code == 400
    assert invalid_limit.payload["error"]["code"] == "invalid_request"

    invalid_status_group = route_console_http_request(
        method="GET",
        target="/api/console/tasks?status_group=unknown",
    )
    assert invalid_status_group.status_code == 400
    assert invalid_status_group.payload["error"]["code"] == "invalid_request"

    invalid_marker = route_console_http_request(
        method="GET",
        target="/api/console/tasks?marker=weird",
    )
    assert invalid_marker.status_code == 400
    assert invalid_marker.payload["error"]["code"] == "invalid_request"

    invalid_group_by = route_console_http_request(
        method="GET",
        target="/api/console/tasks?group_by=weird",
    )
    assert invalid_group_by.status_code == 400
    assert invalid_group_by.payload["error"]["code"] == "invalid_request"
