from __future__ import annotations

import json
from pathlib import Path

from pm15min.cli import main
from pm15min.research.manifests import build_manifest, write_manifest


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def test_console_show_data_overview_reads_persisted_summary(capsys, monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    summary_dir = root / "var" / "backtest" / "state" / "summary" / "cycle=15m" / "asset=sol"
    summary_dir.mkdir(parents=True, exist_ok=True)
    (summary_dir / "latest.json").write_text(
        json.dumps(
            {
                "domain": "data",
                "dataset": "data_surface_summary",
                "market": "sol",
                "cycle": "15m",
                "surface": "backtest",
                "generated_at": "2026-03-23T00-00-00Z",
                "generated_at_iso": "2026-03-23T00:00:00+00:00",
                "summary": {"dataset_count": 2, "existing_dataset_count": 2, "missing_dataset_count": 0},
                "audit": {"status": "ok"},
                "completeness": {"status": "ok"},
                "issues": [],
                "datasets": {
                    "truth_table": {"kind": "single_parquet", "status": "ok", "exists": True, "path": "/tmp/truth.parquet", "row_count": 10},
                    "orderbook_depth_source": {
                        "kind": "partitioned_ndjson_zst",
                        "status": "ok",
                        "exists": True,
                        "root": "/tmp/depth",
                        "partition_count": 2,
                        "date_range": {"min": "2026-03-20", "max": "2026-03-21"},
                    },
                },
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (summary_dir / "latest.manifest.json").write_text('{"object_type":"data_summary_manifest"}', encoding="utf-8")
    _patch_v2_roots(monkeypatch, root)

    rc = main(["console", "show-data-overview", "--market", "sol", "--cycle", "15m", "--surface", "backtest"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_data_overview"
    assert payload["summary_source"] == "persisted"
    assert payload["market"] == "sol"
    assert payload["dataset_rows"][0]["dataset_name"] == "orderbook_depth_source"


def test_console_show_home_and_serve(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.console.cli.handlers.load_console_home",
        lambda: {"dataset": "console_home", "section_count": 5},
    )
    monkeypatch.setattr(
        "pm15min.console.cli.handlers.load_console_runtime_state",
        lambda: {"dataset": "console_runtime_summary", "task_count": 3},
    )
    monkeypatch.setattr(
        "pm15min.console.cli.handlers.load_console_runtime_history",
        lambda: {"dataset": "console_runtime_history", "row_count": 7, "history_limit": 50},
    )
    monkeypatch.setattr(
        "pm15min.console.cli.handlers.load_console_action_catalog",
        lambda: {"dataset": "console_action_catalog", "action_count": 8},
    )
    monkeypatch.setattr(
        "pm15min.console.cli.handlers.build_console_action_request",
        lambda action_id, request=None: {"dataset": "console_action_plan", "action_id": action_id, "normalized_request": dict(request or {})},
    )
    monkeypatch.setattr(
        "pm15min.console.cli.handlers.execute_console_action",
        lambda action_id, request=None: {"dataset": "console_action_execution", "action_id": action_id, "status": "ok", "normalized_request": dict(request or {})},
    )
    monkeypatch.setattr(
        "pm15min.console.cli.handlers.submit_console_action_task",
        lambda action_id, request=None: {"dataset": "console_task", "task_id": "task_1", "action_id": action_id, "status": "queued"},
    )
    monkeypatch.setattr(
        "pm15min.console.cli.handlers.list_console_tasks",
        lambda **kwargs: {
            "dataset": "console_task_list",
            "row_count": 1,
            "rows": [{"task_id": "task_1"}],
            "action_ids_filter": list(kwargs.get("action_ids") or ()),
            "status_group_filter": kwargs.get("status_group"),
            "marker_filter": kwargs.get("marker"),
            "group_by": kwargs.get("group_by"),
        },
    )
    monkeypatch.setattr(
        "pm15min.console.cli.handlers.load_console_task",
        lambda **kwargs: {"dataset": "console_task", "task_id": kwargs["task_id"], "status": "ok"},
    )
    serve_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        "pm15min.console.cli.handlers.serve_console_http",
        lambda **kwargs: serve_calls.append(dict(kwargs)),
    )

    rc = main(["console", "show-home"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_home"
    assert payload["section_count"] == 5

    rc = main(["console", "show-runtime-state"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_runtime_summary"
    assert payload["task_count"] == 3

    rc = main(["console", "show-runtime-history"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_runtime_history"
    assert payload["row_count"] == 7

    rc = main(["console", "show-actions"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_action_catalog"
    assert payload["action_count"] == 8

    rc = main(["console", "build-action", "--action-id", "data_refresh_summary", "--request-json", '{"market":"sol"}'])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_action_plan"
    assert payload["action_id"] == "data_refresh_summary"
    assert payload["normalized_request"]["market"] == "sol"

    rc = main(["console", "execute-action", "--action-id", "research_activate_bundle", "--request-json", '{"bundle_label":"main"}'])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_action_execution"
    assert payload["action_id"] == "research_activate_bundle"
    assert payload["status"] == "ok"

    rc = main(
        [
            "console",
            "execute-action",
            "--action-id",
            "research_train_run",
            "--execution-mode",
            "async",
            "--request-json",
            '{"market":"sol"}',
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_task"
    assert payload["task_id"] == "task_1"

    rc = main(["console", "list-tasks", "--status", "ok", "--limit", "5"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_task_list"
    assert payload["row_count"] == 1

    rc = main(
        [
            "console",
            "list-tasks",
            "--action-id",
            "research_train_run",
            "--action-id",
            "research_backtest_run",
        ]
    )
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["action_ids_filter"] == ["research_train_run", "research_backtest_run"]

    rc = main(["console", "list-tasks", "--status-group", "failed", "--marker", "failed", "--group-by", "action_id"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status_group_filter"] == "failed"
    assert payload["marker_filter"] == "failed"
    assert payload["group_by"] == "action_id"

    rc = main(["console", "show-task", "--task-id", "task_1"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset"] == "console_task"
    assert payload["task_id"] == "task_1"

    rc = main(["console", "serve", "--host", "127.0.0.1", "--port", "9001", "--poll-interval", "0.1"])
    assert rc == 0
    assert serve_calls == [{"host": "127.0.0.1", "port": 9001, "poll_interval": 0.1}]


def test_pm5min_console_http_defaults_are_5m(monkeypatch) -> None:
    from pm15min.console.http.app import route_console_http_request
    from pm5min.console.http import build_pm5min_console_http_handlers

    monkeypatch.setattr(
        "pm5min.console.http.console_service.load_console_data_overview",
        lambda **kwargs: {
            "dataset": "console_data_overview",
            "market": kwargs["market"],
            "cycle": kwargs["cycle"],
            "surface": kwargs["surface"],
        },
    )
    monkeypatch.setattr(
        "pm5min.console.http.console_service.load_console_bundle",
        lambda **kwargs: {
            "dataset": "console_bundle_detail",
            "market": kwargs["market"],
            "cycle": kwargs["cycle"],
            "profile": kwargs["profile"],
            "target": kwargs["target"],
            "bundle_label": kwargs["bundle_label"],
        },
    )
    monkeypatch.setattr(
        "pm5min.console.http.console_service.list_console_bundles",
        lambda **kwargs: {
            "dataset": "console_bundle_list",
            "market": kwargs["market"],
            "cycle": kwargs["cycle"],
            "profile": kwargs["profile"],
            "target": kwargs["target"],
        },
    )
    monkeypatch.setattr(
        "pm5min.console.http.console_service.load_console_backtest",
        lambda **kwargs: {
            "dataset": "console_backtest_detail",
            "market": kwargs["market"],
            "cycle": kwargs["cycle"],
            "profile": kwargs["profile"],
            "spec_name": kwargs["spec_name"],
            "run_label": kwargs["run_label"],
        },
    )
    monkeypatch.setattr(
        "pm5min.console.http.console_service.list_console_backtests",
        lambda **kwargs: {
            "dataset": "console_backtest_list",
            "market": kwargs["market"],
            "cycle": kwargs["cycle"],
            "profile": kwargs["profile"],
            "spec_name": kwargs["spec_name"],
        },
    )

    handlers = build_pm5min_console_http_handlers()

    overview_response = route_console_http_request(
        method="GET",
        target="/api/console/data-overview",
        handlers=handlers,
    )
    overview_payload = json.loads(overview_response.body_bytes().decode("utf-8"))
    assert overview_response.status_code == 200
    assert overview_payload["cycle"] == "5m"
    assert overview_payload["surface"] == "backtest"

    bundle_list_response = route_console_http_request(
        method="GET",
        target="/api/console/bundles",
        handlers=handlers,
    )
    bundle_list_payload = json.loads(bundle_list_response.body_bytes().decode("utf-8"))
    assert bundle_list_response.status_code == 200
    assert bundle_list_payload["cycle"] == "5m"
    assert bundle_list_payload["profile"] is None
    assert bundle_list_payload["target"] is None

    bundles_response = route_console_http_request(
        method="GET",
        target="/api/console/bundles?bundle_label=demo",
        handlers=handlers,
    )
    bundles_payload = json.loads(bundles_response.body_bytes().decode("utf-8"))
    assert bundles_response.status_code == 200
    assert bundles_payload["cycle"] == "5m"
    assert bundles_payload["profile"] == "deep_otm_5m"
    assert bundles_payload["target"] == "direction"
    assert bundles_payload["bundle_label"] == "demo"

    backtest_list_response = route_console_http_request(
        method="GET",
        target="/api/console/backtests",
        handlers=handlers,
    )
    backtest_list_payload = json.loads(backtest_list_response.body_bytes().decode("utf-8"))
    assert backtest_list_response.status_code == 200
    assert backtest_list_payload["cycle"] == "5m"
    assert backtest_list_payload["profile"] is None
    assert backtest_list_payload["spec_name"] is None

    backtests_response = route_console_http_request(
        method="GET",
        target="/api/console/backtests?run_label=demo",
        handlers=handlers,
    )
    backtests_payload = json.loads(backtests_response.body_bytes().decode("utf-8"))
    assert backtests_response.status_code == 200
    assert backtests_payload["cycle"] == "5m"
    assert backtests_payload["profile"] == "deep_otm_5m"
    assert backtests_payload["spec_name"] == "baseline_truth"
    assert backtests_payload["run_label"] == "demo"


def test_console_show_training_run_and_bundle(capsys, monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)

    training_dir = (
        root
        / "research"
        / "training_runs"
        / "cycle=15m"
        / "asset=sol"
        / "model_family=deep_otm"
        / "target=direction"
        / "run=console_run"
    )
    offset_dir = training_dir / "offsets" / "offset=7"
    (offset_dir / "calibration").mkdir(parents=True, exist_ok=True)
    (offset_dir / "models").mkdir(parents=True, exist_ok=True)
    (training_dir / "report.md").write_text("# Training Run Summary\n", encoding="utf-8")
    (training_dir / "summary.json").write_text(
        json.dumps(
            {
                "market": "sol",
                "cycle": "15m",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "window": "2026-03-01_2026-03-01",
                "run_label": "console_run",
                "offsets": [7],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (offset_dir / "summary.json").write_text('{"offset":7,"rows":16}', encoding="utf-8")
    (offset_dir / "metrics.json").write_text('{"offset":7,"rows":16}', encoding="utf-8")
    (offset_dir / "feature_schema.json").write_text("[]", encoding="utf-8")
    (offset_dir / "feature_pruning.json").write_text('{"dropped_columns":[]}', encoding="utf-8")
    (offset_dir / "probe.json").write_text('{"probe_rows":8}', encoding="utf-8")
    (offset_dir / "report.md").write_text("# Offset Report\n", encoding="utf-8")
    (offset_dir / "calibration" / "reliability_bins.json").write_text("{}", encoding="utf-8")
    write_manifest(
        training_dir / "manifest.json",
        build_manifest(
            object_type="training_run",
            object_id="training_run:deep_otm:direction:console_run",
            market="sol",
            cycle="15m",
            path=training_dir,
            spec={"model_family": "deep_otm", "target": "direction", "run_label": "console_run"},
        ),
    )

    bundle_dir = (
        root
        / "research"
        / "model_bundles"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "bundle=console_bundle"
    )
    bundle_offset_dir = bundle_dir / "offsets" / "offset=7"
    (bundle_offset_dir / "models").mkdir(parents=True, exist_ok=True)
    (bundle_offset_dir / "calibration").mkdir(parents=True, exist_ok=True)
    (bundle_offset_dir / "diagnostics").mkdir(parents=True, exist_ok=True)
    (bundle_dir / "report.md").write_text("# Model Bundle Summary\n", encoding="utf-8")
    (bundle_dir / "summary.json").write_text(
        json.dumps(
            {
                "market": "sol",
                "cycle": "15m",
                "profile": "deep_otm",
                "target": "direction",
                "bundle_label": "console_bundle",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "model_family": "deep_otm",
                "offsets": [7],
                "allowed_blacklist_columns": [],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (bundle_offset_dir / "bundle_config.json").write_text('{"offset":7}', encoding="utf-8")
    (bundle_offset_dir / "feature_schema.json").write_text("[]", encoding="utf-8")
    (bundle_offset_dir / "diagnostics" / "summary.json").write_text('{"offset":7}', encoding="utf-8")
    write_manifest(
        bundle_dir / "manifest.json",
        build_manifest(
            object_type="model_bundle",
            object_id="model_bundle:deep_otm:direction:console_bundle",
            market="sol",
            cycle="15m",
            path=bundle_dir,
            spec={"profile": "deep_otm", "target": "direction", "bundle_label": "console_bundle", "offsets": [7]},
        ),
    )
    selection_path = (
        root
        / "research"
        / "active_bundles"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "selection.json"
    )
    selection_path.parent.mkdir(parents=True, exist_ok=True)
    selection_path.write_text(
        json.dumps(
            {
                "market": "sol",
                "cycle": "15m",
                "profile": "deep_otm",
                "target": "direction",
                "bundle_label": "console_bundle",
                "bundle_dir": str(bundle_dir),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    rc = main(["console", "show-training-run", "--market", "sol", "--model-family", "deep_otm", "--target", "direction", "--run-label", "console_run"])
    assert rc == 0
    training_payload = json.loads(capsys.readouterr().out)
    assert training_payload["dataset"] == "console_training_run"
    assert training_payload["run_label"] == "console_run"
    assert len(training_payload["offset_details"]) == 1

    rc = main(["console", "show-bundle", "--market", "sol", "--profile", "deep_otm", "--target", "direction", "--bundle-label", "console_bundle"])
    assert rc == 0
    bundle_payload = json.loads(capsys.readouterr().out)
    assert bundle_payload["dataset"] == "console_model_bundle"
    assert bundle_payload["bundle_label"] == "console_bundle"
    assert bundle_payload["is_active"] is True


def test_console_show_backtest_and_experiment(capsys, monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)

    backtest_dir = (
        root
        / "research"
        / "backtests"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "spec=baseline_truth"
        / "run=bt_console"
    )
    (backtest_dir / "logs").mkdir(parents=True, exist_ok=True)
    (backtest_dir / "report.md").write_text("# Backtest Report\n", encoding="utf-8")
    (backtest_dir / "summary.json").write_text(
        json.dumps({"market": "sol", "cycle": "15m", "profile": "deep_otm", "spec_name": "baseline_truth", "trades": 2, "roi_pct": 10.0}, indent=2),
        encoding="utf-8",
    )
    write_manifest(
        backtest_dir / "manifest.json",
        build_manifest(
            object_type="backtest_run",
            object_id="backtest_run:deep_otm:baseline_truth:bt_console",
            market="sol",
            cycle="15m",
            path=backtest_dir,
            spec={"profile": "deep_otm", "spec_name": "baseline_truth", "run_label": "bt_console"},
        ),
    )

    experiment_dir = root / "research" / "experiments" / "runs" / "suite=console_suite" / "run=exp_console"
    (experiment_dir / "logs").mkdir(parents=True, exist_ok=True)
    (experiment_dir / "report.md").write_text("# Experiment Summary\n", encoding="utf-8")
    (experiment_dir / "summary.json").write_text(
        json.dumps({"suite_name": "console_suite", "run_label": "exp_console", "cases": 3, "completed_cases": 2}, indent=2),
        encoding="utf-8",
    )
    write_manifest(
        experiment_dir / "manifest.json",
        build_manifest(
            object_type="experiment_run",
            object_id="experiment_run:console_suite:exp_console",
            market="sol",
            cycle="15m",
            path=experiment_dir,
            spec={"suite_name": "console_suite", "run_label": "exp_console"},
        ),
    )

    rc = main(["console", "show-backtest", "--market", "sol", "--profile", "deep_otm", "--spec", "baseline_truth", "--run-label", "bt_console"])
    assert rc == 0
    backtest_payload = json.loads(capsys.readouterr().out)
    assert backtest_payload["dataset"] == "console_backtest_run_detail"
    assert backtest_payload["summary"]["roi_pct"] == 10.0

    rc = main(["console", "show-experiment", "--suite", "console_suite", "--run-label", "exp_console"])
    assert rc == 0
    experiment_payload = json.loads(capsys.readouterr().out)
    assert experiment_payload["dataset"] == "console_experiment_run_detail"
    assert experiment_payload["summary"]["cases"] == 3
