from __future__ import annotations

import pytest

from pm15min.console.actions import (
    build_console_action_request,
    list_console_action_descriptors,
    load_console_action_catalog,
)


def test_load_console_action_catalog_lists_expected_actions() -> None:
    payload = load_console_action_catalog()

    assert payload["dataset"] == "console_action_catalog"
    assert payload["for_section"] is None
    assert payload["shell_enabled"] is None
    assert payload["action_count"] == 8
    action_ids = [row["action_id"] for row in payload["actions"]]
    assert action_ids == [
        "data_refresh_summary",
        "data_sync",
        "data_build",
        "research_train_run",
        "research_bundle_build",
        "research_activate_bundle",
        "research_backtest_run",
        "research_experiment_run_suite",
    ]

    descriptors = list_console_action_descriptors()
    assert descriptors[0]["target_domain"] == "data"
    assert descriptors[0]["primary_section"] == "data_overview"
    assert descriptors[0]["section_ids"] == ["data_overview"]
    assert descriptors[0]["shell_enabled"] is True
    assert descriptors[0]["preferred_execution_mode"] == "sync"
    assert descriptors[1]["shell_enabled"] is True
    assert descriptors[1]["preferred_execution_mode"] == "async"
    assert descriptors[2]["shell_enabled"] is True
    assert descriptors[2]["preferred_execution_mode"] == "async"
    assert descriptors[3]["form_fields"][0]["field_id"] == "window_start"
    assert descriptors[3]["supports_async"] is True
    assert descriptors[3]["preferred_execution_mode"] == "async"
    assert descriptors[4]["form_fields"][0]["field_id"] == "bundle_label"
    assert descriptors[4]["supports_async"] is True
    assert descriptors[4]["preferred_execution_mode"] == "async"
    assert descriptors[5]["supports_async"] is True
    assert descriptors[5]["preferred_execution_mode"] == "async"
    assert descriptors[6]["form_fields"][4]["field_id"] == "stake_usd"
    assert descriptors[6]["form_fields"][7]["field_id"] == "parity_json"
    assert descriptors[-1]["form_fields"][0]["field_id"] == "suite_mode"
    assert "feature_set_variants" in [field["field_id"] for field in descriptors[-1]["form_fields"]]
    assert descriptors[-1]["command_role"] == "run_experiment_suite"

    bundles = load_console_action_catalog(for_section="bundles")
    assert bundles["for_section"] == "bundles"
    assert [row["action_id"] for row in bundles["actions"]] == [
        "research_bundle_build",
        "research_activate_bundle",
    ]

    executable = load_console_action_catalog(shell_enabled=True)
    assert [row["action_id"] for row in executable["actions"]] == [
        "data_refresh_summary",
        "data_sync",
        "data_build",
        "research_train_run",
        "research_bundle_build",
        "research_activate_bundle",
        "research_backtest_run",
        "research_experiment_run_suite",
    ]


def test_build_console_action_request_normalizes_data_actions() -> None:
    refresh = build_console_action_request(
        "data_refresh_summary",
        {
            "market": "SOL",
            "cycle": "15m",
            "surface": "backtest",
        },
    )
    assert refresh["normalized_request"]["market"] == "sol"
    assert refresh["normalized_request"]["write_state"] is True
    assert refresh["pm15min_args"] == [
        "data",
        "show-summary",
        "--market",
        "sol",
        "--cycle",
        "15m",
        "--surface",
        "backtest",
        "--write-state",
    ]

    sync = build_console_action_request(
        "data_sync",
        {
            "sync_command": "direct-oracle-prices",
            "market": "sol",
            "cycle": "15m",
            "surface": "backtest",
            "lookback_days": 2,
            "count": 30,
            "timeout_sec": 12.5,
        },
    )
    assert sync["normalized_request"]["sync_command"] == "direct-oracle-prices"
    assert sync["normalized_request"]["surface"] == "backtest"
    assert "--count" in sync["pm15min_args"]
    assert "--timeout-sec" in sync["pm15min_args"]

    build = build_console_action_request(
        "data_build",
        {
            "build_command": "orderbook-index",
            "market": "sol",
            "surface": "live",
            "date": "2026-03-23",
        },
    )
    assert build["normalized_request"]["build_command"] == "orderbook-index"
    assert build["normalized_request"]["date"] == "2026-03-23"
    assert build["pm15min_args"][-2:] == ["--date", "2026-03-23"]


def test_build_console_action_request_normalizes_research_actions() -> None:
    train = build_console_action_request(
        "research_train_run",
        {
            "market": "SOL",
            "profile": "Deep OTM",
            "model_family": "Deep OTM",
            "feature_set": "Deep OTM V1",
            "label_set": "Truth",
            "target": "Direction",
            "window_start": "2026-03-01",
            "window_end": "2026-03-05",
            "offsets": "7,8,9",
            "run_label": "March Run",
        },
    )
    assert train["normalized_request"]["profile"] == "deep_otm"
    assert train["normalized_request"]["run_label"] == "march_run"
    assert train["normalized_request"]["offsets"] == [7, 8, 9]
    assert train["normalized_request"]["parallel_workers"] is None
    assert "--run-label" in train["pm15min_args"]

    train_parallel = build_console_action_request(
        "research_train_run",
        {
            "market": "sol",
            "window_start": "2026-03-01",
            "window_end": "2026-03-05",
            "offsets": "7,8,9",
            "parallel_workers": "2",
        },
    )
    assert train_parallel["normalized_request"]["parallel_workers"] == 2
    assert "--parallel-workers" in train_parallel["pm15min_args"]

    bundle = build_console_action_request(
        "research_bundle_build",
        {
            "market": "sol",
            "profile": "deep_otm",
            "target": "direction",
            "offsets": [7, 8],
            "bundle_label": "Main Bundle",
            "source_training_run": "March Run",
        },
    )
    assert bundle["normalized_request"]["bundle_label"] == "main_bundle"
    assert bundle["normalized_request"]["source_training_run"] == "march_run"
    assert "--source-training-run" in bundle["pm15min_args"]

    activate = build_console_action_request(
        "research_activate_bundle",
        {
            "market": "sol",
            "profile": "deep_otm",
            "target": "direction",
            "bundle_label": "Main Bundle",
            "notes": "promote",
        },
    )
    assert activate["normalized_request"]["bundle_label"] == "main_bundle"
    assert activate["normalized_request"]["notes"] == "promote"

    backtest = build_console_action_request(
        "research_backtest_run",
        {
            "market": "sol",
            "profile": "deep_otm",
            "target": "direction",
            "spec": "baseline_truth",
            "run_label": "BT One",
            "bundle_label": "Main Bundle",
            "secondary_bundle_label": "Shadow Bundle",
            "stake_usd": "5",
            "max_notional_usd": "8",
            "fallback_reasons": "direction_prob,policy_low_confidence",
            "parity_json": '{"regime_enabled": true}',
        },
    )
    assert backtest["normalized_request"]["run_label"] == "bt_one"
    assert backtest["normalized_request"]["bundle_label"] == "main_bundle"
    assert backtest["normalized_request"]["secondary_bundle_label"] == "shadow_bundle"
    assert backtest["normalized_request"]["stake_usd"] == 5.0
    assert backtest["normalized_request"]["max_notional_usd"] == 8.0
    assert backtest["normalized_request"]["fallback_reasons"] == ["direction_prob", "policy_low_confidence"]
    assert backtest["normalized_request"]["parity_json"] == {"regime_enabled": True}
    assert "--parity-json" in backtest["pm15min_args"]

    experiment = build_console_action_request(
        "research_experiment_run_suite",
        {
            "market": "sol",
            "profile": "deep_otm",
            "suite": "Main Suite",
            "run_label": "Exp One",
        },
    )
    assert experiment["normalized_request"]["suite"] == "main_suite"
    assert experiment["normalized_request"]["run_label"] == "exp_one"
    assert experiment["normalized_request"]["suite_mode"] == "existing"
    assert experiment["command_preview"].startswith("PYTHONPATH=src python -m pm15min ")

    inline_experiment = build_console_action_request(
        "research_experiment_run_suite",
        {
            "market": "sol",
            "cycle": "15m",
            "profile": "deep_otm",
            "suite_mode": "inline",
            "suite": "Feature Matrix",
            "run_label": "Inline Exp",
            "window_start": "2026-03-01",
            "window_end": "2026-03-10",
            "markets": "sol,btc",
            "run_name": "stake_matrix",
            "group_name": "main",
            "feature_set_variants": "baseline:deep_otm_v1,wide:deep_otm_v2",
            "stakes_usd": "1,5,10",
            "max_notional_usd": "8",
            "parallel_case_workers": "3",
            "reference_variant_labels": "default,baseline",
            "completed_cases": "resume",
            "failed_cases": "skip",
        },
    )
    assert inline_experiment["normalized_request"]["suite_mode"] == "inline"
    assert inline_experiment["normalized_request"]["suite"] == "feature_matrix"
    assert inline_experiment["normalized_request"]["run_label"] == "inline_exp"
    assert inline_experiment["normalized_request"]["suite_spec_path"] == "research/experiments/suite_specs/feature_matrix.json"
    assert inline_experiment["normalized_request"]["feature_set_variants"] == [
        {"label": "baseline", "feature_set": "deep_otm_v1"},
        {"label": "wide", "feature_set": "deep_otm_v2"},
    ]
    assert inline_experiment["normalized_request"]["stakes_usd"] == [1.0, 5.0, 10.0]
    assert inline_experiment["normalized_request"]["max_notional_usd"] == 8.0
    assert inline_experiment["normalized_request"]["runtime_policy"] == {
        "completed_cases": "resume",
        "failed_cases": "skip",
        "parallel_case_workers": 3,
    }
    assert inline_experiment["normalized_request"]["compare_policy"] == {
        "reference_variant_labels": ["default", "baseline"],
    }
    inline_payload = inline_experiment["normalized_request"]["inline_suite_payload"]
    assert inline_payload["markets"][0]["feature_set_variants"][0]["feature_set"] == "deep_otm_v1"
    assert inline_payload["markets"][0]["stakes_usd"] == [1.0, 5.0, 10.0]
    assert inline_payload["runtime_policy"]["parallel_case_workers"] == 3
    assert "--suite" in inline_experiment["pm15min_args"]
    assert "research/experiments/suite_specs/feature_matrix.json" in inline_experiment["command_preview"]


def test_build_console_action_request_rejects_invalid_requests() -> None:
    with pytest.raises(ValueError, match="不支持的 console action_id"):
        build_console_action_request("missing_action", {})

    with pytest.raises(ValueError, match="不支持的 sync_command"):
        build_console_action_request(
            "data_sync",
            {"sync_command": "legacy-orderbook-depth", "market": "sol"},
        )

    with pytest.raises(ValueError, match="缺少必填 action 参数: suite"):
        build_console_action_request(
            "research_experiment_run_suite",
            {"market": "sol", "profile": "deep_otm"},
        )

    with pytest.raises(ValueError, match="window_start"):
        build_console_action_request(
            "research_experiment_run_suite",
            {
                "market": "sol",
                "profile": "deep_otm",
                "suite_mode": "inline",
                "suite": "feature_matrix",
                "run_label": "planned",
                "window_end": "2026-03-10",
            },
        )
