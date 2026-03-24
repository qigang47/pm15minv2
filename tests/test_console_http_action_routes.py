from __future__ import annotations

import pytest

from pm15min.console.http.action_routes import (
    execute_console_action_payload,
    load_console_action_catalog_payload,
    load_console_action_plan_payload,
)


def test_action_route_helpers_build_catalog_and_plan(monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.console.http.action_routes.load_console_action_catalog",
        lambda **kwargs: {
            "dataset": "console_action_catalog",
            "action_count": 8,
            "for_section": kwargs.get("for_section"),
            "shell_enabled": kwargs.get("shell_enabled"),
        },
    )
    monkeypatch.setattr(
        "pm15min.console.http.action_routes.build_console_action_request",
        lambda action_id, request=None: {
            "dataset": "console_action_plan",
            "action_id": action_id,
            "normalized_request": dict(request or {}),
        },
    )

    catalog = load_console_action_catalog_payload({"for_section": "bundles", "shell_enabled": "true"})
    plan = load_console_action_plan_payload({"action_id": "data_refresh_summary", "market": "sol"})

    assert catalog["dataset"] == "console_action_catalog"
    assert catalog["section"] == "actions"
    assert catalog["for_section"] == "bundles"
    assert catalog["shell_enabled"] is True
    assert plan["dataset"] == "console_action_plan"
    assert plan["action_id"] == "data_refresh_summary"
    assert plan["normalized_request"]["market"] == "sol"


def test_action_route_helpers_execute_and_validate(monkeypatch) -> None:
    payload = execute_console_action_payload(
        {"action_id": "research_activate_bundle", "request": {"bundle_label": "main"}},
        executor=lambda action_id, request=None: {
            "dataset": "console_action_execution",
            "action_id": action_id,
            "status": "ok",
            "normalized_request": dict(request or {}),
        },
    )
    assert payload["dataset"] == "console_action_execution"
    assert payload["section"] == "actions"
    assert payload["action_id"] == "research_activate_bundle"

    async_payload = execute_console_action_payload(
        {
            "action_id": "research_train_run",
            "execution_mode": "async",
            "request": {"market": "sol"},
            "root": "/tmp/v2",
        },
        task_submitter=lambda action_id, request=None, root=None: {
            "dataset": "console_task",
            "task_id": "task_1",
            "action_id": action_id,
            "root": root,
        },
    )
    assert async_payload["dataset"] == "console_task"
    assert async_payload["execution_mode"] == "async"
    assert async_payload["task_id"] == "task_1"

    with pytest.raises(ValueError, match="Missing required action field: action_id"):
        load_console_action_plan_payload({})

    with pytest.raises(ValueError, match="request must be a mapping"):
        execute_console_action_payload({"action_id": "x", "request": ["bad"]})

    with pytest.raises(ValueError, match="Invalid boolean token"):
        load_console_action_catalog_payload({"shell_enabled": "maybe"})

    with pytest.raises(ValueError, match="Invalid execution_mode"):
        execute_console_action_payload({"action_id": "x", "execution_mode": "later"})
