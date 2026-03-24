from __future__ import annotations

from pm15min.console.action_runner import execute_console_action


def test_execute_console_action_captures_stdout_and_return_code(monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.console.action_runner.build_console_action_request",
        lambda action_id, request=None: {
            "action_id": action_id,
            "normalized_request": {"market": "sol"},
            "pm15min_args": ["data", "show-summary", "--market", "sol"],
            "command_preview": "PYTHONPATH=v2/src python -m pm15min data show-summary --market sol",
        },
    )

    def _fake_main(argv):
        print('{"result":"ok","argv":["data","show-summary","--market","sol"]}')
        return 0

    payload = execute_console_action(
        action_id="data_refresh_summary",
        request={"market": "sol"},
        main_fn=_fake_main,
    )

    assert payload["dataset"] == "console_action_execution"
    assert payload["status"] == "ok"
    assert payload["return_code"] == 0
    assert payload["parsed_stdout"]["result"] == "ok"
    assert payload["execution_summary"]["has_parsed_stdout"] is True
    assert payload["execution_summary"]["parsed_stdout_type"] == "dict"


def test_execute_console_action_captures_exceptions(monkeypatch) -> None:
    monkeypatch.setattr(
        "pm15min.console.action_runner.build_console_action_request",
        lambda action_id, request=None: {
            "action_id": action_id,
            "normalized_request": {},
            "pm15min_args": ["research", "activate-bundle"],
            "command_preview": "preview",
        },
    )

    def _boom(argv):
        raise RuntimeError("boom")

    payload = execute_console_action(
        action_id="research_activate_bundle",
        main_fn=_boom,
    )

    assert payload["status"] == "error"
    assert payload["return_code"] == 1
    assert "RuntimeError: boom" in payload["stderr"]
    assert payload["parsed_stdout"] is None
    assert payload["execution_summary"]["stderr_line_count"] >= 1
