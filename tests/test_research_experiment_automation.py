from __future__ import annotations

import csv
import json
from pathlib import Path

from pm15min.research.automation import (
    apply_codex_auth_override,
    apply_codex_provider_override,
    build_codex_exec_command,
    build_codex_exec_extra_args,
    build_codex_cycle_prompt,
    build_autorun_status_report,
    find_incomplete_experiment_runs,
    is_transient_codex_provider_failure,
    next_autorun_failure_state,
    prepare_codex_home,
    record_session_update,
    resolve_autorun_session_dir,
    resolve_codex_exec_binary,
    summarize_experiment_run,
)


def test_summarize_experiment_run_reads_summary_and_top_case(tmp_path: Path) -> None:
    run_dir = tmp_path / "research" / "experiments" / "runs" / "suite=test" / "run=test-run"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "suite_name": "test_suite",
                "run_label": "test_run",
                "cases": 3,
                "completed_cases": 2,
                "failed_cases": 1,
                "leaderboard_rows": 2,
                "top_roi_pct": 12.5,
                "markets": ["btc"],
            }
        ),
        encoding="utf-8",
    )
    with (run_dir / "leaderboard.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["market", "group_name", "run_name", "target", "variant_label", "roi_pct", "pnl_sum", "trades"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "market": "btc",
                "group_name": "core",
                "run_name": "baseline",
                "target": "direction",
                "variant_label": "default",
                "roi_pct": "12.5",
                "pnl_sum": "4.0",
                "trades": "7",
            }
        )

    payload = summarize_experiment_run(run_dir)

    assert payload["suite_name"] == "test_suite"
    assert payload["run_label"] == "test_run"
    assert payload["cases"] == 3
    assert payload["completed_cases"] == 2
    assert payload["failed_cases"] == 1
    assert payload["top_case"] == {
        "market": "btc",
        "group_name": "core",
        "run_name": "baseline",
        "target": "direction",
        "variant_label": "default",
        "roi_pct": 12.5,
        "pnl_sum": 4.0,
        "trades": 7,
    }


def test_summarize_experiment_run_reads_quick_screen_summary_and_top_case(tmp_path: Path) -> None:
    run_dir = tmp_path / "research" / "experiments" / "runs" / "suite=test" / "run=test-quick-screen"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "quick_screen_summary.json").write_text(
        json.dumps(
            {
                "suite_name": "quick_screen_suite",
                "run_label": "quick_screen_run",
                "top_k": 2,
                "markets": ["sol"],
                "rows": 3,
                "selected_rows": 2,
            }
        ),
        encoding="utf-8",
    )
    with (run_dir / "quick_screen_leaderboard.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "market",
                "group_name",
                "run_name",
                "feature_set",
                "variant_label",
                "trade_rows",
                "traded_winner_in_band_rows",
                "backed_winner_in_band_rows",
                "rank",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "market": "sol",
                "group_name": "focus_search",
                "run_name": "focus_search__swap_obv",
                "feature_set": "focus_sol_34_v6_swap_dow_sin_for_obv_z",
                "variant_label": "default",
                "trade_rows": "5",
                "traded_winner_in_band_rows": "2",
                "backed_winner_in_band_rows": "4",
                "rank": "1",
            }
        )

    payload = summarize_experiment_run(run_dir)

    assert payload["suite_name"] == "quick_screen_suite"
    assert payload["run_label"] == "quick_screen_run"
    assert payload["cases"] == 3
    assert payload["completed_cases"] == 3
    assert payload["failed_cases"] == 0
    assert payload["leaderboard_rows"] == 3
    assert payload["top_roi_pct"] is None
    assert payload["top_case"] == {
        "market": "sol",
        "group_name": "focus_search",
        "run_name": "focus_search__swap_obv",
        "target": None,
        "variant_label": "default",
        "feature_set": "focus_sol_34_v6_swap_dow_sin_for_obv_z",
        "roi_pct": None,
        "pnl_sum": None,
        "trades": 5,
        "trade_rows": 5,
        "traded_winner_in_band_rows": 2,
        "backed_winner_in_band_rows": 4,
        "rank": 1,
    }


def test_summarize_experiment_run_reads_incomplete_formal_run_from_logs_and_suite_spec(tmp_path: Path) -> None:
    suite_name = "demo_suite"
    run_label = "demo_run"
    suite_spec_dir = tmp_path / "research" / "experiments" / "suite_specs"
    suite_spec_dir.mkdir(parents=True, exist_ok=True)
    (suite_spec_dir / f"{suite_name}.json").write_text(
        json.dumps(
            {
                "suite_name": suite_name,
                "stakes": [2.0],
                "max_trades_per_market_values": [5],
                "markets": {
                    "xrp": {
                        "groups": {
                            "focus_search": {
                                "runs": [
                                    {
                                        "run_name": "focus_search",
                                        "feature_set_variants": [
                                            {"label": "38_v3", "feature_set": "focus_xrp_38_v3"},
                                            {"label": "38_v4", "feature_set": "focus_xrp_38_v4"},
                                        ],
                                        "weight_variants": [
                                            {"label": "current_default"},
                                            {"label": "offset_reversal_mild"},
                                            {"label": "offset_reversal_strong"},
                                        ],
                                    }
                                ]
                            }
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    run_dir = tmp_path / "research" / "experiments" / "runs" / f"suite={suite_name}" / f"run={run_label}"
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    (logs_dir / "suite.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "event": "execution_group_started",
                        "group_label": "xrp/focus_search/focus_search__fs_38_v3__w_current_default__max5",
                        "cases": 1,
                    }
                ),
                json.dumps(
                    {
                        "event": "market_cache_resolved",
                        "market": "xrp",
                        "run_name": "focus_search__fs_38_v3__w_current_default__max5__stake_2usd__max_10usd",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    payload = summarize_experiment_run(run_dir)

    assert payload["suite_name"] == suite_name
    assert payload["run_label"] == run_label
    assert payload["cases"] == 6
    assert payload["completed_cases"] == 0
    assert payload["failed_cases"] == 0
    assert payload["leaderboard_rows"] == 0
    assert payload["markets"] == ["xrp"]
    assert payload["top_case"] is None
    assert payload["raw_summary"]["state"] == "stuck_seed_case"
    assert payload["raw_summary"]["last_event"] == "market_cache_resolved"
    assert payload["raw_summary"]["summary_exists"] is False


def test_record_session_update_appends_results_and_session_sections(tmp_path: Path) -> None:
    session_dir = tmp_path / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    (session_dir / "session.md").write_text(
        "\n".join(
            [
                "# Demo Session",
                "",
                "## Cycles completed",
                "",
                "## What's been tried",
                "",
                "## Open issues",
                "",
            ]
        ),
        encoding="utf-8",
    )

    outputs = record_session_update(
        session_dir=session_dir,
        cycle="007",
        team="green",
        metric="roi_pct=12.5",
        status="partial",
        description="ran one cycle",
        files_changed=["program.md", "scripts/research/run_one_experiment.sh"],
        timestamp="2026-04-04T16:00:00+08:00",
        cycle_eval_md="# Cycle 007\n\nsummary",
        cycle_notes=["started codex background automation mvp"],
        tried_lines=["added program.md and one-shot runner"],
        open_issue_lines=["status script still needs operator validation"],
    )

    results_lines = (session_dir / "results.tsv").read_text(encoding="utf-8").strip().splitlines()
    assert results_lines[0] == "cycle\tteam\tmetric\tstatus\tdescription\tfiles_changed\ttimestamp"
    assert results_lines[1].startswith("007\tgreen\troi_pct=12.5\tpartial\tran one cycle\tprogram.md,scripts/research/run_one_experiment.sh\t2026-04-04T16:00:00+08:00")
    session_text = (session_dir / "session.md").read_text(encoding="utf-8")
    assert "- `007`" in session_text
    assert "started codex background automation mvp" in session_text
    assert "- added program.md and one-shot runner" in session_text
    assert "- status script still needs operator validation" in session_text
    assert Path(outputs["cycle_eval_path"]).read_text(encoding="utf-8") == "# Cycle 007\n\nsummary"


def test_build_codex_cycle_prompt_references_program_and_session(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    program_path = root / "program_custom.md"
    program_path.write_text("# demo program\n", encoding="utf-8")
    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert str(root) in prompt
    assert str(session_dir) in prompt
    assert str(program_path) in prompt
    assert "read program_custom.md and the latest session artifacts before making changes." in prompt.lower()
    assert "one completed cycle only" in prompt.lower()
    assert "two simultaneous formal market runs" in prompt
    assert "do not scan the entire repository" in prompt.lower()
    assert "prefer resuming or launching one formal experiment" in prompt.lower()
    assert "if `rg` is unavailable" in prompt.lower()


def test_build_codex_cycle_prompt_includes_existing_autorun_snapshot(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    autorun_dir = root / "var" / "research" / "autorun"
    autorun_dir.mkdir(parents=True, exist_ok=True)
    (autorun_dir / "codex-background.status.json").write_text(
        json.dumps(
            {
                "state": "idle",
                "iteration": 3,
                "pid": None,
                "last_started_at": "2026-04-12T00:00:00Z",
                "last_finished_at": "2026-04-12T00:10:00Z",
            }
        ),
        encoding="utf-8",
    )
    run_dir = root / "research" / "experiments" / "runs" / "suite=test_suite" / "run=test_run"
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "logs" / "suite.jsonl").write_text(
        json.dumps({"event": "market_cache_resolved"}) + "\n",
        encoding="utf-8",
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert "current autorun snapshot already collected for you:" in prompt.lower()
    assert "autorun state: idle" in prompt.lower()
    assert "test_suite / test_run / state=stuck_seed_case" in prompt


def test_build_codex_exec_extra_args_adds_skip_git_repo_check_once() -> None:
    assert build_codex_exec_extra_args() == ("--skip-git-repo-check",)
    assert build_codex_exec_extra_args("--model gpt-5.4") == (
        "--model",
        "gpt-5.4",
        "--skip-git-repo-check",
    )
    assert build_codex_exec_extra_args("--skip-git-repo-check --model gpt-5.4") == (
        "--skip-git-repo-check",
        "--model",
        "gpt-5.4",
    )


def test_build_codex_exec_command_places_skip_git_check_before_stdin_prompt(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    output_path = root / "last-output.txt"

    command = build_codex_exec_command(
        project_root=root,
        output_path=output_path,
        sandbox_mode="danger-full-access",
        model="gpt-5.4",
        extra_args=None,
    )

    assert Path(command[0]).name == "codex"
    assert command[1] == "exec"
    assert "--skip-git-repo-check" in command
    assert command[-1] == "-"
    assert command.index("--skip-git-repo-check") < len(command) - 1
    assert command[2:8] == (
        "--cd",
        str(root.resolve()),
        "--output-last-message",
        str(output_path.resolve()),
        "--sandbox",
        "danger-full-access",
    )


def test_resolve_autorun_session_dir_prefers_explicit_value(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    explicit = root / "sessions" / "manual"

    resolved = resolve_autorun_session_dir(
        root,
        explicit_session_dir=explicit,
        program_path=root / "program.md",
    )

    assert resolved == explicit.resolve()


def test_resolve_autorun_session_dir_reads_active_session_from_program(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    program_path = root / "program.md"
    program_path.write_text(
        "\n".join(
            [
                "# Demo Program",
                "",
                "## Canonical References",
                "",
                "- Active session: `sessions/deep_otm_baseline_retrain_autoresearch/session.md`",
                "- Archived session: `sessions/old_line/session.md`",
            ]
        ),
        encoding="utf-8",
    )

    resolved = resolve_autorun_session_dir(root, program_path=program_path)

    assert resolved == (root / "sessions" / "deep_otm_baseline_retrain_autoresearch").resolve()


def test_resolve_codex_exec_binary_falls_back_to_local_bin(tmp_path: Path) -> None:
    home_root = tmp_path / "home"
    local_bin = home_root / ".local" / "bin"
    local_bin.mkdir(parents=True, exist_ok=True)
    codex_path = local_bin / "codex"
    codex_path.write_text("#!/bin/sh\n", encoding="utf-8")

    resolved = resolve_codex_exec_binary(home_root=home_root, env_path="/usr/bin:/bin")

    assert resolved == str(codex_path.resolve())


def test_prepare_codex_home_copies_minimal_runtime_files_without_skills(tmp_path: Path) -> None:
    source_home = tmp_path / "source-home"
    source_codex_dir = source_home / ".codex"
    source_codex_dir.mkdir(parents=True, exist_ok=True)
    (source_codex_dir / "auth.json").write_text('{"token":"demo"}', encoding="utf-8")
    (source_codex_dir / "config.toml").write_text('model = "gpt-5"', encoding="utf-8")
    (source_codex_dir / "AGENTS.md").write_text("# local guidance", encoding="utf-8")
    (source_codex_dir / "version.json").write_text('{"version":"1"}', encoding="utf-8")
    (source_codex_dir / "skills" / "autoresearch").mkdir(parents=True, exist_ok=True)
    (source_codex_dir / "skills" / "autoresearch" / "SKILL.md").write_text(
        "---\ndescription: broken\n---\n",
        encoding="utf-8",
    )

    isolated_home = tmp_path / "isolated-home"
    payload = prepare_codex_home(isolated_home, source_home=source_home)

    isolated_codex_dir = isolated_home / ".codex"
    assert payload["home_root"] == str(isolated_home)
    assert payload["codex_dir"] == str(isolated_codex_dir)
    assert (isolated_codex_dir / "auth.json").read_text(encoding="utf-8") == '{"token":"demo"}'
    assert (isolated_codex_dir / "config.toml").read_text(encoding="utf-8") == 'model = "gpt-5"'
    assert (isolated_codex_dir / "AGENTS.md").read_text(encoding="utf-8") == "# local guidance"
    assert (isolated_codex_dir / "version.json").read_text(encoding="utf-8") == '{"version":"1"}'
    assert not (isolated_codex_dir / "skills").exists()


def test_apply_codex_provider_override_updates_only_isolated_home(tmp_path: Path) -> None:
    source_home = tmp_path / "source-home"
    source_codex_dir = source_home / ".codex"
    source_codex_dir.mkdir(parents=True, exist_ok=True)
    (source_codex_dir / "auth.json").write_text('{"OPENAI_API_KEY":"primary-key"}', encoding="utf-8")
    (source_codex_dir / "config.toml").write_text(
        '\n'.join(
            [
                'model = "gpt-5.4"',
                'model_provider = "codex"',
                "",
                "[model_providers.codex]",
                'name = "codex"',
                'base_url = "https://nimabo.cn/v1"',
                'wire_api = "responses"',
                "requires_openai_auth = true",
                "",
            ]
        ),
        encoding="utf-8",
    )

    isolated_home = tmp_path / "isolated-home"
    prepare_codex_home(isolated_home, source_home=source_home)
    payload = apply_codex_provider_override(
        isolated_home,
        base_url="https://ai.changyou.club/v1",
        api_key="fallback-key",
    )

    isolated_codex_dir = isolated_home / ".codex"
    assert payload["codex_dir"] == str(isolated_codex_dir)
    config_text = (isolated_codex_dir / "config.toml").read_text(encoding="utf-8")
    assert 'base_url = "https://ai.changyou.club/v1"' in config_text
    auth_payload = json.loads((isolated_codex_dir / "auth.json").read_text(encoding="utf-8"))
    assert auth_payload["OPENAI_API_KEY"] == "fallback-key"
    source_auth_payload = json.loads((source_codex_dir / "auth.json").read_text(encoding="utf-8"))
    assert source_auth_payload["OPENAI_API_KEY"] == "primary-key"


def test_apply_codex_auth_override_replaces_auth_and_clears_provider_override(tmp_path: Path) -> None:
    source_home = tmp_path / "source-home"
    source_codex_dir = source_home / ".codex"
    source_codex_dir.mkdir(parents=True, exist_ok=True)
    (source_codex_dir / "auth.json").write_text('{"OPENAI_API_KEY":"primary-key"}', encoding="utf-8")
    (source_codex_dir / "config.toml").write_text(
        '\n'.join(
            [
                'model = "gpt-5.4"',
                'model_provider = "codex"',
                "",
                "[model_providers.codex]",
                'name = "codex"',
                'base_url = "https://nimabo.cn/v1"',
                'wire_api = "responses"',
                "requires_openai_auth = true",
                "",
            ]
        ),
        encoding="utf-8",
    )

    isolated_home = tmp_path / "isolated-home"
    prepare_codex_home(isolated_home, source_home=source_home)
    payload = apply_codex_auth_override(
        isolated_home,
        auth_payload={
            "auth_mode": "chatgpt",
            "tokens": {
                "access_token": "demo-token",
            },
        },
    )

    isolated_codex_dir = isolated_home / ".codex"
    assert payload["codex_dir"] == str(isolated_codex_dir)
    config_text = (isolated_codex_dir / "config.toml").read_text(encoding="utf-8")
    assert "[model_providers.codex]" not in config_text
    assert 'model_provider = "openai"' in config_text
    auth_payload = json.loads((isolated_codex_dir / "auth.json").read_text(encoding="utf-8"))
    assert auth_payload["auth_mode"] == "chatgpt"
    assert auth_payload["tokens"]["access_token"] == "demo-token"


def test_is_transient_codex_provider_failure_matches_service_unavailable_retry_log() -> None:
    output = """
    ERROR codex_api::endpoint::responses: error=http 503 Service Unavailable
    Reconnecting... 3/5 (unexpected status 503 Service Unavailable: Service temporarily unavailable, url: https://nimabo.cn/v1/responses)
    """
    assert is_transient_codex_provider_failure(output) is True


def test_is_transient_codex_provider_failure_matches_websocket_internal_error() -> None:
    output = """
    ERROR codex_api::endpoint::responses_websocket: failed to connect to websocket: HTTP error: 500 Internal Server Error, url: wss://api.openai.com/v1/responses
    ERROR: Reconnecting... 2/5
    """
    assert is_transient_codex_provider_failure(output) is True


def test_is_transient_codex_provider_failure_matches_rate_limit_retry_log() -> None:
    output = """
    ERROR: exceeded retry limit, last status: 429 Too Many Requests
    """
    assert is_transient_codex_provider_failure(output) is True


def test_is_transient_codex_provider_failure_ignores_regular_traceback() -> None:
    output = """
    Traceback (most recent call last):
      File "demo.py", line 1, in <module>
        raise RuntimeError("boom")
    RuntimeError: boom
    """
    assert is_transient_codex_provider_failure(output) is False


def test_next_autorun_failure_state_stops_after_threshold() -> None:
    first = next_autorun_failure_state(previous_failures=0, exit_code=1, max_consecutive_failures=3)
    second = next_autorun_failure_state(
        previous_failures=int(first["failure_count"]),
        exit_code=1,
        max_consecutive_failures=3,
    )
    third = next_autorun_failure_state(
        previous_failures=int(second["failure_count"]),
        exit_code=1,
        max_consecutive_failures=3,
    )
    recovered = next_autorun_failure_state(
        previous_failures=int(third["failure_count"]),
        exit_code=0,
        max_consecutive_failures=3,
    )

    assert first == {"failure_count": 1, "should_stop": False}
    assert second == {"failure_count": 2, "should_stop": False}
    assert third == {"failure_count": 3, "should_stop": True}
    assert recovered == {"failure_count": 0, "should_stop": False}


def test_find_incomplete_experiment_runs_marks_seed_case_stall(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    stalled_run = root / "research" / "experiments" / "runs" / "suite=demo" / "run=stalled"
    stalled_logs = stalled_run / "logs"
    stalled_logs.mkdir(parents=True, exist_ok=True)
    (stalled_logs / "suite.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"event": "execution_group_started", "group_label": "eth/baseline_grid/baseline__max1"}),
                json.dumps({"event": "execution_group_seed_case_started", "case_label": "eth/baseline__max1__stake_1usd"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    complete_run = root / "research" / "experiments" / "runs" / "suite=demo" / "run=complete"
    (complete_run / "logs").mkdir(parents=True, exist_ok=True)
    (complete_run / "summary.json").write_text('{"suite_name":"demo"}', encoding="utf-8")
    (complete_run / "logs" / "suite.jsonl").write_text(
        json.dumps({"event": "market_completed", "case_label": "done"}) + "\n",
        encoding="utf-8",
    )

    payload = find_incomplete_experiment_runs(root)

    assert len(payload) == 1
    assert payload[0]["run_dir"] == str(stalled_run)
    assert payload[0]["state"] == "stuck_seed_case"
    assert payload[0]["last_event"] == "execution_group_seed_case_started"
    assert payload[0]["completed_cases"] == 0


def test_build_autorun_status_report_includes_incomplete_runs(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    autorun_dir = root / "var" / "research" / "autorun"
    autorun_dir.mkdir(parents=True, exist_ok=True)
    status_path = autorun_dir / "codex-background.status.json"
    status_path.write_text(
        json.dumps(
            {
                "state": "idle",
                "iteration": 4,
                "last_exit_code": 1,
                "failure_count": 2,
                "last_output_path": str(autorun_dir / "codex-last-output.txt"),
                "last_prompt_path": str(autorun_dir / "codex-last-prompt.md"),
            }
        ),
        encoding="utf-8",
    )
    (autorun_dir / "codex-background.log").write_text("line1\nline2\nline3\n", encoding="utf-8")

    stalled_run = root / "research" / "experiments" / "runs" / "suite=demo" / "run=stalled"
    stalled_logs = stalled_run / "logs"
    stalled_logs.mkdir(parents=True, exist_ok=True)
    (stalled_logs / "suite.jsonl").write_text(
        json.dumps({"event": "execution_group_seed_case_started", "case_label": "sol/baseline__max1__stake_1usd"})
        + "\n",
        encoding="utf-8",
    )

    payload = build_autorun_status_report(root, log_tail_lines=2)

    assert payload["status"]["failure_count"] == 2
    assert payload["log_tail"] == ["line2", "line3"]
    assert len(payload["incomplete_runs"]) == 1
    assert payload["incomplete_runs"][0]["state"] == "stuck_seed_case"


def test_build_autorun_status_report_marks_missing_running_pid_as_stale(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    autorun_dir = root / "var" / "research" / "autorun"
    autorun_dir.mkdir(parents=True, exist_ok=True)
    (autorun_dir / "codex-background.status.json").write_text(
        json.dumps(
            {
                "state": "running",
                "pid": 999999,
                "iteration": 1,
                "failure_count": 0,
            }
        ),
        encoding="utf-8",
    )

    payload = build_autorun_status_report(root)

    assert payload["status"]["state"] == "stale"
    assert payload["status"]["state_reason"] == "missing_pid"
