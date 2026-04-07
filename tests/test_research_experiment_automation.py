from __future__ import annotations

import csv
import json
from pathlib import Path

from pm15min.research.automation import (
    build_codex_cycle_prompt,
    build_autorun_status_report,
    find_incomplete_experiment_runs,
    next_autorun_failure_state,
    prepare_codex_home,
    record_session_update,
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
    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert str(root) in prompt
    assert str(session_dir) in prompt
    assert "program.md" in prompt
    assert "one completed cycle only" in prompt.lower()
    assert "two simultaneous formal market runs" in prompt


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
