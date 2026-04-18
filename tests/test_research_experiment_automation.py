from __future__ import annotations

import csv
import json
import os
import subprocess
from pathlib import Path

import pytest

from pm15min.research.automation import (
    apply_codex_auth_override,
    apply_codex_provider_override,
    build_codex_exec_command,
    build_codex_exec_extra_args,
    build_codex_cycle_prompt,
    build_autorun_status_report,
    find_recent_completed_experiment_runs,
    find_incomplete_experiment_runs,
    is_transient_codex_provider_failure,
    next_autorun_failure_state,
    prepare_codex_home,
    record_session_update,
    resolve_autorun_session_dir,
    resolve_codex_exec_binary,
    resolve_codex_exec_path_prefix,
    summarize_experiment_run,
)
from pm15min.research.automation import control_plane
from pm15min.research.automation.queue_state import build_queue_item, upsert_queue_item


def _write_autorun_runtime_snapshot(
    autorun_dir: Path,
    *,
    state: str,
    iteration: int,
    failure_count: int,
    log_lines: list[str],
    extra_fields: dict[str, object] | None = None,
) -> Path:
    autorun_dir.mkdir(parents=True, exist_ok=True)
    status_path = autorun_dir / "codex-background.status.json"
    payload: dict[str, object] = {
        "state": state,
        "iteration": iteration,
        "failure_count": failure_count,
        "pid": None,
    }
    if extra_fields:
        payload.update(extra_fields)
    status_path.write_text(json.dumps(payload), encoding="utf-8")
    (autorun_dir / "codex-background.log").write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    return status_path


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
                "profitable_pool_rows",
                "profitable_pool_capture_rows",
                "profitable_pool_coverage_ratio",
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
                "profitable_pool_rows": "10",
                "profitable_pool_capture_rows": "7",
                "profitable_pool_coverage_ratio": "0.7",
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
        "profitable_pool_rows": 10,
        "profitable_pool_capture_rows": 7,
        "profitable_pool_correct_side_rows": None,
        "profitable_pool_coverage_ratio": 0.7,
        "rank": 1,
    }


def test_collect_coin_slot_statuses_marks_major_rework_after_three_zero_capture_runs(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    run_payloads: list[dict[str, object]] = []
    for idx in range(3):
        suite_name = f"baseline_focus_feature_search_eth_direction_48v1r{idx+1}"
        run_label = f"auto_eth_direction_r{idx+1}"
        run_dir = root / "research" / "experiments" / "runs" / f"suite={suite_name}" / f"run={run_label}"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "quick_screen_summary.json").write_text(
            json.dumps(
                {
                    "suite_name": suite_name,
                    "run_label": run_label,
                    "top_k": 1,
                    "markets": ["eth"],
                    "rows": 1,
                    "selected_rows": 1,
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
                    "profitable_pool_rows",
                    "profitable_pool_capture_rows",
                    "profitable_pool_correct_side_rows",
                    "profitable_pool_coverage_ratio",
                    "rank",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "market": "eth",
                    "group_name": "focus_search",
                    "run_name": f"run_{idx+1}",
                    "feature_set": f"focus_eth_48_v1r{idx+1}",
                    "variant_label": "default",
                    "trade_rows": "3",
                    "profitable_pool_rows": "100",
                    "profitable_pool_capture_rows": "0",
                    "profitable_pool_correct_side_rows": "4",
                    "profitable_pool_coverage_ratio": "0.0",
                    "rank": "1",
                }
            )
        run_payloads.append(
            {
                "suite_name": suite_name,
                "run_label": run_label,
                "completed_at": f"2026-04-17T1{idx}:00:00Z",
                "completed_cases": 1,
                "cases": 1,
                "top_case": control_plane._read_quick_screen_top_case(run_dir / "quick_screen_leaderboard.csv"),
            }
        )

    statuses = control_plane._collect_coin_slot_statuses(
        project_root=root,
        markets=["eth"],
        incomplete_runs=[],
        completed_runs=list(reversed(run_payloads)),
        live_run_labels=set(),
    )

    eth = statuses["eth"]
    assert eth["recent_no_capture_streak"] == 3
    assert eth["major_rework_required"] is True

    summary_lines = control_plane._format_machine_decision_summary(
        markets=["eth"],
        slot_statuses=statuses,
        allowed_live_runs=8,
        queue_payload={"items": [], "max_queued_items": 24},
        live_worker_count=0,
    )
    assert any("action=major_rework_now" in line for line in summary_lines)


def test_dense_prompt_guidance_mentions_three_zero_capture_major_rework(tmp_path: Path) -> None:
    program = tmp_path / "program_direction_dense.md"
    program.write_text(
        "\n".join(
            [
                "# Codex Research Program",
                "- target fixed to `direction`",
                "- dense goal: 10-20 trades per coin per day",
                "- allowed width ladder: `30 / 34 / 38 / 40 / 44 / 48`",
                "- profitable offset pool is coin-level and shared by both dense tracks",
            ]
        ),
        encoding="utf-8",
    )

    lines = control_plane._dense_prompt_guidance(program)

    assert any("3 consecutive completed fast screens with zero profitable-pool captures" in line for line in lines)


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
        files_changed=["auto_research/program.md", "auto_research/run_one_experiment.sh"],
        timestamp="2026-04-04T16:00:00+08:00",
        cycle_eval_md="# Cycle 007\n\nsummary",
        cycle_notes=["started codex background automation mvp"],
        tried_lines=["added program.md and one-shot runner"],
        open_issue_lines=["status script still needs operator validation"],
    )

    results_lines = (session_dir / "results.tsv").read_text(encoding="utf-8").strip().splitlines()
    assert results_lines[0] == "cycle\tteam\tmetric\tstatus\tdescription\tfiles_changed\ttimestamp"
    assert results_lines[1].startswith("007\tgreen\troi_pct=12.5\tpartial\tran one cycle\tauto_research/program.md,auto_research/run_one_experiment.sh\t2026-04-04T16:00:00+08:00")
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
    assert "read the machine decision summary plus program_custom.md before making changes; open results.tsv plus the newest cycle eval only if you still need historical rationale after accepting the current occupancy in the summary." in prompt.lower()
    assert "your codex decision pass must end after this cycle" in prompt.lower()
    assert "healthy formal experiment workers you started or observed may continue running after you exit" in prompt.lower()
    assert "16 simultaneous formal market runs" in prompt
    assert "keep occupancy near 16" in prompt
    assert "do not scan the entire repository" in prompt.lower()
    assert "prefer formal experiment launches over unrelated environment or infrastructure edits" in prompt.lower()
    assert "if `rg` is unavailable" in prompt.lower()
    assert "trust the current run directories" in prompt.lower()
    assert "historical cycle eval notes about live workers or cpu health are not authoritative for the current cycle" in prompt.lower()
    assert "finished only when `completed_cases + failed_cases` reaches `cases`" in prompt.lower()
    assert "idle coin slots" in prompt.lower()
    assert "newest cycle eval" in prompt.lower()
    assert "fill every allowed idle slot" in prompt.lower()
    assert "do not leave an idle coin slot unfilled solely because the latest result is thin-sample" in prompt.lower()
    assert "still counts as one bounded cycle" in prompt.lower()
    assert "resume as many checkpointed current-line runs as needed to fill those live slots in the same cycle" in prompt.lower()
    assert "do not end the cycle with unused live capacity" in prompt.lower()
    assert "if the current autorun snapshot reports `live formal workers: 0`, you are expected to queue or resume work for every coin slot" in prompt.lower()
    assert "if a feature-set name mentioned by old session artifacts is missing from the current registry, treat that as historical drift rather than a blocker" in prompt.lower()
    assert "do not stop or checkpoint a healthy live formal run merely to end the current codex cycle" in prompt.lower()
    assert "run_one_experiment_background.sh" in prompt


def test_build_codex_cycle_prompt_first_cycle_starts_from_summary_not_results_tsv(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    (root / "auto_research").mkdir(parents=True, exist_ok=True)
    program_path = root / "auto_research" / "program.md"
    program_path.write_text("# demo program\n- coins: btc, eth\n", encoding="utf-8")
    (root / "research").mkdir(parents=True, exist_ok=True)
    (root / "research" / "AGENTS.md").write_text("# repo guidance\n", encoding="utf-8")

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    start_section = prompt.split("Start with only these files unless they prove insufficient:", 1)[1]
    start_section = start_section.split("Use repository commands sparingly.", 1)[0]
    assert str(session_dir / "results.tsv") not in start_section
    assert "use the machine decision summary and current autorun snapshot first" in prompt.lower()
    assert "queue or resume formal work for the idle coin slots first" in prompt.lower()


def test_build_codex_cycle_prompt_falls_back_to_research_agents_path(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    research_dir = root / "research"
    research_dir.mkdir(parents=True, exist_ok=True)
    agents_path = research_dir / "AGENTS.md"
    agents_path.write_text("# repo guidance\n", encoding="utf-8")
    program_path = root / "program_custom.md"
    program_path.write_text("# demo program\n", encoding="utf-8")

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert str(agents_path) in prompt
    assert str(root / "AGENTS.md") not in prompt


def test_build_codex_cycle_prompt_mentions_dense_trade_gates(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "dense_direction"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_direction_dense.md"
    program_path.write_text(
        "\n".join(
            [
                "# dense direction program",
                "- coins: btc",
                "- target fixed to `direction`",
                "- target `10-20` trades per coin per day",
                "- frozen-window target: `140-280` trades per coin",
                "- feature-set width is not fixed to `40`",
                "- allowed width ladder: `30 / 34 / 38 / 40 / 44 / 48`",
                "- move width by one bucket per bounded cycle only",
                "- profitable offset pool is coin-level and shared by both dense tracks",
                "- profitable offset pool window: `2026-04-01` through `2026-04-15`, `2usd`",
                "- one `offset` equals one exact window",
                "- only final tradeable winner-side entries at `<= 0.30` count as pool captures",
                "- prefer profitable-pool coverage before formal ROI comparisons",
            ]
        ),
        encoding="utf-8",
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert "10-20 trades per coin per day" in prompt
    assert "140-280 trades per coin" in prompt
    assert "check count before roi" in prompt.lower()
    assert "do not promote sparse winners" in prompt.lower()
    assert "width is not fixed to 40" in prompt.lower()
    assert "30 / 34 / 38 / 40 / 44 / 48" in prompt
    assert "one bucket per bounded cycle" in prompt.lower()
    assert "prefer the next wider bucket" in prompt.lower()
    assert "profitable-offset-pool" in prompt.lower()
    assert "shared by both dense tracks" in prompt.lower()
    assert "2026-04-01 through 2026-04-15" in prompt
    assert "<= 0.30" in prompt or "<= 0.3" in prompt
    assert "coverage before formal roi comparisons" in prompt.lower()


def test_build_codex_cycle_prompt_queue_snapshot_includes_track_for_queue_items(tmp_path: Path) -> None:
    from pm15min.research.automation.queue_state import build_queue_item, upsert_queue_item

    root = tmp_path / "repo"
    session_dir = root / "sessions" / "dense_direction"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_direction_dense.md"
    program_path.write_text("# dense direction program\n- coins: btc\n", encoding="utf-8")

    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_direction_suite",
            run_label="btc_direction_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert "track=direction_dense / market=btc / status=queued" in prompt


def test_build_autorun_status_report_filters_to_current_dense_track(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from pm15min.research.automation.queue_state import build_queue_item, upsert_queue_item

    root = tmp_path / "repo"
    autorun_dir = root / "var" / "research" / "autorun" / "direction_dense"
    status_path = _write_autorun_runtime_snapshot(
        autorun_dir,
        state="running",
        iteration=3,
        failure_count=0,
        log_lines=["direction-log-line"],
        extra_fields={"session_dir": str(root / "sessions" / "deep_otm_baseline_direction_dense_autoresearch")},
    )

    suite_specs_dir = root / "research" / "experiments" / "suite_specs"
    suite_specs_dir.mkdir(parents=True, exist_ok=True)
    (suite_specs_dir / "direction_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "direction_suite",
                "markets": {
                    "btc": {
                        "groups": {
                            "focus_search": {
                                "runs": [
                                    {
                                        "run_name": "focus_search",
                                        "target": "direction",
                                        "feature_set_variants": [{"label": "frontier", "feature_set": "focus_btc_direction"}],
                                        "weight_variants": [{"label": "current_default"}],
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
    (suite_specs_dir / "reversal_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "reversal_suite",
                "markets": {
                    "btc": {
                        "groups": {
                            "focus_search": {
                                "runs": [
                                    {
                                        "run_name": "focus_search",
                                        "target": "reversal",
                                        "feature_set_variants": [{"label": "frontier", "feature_set": "focus_btc_reversal"}],
                                        "weight_variants": [{"label": "current_default"}],
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

    for suite_name, run_label in (("direction_suite", "direction_run"), ("reversal_suite", "reversal_run")):
        run_dir = root / "research" / "experiments" / "runs" / f"suite={suite_name}" / f"run={run_label}"
        (run_dir / "logs").mkdir(parents=True, exist_ok=True)
        (run_dir / "logs" / "suite.jsonl").write_text(
            json.dumps({"event": "market_cache_resolved", "run_name": "focus_search"}) + "\n",
            encoding="utf-8",
        )

    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="direction_suite",
            run_label="direction_run",
            action="resume",
            status="running",
            track="direction_dense",
            session_dir=root / "sessions" / "deep_otm_baseline_direction_dense_autoresearch",
            program_path=root / "auto_research" / "program_direction_dense.md",
        ),
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="reversal_suite",
            run_label="reversal_run",
            action="resume",
            status="running",
            track="reversal_dense",
            session_dir=root / "sessions" / "deep_otm_baseline_reversal_dense_autoresearch",
            program_path=root / "auto_research" / "program_reversal_dense.md",
        ),
    )

    monkeypatch.setattr(
        control_plane,
        "find_live_formal_workers",
        lambda _root: [
            {"pid": 101, "ppid": 1, "suite_name": "direction_suite", "run_label": "direction_run", "market": "btc", "cmd": "direction"},
            {"pid": 202, "ppid": 1, "suite_name": "reversal_suite", "run_label": "reversal_run", "market": "btc", "cmd": "reversal"},
        ],
    )

    payload = build_autorun_status_report(root, status_path=status_path, log_tail_lines=1, max_incomplete_runs=10)

    assert [item["suite_name"] for item in payload["queue"]["items"]] == ["direction_suite"]
    assert [item["suite_name"] for item in payload["formal_workers"]] == ["direction_suite"]
    assert [item["suite_name"] for item in payload["incomplete_runs"]] == ["direction_suite"]


def test_build_codex_cycle_prompt_ignores_opposite_track_occupancy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_baseline_direction_dense_autoresearch"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_direction_dense.md"
    program_path.write_text(
        "\n".join(
            [
                "# dense direction program",
                "- coins: btc",
                "- target fixed to `direction`",
                "- target `10-20` trades per coin per day",
            ]
        ),
        encoding="utf-8",
    )

    autorun_dir = root / "var" / "research" / "autorun"
    autorun_dir.mkdir(parents=True, exist_ok=True)
    (autorun_dir / "experiment-queue.json").write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": "2026-04-15T12:00:00Z",
                "max_live_runs": 4,
                "track_slot_caps": {"direction_dense": 2, "reversal_dense": 2},
                "items": [],
            }
        ),
        encoding="utf-8",
    )

    suite_specs_dir = root / "research" / "experiments" / "suite_specs"
    suite_specs_dir.mkdir(parents=True, exist_ok=True)
    (suite_specs_dir / "reversal_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "reversal_suite",
                "markets": {
                    "btc": {
                        "groups": {
                            "focus_search": {
                                "runs": [
                                    {
                                        "run_name": "focus_search",
                                        "target": "reversal",
                                        "feature_set_variants": [{"label": "frontier", "feature_set": "focus_btc_reversal"}],
                                        "weight_variants": [{"label": "current_default"}],
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
    reversal_run = root / "research" / "experiments" / "runs" / "suite=reversal_suite" / "run=reversal_run"
    (reversal_run / "logs").mkdir(parents=True, exist_ok=True)
    (reversal_run / "logs" / "suite.jsonl").write_text(
        json.dumps({"event": "market_cache_resolved", "run_name": "focus_search"}) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        control_plane,
        "find_live_formal_workers",
        lambda _root: [
            {"pid": 202, "ppid": 1, "suite_name": "reversal_suite", "run_label": "reversal_run", "market": "btc", "cmd": "reversal"}
        ],
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert "occupancy=0/2" in prompt.lower()
    assert "btc: slot=idle / action=refill_now" in prompt.lower()
    assert "reversal_suite / reversal_run" not in prompt


def test_build_codex_cycle_prompt_uses_queue_max_live_runs_for_concurrency_guard(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    autorun_dir = root / "var" / "research" / "autorun"
    autorun_dir.mkdir(parents=True, exist_ok=True)
    (autorun_dir / "experiment-queue.json").write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": "2026-04-13T16:00:00Z",
                "max_live_runs": 4,
                "items": [],
            }
        ),
        encoding="utf-8",
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert "keeping up to 4 live formal runs active" in prompt.lower()
    assert "keep occupancy near 4" in prompt.lower()
    assert "4 simultaneous formal market runs" in prompt


def test_build_codex_cycle_prompt_reports_queue_capacity_from_queue_state(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    autorun_dir = root / "var" / "research" / "autorun"
    autorun_dir.mkdir(parents=True, exist_ok=True)
    (autorun_dir / "experiment-queue.json").write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": "2026-04-17T10:00:00Z",
                "max_live_runs": 16,
                "max_queued_items": 24,
                "track_slot_caps": {"direction_dense": 8, "reversal_dense": 8},
                "items": [],
            }
        ),
        encoding="utf-8",
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert "queued=0/24" in prompt.lower()


def test_find_live_formal_workers_deduplicates_same_run_label(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    duplicate_output = "\n".join(
        [
            f"101 1 /bin/bash {root}/auto_research/run_one_experiment.sh --suite demo_suite --run-label demo_run --market btc",
            f"202 101 /bin/bash {root}/auto_research/run_one_experiment.sh --suite demo_suite --run-label demo_run --market btc",
            f"303 1 /bin/bash {root}/auto_research/run_one_experiment.sh --suite other_suite --run-label other_run --market eth",
        ]
    )

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args=args[0], returncode=0, stdout=duplicate_output, stderr=""),
    )

    workers = control_plane.find_live_formal_workers(root)

    assert workers == [
        {
            "pid": 101,
            "ppid": 1,
            "run_label": "demo_run",
            "suite_name": "demo_suite",
            "market": "btc",
            "cmd": f"/bin/bash {root}/auto_research/run_one_experiment.sh --suite demo_suite --run-label demo_run --market btc",
        },
        {
            "pid": 303,
            "ppid": 1,
            "run_label": "other_run",
            "suite_name": "other_suite",
            "market": "eth",
            "cmd": f"/bin/bash {root}/auto_research/run_one_experiment.sh --suite other_suite --run-label other_run --market eth",
        },
    ]


def test_find_live_formal_workers_includes_direct_run_suite_processes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    direct_output = "\n".join(
        [
            f"101 1 /home/demo/.venv_server/bin/python -m pm15min research experiment run-suite --suite sol_suite --run-label sol_run --market sol --project-root {root}",
            f"202 1 /home/demo/.venv_server/bin/python -m pm15min research experiment run-suite --suite btc_suite --run-label btc_run --market btc --root /tmp/other",
        ]
    )

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args=args[0], returncode=0, stdout=direct_output, stderr=""),
    )

    workers = control_plane.find_live_formal_workers(root)

    assert workers == [
        {
            "pid": 101,
            "ppid": 1,
            "run_label": "sol_run",
            "suite_name": "sol_suite",
            "market": "sol",
            "cmd": f"/home/demo/.venv_server/bin/python -m pm15min research experiment run-suite --suite sol_suite --run-label sol_run --market sol --project-root {root}",
        }
    ]


def test_find_live_formal_workers_includes_quick_screen_processes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    quick_screen_output = "\n".join(
        [
            f"101 1 /home/demo/.venv_server/bin/python {root}/scripts/research/run_quick_screen_suite.py --suite eth_suite --run-label eth_run --top-k 1",
            f"202 1 /home/demo/.venv_server/bin/python /tmp/other/scripts/research/run_quick_screen_suite.py --suite other_suite --run-label other_run --top-k 1",
        ]
    )

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args=args[0], returncode=0, stdout=quick_screen_output, stderr=""),
    )

    workers = control_plane.find_live_formal_workers(root)

    assert workers == [
        {
            "pid": 101,
            "ppid": 1,
            "run_label": "eth_run",
            "suite_name": "eth_suite",
            "market": None,
            "cmd": f"/home/demo/.venv_server/bin/python {root}/scripts/research/run_quick_screen_suite.py --suite eth_suite --run-label eth_run --top-k 1",
        }
    ]


def test_find_live_autorun_processes_matches_loop_and_codex_exec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / "var" / "research" / "autorun" / "codex-last-output.txt"
    script_path = root / "auto_research" / "codex_background_loop.sh"
    ps_output = "\n".join(
        [
            f"101 1 /bin/bash {script_path} __run_loop",
            f"202 101 /home/demo/.local/bin/codex exec --cd {root} --output-last-message {output_path} --sandbox danger-full-access -",
            f"303 1 /bin/bash {root}/scripts/research/other_loop.sh __run_loop",
            f"404 1 /home/demo/.local/bin/codex exec --cd /tmp/other --output-last-message {output_path} --sandbox danger-full-access -",
        ]
    )

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args=args[0], returncode=0, stdout=ps_output, stderr=""),
    )

    processes = control_plane.find_live_autorun_processes(root)

    assert processes == [
        {
            "pid": 101,
            "ppid": 1,
            "kind": "background_loop",
            "cmd": f"/bin/bash {script_path} __run_loop",
        },
        {
            "pid": 202,
            "ppid": 101,
            "kind": "codex_exec",
            "cmd": f"/home/demo/.local/bin/codex exec --cd {root} --output-last-message {output_path} --sandbox danger-full-access -",
        },
    ]


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


def test_build_codex_cycle_prompt_includes_recent_completed_runs(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    complete_run = root / "research" / "experiments" / "runs" / "suite=demo" / "run=complete"
    (complete_run / "logs").mkdir(parents=True, exist_ok=True)
    (complete_run / "summary.json").write_text(
        '{"suite_name":"demo","run_label":"complete","completed_cases":9,"failed_cases":0}',
        encoding="utf-8",
    )
    (complete_run / "logs" / "suite.jsonl").write_text(
        json.dumps({"event": "market_completed", "case_label": "done"}) + "\n",
        encoding="utf-8",
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert "recent completed runs:" in prompt.lower()
    assert "demo / complete / completed=9 / failed=0" in prompt


def test_build_codex_cycle_prompt_prefers_latest_cycle_eval_before_full_session(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    cycles_dir = session_dir / "cycles" / "016"
    cycles_dir.mkdir(parents=True, exist_ok=True)
    (cycles_dir / "eval-results.md").write_text("# Cycle 016\n", encoding="utf-8")

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert str(cycles_dir / "eval-results.md") in prompt
    start_section = prompt.split("Start with only these files unless they prove insufficient:", 1)[1]
    start_section = start_section.split("Use repository commands sparingly.", 1)[0]
    assert str(session_dir / "session.md") not in start_section


def test_build_codex_cycle_prompt_includes_coin_slot_snapshot_and_feature_brief(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    (root / "auto_research").mkdir(parents=True, exist_ok=True)
    (root / "auto_research" / "program.md").write_text(
        "\n".join(
            [
                "# Demo Program",
                "",
                "- coins: `btc / eth`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    cycles_dir = session_dir / "cycles" / "016"
    cycles_dir.mkdir(parents=True, exist_ok=True)
    (cycles_dir / "eval-results.md").write_text(
        "# Cycle 016\n\n- frontier: `focus_btc_40_v4`\n",
        encoding="utf-8",
    )

    experiments_root = root / "research" / "experiments"
    experiments_root.mkdir(parents=True, exist_ok=True)
    (experiments_root / "custom_feature_sets.json").write_text(
        json.dumps(
            {
                "focus_btc_40_v4": {
                    "market": "btc",
                    "width": 40,
                    "columns": ["ret_1m", "ret_3m", "ret_5m"],
                    "notes": "btc frontier",
                },
                "focus_eth_40_v4": {
                    "market": "eth",
                    "width": 40,
                    "columns": ["ret_1m", "ret_3m", "obv_z"],
                    "notes": "eth frontier",
                },
                "focus_eth_40_v5": {
                    "market": "eth",
                    "width": 40,
                    "columns": ["ret_1m", "ret_3m", "atr_14"],
                    "notes": "eth challenger",
                },
            }
        ),
        encoding="utf-8",
    )

    suite_specs_dir = experiments_root / "suite_specs"
    suite_specs_dir.mkdir(parents=True, exist_ok=True)
    (suite_specs_dir / "btc_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "btc_suite",
                "parallel_case_workers": 1,
                "markets": {
                    "btc": {
                        "groups": {
                            "focus_search": {
                                "runs": [
                                    {
                                        "run_name": "focus_search",
                                        "target": "reversal",
                                        "feature_set_variants": [
                                            {"label": "frontier", "feature_set": "focus_btc_40_v4"}
                                        ],
                                        "weight_variants": [
                                            {"label": "current_default"},
                                            {"label": "offset_reversal_mild"},
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
    (suite_specs_dir / "eth_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "eth_suite",
                "parallel_case_workers": 1,
                "markets": {
                    "eth": {
                        "groups": {
                            "focus_search": {
                                "runs": [
                                    {
                                        "run_name": "focus_search",
                                        "target": "reversal",
                                        "feature_set_variants": [
                                            {"label": "frontier", "feature_set": "focus_eth_40_v4"},
                                            {"label": "challenger", "feature_set": "focus_eth_40_v5"},
                                        ],
                                        "weight_variants": [
                                            {"label": "current_default"},
                                            {"label": "offset_reversal_mild"},
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

    btc_run = experiments_root / "runs" / "suite=btc_suite" / "run=btc_complete"
    (btc_run / "logs").mkdir(parents=True, exist_ok=True)
    (btc_run / "summary.json").write_text(
        json.dumps(
            {
                "suite_name": "btc_suite",
                "run_label": "btc_complete",
                "cases": 2,
                "completed_cases": 2,
                "failed_cases": 0,
                "leaderboard_rows": 1,
                "top_roi_pct": 12.5,
                "markets": ["btc"],
            }
        ),
        encoding="utf-8",
    )
    (btc_run / "leaderboard.csv").write_text(
        "\n".join(
            [
                "market,group_name,run_name,target,variant_label,roi_pct,pnl_sum,trades",
                "btc,focus_search,focus_search,reversal,offset_reversal_mild,12.5,4.0,3",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (btc_run / "logs" / "suite.jsonl").write_text(
        json.dumps({"event": "market_completed", "case_label": "btc/focus_search"}) + "\n",
        encoding="utf-8",
    )

    eth_run = experiments_root / "runs" / "suite=eth_suite" / "run=eth_active"
    (eth_run / "logs").mkdir(parents=True, exist_ok=True)
    (eth_run / "logs" / "suite.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"event": "execution_group_started", "group_label": "eth/focus_search"}),
                json.dumps({"event": "market_cache_resolved", "run_name": "focus_search"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(control_plane, "find_live_formal_workers", lambda _root: [])

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert "machine decision summary already collected for you:" in prompt.lower()
    assert "btc: slot=idle / action=refill_now" in prompt.lower()
    assert "eth: slot=checkpointed / action=resume_or_replace_now" in prompt.lower()
    assert "use the machine decision summary and current autorun snapshot first; open `results.tsv` or historical cycle eval only if you still need extra rationale after accepting the current occupancy in the summary." in prompt.lower()
    assert "coin slot snapshot already collected for you:" in prompt.lower()
    assert "btc: state=idle" in prompt.lower()
    assert "latest_completed=btc_suite" in prompt
    assert "feature_sets=focus_btc_40_v4" in prompt
    assert "weights=current_default,offset_reversal_mild" in prompt
    assert "eth: state=checkpointed" in prompt.lower()
    assert "feature_sets=focus_eth_40_v4,focus_eth_40_v5" in prompt
    assert "relevant feature-family brief already extracted for you:" in prompt.lower()
    assert "focus_btc_40_v4: market=btc / width=40 / notes=btc frontier" in prompt
    assert "columns: ret_1m, ret_3m, ret_5m" not in prompt
    assert "diagnosis_groups:" in prompt
    assert "protect_core=q_bs_up_strike,ret_from_strike,basis_bp,ret_from_cycle_open,first_half_ret,cycle_range_pos,rv_30,macd_z,volume_z,obv_z,vwap_gap_60,bias_60,regime_high_vol" in prompt
    assert "drop_from_first=short_mid_returns,price_position,momentum_oscillator" in prompt
    assert "add_toward=timing,persistence,strike_distance,flip_feasibility,market_quality,junk_cheap_filter" in prompt
    assert "do not open large raw registry files like `research/experiments/custom_feature_sets.json`" in prompt.lower()


def test_build_codex_cycle_prompt_backfills_latest_completed_run_per_program_coin(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    (root / "auto_research").mkdir(parents=True, exist_ok=True)
    (root / "auto_research" / "program.md").write_text(
        "# Demo Program\n\n- coins: `btc / eth`\n",
        encoding="utf-8",
    )

    experiments_root = root / "research" / "experiments"
    suite_specs_dir = experiments_root / "suite_specs"
    suite_specs_dir.mkdir(parents=True, exist_ok=True)
    (experiments_root / "custom_feature_sets.json").write_text(
        json.dumps(
            {
                "focus_btc_latest": {
                    "market": "btc",
                    "width": 48,
                    "columns": ["ret_15m", "volume_z"],
                    "notes": "btc latest",
                },
                "focus_eth_latest": {
                    "market": "eth",
                    "width": 48,
                    "columns": ["ret_30m", "obv_z"],
                    "notes": "eth latest",
                },
            }
        ),
        encoding="utf-8",
    )
    (suite_specs_dir / "btc_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "btc_suite",
                "markets": {
                    "btc": {
                        "groups": {
                            "focus_search": {
                                "runs": [
                                    {
                                        "run_name": "focus_search",
                                        "feature_set_variants": [
                                            {"label": "frontier", "feature_set": "focus_btc_latest"}
                                        ],
                                        "weight_variants": [{"label": "nvol"}],
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
    (suite_specs_dir / "eth_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "eth_suite",
                "markets": {
                    "eth": {
                        "groups": {
                            "focus_search": {
                                "runs": [
                                    {
                                        "run_name": "focus_search",
                                        "feature_set_variants": [
                                            {"label": "frontier", "feature_set": "focus_eth_latest"}
                                        ],
                                        "weight_variants": [{"label": "nvol"}],
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

    def write_completed_run(suite_name: str, run_label: str, market: str, mtime: int) -> None:
        run_dir = experiments_root / "runs" / f"suite={suite_name}" / f"run={run_label}"
        logs_dir = run_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "suite_name": suite_name,
                    "run_label": run_label,
                    "cases": 2,
                    "completed_cases": 2,
                    "failed_cases": 0,
                    "markets": [market],
                }
            ),
            encoding="utf-8",
        )
        (logs_dir / "suite.jsonl").write_text(
            json.dumps({"event": "market_completed", "case_label": f"{market}/focus_search"}) + "\n",
            encoding="utf-8",
        )
        os.utime(summary_path, (mtime, mtime))

    write_completed_run("eth_suite", "eth_complete", "eth", 100)
    for index in range(5):
        write_completed_run("btc_suite", f"btc_complete_{index}", "btc", 200 + index)

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert "btc: state=idle / latest_completed=btc_suite" in prompt.lower()
    assert "eth: state=idle / latest_completed=eth_suite" in prompt.lower()
    assert "feature_sets=focus_eth_latest" in prompt


def test_build_codex_cycle_prompt_prefers_same_target_completed_runs_when_suite_specs_omit_target(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    (auto_research_dir / "program_direction_dense.md").write_text(
        "# Demo Program\n\n- coins: `btc`\n- target fixed to `direction`\n",
        encoding="utf-8",
    )

    experiments_root = root / "research" / "experiments"
    suite_specs_dir = experiments_root / "suite_specs"
    suite_specs_dir.mkdir(parents=True, exist_ok=True)
    (experiments_root / "custom_feature_sets.json").write_text(
        json.dumps(
            {
                "focus_btc_direction": {
                    "market": "btc",
                    "width": 48,
                    "columns": ["ret_15m", "volume_z"],
                    "notes": "btc direction",
                },
                "focus_btc_reversal": {
                    "market": "btc",
                    "width": 48,
                    "columns": ["ret_30m", "obv_z"],
                    "notes": "btc reversal",
                },
            }
        ),
        encoding="utf-8",
    )
    (suite_specs_dir / "btc_direction_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "btc_direction_suite",
                "markets": {
                    "btc": {
                        "groups": {
                            "focus_search": {
                                "runs": [
                                    {
                                        "run_name": "focus_search",
                                        "feature_set_variants": [
                                            {"label": "frontier", "feature_set": "focus_btc_direction"}
                                        ],
                                        "weight_variants": [{"label": "nvol"}],
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
    (suite_specs_dir / "btc_reversal_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "btc_reversal_suite",
                "markets": {
                    "btc": {
                        "groups": {
                            "focus_search": {
                                "runs": [
                                    {
                                        "run_name": "focus_search",
                                        "feature_set_variants": [
                                            {"label": "frontier", "feature_set": "focus_btc_reversal"}
                                        ],
                                        "weight_variants": [{"label": "nvol"}],
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

    def write_completed_run(suite_name: str, run_label: str, mtime: int) -> None:
        run_dir = experiments_root / "runs" / f"suite={suite_name}" / f"run={run_label}"
        logs_dir = run_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "suite_name": suite_name,
                    "run_label": run_label,
                    "cases": 2,
                    "completed_cases": 2,
                    "failed_cases": 0,
                    "markets": ["btc"],
                }
            ),
            encoding="utf-8",
        )
        (logs_dir / "suite.jsonl").write_text(
            json.dumps({"event": "market_completed", "case_label": "btc/focus_search"}) + "\n",
            encoding="utf-8",
        )
        os.utime(summary_path, (mtime, mtime))

    write_completed_run("btc_direction_suite", "btc_direction_complete", 100)
    write_completed_run("btc_reversal_suite", "btc_reversal_complete", 200)

    prompt = build_codex_cycle_prompt(
        project_root=root,
        session_dir=session_dir,
        program_path=auto_research_dir / "program_direction_dense.md",
    )

    assert "latest_completed=btc_direction_suite" in prompt
    assert "latest_completed=btc_reversal_suite" not in prompt


def test_build_codex_cycle_prompt_marks_live_worker_slots_active(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    (root / "auto_research").mkdir(parents=True, exist_ok=True)
    (root / "auto_research" / "program.md").write_text("# Demo Program\n\n- coins: `btc`\n", encoding="utf-8")
    experiments_root = root / "research" / "experiments"
    experiments_root.mkdir(parents=True, exist_ok=True)
    suite_specs_dir = experiments_root / "suite_specs"
    suite_specs_dir.mkdir(parents=True, exist_ok=True)
    (suite_specs_dir / "btc_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "btc_suite",
                "markets": {
                    "btc": {
                        "groups": {
                            "focus_search": {
                                "runs": [
                                    {
                                        "run_name": "focus_search",
                                        "target": "reversal",
                                        "feature_set_variants": [{"label": "frontier", "feature_set": "focus_btc_40_v4"}],
                                        "weight_variants": [{"label": "current_default"}],
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
    (experiments_root / "custom_feature_sets.json").write_text(
        json.dumps(
            {
                "focus_btc_40_v4": {
                    "market": "btc",
                    "width": 40,
                    "columns": ["q_bs_up_strike", "ret_from_strike", "basis_bp"],
                    "notes": "btc frontier",
                }
            }
        ),
        encoding="utf-8",
    )
    run_dir = experiments_root / "runs" / "suite=btc_suite" / "run=btc_live"
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "logs" / "suite.jsonl").write_text(
        json.dumps({"event": "execution_group_warmup_started", "run_name": "focus_search"}) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        control_plane,
        "find_live_formal_workers",
        lambda _root: [{"run_label": "btc_live", "suite_name": "btc_suite", "pid": 123}],
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert "btc: state=active" in prompt.lower()
    assert "live_worker=yes" in prompt.lower()


def test_build_codex_cycle_prompt_marks_active_slots_without_successor_for_next_queueing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    (root / "auto_research").mkdir(parents=True, exist_ok=True)
    (root / "auto_research" / "program.md").write_text("# Demo Program\n\n- coins: `btc`\n", encoding="utf-8")
    run_dir = root / "research" / "experiments" / "runs" / "suite=btc_suite" / "run=btc_live"
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "logs" / "suite.jsonl").write_text(
        json.dumps({"event": "execution_group_warmup_started", "run_name": "focus_search"}) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        control_plane,
        "find_live_formal_workers",
        lambda _root: [{"run_label": "btc_live", "suite_name": "btc_suite", "pid": 123}],
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert "btc: slot=active / action=prepare_next_now" in prompt.lower()
    assert "queued_branches=0" in prompt.lower()
    assert "you may queue multiple bounded queued branches for that same coin and track" in prompt.lower()


def test_build_codex_cycle_prompt_keeps_active_slots_running_when_successor_already_queued(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    (root / "auto_research").mkdir(parents=True, exist_ok=True)
    (root / "auto_research" / "program.md").write_text("# Demo Program\n\n- coins: `btc`\n", encoding="utf-8")
    run_dir = root / "research" / "experiments" / "runs" / "suite=btc_suite" / "run=btc_live"
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "logs" / "suite.jsonl").write_text(
        json.dumps({"event": "execution_group_warmup_started", "run_name": "focus_search"}) + "\n",
        encoding="utf-8",
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_followup_suite",
            run_label="btc_followup",
            action="launch",
            reason="queued successor",
        ),
    )

    monkeypatch.setattr(
        control_plane,
        "find_live_formal_workers",
        lambda _root: [{"run_label": "btc_live", "suite_name": "btc_suite", "pid": 123}],
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert "btc: slot=active / action=keep_running" in prompt.lower()
    assert "queued_branches=1" in prompt.lower()


def test_build_codex_cycle_prompt_counts_real_live_workers_for_same_market_track(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_baseline_direction_dense_autoresearch"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_direction_dense.md"
    program_path.write_text(
        "\n".join(
            [
                "# dense direction program",
                "- coins: btc",
                "- target fixed to `direction`",
            ]
        ),
        encoding="utf-8",
    )

    autorun_dir = root / "var" / "research" / "autorun"
    autorun_dir.mkdir(parents=True, exist_ok=True)
    (autorun_dir / "experiment-queue.json").write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": "2026-04-15T12:00:00Z",
                "max_live_runs": 16,
                "track_slot_caps": {"direction_dense": 16, "reversal_dense": 16},
                "items": [],
            }
        ),
        encoding="utf-8",
    )
    for run_label in ("btc_live_a", "btc_live_b"):
        run_dir = root / "research" / "experiments" / "runs" / f"suite=btc_direction_{run_label}" / f"run={run_label}"
        (run_dir / "logs").mkdir(parents=True, exist_ok=True)
        (run_dir / "logs" / "suite.jsonl").write_text(
            json.dumps({"event": "execution_group_warmup_started", "run_name": "focus_search"}) + "\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(
        control_plane,
        "find_live_formal_workers",
        lambda _root: [
            {
                "pid": 101,
                "ppid": 1,
                "suite_name": "btc_direction_btc_live_a",
                "run_label": "btc_live_a",
                "market": "btc",
                "track": "direction_dense",
                "cmd": "direction",
            },
            {
                "pid": 102,
                "ppid": 1,
                "suite_name": "btc_direction_btc_live_b",
                "run_label": "btc_live_b",
                "market": "btc",
                "track": "direction_dense",
                "cmd": "direction",
            },
        ],
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert "occupancy=2/16" in prompt.lower()


def test_build_autorun_status_report_tolerates_non_utf8_log_bytes(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    autorun_dir = root / "var" / "research" / "autorun"
    autorun_dir.mkdir(parents=True, exist_ok=True)
    (autorun_dir / "codex-background.status.json").write_text(
        json.dumps({"state": "idle", "iteration": 1, "pid": None}),
        encoding="utf-8",
    )
    (autorun_dir / "codex-background.log").write_bytes(b"good line\nbad byte:\x8d\nlast line\n")

    payload = build_autorun_status_report(root, log_tail_lines=5, max_incomplete_runs=1)

    assert payload["status"]["state"] == "idle"
    assert payload["log_tail"][-1] == "last line"


def test_build_autorun_status_report_prefers_explicit_status_path_over_default_autorun_dir(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    default_dir = root / "var" / "research" / "autorun"
    instance_dir = default_dir / "direction_dense"
    _write_autorun_runtime_snapshot(
        default_dir,
        state="default-idle",
        iteration=1,
        failure_count=9,
        log_lines=["default-log-1"],
    )
    instance_status = _write_autorun_runtime_snapshot(
        instance_dir,
        state="instance-idle",
        iteration=7,
        failure_count=2,
        log_lines=["instance-log-1"],
    )

    payload = build_autorun_status_report(root, status_path=instance_status, log_tail_lines=1, max_incomplete_runs=1)

    assert payload["status_path"] == str(instance_status)
    assert payload["status"]["state"] == "instance-idle"
    assert payload["status"]["iteration"] == 7
    assert payload["status"]["failure_count"] == 2


def test_build_autorun_status_report_uses_sibling_log_for_explicit_status_path(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    default_dir = root / "var" / "research" / "autorun"
    instance_dir = default_dir / "direction_dense"
    _write_autorun_runtime_snapshot(
        default_dir,
        state="default-idle",
        iteration=1,
        failure_count=0,
        log_lines=["default-log-1", "default-log-2"],
    )
    instance_status = _write_autorun_runtime_snapshot(
        instance_dir,
        state="instance-idle",
        iteration=3,
        failure_count=1,
        log_lines=["instance-log-1", "instance-log-2", "instance-log-3"],
    )

    payload = build_autorun_status_report(root, status_path=instance_status, log_tail_lines=2, max_incomplete_runs=1)

    assert payload["log_path"] == str(instance_dir / "codex-background.log")
    assert payload["log_tail"] == ["instance-log-2", "instance-log-3"]
    assert "default-log-2" not in payload["log_tail"]


def test_status_autorun_runtime_respects_autorun_dir_override(tmp_path: Path) -> None:
    workspace_root = Path(__file__).resolve().parents[1]
    temp_root = tmp_path / "repo"
    script_path = temp_root / "auto_research" / "status_autorun.sh"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text(
        (workspace_root / "auto_research" / "status_autorun.sh").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (temp_root / "src").symlink_to(workspace_root / "src", target_is_directory=True)

    default_dir = temp_root / "var" / "research" / "autorun"
    instance_dir = default_dir / "direction_dense"
    _write_autorun_runtime_snapshot(
        default_dir,
        state="default-idle",
        iteration=1,
        failure_count=9,
        log_lines=["default-log-line"],
        extra_fields={"session_dir": "sessions/default"},
    )
    _write_autorun_runtime_snapshot(
        instance_dir,
        state="instance-idle",
        iteration=8,
        failure_count=3,
        log_lines=["instance-log-line"],
        extra_fields={"session_dir": "sessions/direction_dense"},
    )

    result = subprocess.run(
        ["/bin/bash", str(script_path)],
        cwd=temp_root,
        env={**os.environ, "AUTORUN_DIR": str(instance_dir)},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "state: instance-idle" in result.stdout
    assert "iteration: 8" in result.stdout
    assert "failure_count: 3" in result.stdout
    assert "session_dir: sessions/direction_dense" in result.stdout
    assert "instance-log-line" in result.stdout
    assert "default-idle" not in result.stdout
    assert "default-log-line" not in result.stdout


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
        program_path=root / "auto_research" / "program.md",
    )

    assert resolved == explicit.resolve()


def test_resolve_autorun_session_dir_reads_active_session_from_program(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    (root / "auto_research").mkdir(parents=True, exist_ok=True)
    program_path = root / "auto_research" / "program.md"
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


def test_resolve_codex_exec_path_prefix_prefers_repo_venv_server(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    tools_bin = root / "tools" / "bin"
    tools_bin.mkdir(parents=True, exist_ok=True)
    bin_dir = root / ".venv_server" / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    (bin_dir / "python").write_text("", encoding="utf-8")

    resolved = resolve_codex_exec_path_prefix(root)

    assert resolved == f"{tools_bin.resolve()}:{bin_dir.resolve()}"


def test_repository_provides_rg_fallback_script() -> None:
    script_path = Path("tools/bin/rg")

    assert script_path.exists()
    script_text = script_path.read_text(encoding="utf-8")
    assert "grep" in script_text or "os.walk" in script_text
    assert "--files" in script_text


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
    assert "requires_openai_auth = false" in config_text
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


def test_is_transient_codex_provider_failure_ignores_plain_base_url_mentions() -> None:
    output = """
    Switching to fallback provider https://ai.changyou.club/v1 for the next attempt.
    The previous attempt failed because /bin/bash: python: command not found
    """
    assert (
        is_transient_codex_provider_failure(
            output,
            base_url="https://ai.changyou.club/v1",
        )
        is False
    )


def test_codex_background_loop_includes_secondary_nimabo_fallback_layer() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert "CODEX_SECONDARY_BASE_URL" in script_text
    assert "CODEX_SECONDARY_API_KEY" in script_text
    assert "CODEX_SECONDARY_HOME_DIR" in script_text
    assert "retrying with secondary fallback provider" in script_text
    assert script_text.index("retrying with secondary fallback provider") < script_text.index(
        "retrying with fallback provider"
    )
    assert script_text.index("retrying with fallback provider") < script_text.index(
        "retrying with official auth fallback"
    )


def test_codex_background_loop_terminates_full_attempt_process_group() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert "setsid" in script_text
    assert "kill -- -\"$attempt_pid\"" in script_text or "kill -TERM -- -\"$attempt_pid\"" in script_text
    assert "kill -9 -- -\"$attempt_pid\"" in script_text or "kill -KILL -- -\"$attempt_pid\"" in script_text


def test_codex_background_loop_does_not_launch_attempt_pid_via_command_substitution() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert 'attempt_pid="$(start_codex_attempt_process' not in script_text
    assert 'echo "$!"' not in script_text
    assert "STARTED_ATTEMPT_PID" in script_text


def test_codex_background_loop_treats_any_initial_output_as_startup_progress() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert 'if [[ "$current_size" -gt 0 ]]; then' in script_text
    assert "startup_progress=1" in script_text
    assert '"$current_size" -gt "$startup_baseline_size"' not in script_text


def test_research_readme_documents_secondary_nimabo_fallback_order() -> None:
    readme_text = Path("auto_research/README.md").read_text(encoding="utf-8")

    assert "CODEX_SECONDARY_BASE_URL" in readme_text
    assert "CODEX_SECONDARY_API_KEY" in readme_text
    assert "CODEX_OFFICIAL_NETWORK_PROXY_MODE" in readme_text
    assert "primary Nimabo" in readme_text
    assert "secondary Nimabo" in readme_text
    assert "ai.changyou.club" in readme_text
    assert "official" in readme_text
    assert "shared `var/research/autorun/codex-official-auth.json`" in readme_text


def test_research_readme_documents_dense_dual_track_startup() -> None:
    readme_text = Path("auto_research/README.md").read_text(encoding="utf-8")

    assert "start_direction_dense.sh" in readme_text
    assert "start_reversal_dense.sh" in readme_text
    assert "direction_dense" in readme_text
    assert "reversal_dense" in readme_text


def test_dense_program_files_exist_and_define_track_targets() -> None:
    direction_text = Path("auto_research/program_direction_dense.md").read_text(encoding="utf-8")
    reversal_text = Path("auto_research/program_reversal_dense.md").read_text(encoding="utf-8")

    assert "target fixed to `direction`" in direction_text
    assert "target fixed to `reversal`" in reversal_text
    assert "10-20 trades per coin per day" in direction_text
    assert "10-20 trades per coin per day" in reversal_text
    assert "140-280" in direction_text
    assert "140-280" in reversal_text
    assert "not fixed to `40`" in direction_text
    assert "not fixed to `40`" in reversal_text
    assert "30 / 34 / 38 / 40 / 44 / 48" in direction_text
    assert "30 / 34 / 38 / 40 / 44 / 48" in reversal_text
    assert "one bucket per bounded cycle" in direction_text.lower()
    assert "one bucket per bounded cycle" in reversal_text.lower()
    assert "Profitable Offset Pool Gate" in direction_text
    assert "Profitable Offset Pool Gate" in reversal_text
    assert "shared by both dense tracks" in direction_text
    assert "shared by both dense tracks" in reversal_text
    assert "2026-04-01" in direction_text and "2026-04-15" in direction_text
    assert "2026-04-01" in reversal_text and "2026-04-15" in reversal_text
    assert "<= 0.30" in direction_text
    assert "<= 0.30" in reversal_text
    assert "70%" in direction_text
    assert "70%" in reversal_text


def test_dense_start_wrappers_bind_distinct_program_and_autorun_dirs() -> None:
    direction_text = Path("auto_research/start_direction_dense.sh").read_text(encoding="utf-8")
    reversal_text = Path("auto_research/start_reversal_dense.sh").read_text(encoding="utf-8")

    assert "program_direction_dense.md" in direction_text
    assert "program_reversal_dense.md" in reversal_text
    assert "var/research/autorun/direction_dense" in direction_text
    assert "var/research/autorun/reversal_dense" in reversal_text
    assert 'CODEX_OFFICIAL_AUTH_PATH="$ROOT_DIR/var/research/autorun/codex-official-auth.json"' in direction_text
    assert 'CODEX_OFFICIAL_AUTH_PATH="$ROOT_DIR/var/research/autorun/codex-official-auth.json"' in reversal_text
    assert 'CODEX_NETWORK_PROXY_MODE="${CODEX_NETWORK_PROXY_MODE:-direct}"' in direction_text
    assert 'CODEX_NETWORK_PROXY_MODE="${CODEX_NETWORK_PROXY_MODE:-direct}"' in reversal_text
    assert 'CODEX_OFFICIAL_NETWORK_PROXY_MODE="${CODEX_OFFICIAL_NETWORK_PROXY_MODE:-inherit}"' in direction_text
    assert 'CODEX_OFFICIAL_NETWORK_PROXY_MODE="${CODEX_OFFICIAL_NETWORK_PROXY_MODE:-inherit}"' in reversal_text
    assert 'LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-60}"' in direction_text
    assert 'LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-60}"' in reversal_text
    assert 'CODEX_ATTEMPT_TIMEOUT_SEC="${CODEX_ATTEMPT_TIMEOUT_SEC:-600}"' in direction_text
    assert 'CODEX_ATTEMPT_TIMEOUT_SEC="${CODEX_ATTEMPT_TIMEOUT_SEC:-600}"' in reversal_text
    assert 'MAX_CONSECUTIVE_FAILURES="${MAX_CONSECUTIVE_FAILURES:-12}"' in direction_text
    assert 'MAX_CONSECUTIVE_FAILURES="${MAX_CONSECUTIVE_FAILURES:-12}"' in reversal_text


def test_run_one_experiment_supports_quick_screen_launch_mode() -> None:
    script_text = Path("auto_research/run_one_experiment.sh").read_text(encoding="utf-8")

    assert 'PM15MIN_EXPERIMENT_LAUNCH_MODE="${PM15MIN_EXPERIMENT_LAUNCH_MODE:-formal}"' in script_text
    assert 'PM15MIN_QUICK_SCREEN_TOP_K="${PM15MIN_QUICK_SCREEN_TOP_K:-1}"' in script_text
    assert 'PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS="${PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS:-3}"' in script_text
    assert 'PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY="${PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY:-16}"' in script_text
    assert 'PM15MIN_EXPERIMENT_CPU_THREADS="${PM15MIN_EXPERIMENT_CPU_THREADS:-}"' in script_text
    assert 'OMP_NUM_THREADS="${OMP_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"' in script_text
    assert 'OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"' in script_text
    assert 'MKL_NUM_THREADS="${MKL_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"' in script_text
    assert 'NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"' in script_text
    assert "run_quick_screen_suite.py" in script_text
    assert 'case "$LAUNCH_MODE" in' in script_text
    assert 'quick_screen)' in script_text


def test_experiment_queue_supervisor_defaults_to_quick_screen_launch_mode() -> None:
    script_text = Path("auto_research/experiment_queue_supervisor.sh").read_text(encoding="utf-8")

    assert 'MAX_LIVE_RUNS="${MAX_LIVE_RUNS:-16}"' in script_text
    assert 'MAX_QUEUED_ITEMS="${MAX_QUEUED_ITEMS:-24}"' in script_text
    assert 'PM15MIN_EXPERIMENT_LAUNCH_MODE="${PM15MIN_EXPERIMENT_LAUNCH_MODE:-quick_screen}"' in script_text
    assert 'PM15MIN_QUICK_SCREEN_TOP_K="${PM15MIN_QUICK_SCREEN_TOP_K:-1}"' in script_text
    assert 'PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS="${PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS:-3}"' in script_text
    assert 'PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY="${PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY:-$MAX_LIVE_RUNS}"' in script_text
    assert '--max-queued-items "$MAX_QUEUED_ITEMS"' in script_text
    assert 'export PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY' in script_text


def test_quick_screen_suite_script_preserves_float_rank_precision() -> None:
    script_text = Path("scripts/research/run_quick_screen_suite.py").read_text(encoding="utf-8")

    assert "tuple(int(v) for v in item)" not in script_text
    assert "_sortable_rank_tuple" in script_text


def test_status_dense_autorun_reads_both_dense_instances() -> None:
    script_text = Path("auto_research/status_dense_autorun.sh").read_text(encoding="utf-8")

    assert "direction_dense" in script_text
    assert "reversal_dense" in script_text
    assert "status_autorun.sh" in script_text


def test_codex_background_loop_allows_autorun_dir_override() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert 'AUTORUN_DIR="${AUTORUN_DIR:-$ROOT_DIR/var/research/autorun}"' in script_text
    assert 'STATUS_PATH="${STATUS_PATH:-$AUTORUN_DIR/codex-background.status.json}"' in script_text
    assert 'LOG_PATH="${LOG_PATH:-$AUTORUN_DIR/codex-background.log}"' in script_text


def test_codex_background_loop_refreshes_prompt_after_run_finishes() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert script_text.count('build_prompt > "$LAST_PROMPT_PATH"') >= 2


def test_codex_background_loop_supports_official_proxy_mode_override() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert 'CODEX_OFFICIAL_NETWORK_PROXY_MODE="${CODEX_OFFICIAL_NETWORK_PROXY_MODE:-$CODEX_NETWORK_PROXY_MODE}"' in script_text
    assert 'build_env_prefix "$CODEX_NETWORK_PROXY_MODE" "$home_root"' in script_text
    assert 'build_env_prefix "$CODEX_OFFICIAL_NETWORK_PROXY_MODE" "$home_root"' in script_text


def test_status_autorun_allows_status_path_override() -> None:
    script_text = Path("auto_research/status_autorun.sh").read_text(encoding="utf-8")

    assert 'AUTORUN_DIR="${AUTORUN_DIR:-$ROOT_DIR/var/research/autorun}"' in script_text
    assert "build_autorun_status_report(" in script_text
    assert "status_path=" in script_text


def test_build_codex_cycle_prompt_accepts_status_path_override() -> None:
    source = Path("src/pm15min/research/automation/control_plane.py").read_text(encoding="utf-8")

    assert "def build_codex_cycle_prompt(" in source
    assert "status_path: Path | None = None" in source
    assert (
        "build_autorun_status_report(root, log_tail_lines=5, max_incomplete_runs=5, status_path=status_path)"
        in source
    )


def test_build_codex_cycle_prompt_warns_against_column_dumping_before_refill() -> None:
    source = Path("src/pm15min/research/automation/control_plane.py").read_text(encoding="utf-8")

    assert "do not spend the cycle dumping full factor lists" in source
    assert "avoid full 48-column dumps" in source


def test_auto_research_scripts_resolve_repo_root_from_new_directory_layout() -> None:
    shell_scripts = [
        Path("auto_research/bootstrap_keepalive.sh"),
        Path("auto_research/codex_background_loop.sh"),
        Path("auto_research/experiment_queue_supervisor.sh"),
        Path("auto_research/run_one_experiment.sh"),
        Path("auto_research/run_one_experiment_background.sh"),
        Path("auto_research/status_autorun.sh"),
    ]
    python_scripts = [
        Path("auto_research/experiment_queue.py"),
        Path("auto_research/summarize_experiment.py"),
        Path("auto_research/update_session.py"),
    ]

    for path in shell_scripts:
        script_text = path.read_text(encoding="utf-8")
        assert 'ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"' in script_text
        assert 'ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"' not in script_text

    for path in python_scripts:
        script_text = path.read_text(encoding="utf-8")
        assert "parents[1]" in script_text
        assert "parents[2]" not in script_text


def test_python_env_activation_uses_real_home_when_home_is_overridden(tmp_path: Path) -> None:
    fake_real_home = tmp_path / "real-home"
    fake_diag_home = tmp_path / "diag-home"
    fake_conda_base = fake_real_home / "miniconda3"
    fake_python = fake_conda_base / "envs" / "pm15min" / "bin" / "python"
    conda_sh = fake_conda_base / "etc" / "profile.d" / "conda.sh"

    fake_diag_home.mkdir(parents=True, exist_ok=True)
    fake_python.parent.mkdir(parents=True, exist_ok=True)
    conda_sh.parent.mkdir(parents=True, exist_ok=True)

    fake_python.write_text(
        "#!/bin/bash\n"
        "set -euo pipefail\n"
        "if [[ \"${1:-}\" == \"-c\" ]]; then\n"
        "  printf '%s\\n' \"$0\"\n"
        "  exit 0\n"
        "fi\n"
        "exit 0\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)
    conda_sh.write_text(
        "conda() {\n"
        "  if [[ \"$1\" == \"info\" && \"${2:-}\" == \"--base\" ]]; then\n"
        "    printf '%s\\n' \"$FAKE_CONDA_BASE\"\n"
        "    return 0\n"
        "  fi\n"
        "  if [[ \"$1\" == \"env\" && \"${2:-}\" == \"list\" ]]; then\n"
        "    printf '# conda environments:\\n'\n"
        "    printf 'base * %s\\n' \"$FAKE_CONDA_BASE\"\n"
        "    printf 'pm15min %s/envs/pm15min\\n' \"$FAKE_CONDA_BASE\"\n"
        "    return 0\n"
        "  fi\n"
        "  if [[ \"$1\" == \"activate\" ]]; then\n"
        "    export CONDA_PREFIX=\"$FAKE_CONDA_BASE/envs/$2\"\n"
        "    return 0\n"
        "  fi\n"
        "  return 0\n"
        "}\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env.update(
        {
            "HOME": str(fake_diag_home),
            "PM15MIN_REAL_HOME": str(fake_real_home),
            "FAKE_CONDA_BASE": str(fake_conda_base),
            "PATH": "/usr/bin:/bin",
        }
    )
    env.pop("CONDA_EXE", None)
    env.pop("CONDA_PREFIX", None)

    result = subprocess.run(
        [
            "/bin/bash",
            "-lc",
            "\n".join(
                [
                    "set -euo pipefail",
                    "source scripts/entrypoints/_python_env.sh",
                    "pm15min_activate_python",
                    "printf 'PM15MIN_CONDA_ENV=%s\\n' \"$PM15MIN_CONDA_ENV\"",
                    "printf 'PYTHON_BIN=%s\\n' \"$PYTHON_BIN\"",
                ]
            ),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert f"PM15MIN_CONDA_ENV=pm15min" in result.stdout
    assert f"PYTHON_BIN={fake_python}" in result.stdout


def test_python_env_can_load_managed_proxy_env_when_enabled(tmp_path: Path) -> None:
    proxy_env = tmp_path / "managed_proxy.env"
    proxy_env.write_text(
        "export HTTP_PROXY='socks5h://127.0.0.1:36897'\n"
        "export HTTPS_PROXY='socks5h://127.0.0.1:36897'\n"
        "export ALL_PROXY='socks5h://127.0.0.1:36897'\n"
        "export PM15MIN_MANAGED_PROXY_ACTIVE_PORT='36897'\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env.update(
        {
            "PM15MIN_MANAGED_PROXY_ENABLE": "1",
            "PM15MIN_MANAGED_PROXY_ENV_FILE": str(proxy_env),
        }
    )
    env.pop("HTTP_PROXY", None)
    env.pop("HTTPS_PROXY", None)
    env.pop("ALL_PROXY", None)

    result = subprocess.run(
        [
            "/bin/bash",
            "-lc",
            "\n".join(
                [
                    "set -euo pipefail",
                    "source scripts/entrypoints/_python_env.sh",
                    "pm15min_load_managed_proxy_env",
                    "printf 'HTTP_PROXY=%s\\n' \"$HTTP_PROXY\"",
                    "printf 'PM15MIN_MANAGED_PROXY_ACTIVE_PORT=%s\\n' \"$PM15MIN_MANAGED_PROXY_ACTIVE_PORT\"",
                ]
            ),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "HTTP_PROXY=socks5h://127.0.0.1:36897" in result.stdout
    assert "PM15MIN_MANAGED_PROXY_ACTIVE_PORT=36897" in result.stdout


def test_python_env_keeps_explicit_proxy_when_managed_proxy_enabled(tmp_path: Path) -> None:
    proxy_env = tmp_path / "managed_proxy.env"
    proxy_env.write_text(
        "export HTTP_PROXY='socks5h://127.0.0.1:36897'\n"
        "export HTTPS_PROXY='socks5h://127.0.0.1:36897'\n"
        "export ALL_PROXY='socks5h://127.0.0.1:36897'\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env.update(
        {
            "PM15MIN_MANAGED_PROXY_ENABLE": "1",
            "PM15MIN_MANAGED_PROXY_ENV_FILE": str(proxy_env),
            "HTTP_PROXY": "http://127.0.0.1:20171",
            "HTTPS_PROXY": "http://127.0.0.1:20171",
            "ALL_PROXY": "http://127.0.0.1:20171",
        }
    )

    result = subprocess.run(
        [
            "/bin/bash",
            "-lc",
            "\n".join(
                [
                    "set -euo pipefail",
                    "source scripts/entrypoints/_python_env.sh",
                    "pm15min_load_managed_proxy_env",
                    "printf 'HTTP_PROXY=%s\\n' \"$HTTP_PROXY\"",
                ]
            ),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "HTTP_PROXY=http://127.0.0.1:20171" in result.stdout


def test_python_env_replaces_stale_lowercase_proxy_when_managed_proxy_enabled(tmp_path: Path) -> None:
    proxy_env = tmp_path / "managed_proxy.env"
    proxy_env.write_text(
        "export HTTP_PROXY='socks5h://127.0.0.1:36897'\n"
        "export HTTPS_PROXY='socks5h://127.0.0.1:36897'\n"
        "export ALL_PROXY='socks5h://127.0.0.1:36897'\n"
        "export PM15MIN_MANAGED_PROXY_ACTIVE_PORT='36897'\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env.update(
        {
            "PM15MIN_MANAGED_PROXY_ENABLE": "1",
            "PM15MIN_MANAGED_PROXY_ENV_FILE": str(proxy_env),
            "http_proxy": "http://127.0.0.1:20171",
            "https_proxy": "http://127.0.0.1:20171",
            "all_proxy": "http://127.0.0.1:20171",
        }
    )
    env.pop("HTTP_PROXY", None)
    env.pop("HTTPS_PROXY", None)
    env.pop("ALL_PROXY", None)

    result = subprocess.run(
        [
            "/bin/bash",
            "-lc",
            "\n".join(
                [
                    "set -euo pipefail",
                    "source scripts/entrypoints/_python_env.sh",
                    "pm15min_load_managed_proxy_env",
                    "printf 'HTTP_PROXY=%s\\n' \"$HTTP_PROXY\"",
                    "printf 'http_proxy=%s\\n' \"$http_proxy\"",
                ]
            ),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "HTTP_PROXY=socks5h://127.0.0.1:36897" in result.stdout
    assert "http_proxy=socks5h://127.0.0.1:36897" in result.stdout


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
    stalled_log_path = stalled_logs / "suite.jsonl"
    summary_path = complete_run / "summary.json"
    stalled_stat = stalled_log_path.stat()
    summary_stat = summary_path.stat()
    newer_time = max(stalled_stat.st_mtime, summary_stat.st_mtime) + 5
    older_time = min(stalled_stat.st_mtime, summary_stat.st_mtime) - 5
    os.utime(summary_path, (older_time, older_time))
    os.utime(stalled_log_path, (newer_time, newer_time))

    payload = find_incomplete_experiment_runs(root)

    assert len(payload) == 1
    assert payload[0]["run_dir"] == str(stalled_run)
    assert payload[0]["state"] == "stuck_seed_case"
    assert payload[0]["last_event"] == "execution_group_seed_case_started"
    assert payload[0]["completed_cases"] == 0


def test_find_incomplete_experiment_runs_ignores_stale_run_when_newer_completed_run_exists(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    stale_run = root / "research" / "experiments" / "runs" / "suite=demo" / "run=stale"
    stale_logs = stale_run / "logs"
    stale_logs.mkdir(parents=True, exist_ok=True)
    stale_log_path = stale_logs / "suite.jsonl"
    stale_log_path.write_text(
        json.dumps({"event": "market_cache_resolved", "case_label": "stale"}) + "\n",
        encoding="utf-8",
    )

    complete_run = root / "research" / "experiments" / "runs" / "suite=demo" / "run=complete"
    complete_logs = complete_run / "logs"
    complete_logs.mkdir(parents=True, exist_ok=True)
    (complete_logs / "suite.jsonl").write_text(
        json.dumps({"event": "market_completed", "case_label": "done"}) + "\n",
        encoding="utf-8",
    )
    summary_path = complete_run / "summary.json"
    summary_path.write_text('{"suite_name":"demo"}', encoding="utf-8")

    stale_stat = stale_log_path.stat()
    summary_stat = summary_path.stat()
    older_time = min(stale_stat.st_mtime, summary_stat.st_mtime) - 5
    newer_time = max(stale_stat.st_mtime, summary_stat.st_mtime) + 5
    os.utime(stale_log_path, (older_time, older_time))
    os.utime(summary_path, (newer_time, newer_time))

    payload = find_incomplete_experiment_runs(root)

    assert payload == []


def test_find_incomplete_experiment_runs_keeps_partial_summary_run_resumable(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    partial_run = root / "research" / "experiments" / "runs" / "suite=demo" / "run=partial"
    partial_logs = partial_run / "logs"
    partial_logs.mkdir(parents=True, exist_ok=True)
    (partial_logs / "suite.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"event": "market_completed", "case_label": "done-1"}),
                json.dumps({"event": "market_cache_resolved", "case_label": "pending-2"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (partial_run / "summary.json").write_text(
        json.dumps(
            {
                "suite_name": "demo",
                "run_label": "partial",
                "cases": 8,
                "completed_cases": 1,
                "failed_cases": 0,
            }
        ),
        encoding="utf-8",
    )

    payload = find_incomplete_experiment_runs(root)

    assert len(payload) == 1
    assert payload[0]["run_dir"] == str(partial_run)
    assert payload[0]["state"] == "checkpointed"
    assert payload[0]["completed_cases"] == 1
    assert payload[0]["cases"] == 8


def test_inspect_experiment_run_treats_quick_screen_summary_as_completed(tmp_path: Path) -> None:
    run_dir = tmp_path / "research" / "experiments" / "runs" / "suite=demo" / "run=quick"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "quick_screen_summary.json").write_text(
        json.dumps(
            {
                "suite_name": "demo",
                "run_label": "quick",
                "rows": 4,
                "selected_rows": 1,
                "markets": ["btc"],
            }
        ),
        encoding="utf-8",
    )

    payload = control_plane.inspect_experiment_run(run_dir)

    assert payload["state"] == "completed"
    assert payload["summary_exists"] is True
    assert payload["completed_cases"] == 4
    assert payload["failed_cases"] == 0


def test_find_recent_completed_experiment_runs_ignores_partial_summary_runs(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    partial_run = root / "research" / "experiments" / "runs" / "suite=demo" / "run=partial"
    (partial_run / "logs").mkdir(parents=True, exist_ok=True)
    (partial_run / "summary.json").write_text(
        '{"suite_name":"demo","run_label":"partial","cases":8,"completed_cases":1,"failed_cases":0}',
        encoding="utf-8",
    )
    (partial_run / "logs" / "suite.jsonl").write_text(
        json.dumps({"event": "market_completed", "case_label": "done-1"}) + "\n",
        encoding="utf-8",
    )

    full_run = root / "research" / "experiments" / "runs" / "suite=demo" / "run=full"
    (full_run / "logs").mkdir(parents=True, exist_ok=True)
    (full_run / "summary.json").write_text(
        '{"suite_name":"demo","run_label":"full","cases":1,"completed_cases":1,"failed_cases":0}',
        encoding="utf-8",
    )
    (full_run / "logs" / "suite.jsonl").write_text(
        json.dumps({"event": "market_completed", "case_label": "done"}) + "\n",
        encoding="utf-8",
    )

    payload = find_recent_completed_experiment_runs(root)

    assert len(payload) == 1
    assert payload[0]["run_dir"] == str(full_run)
    assert payload[0]["run_label"] == "full"


def test_find_recent_completed_experiment_runs_returns_latest_completed_runs(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    first_run = root / "research" / "experiments" / "runs" / "suite=demo" / "run=first"
    (first_run / "logs").mkdir(parents=True, exist_ok=True)
    (first_run / "summary.json").write_text(
        '{"suite_name":"demo","run_label":"first","completed_cases":1,"failed_cases":0}',
        encoding="utf-8",
    )
    (first_run / "logs" / "suite.jsonl").write_text(
        json.dumps({"event": "market_completed", "case_label": "done"}) + "\n",
        encoding="utf-8",
    )

    second_run = root / "research" / "experiments" / "runs" / "suite=demo" / "run=second"
    (second_run / "logs").mkdir(parents=True, exist_ok=True)
    (second_run / "summary.json").write_text(
        '{"suite_name":"demo","run_label":"second","completed_cases":9,"failed_cases":0}',
        encoding="utf-8",
    )
    (second_run / "logs" / "suite.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"event": "market_completed", "case_label": "done-1"}),
                json.dumps({"event": "market_completed", "case_label": "done-2"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    payload = find_recent_completed_experiment_runs(root)

    assert len(payload) == 2
    assert payload[0]["run_dir"] == str(second_run)
    assert payload[0]["state"] == "completed"
    assert payload[0]["completed_cases"] == 9
    assert payload[0]["failed_cases"] == 0
    assert payload[1]["run_dir"] == str(first_run)


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
    assert payload["completed_runs"] == []


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


def test_reseed_empty_tracks_from_recent_done_refills_underfilled_track_markets(tmp_path: Path) -> None:
    from pm15min.research.automation.queue_state import (
        load_experiment_queue,
        reseed_empty_tracks_from_recent_done,
        save_experiment_queue,
    )

    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)

    base_payload = load_experiment_queue(root)
    base_payload["track_slot_caps"] = {"direction_dense": 4, "reversal_dense": 0}
    save_experiment_queue(root, base_payload)

    running_sol = build_queue_item(
        market="sol",
        suite_name="sol_direction_suite",
        run_label="sol_live",
        action="repair",
        status="running",
        track="direction_dense",
        session_dir=root / "sessions" / "direction",
        program_path=root / "auto_research" / "program_direction_dense.md",
    )
    running_xrp = build_queue_item(
        market="xrp",
        suite_name="xrp_direction_suite",
        run_label="xrp_live",
        action="repair",
        status="running",
        track="direction_dense",
        session_dir=root / "sessions" / "direction",
        program_path=root / "auto_research" / "program_direction_dense.md",
    )
    done_btc = build_queue_item(
        market="btc",
        suite_name="btc_direction_suite",
        run_label="btc_done",
        action="launch",
        status="done",
        track="direction_dense",
        session_dir=root / "sessions" / "direction",
        program_path=root / "auto_research" / "program_direction_dense.md",
    )
    done_eth = build_queue_item(
        market="eth",
        suite_name="eth_direction_suite",
        run_label="eth_done",
        action="launch",
        status="done",
        track="direction_dense",
        session_dir=root / "sessions" / "direction",
        program_path=root / "auto_research" / "program_direction_dense.md",
    )

    upsert_queue_item(root, running_sol)
    upsert_queue_item(root, running_xrp)
    upsert_queue_item(root, done_btc)
    upsert_queue_item(root, done_eth)

    payload, reseeded = reseed_empty_tracks_from_recent_done(
        root,
        live_workers=[
            {"market": "sol", "suite_name": "sol_direction_suite", "run_label": "sol_live", "track": "direction_dense"},
            {"market": "xrp", "suite_name": "xrp_direction_suite", "run_label": "xrp_live", "track": "direction_dense"},
        ],
        inspect_run=lambda _run_dir: {"state": "completed"},
    )

    reseeded_labels = {str(item.get("run_label")) for item in reseeded}
    assert reseeded_labels == {"btc_done", "eth_done"}

    items_by_label = {
        str(item.get("run_label")): dict(item)
        for item in payload.get("items") or []
        if isinstance(item, dict)
    }
    assert items_by_label["btc_done"]["status"] == "repair"
    assert items_by_label["eth_done"]["status"] == "repair"
    assert items_by_label["btc_done"]["reason"] == "auto_refill_underfilled_track_from_recent_done"
    assert items_by_label["eth_done"]["reason"] == "auto_refill_underfilled_track_from_recent_done"
