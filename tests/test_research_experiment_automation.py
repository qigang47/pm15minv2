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
    assert "read program_custom.md and the latest session artifacts before making changes; start with results.tsv plus the newest cycle eval" in prompt.lower()
    assert "your codex decision pass must end after this cycle" in prompt.lower()
    assert "healthy formal experiment workers you started or observed may continue running after you exit" in prompt.lower()
    assert "4 simultaneous formal market runs" in prompt
    assert "keep occupancy near 4" in prompt
    assert "do not scan the entire repository" in prompt.lower()
    assert "prefer formal experiment launches over unrelated environment or infrastructure edits" in prompt.lower()
    assert "if `rg` is unavailable" in prompt.lower()
    assert "trust the current run directories" in prompt.lower()
    assert "finished only when `completed_cases + failed_cases` reaches `cases`" in prompt.lower()
    assert "idle coin slots" in prompt.lower()
    assert "newest cycle eval" in prompt.lower()
    assert "fill every allowed idle slot" in prompt.lower()
    assert "do not leave an idle coin slot unfilled solely because the latest result is thin-sample" in prompt.lower()
    assert "still counts as one bounded cycle" in prompt.lower()
    assert "resume as many checkpointed current-line runs as needed to fill those live slots in the same cycle" in prompt.lower()
    assert "do not stop or checkpoint a healthy live formal run merely to end the current codex cycle" in prompt.lower()
    assert "run_one_experiment_background.sh" in prompt


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

    assert "coin slot snapshot already collected for you:" in prompt.lower()
    assert "btc: state=idle" in prompt.lower()
    assert "latest_completed=btc_suite" in prompt
    assert "feature_sets=focus_btc_40_v4" in prompt
    assert "weights=current_default,offset_reversal_mild" in prompt
    assert "eth: state=checkpointed" in prompt.lower()
    assert "feature_sets=focus_eth_40_v4,focus_eth_40_v5" in prompt
    assert "relevant feature-family brief already extracted for you:" in prompt.lower()
    assert "focus_btc_40_v4: market=btc / width=40 / notes=btc frontier" in prompt
    assert "columns: ret_1m, ret_3m, ret_5m" in prompt
    assert "diagnosis_groups:" in prompt
    assert "protect_core=q_bs_up_strike,ret_from_strike,basis_bp,ret_from_cycle_open,first_half_ret,cycle_range_pos,rv_30,macd_z,volume_z,obv_z,vwap_gap_60,bias_60,regime_high_vol" in prompt
    assert "drop_from_first=short_mid_returns,price_position,momentum_oscillator" in prompt
    assert "add_toward=timing,persistence,strike_distance,flip_feasibility,market_quality,junk_cheap_filter" in prompt
    assert "do not open large raw registry files like `research/experiments/custom_feature_sets.json`" in prompt.lower()


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
    bin_dir = root / ".venv_server" / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    (bin_dir / "python").write_text("", encoding="utf-8")

    resolved = resolve_codex_exec_path_prefix(root)

    assert resolved == str(bin_dir.resolve())


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


def test_research_readme_documents_secondary_nimabo_fallback_order() -> None:
    readme_text = Path("auto_research/README.md").read_text(encoding="utf-8")

    assert "CODEX_SECONDARY_BASE_URL" in readme_text
    assert "CODEX_SECONDARY_API_KEY" in readme_text
    assert "primary Nimabo" in readme_text
    assert "secondary Nimabo" in readme_text
    assert "ai.changyou.club" in readme_text
    assert "official" in readme_text


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
