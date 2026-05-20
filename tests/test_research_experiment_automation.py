from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
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
from pm15min.research.automation.factor_scout import (
    build_factor_scout_prompt,
    factor_scout_backlog_path,
    should_refresh_factor_scout_backlog,
    summarize_factor_scout_backlog,
)
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
                "- allowed width ladder: `30 / 34 / 38 / 40 / 44 / 48 / 56`",
                "- profitable offset pool is coin-level and shared by both dense tracks",
            ]
        ),
        encoding="utf-8",
    )

    lines = control_plane._dense_prompt_guidance(program)

    assert any("3 consecutive completed fast screens with zero profitable-pool captures" in line for line in lines)
    assert any("next_route=weight_search_first" in line for line in lines)
    assert any("winner_in_band_weight" in line for line in lines)
    assert any("next_route=factor_rework_first" in line for line in lines)
    assert any("capture quality first, then total trades, then roi" in line.lower() for line in lines)
    assert any("reject-sparse candidate outrank" in line.lower() for line in lines)
    assert any("dense gate first, then total trades" in line.lower() for line in lines)


def test_build_market_history_summary_routes_to_weight_search_from_best_quick_signal(tmp_path: Path) -> None:
    session_dir = tmp_path / "sessions" / "dense_direction"
    session_dir.mkdir(parents=True, exist_ok=True)

    completed_runs = [
        {
            "top_case": {
                "trades": 2,
                "profitable_pool_capture_rows": 0,
            }
        },
        {
            "top_case": {
                "trades": 2,
                "profitable_pool_capture_rows": 0,
            }
        },
        {
            "top_case": {
                "trades": 1,
                "profitable_pool_capture_rows": 0,
            }
        },
    ]

    summary = control_plane._build_market_history_summary(
        project_root=tmp_path,
        session_dir=session_dir,
        market="xrp",
        completed_runs=completed_runs,
        best_quick_run={
            "top_case": {
                "feature_set": "focus_xrp_48_v1",
                "trade_rows": 8,
                "trades": 8,
                "profitable_pool_correct_side_rows": 10,
                "profitable_pool_capture_rows": 7,
                "profitable_pool_rows": 295,
                "profitable_pool_coverage_ratio": 7 / 295,
            }
        },
    )

    assert summary["next_route"] == "weight_search_first"


def test_build_market_history_summary_routes_to_factor_rework_when_best_quick_has_no_correct_side(
    tmp_path: Path,
) -> None:
    session_dir = tmp_path / "sessions" / "dense_reversal"
    session_dir.mkdir(parents=True, exist_ok=True)

    completed_runs = [
        {
            "top_case": {
                "trades": 0,
                "profitable_pool_capture_rows": 0,
            }
        },
        {
            "top_case": {
                "trades": 0,
                "profitable_pool_capture_rows": 0,
            }
        },
        {
            "top_case": {
                "trades": 0,
                "profitable_pool_capture_rows": 0,
            }
        },
    ]

    summary = control_plane._build_market_history_summary(
        project_root=tmp_path,
        session_dir=session_dir,
        market="sol",
        completed_runs=completed_runs,
        best_quick_run={
            "top_case": {
                "feature_set": "focus_sol_48_v9",
                "trade_rows": 0,
                "trades": 0,
                "profitable_pool_correct_side_rows": 0,
                "profitable_pool_capture_rows": 0,
                "profitable_pool_rows": 278,
                "profitable_pool_coverage_ratio": 0.0,
            }
        },
    )

    assert summary["next_route"] == "factor_rework_first"


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
    assert "read the historical decision digest plus program_custom.md before making changes; open results.tsv or older cycle eval files only if the digest still leaves a strategy gap." in prompt.lower()
    assert "your codex decision pass must end after this cycle" in prompt.lower()
    assert "healthy formal experiment workers you started or observed may continue running after you exit" in prompt.lower()
    assert "10 simultaneous formal market runs" in prompt
    assert "keep occupancy near 10" in prompt
    assert "do not scan the entire repository" in prompt.lower()
    assert "prefer formal experiment launches over unrelated environment or infrastructure edits" in prompt.lower()
    assert "if `rg` is unavailable" in prompt.lower()
    assert "trust the current run directories" in prompt.lower()
    assert "historical cycle eval notes about live workers or cpu health are not authoritative for the current cycle" in prompt.lower()
    assert "finished only when `completed_cases + failed_cases` reaches `cases`" in prompt.lower()
    assert "idle coin slots" in prompt.lower()
    assert "historical decision digest already collected for you:" in prompt.lower()
    assert "fill every allowed idle slot" in prompt.lower()
    assert "do not leave an idle coin slot unfilled solely because the latest result is thin-sample" in prompt.lower()
    assert "still counts as one bounded cycle" in prompt.lower()
    assert "resume as many checkpointed current-line runs as needed to fill those live slots in the same cycle" in prompt.lower()
    assert "do not end the cycle with unused live capacity" in prompt.lower()
    assert "if the current autorun snapshot reports `live formal workers: 0`, you are expected to queue or resume work for every coin slot" in prompt.lower()
    assert "if a feature-set name mentioned by old session artifacts is missing from the current registry, treat that as historical drift rather than a blocker" in prompt.lower()
    assert "do not stop or checkpoint a healthy live formal run merely to end the current codex cycle" in prompt.lower()
    assert "run_one_experiment_background.sh" in prompt


def test_build_codex_cycle_prompt_uses_direct_launch_for_standalone_midprice_lines(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_midprice_direction_btc_autoresearch"
    session_dir.mkdir(parents=True, exist_ok=True)
    program_path = root / "auto_research" / "program_direction_midprice_btc.md"
    program_path.parent.mkdir(parents=True, exist_ok=True)
    program_path.write_text(
        "\n".join(
            [
                "# Codex Research Program",
                "",
                "- Active session: `sessions/deep_otm_midprice_direction_btc_autoresearch/session.md`",
                "- Active results log: `sessions/deep_otm_midprice_direction_btc_autoresearch/results.tsv`",
                "- coin: `btc`",
                "- target fixed to `direction`",
                "- run full formal experiments only",
                "- do not use the shared SOL/XRP quick-screen queue",
                "- suite seed: `baseline_midprice_direction_btc_2usd_5max_20260424`",
                "- baseline run to compare against: `auto_btc_direction_entry45_50_prob60_formal_after_full_backfill_20260424`",
                "- launch through `auto_research/run_one_experiment_background.sh`",
            ]
        ),
        encoding="utf-8",
    )

    prompt = build_codex_cycle_prompt(
        project_root=root,
        session_dir=session_dir,
        program_path=program_path,
        status_path=root / "var" / "research" / "autorun" / "midprice_direction_btc" / "codex-background.status.json",
    )
    lower_prompt = prompt.lower()

    assert "standalone direct-launch mode" in lower_prompt
    assert "do not enqueue into the shared experiment queue" in lower_prompt
    assert "do not use quick_screen" in lower_prompt
    assert "do not inspect or launch focus_search" in lower_prompt
    assert "if live formal workers is below 2, your first command must be the direct background launch" in lower_prompt
    assert "--expected-concurrency 2" in lower_prompt
    assert "do not read the factor backlog, global factor inventory, or custom feature set files before that launch" in lower_prompt
    assert "baseline_midprice_direction_btc_2usd_5max_20260424" in prompt
    assert "run_one_experiment_background.sh" in prompt
    assert "Use `auto_research/experiment_queue.py enqueue" not in prompt
    assert "the queue supervisor is responsible" not in lower_prompt


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
    assert "read the historical decision digest before the occupancy snapshots" in prompt.lower()
    assert "queue or resume formal work for the idle coin slots first" in prompt.lower()


def test_build_codex_cycle_prompt_includes_historical_decision_digest_before_machine_summary(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "dense_direction"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_direction_dense.md"
    program_path.write_text(
        "\n".join(
            [
                "# Dense Direction",
                "- coins: `btc`",
                "- target fixed to `direction`",
                "- dense goal: 10-20 trades per coin per day",
            ]
        ),
        encoding="utf-8",
    )
    (session_dir / "results.tsv").write_text(
        "\n".join(
            [
                "cycle\tteam\tmetric\tstatus\tdescription\tfiles_changed\ttimestamp",
                "018\tdirection\ttrades\tobserve\tbtc stayed at 3 trades after another same-width tweak\t\t2026-04-18T10:00:00+00:00",
                "019\tdirection\ttrades\tobserve\tbtc still low trade count and no refresh after similar branch\t\t2026-04-18T10:30:00+00:00",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    cycles_dir = session_dir / "cycles" / "019"
    cycles_dir.mkdir(parents=True, exist_ok=True)
    (cycles_dir / "eval-results.md").write_text(
        "# Cycle 019\n\n- btc repeated the same narrow timing idea and stayed sparse.\n",
        encoding="utf-8",
    )

    experiments_root = root / "research" / "experiments"
    suite_specs_dir = experiments_root / "suite_specs"
    suite_specs_dir.mkdir(parents=True, exist_ok=True)
    (experiments_root / "custom_feature_sets.json").write_text(
        json.dumps(
            {
                "focus_btc_48_v1r1": {
                    "market": "btc",
                    "width": 48,
                    "columns": ["ret_1m", "ret_3m", "volume_z", "obv_z"],
                    "notes": "btc low-trade parent",
                }
            }
        ),
        encoding="utf-8",
    )
    (suite_specs_dir / "btc_direction_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "btc_direction_suite",
                "targets": ["direction"],
                "markets": {
                    "btc": {
                        "groups": {
                            "focus_search": {
                                "runs": [
                                    {
                                        "run_name": "focus_search",
                                        "feature_set_variants": [{"label": "frontier", "feature_set": "focus_btc_48_v1r1"}],
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

    def write_completed_run(run_label: str, trades: int, mtime: int) -> None:
        run_dir = experiments_root / "runs" / "suite=btc_direction_suite" / f"run={run_label}"
        (run_dir / "logs").mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "suite_name": "btc_direction_suite",
                    "run_label": run_label,
                    "cases": 1,
                    "completed_cases": 1,
                    "failed_cases": 0,
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
                    "group_name": "focus_search",
                    "run_name": "focus_search",
                    "target": "direction",
                    "variant_label": "default",
                    "roi_pct": "5.0",
                    "pnl_sum": "1.0",
                    "trades": str(trades),
                }
            )
        (run_dir / "logs" / "suite.jsonl").write_text(
            json.dumps({"event": "market_completed", "case_label": "btc/focus_search"}) + "\n",
            encoding="utf-8",
        )
        os.utime(summary_path, (mtime, mtime))

    write_completed_run("auto_btc_direction_r1", trades=1, mtime=100)
    write_completed_run("auto_btc_direction_r2", trades=2, mtime=200)
    write_completed_run("auto_btc_direction_r3", trades=3, mtime=300)

    prompt = build_codex_cycle_prompt(
        project_root=root,
        session_dir=session_dir,
        program_path=program_path,
        prompt_budget_mode="compact",
    )

    assert "historical decision digest already collected for you:" in prompt.lower()
    assert prompt.lower().index("historical decision digest already collected for you:") < prompt.lower().index(
        "machine decision summary already collected for you:"
    )
    assert "decision_mode=heavy_analysis" in prompt.lower()
    assert "btc: recommendation=heavy_rework" in prompt.lower()
    assert "recent_trades=3,2,1" in prompt.lower()
    assert "recent_session_notes:" in prompt.lower()
    assert "btc stayed at 3 trades after another same-width tweak" in prompt
    assert "global factor inventory omitted in compact prompt mode" not in prompt


def test_build_codex_cycle_prompt_keeps_normal_mode_when_recent_best_trade_count_is_improving(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "dense_reversal"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_reversal_dense.md"
    program_path.write_text(
        "\n".join(
            [
                "# Dense Reversal",
                "- coins: `eth`",
                "- target fixed to `reversal`",
                "- dense goal: 10-20 trades per coin per day",
            ]
        ),
        encoding="utf-8",
    )

    experiments_root = root / "research" / "experiments"
    suite_specs_dir = experiments_root / "suite_specs"
    suite_specs_dir.mkdir(parents=True, exist_ok=True)
    (experiments_root / "custom_feature_sets.json").write_text(
        json.dumps(
            {
                "focus_eth_48_v1r1": {
                    "market": "eth",
                    "width": 48,
                    "columns": ["ret_15m", "rv_30", "obv_z"],
                    "notes": "eth improving branch",
                }
            }
        ),
        encoding="utf-8",
    )
    (suite_specs_dir / "eth_reversal_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "eth_reversal_suite",
                "targets": ["reversal"],
                "markets": {
                    "eth": {
                        "groups": {
                            "focus_search": {
                                "runs": [
                                    {
                                        "run_name": "focus_search",
                                        "feature_set_variants": [{"label": "frontier", "feature_set": "focus_eth_48_v1r1"}],
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

    def write_completed_run(run_label: str, trades: int, mtime: int) -> None:
        run_dir = experiments_root / "runs" / "suite=eth_reversal_suite" / f"run={run_label}"
        (run_dir / "logs").mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "suite_name": "eth_reversal_suite",
                    "run_label": run_label,
                    "cases": 1,
                    "completed_cases": 1,
                    "failed_cases": 0,
                    "markets": ["eth"],
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
                    "market": "eth",
                    "group_name": "focus_search",
                    "run_name": "focus_search",
                    "target": "reversal",
                    "variant_label": "default",
                    "roi_pct": "8.0",
                    "pnl_sum": "2.0",
                    "trades": str(trades),
                }
            )
        (run_dir / "logs" / "suite.jsonl").write_text(
            json.dumps({"event": "market_completed", "case_label": "eth/focus_search"}) + "\n",
            encoding="utf-8",
        )
        os.utime(summary_path, (mtime, mtime))

    write_completed_run("auto_eth_reversal_r1", trades=2, mtime=100)
    write_completed_run("auto_eth_reversal_r2", trades=5, mtime=200)
    write_completed_run("auto_eth_reversal_r3", trades=9, mtime=300)

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert "historical decision digest already collected for you:" in prompt.lower()
    assert "decision_mode=heavy_analysis" in prompt.lower()
    assert "eth: recommendation=heavy_rework" in prompt.lower()
    assert "best_trades=9" in prompt.lower()
    assert "recent_trades=9,5,2" in prompt.lower()
    assert "same suite sparse streak=3" in prompt.lower()
    assert "required_next_lever=feature_width" in prompt


def test_build_codex_cycle_prompt_includes_best_historical_quick_screen_digest(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "dense_direction"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_direction_dense.md"
    program_path.write_text(
        "\n".join(
            [
                "# Dense Direction",
                "- coins: `xrp`",
                "- target fixed to `direction`",
                "- dense goal: 10-20 trades per coin per day",
                "- target about `70%` profitable-pool coverage before spending a full formal slot",
            ]
        ),
        encoding="utf-8",
    )

    experiments_root = root / "research" / "experiments" / "runs"

    def write_quick_run(
        suite_name: str,
        run_label: str,
        *,
        feature_set: str,
        trade_rows: int,
        pool_rows: int,
        capture_rows: int,
        correct_side_rows: int,
        coverage_ratio: float,
        mtime: int,
    ) -> None:
        run_dir = experiments_root / f"suite={suite_name}" / f"run={run_label}"
        run_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "quick_screen_summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "suite_name": suite_name,
                    "run_label": run_label,
                    "top_k": 1,
                    "markets": ["xrp"],
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
                    "target",
                    "feature_set",
                    "variant_label",
                    "trade_rows",
                    "profitable_pool_rows",
                    "profitable_pool_capture_rows",
                    "profitable_pool_correct_side_rows",
                    "profitable_pool_coverage_ratio",
                    "rank",
                    "selected_for_formal",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "market": "xrp",
                    "group_name": "focus_search",
                    "run_name": "focus_search",
                    "target": "direction",
                    "feature_set": feature_set,
                    "variant_label": "default",
                    "trade_rows": str(trade_rows),
                    "profitable_pool_rows": str(pool_rows),
                    "profitable_pool_capture_rows": str(capture_rows),
                    "profitable_pool_correct_side_rows": str(correct_side_rows),
                    "profitable_pool_coverage_ratio": str(coverage_ratio),
                    "rank": "1",
                    "selected_for_formal": "True",
                }
            )
        os.utime(summary_path, (mtime, mtime))

    write_quick_run(
        "baseline_focus_feature_search_xrp_direction_v1",
        "auto_xrp_direction_v1",
        feature_set="focus_xrp_48_v1",
        trade_rows=8,
        pool_rows=295,
        capture_rows=7,
        correct_side_rows=10,
        coverage_ratio=7 / 295,
        mtime=100,
    )
    write_quick_run(
        "baseline_focus_feature_search_xrp_direction_v2",
        "auto_xrp_direction_v2",
        feature_set="focus_xrp_48_v2",
        trade_rows=4,
        pool_rows=295,
        capture_rows=2,
        correct_side_rows=5,
        coverage_ratio=2 / 295,
        mtime=200,
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert "best_quick=7/295 (0.0237)" in prompt.lower()
    assert "best_quick_trades=8" in prompt.lower()
    assert "best_quick_correct_side=10" in prompt.lower()
    assert "best_quick_feature_set=focus_xrp_48_v1" in prompt.lower()
    assert "best quick-screen pool result" in prompt.lower()


def test_resolve_codex_attempt_timeout_sec_uses_heavy_timeout_for_heavy_analysis(tmp_path: Path) -> None:
    prompt_path = tmp_path / "heavy_prompt.md"
    prompt_path.write_text(
        "\n".join(
            [
                "Historical decision digest already collected for you:",
                "- decision_mode=heavy_analysis / heavy_markets=sol / normal_markets=btc,eth,xrp",
                "Machine decision summary already collected for you:",
            ]
        ),
        encoding="utf-8",
    )

    timeout_sec = control_plane.resolve_codex_attempt_timeout_sec(
        prompt_path,
        default_timeout_sec=600,
        heavy_analysis_timeout_sec=1800,
    )

    assert timeout_sec == 1800


def test_resolve_codex_attempt_timeout_sec_keeps_default_for_normal_mode(tmp_path: Path) -> None:
    prompt_path = tmp_path / "normal_prompt.md"
    prompt_path.write_text(
        "\n".join(
            [
                "Historical decision digest already collected for you:",
                "- decision_mode=normal / heavy_markets=none / normal_markets=btc,eth,sol,xrp",
                "Machine decision summary already collected for you:",
            ]
        ),
        encoding="utf-8",
    )

    timeout_sec = control_plane.resolve_codex_attempt_timeout_sec(
        prompt_path,
        default_timeout_sec=600,
        heavy_analysis_timeout_sec=1800,
    )

    assert timeout_sec == 600


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
                "- frozen-window target: `110-220` trades per coin",
                "- feature-set width is not fixed to `40`",
                "- allowed width ladder: `30 / 34 / 38 / 40 / 44 / 48 / 56`",
                "- move width by one bucket per bounded cycle only",
                "- profitable offset pool is coin-level and shared by both dense tracks",
                "- profitable offset pool window: `2026-04-15` through `2026-05-07`, `2usd`",
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
    assert "30 / 34 / 38 / 40 / 44 / 48 / 56" in prompt
    assert "one bucket per bounded cycle" in prompt.lower()
    assert "prefer the next wider bucket" in prompt.lower()
    assert "profitable-offset-pool" in prompt.lower()
    assert "shared by both dense tracks" in prompt.lower()
    assert "2026-04-15 through 2026-05-07" in prompt
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


def test_build_codex_cycle_prompt_uses_queue_file_track_caps_over_stale_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_baseline_direction_dense_sol_xrp_autoresearch"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_direction_dense_sol_xrp.md"
    program_path.write_text(
        "\n".join(
            [
                "# SOL/XRP Dense Direction",
                "- target fixed to `direction`",
                "- coins: `sol`, `xrp`",
                "- active session: `sessions/deep_otm_baseline_direction_dense_sol_xrp_autoresearch/session.md`",
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
                "updated_at": "2026-05-20T13:30:00Z",
                "max_live_runs": 5,
                "max_queued_items": 24,
                "track_slot_caps": {"direction_dense": 5, "reversal_dense": 5},
                "items": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "PM15MIN_FIXED_TRACK_SLOT_CAPS_JSON",
        '{"direction_dense":3,"reversal_dense":2}',
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)
    lower_prompt = prompt.lower()

    assert "current shared caps: direction_dense=5, reversal_dense=5" in lower_prompt
    assert "current-track target is 5" in lower_prompt
    assert "current-track gap is 5" in lower_prompt
    assert "queue 5 additional distinct quick-screen branches now" in lower_prompt
    assert "direction_dense=3, reversal_dense=2" not in lower_prompt


def test_build_codex_cycle_prompt_requires_dense_track_slot_fill_not_one_per_coin(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_baseline_direction_dense_sol_xrp_autoresearch"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_direction_dense_sol_xrp.md"
    program_path.write_text(
        "\n".join(
            [
                "# SOL/XRP Dense Direction",
                "- target fixed to `direction`",
                "- coins: `sol`, `xrp`",
                "- active session: `sessions/deep_otm_baseline_direction_dense_sol_xrp_autoresearch/session.md`",
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
                "updated_at": "2026-05-10T13:30:00Z",
                "max_live_runs": 10,
                "max_queued_items": 24,
                "track_slot_caps": {"direction_dense": 5, "reversal_dense": 5},
                "items": [],
            }
        ),
        encoding="utf-8",
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)
    lower_prompt = prompt.lower()

    assert "occupancy=0/5" in lower_prompt
    assert "track_gap=5" in lower_prompt
    assert "track_target=5" in lower_prompt
    assert "do not stop at one successor per coin" in lower_prompt
    assert "same coin can have multiple queued branches" in lower_prompt
    assert "try to queue up to 5 distinct quick-screen branches" in lower_prompt


def test_build_codex_cycle_prompt_names_exact_dense_branch_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_baseline_reversal_dense_sol_xrp_autoresearch"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_reversal_dense_sol_xrp.md"
    program_path.write_text(
        "\n".join(
            [
                "# SOL/XRP Dense Reversal",
                "- target fixed to `reversal`",
                "- coins: `sol`, `xrp`",
                "- active session: `sessions/deep_otm_baseline_reversal_dense_sol_xrp_autoresearch/session.md`",
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
                "updated_at": "2026-05-10T13:50:00Z",
                "max_live_runs": 10,
                "max_queued_items": 24,
                "track_slot_caps": {"direction_dense": 5, "reversal_dense": 5},
                "items": [],
            }
        ),
        encoding="utf-8",
    )
    for market in ("sol", "xrp"):
        run_label = f"{market}_reversal_live"
        run_dir = root / "research" / "experiments" / "runs" / f"suite={market}_reversal_suite" / f"run={run_label}"
        (run_dir / "logs").mkdir(parents=True, exist_ok=True)
        (run_dir / "logs" / "suite.jsonl").write_text(
            json.dumps({"event": "execution_group_warmup_started", "run_name": "quick_screen"}) + "\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(
        control_plane,
        "find_live_formal_workers",
        lambda _root: [
            {
                "pid": 91,
                "suite_name": "sol_direction_suite",
                "run_label": "sol_direction_live",
                "market": "sol",
                "track": "direction_dense",
            },
            {
                "pid": 92,
                "suite_name": "xrp_direction_suite",
                "run_label": "xrp_direction_live",
                "market": "xrp",
                "track": "direction_dense",
            },
            {
                "pid": 93,
                "suite_name": "sol_direction_extra_suite",
                "run_label": "sol_direction_extra_live",
                "market": "sol",
                "track": "direction_dense",
            },
            {
                "pid": 101,
                "suite_name": "sol_reversal_suite",
                "run_label": "sol_reversal_live",
                "market": "sol",
                "track": "reversal_dense",
            },
            {
                "pid": 102,
                "suite_name": "xrp_reversal_suite",
                "run_label": "xrp_reversal_live",
                "market": "xrp",
                "track": "reversal_dense",
            },
        ],
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)
    lower_prompt = prompt.lower()

    assert "occupancy=2/5" in lower_prompt
    assert "track_gap=3" in lower_prompt
    assert "target_new_branches_now=3" in lower_prompt
    assert "queue 3 additional distinct quick-screen branches" in lower_prompt
    assert "track-level capacity, not one slot per market" in lower_prompt
    assert "do not treat active sol and active xrp workers as full coverage" in lower_prompt


def test_dense_sol_xrp_prompt_does_not_count_btc_eth_direction_workers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_baseline_direction_dense_sol_xrp_autoresearch"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_direction_dense_sol_xrp.md"
    program_path.write_text(
        "\n".join(
            [
                "# SOL/XRP Dense Direction",
                "- target fixed to `direction`",
                "- coins: `sol`, `xrp`",
                "- active session: `sessions/deep_otm_baseline_direction_dense_sol_xrp_autoresearch/session.md`",
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
                "updated_at": "2026-05-20T13:50:00Z",
                "max_live_runs": 5,
                "max_queued_items": 24,
                "track_slot_caps": {"direction_dense": 5, "reversal_dense": 5},
                "items": [],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        control_plane,
        "find_live_formal_workers",
        lambda _root: [
            {
                "pid": 101,
                "suite_name": "sol_direction_suite",
                "run_label": "sol_direction_live_a",
                "market": "sol",
                "track": "direction_dense",
            },
            {
                "pid": 102,
                "suite_name": "xrp_direction_suite",
                "run_label": "xrp_direction_live_a",
                "market": "xrp",
                "track": "direction_dense",
            },
            {
                "pid": 103,
                "suite_name": "sol_direction_suite_b",
                "run_label": "sol_direction_live_b",
                "market": "sol",
                "track": "direction_dense",
            },
            {
                "pid": 104,
                "suite_name": "xrp_direction_suite_b",
                "run_label": "xrp_direction_live_b",
                "market": "xrp",
                "track": "direction_dense",
            },
            {
                "pid": 201,
                "suite_name": "btc_direction_formal_suite",
                "run_label": "btc_direction_formal_live",
                "market": "btc",
            },
            {
                "pid": 202,
                "suite_name": "eth_direction_formal_suite",
                "run_label": "eth_direction_formal_live",
                "market": "eth",
            },
        ],
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)
    lower_prompt = prompt.lower()

    assert "occupancy=4/5" in lower_prompt
    assert "track_gap=1" in lower_prompt
    assert "queue 1 additional distinct quick-screen branches" in lower_prompt
    assert "btc_direction_formal_live" not in lower_prompt
    assert "eth_direction_formal_live" not in lower_prompt


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
            f"101 1 S /bin/bash {root}/auto_research/run_one_experiment.sh --suite demo_suite --run-label demo_run --market btc",
            f"202 101 S /bin/bash {root}/auto_research/run_one_experiment.sh --suite demo_suite --run-label demo_run --market btc",
            f"303 1 S /bin/bash {root}/auto_research/run_one_experiment.sh --suite other_suite --run-label other_run --market eth",
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
            f"101 1 S /home/demo/.venv_server/bin/python -m pm15min research experiment run-suite --suite sol_suite --run-label sol_run --market sol --project-root {root}",
            f"202 1 S /home/demo/.venv_server/bin/python -m pm15min research experiment run-suite --suite btc_suite --run-label btc_run --market btc --root /tmp/other",
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
            f"101 1 S /home/demo/.venv_server/bin/python {root}/scripts/research/run_quick_screen_suite.py --suite eth_suite --run-label eth_run --top-k 1",
            f"202 1 S /home/demo/.venv_server/bin/python /tmp/other/scripts/research/run_quick_screen_suite.py --suite other_suite --run-label other_run --top-k 1",
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


def test_find_scoped_experiment_worker_pids_matches_direct_quick_screen_by_market_and_track(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    ps_output = "\n".join(
        [
            f"101 1 S 3000000 /home/demo/.venv_server/bin/python {root}/scripts/research/run_quick_screen_suite.py --suite baseline_focus_feature_search_sol_direction_44v1 --run-label cycle156_sol_44 --top-k 1",
            f"202 1 S 3000000 /home/demo/.venv_server/bin/python {root}/scripts/research/run_quick_screen_suite.py --suite baseline_focus_feature_search_sol_reversal_56v1 --run-label cycle_reversal_sol_56 --top-k 1",
            f"303 1 S 3000000 /home/demo/.venv_server/bin/python {root}/scripts/research/run_quick_screen_suite.py --suite baseline_focus_feature_search_xrp_direction_56v1 --run-label cycle141_xrp_56 --top-k 1",
            f"404 1 S 3000000 /home/demo/.venv_server/bin/python /tmp/other/scripts/research/run_quick_screen_suite.py --suite baseline_focus_feature_search_sol_direction_other --run-label other_sol_direction --top-k 1",
            f"505 1 S 3000000 /home/demo/.venv_server/bin/python -m pm15min research experiment run-suite --suite eth_suite --run-label eth_run --market eth --project-root {root}",
        ]
    )

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args=args[0], returncode=0, stdout=ps_output, stderr=""),
    )

    pids = control_plane.find_scoped_experiment_worker_pids(
        root,
        allowed_markets=["sol", "xrp"],
        track="direction_dense",
    )

    assert pids == [101, 303]


def test_find_scoped_experiment_worker_pids_keeps_wrapper_and_child_for_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    suite_name = "baseline_focus_feature_search_sol_direction_44v1"
    run_label = "cycle156_sol_44"
    ps_output = "\n".join(
        [
            f"101 1 S 440 /bin/bash {root}/auto_research/run_one_experiment.sh --suite {suite_name} --run-label {run_label} --market sol --launch-mode quick_screen",
            f"202 101 S 12000000 /home/demo/.venv_server/bin/python {root}/scripts/research/run_quick_screen_suite.py --suite {suite_name} --run-label {run_label} --top-k 1",
            f"303 1 S 12000000 /home/demo/.venv_server/bin/python {root}/scripts/research/run_quick_screen_suite.py --suite baseline_focus_feature_search_sol_reversal_56v1 --run-label cycle_reversal_sol_56 --top-k 1",
        ]
    )

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args=args[0], returncode=0, stdout=ps_output, stderr=""),
    )

    pids = control_plane.find_scoped_experiment_worker_pids(
        root,
        allowed_markets=["sol"],
        track="direction_dense",
    )

    assert pids == [101, 202]


def test_find_scoped_experiment_worker_pids_keeps_formal_child_without_project_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    ps_output = "\n".join(
        [
            f"101 1 S 440 /bin/bash {root}/auto_research/run_one_experiment.sh --suite btc_direction_suite --run-label btc_run --market btc --launch-mode formal",
            "202 101 R 18000000 /home/demo/.venv_server/bin/python -m pm15min research experiment run-suite --suite btc_direction_suite --run-label btc_run --market btc",
            "303 1 R 18000000 /home/demo/.venv_server/bin/python -m pm15min research experiment run-suite --suite eth_direction_suite --run-label eth_run --market eth",
        ]
    )

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args=args[0], returncode=0, stdout=ps_output, stderr=""),
    )

    pids = control_plane.find_scoped_experiment_worker_pids(
        root,
        allowed_markets=["btc"],
        track="direction_dense",
    )

    assert pids == [101, 202]


def test_find_live_formal_workers_includes_quick_screen_batch_processes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "var" / "research" / "autorun" / "queue" / "batch.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "batch_id": "batch-abc",
                "items": [
                    {
                        "market": "sol",
                        "suite_name": "sol_suite",
                        "run_label": "sol_run",
                        "track": "reversal_dense",
                    },
                    {
                        "market": "xrp",
                        "suite_name": "xrp_suite",
                        "run_label": "xrp_run",
                        "track": "reversal_dense",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    quick_screen_output = "\n".join(
        [
            f"101 1 S /home/demo/.venv_server/bin/python {root}/scripts/research/run_quick_screen_queue_batch.py --manifest {manifest_path} --batch-id batch-abc",
            f"202 1 S /home/demo/.venv_server/bin/python /tmp/other/scripts/research/run_quick_screen_queue_batch.py --manifest /tmp/other/batch.json --batch-id batch-other",
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
            "run_label": "sol_run",
            "suite_name": "sol_suite",
            "market": "sol",
            "track": "reversal_dense",
            "batch_id": "batch-abc",
            "batch_manifest_path": str(manifest_path),
            "cmd": f"/home/demo/.venv_server/bin/python {root}/scripts/research/run_quick_screen_queue_batch.py --manifest {manifest_path} --batch-id batch-abc",
        },
        {
            "pid": 101,
            "ppid": 1,
            "run_label": "xrp_run",
            "suite_name": "xrp_suite",
            "market": "xrp",
            "track": "reversal_dense",
            "batch_id": "batch-abc",
            "batch_manifest_path": str(manifest_path),
            "cmd": f"/home/demo/.venv_server/bin/python {root}/scripts/research/run_quick_screen_queue_batch.py --manifest {manifest_path} --batch-id batch-abc",
        },
    ]


def test_find_live_formal_workers_handles_batch_and_direct_processes_together(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "var" / "research" / "autorun" / "queue" / "batch.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "batch_id": "batch-abc",
                "items": [
                    {
                        "market": "sol",
                        "suite_name": "sol_suite",
                        "run_label": "sol_run",
                        "track": "reversal_dense",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    output = "\n".join(
        [
            f"101 1 S /home/demo/.venv_server/bin/python {root}/scripts/research/run_quick_screen_queue_batch.py --manifest {manifest_path} --batch-id batch-abc",
            f"202 1 S /home/demo/.venv_server/bin/python -m pm15min research experiment run-suite --suite btc_suite --run-label btc_run --market btc --project-root {root}",
        ]
    )

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args=args[0], returncode=0, stdout=output, stderr=""),
    )

    workers = control_plane.find_live_formal_workers(root)

    assert [(worker["run_label"], worker["market"]) for worker in workers] == [
        ("sol_run", "sol"),
        ("btc_run", "btc"),
    ]


def test_find_live_formal_workers_deduplicates_quick_screen_parent_and_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    output = "\n".join(
        [
            f"101 1 S /bin/bash {root}/auto_research/run_one_experiment.sh --suite sol_suite --run-label sol_run --market sol --launch-mode quick_screen",
            f"202 101 S /bin/bash {root}/auto_research/run_one_experiment.sh --suite sol_suite --run-label sol_run --market sol --launch-mode quick_screen",
            f"303 202 R /home/demo/.venv_server/bin/python {root}/scripts/research/run_quick_screen_suite.py --suite sol_suite --run-label sol_run --top-k 1",
        ]
    )

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args=args[0], returncode=0, stdout=output, stderr=""),
    )

    workers = control_plane.find_live_formal_workers(root)

    assert workers == [
        {
            "pid": 101,
            "ppid": 1,
            "run_label": "sol_run",
            "suite_name": "sol_suite",
            "market": "sol",
            "cmd": f"/bin/bash {root}/auto_research/run_one_experiment.sh --suite sol_suite --run-label sol_run --market sol --launch-mode quick_screen",
        }
    ]


def test_find_live_formal_workers_uses_child_python_rss_for_same_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    output = "\n".join(
        [
            f"101 1 S 80 /bin/bash {root}/auto_research/run_one_experiment.sh --suite eth_suite --run-label eth_run --market eth",
            f"202 101 R 9000000 /home/demo/.venv_server/bin/python -m pm15min research experiment run-suite --suite eth_suite --run-label eth_run --market eth",
        ]
    )

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args=args[0], returncode=0, stdout=output, stderr=""),
    )

    workers = control_plane.find_live_formal_workers(root)

    assert workers == [
        {
            "pid": 202,
            "ppid": 101,
            "rss_kb": 9000000,
            "run_label": "eth_run",
            "suite_name": "eth_suite",
            "market": "eth",
            "cmd": "/home/demo/.venv_server/bin/python -m pm15min research experiment run-suite --suite eth_suite --run-label eth_run --market eth",
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
            f"101 1 S /bin/bash {script_path} __run_loop",
            f"202 101 S /home/demo/.local/bin/codex exec --cd {root} --output-last-message {output_path} --sandbox danger-full-access -",
            f"303 1 S /bin/bash {root}/scripts/research/other_loop.sh __run_loop",
            f"404 1 S /home/demo/.local/bin/codex exec --cd /tmp/other --output-last-message {output_path} --sandbox danger-full-access -",
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


def test_find_live_formal_workers_ignores_zombies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    ps_output = "\n".join(
        [
            f"101 1 Z /bin/bash {root}/auto_research/run_one_experiment.sh --suite dead_suite --run-label dead_run --market btc",
            f"202 1 S /bin/bash {root}/auto_research/run_one_experiment.sh --suite live_suite --run-label live_run --market eth",
        ]
    )

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args=args[0], returncode=0, stdout=ps_output, stderr=""),
    )

    workers = control_plane.find_live_formal_workers(root)

    assert workers == [
        {
            "pid": 202,
            "ppid": 1,
            "run_label": "live_run",
            "suite_name": "live_suite",
            "market": "eth",
            "cmd": f"/bin/bash {root}/auto_research/run_one_experiment.sh --suite live_suite --run-label live_run --market eth",
        }
    ]


def test_find_live_autorun_processes_ignores_zombies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    root.mkdir(parents=True, exist_ok=True)
    output_path = root / "var" / "research" / "autorun" / "codex-last-output.txt"
    script_path = root / "auto_research" / "codex_background_loop.sh"
    ps_output = "\n".join(
        [
            f"101 1 Z /bin/bash {script_path} __run_loop",
            f"202 1 S /home/demo/.local/bin/codex exec --cd {root} --output-last-message {output_path} --sandbox danger-full-access -",
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
            "pid": 202,
            "ppid": 1,
            "kind": "codex_exec",
            "cmd": f"/home/demo/.local/bin/codex exec --cd {root} --output-last-message {output_path} --sandbox danger-full-access -",
        }
    ]


def test_pid_is_live_rejects_zombie_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(control_plane.os, "kill", lambda _pid, _sig: None)
    monkeypatch.setattr(control_plane, "_pid_proc_state", lambda _pid: "Z")

    assert control_plane._pid_is_live(12345) is False


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
    assert "read the historical decision digest before the occupancy snapshots. treat it as required decision input, not optional extra context." in prompt.lower()
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


def test_build_codex_cycle_prompt_includes_factor_backlog_reference_and_priority_features(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    (root / "auto_research").mkdir(parents=True, exist_ok=True)
    (root / "auto_research" / "program.md").write_text(
        "# Demo Program\n\n- coins: `btc`\n",
        encoding="utf-8",
    )
    (root / "docs").mkdir(parents=True, exist_ok=True)
    (root / "docs" / "DEEP_OTM_BASELINE_FACTOR_BACKLOG.md").write_text(
        "# Factor Backlog\n",
        encoding="utf-8",
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert "docs/DEEP_OTM_BASELINE_FACTOR_BACKLOG.md" in prompt
    assert "priority_backlog=minutes_left_to_settle,up_move_remaining_per_minute,up_move_remaining_z_per_minute,first_up_cross_offset,minutes_since_first_up_cross,up_hold_minutes,rel_strength_15m,btc_ret_5m,btc_vol_30m,taker_buy_ratio_change,rv_30_change" in prompt


def test_factor_scout_prompt_requires_public_sources_and_backlog_only(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    prompt = build_factor_scout_prompt(
        project_root=root,
        target="reversal",
        markets=["xrp", "sol"],
        max_candidates=4,
    )

    assert str(root) in prompt
    assert "public sources only" in prompt.lower()
    assert "do not modify experiment suite specs" in prompt.lower()
    assert "do not edit custom_feature_sets.json" in prompt
    assert "write candidates only to" in prompt.lower()
    assert "docs/DEEP_OTM_BASELINE_FACTOR_SCOUT_BACKLOG.md" in prompt
    assert "xrp / sol" in prompt
    assert "reversal" in prompt
    assert "source url" in prompt.lower()
    assert "candidate factor" in prompt.lower()


def test_summarize_factor_scout_backlog_extracts_recent_candidates(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    backlog = factor_scout_backlog_path(root)
    backlog.parent.mkdir(parents=True, exist_ok=True)
    backlog.write_text(
        "\n".join(
            [
                "# Factor Scout Backlog",
                "",
                "## Candidate: orderbook imbalance persistence",
                "- source_url: https://example.com/orderbook",
                "- target: reversal",
                "- markets: xrp, sol",
                "- status: proposed",
                "",
                "## Candidate: funding pressure carry",
                "- source_url: https://example.com/funding",
                "- target: direction",
                "- markets: btc",
                "- status: parked",
            ]
        ),
        encoding="utf-8",
    )

    summary = summarize_factor_scout_backlog(root, limit=1)

    assert summary["path"] == str(backlog)
    assert summary["candidate_count"] == 2
    assert summary["lines"] == [
        "- orderbook imbalance persistence / target=reversal / markets=xrp, sol / status=proposed / source=https://example.com/orderbook"
    ]


def test_summarize_factor_scout_backlog_ignores_candidate_format_template(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    backlog = factor_scout_backlog_path(root)
    backlog.parent.mkdir(parents=True, exist_ok=True)
    backlog.write_text(
        "\n".join(
            [
                "# Factor Scout Backlog",
                "",
                "## Candidate Format",
                "",
                "```text",
                "## Candidate: <candidate factor name>",
                "- source_url: <public URL>",
                "- target: <direction|reversal|both>",
                "- markets: <comma-separated markets>",
                "- status: proposed",
                "```",
            ]
        ),
        encoding="utf-8",
    )

    summary = summarize_factor_scout_backlog(root)

    assert summary["candidate_count"] == 0
    assert summary["lines"] == []


def test_should_refresh_factor_scout_backlog_refreshes_empty_backlog(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    backlog = factor_scout_backlog_path(root)
    backlog.parent.mkdir(parents=True, exist_ok=True)
    backlog.write_text("# Factor Scout Backlog\n", encoding="utf-8")
    stamp = root / "var" / "research" / "autorun" / "factor-scout.last-success"

    decision = should_refresh_factor_scout_backlog(
        root,
        stamp_path=stamp,
        now_epoch=1000,
        min_interval_sec=3600,
    )

    assert decision["should_refresh"] is True
    assert decision["reason"] == "empty_backlog"


def test_should_refresh_factor_scout_backlog_throttles_recent_empty_attempt(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    backlog = factor_scout_backlog_path(root)
    backlog.parent.mkdir(parents=True, exist_ok=True)
    backlog.write_text("# Factor Scout Backlog\n", encoding="utf-8")
    stamp = root / "var" / "research" / "autorun" / "factor-scout.last-success"
    stamp.parent.mkdir(parents=True, exist_ok=True)
    stamp.write_text("900\n", encoding="utf-8")

    decision = should_refresh_factor_scout_backlog(
        root,
        stamp_path=stamp,
        now_epoch=1000,
        min_interval_sec=3600,
    )

    assert decision["should_refresh"] is False
    assert decision["reason"] == "recent_empty_attempt"


def test_should_refresh_factor_scout_backlog_refreshes_stale_candidates(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    backlog = factor_scout_backlog_path(root)
    backlog.parent.mkdir(parents=True, exist_ok=True)
    backlog.write_text(
        "\n".join(
            [
                "# Factor Scout Backlog",
                "",
                "## Candidate: orderbook imbalance persistence",
                "- source_url: https://example.com/orderbook",
                "- target: reversal",
                "- markets: xrp",
                "- status: proposed",
            ]
        ),
        encoding="utf-8",
    )
    stamp = root / "var" / "research" / "autorun" / "factor-scout.last-success"
    stamp.parent.mkdir(parents=True, exist_ok=True)
    stamp.write_text("1000\n", encoding="utf-8")

    decision = should_refresh_factor_scout_backlog(
        root,
        stamp_path=stamp,
        now_epoch=10_000,
        min_interval_sec=3600,
    )

    assert decision["should_refresh"] is True
    assert decision["reason"] == "stale_backlog"


def test_should_refresh_factor_scout_backlog_treats_recent_candidate_file_as_fresh_without_stamp(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    backlog = factor_scout_backlog_path(root)
    backlog.parent.mkdir(parents=True, exist_ok=True)
    backlog.write_text(
        "\n".join(
            [
                "# Factor Scout Backlog",
                "",
                "## Candidate: orderbook imbalance persistence",
                "- source_url: https://example.com/orderbook",
                "- target: reversal",
                "- markets: xrp",
                "- status: proposed",
            ]
        ),
        encoding="utf-8",
    )
    os.utime(backlog, (900, 900))
    stamp = root / "var" / "research" / "autorun" / "factor-scout.last-success"

    decision = should_refresh_factor_scout_backlog(
        root,
        stamp_path=stamp,
        now_epoch=1000,
        min_interval_sec=3600,
    )

    assert decision["should_refresh"] is False
    assert decision["reason"] == "fresh_backlog_file"


def test_should_refresh_factor_scout_backlog_refreshes_when_requested_market_has_no_candidates(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    backlog = factor_scout_backlog_path(root)
    backlog.parent.mkdir(parents=True, exist_ok=True)
    backlog.write_text(
        "\n".join(
            [
                "# Factor Scout Backlog",
                "",
                "## Candidate: orderbook imbalance persistence",
                "- source_url: https://example.com/orderbook",
                "- target: reversal",
                "- markets: sol, xrp",
                "- status: proposed",
            ]
        ),
        encoding="utf-8",
    )
    os.utime(backlog, (900, 900))
    stamp = root / "var" / "research" / "autorun" / "btc" / "factor-scout.last-success"

    decision = should_refresh_factor_scout_backlog(
        root,
        stamp_path=stamp,
        now_epoch=1000,
        min_interval_sec=3600,
        markets=["btc"],
        target="direction",
    )

    assert decision["should_refresh"] is True
    assert decision["reason"] == "missing_market_candidates"
    assert decision["candidate_count"] == 1
    assert decision["matching_candidate_count"] == 0


def test_should_refresh_factor_scout_backlog_throttles_recent_missing_market_attempt(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    backlog = factor_scout_backlog_path(root)
    backlog.parent.mkdir(parents=True, exist_ok=True)
    backlog.write_text(
        "\n".join(
            [
                "# Factor Scout Backlog",
                "",
                "## Candidate: orderbook imbalance persistence",
                "- source_url: https://example.com/orderbook",
                "- target: reversal",
                "- markets: sol, xrp",
                "- status: proposed",
            ]
        ),
        encoding="utf-8",
    )
    stamp = root / "var" / "research" / "autorun" / "btc" / "factor-scout.last-success"
    stamp.parent.mkdir(parents=True, exist_ok=True)
    stamp.write_text("950\n", encoding="utf-8")

    decision = should_refresh_factor_scout_backlog(
        root,
        stamp_path=stamp,
        now_epoch=1000,
        min_interval_sec=3600,
        markets=["btc"],
        target="direction",
    )

    assert decision["should_refresh"] is False
    assert decision["reason"] == "recent_market_empty_attempt"
    assert decision["candidate_count"] == 1
    assert decision["matching_candidate_count"] == 0


def test_build_codex_cycle_prompt_references_factor_scout_backlog(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    (root / "auto_research").mkdir(parents=True, exist_ok=True)
    (root / "auto_research" / "program.md").write_text(
        "# Demo Program\n\n- coins: `xrp`\n",
        encoding="utf-8",
    )
    backlog = factor_scout_backlog_path(root)
    backlog.parent.mkdir(parents=True, exist_ok=True)
    backlog.write_text(
        "\n".join(
            [
                "# Factor Scout Backlog",
                "",
                "## Candidate: orderbook imbalance persistence",
                "- source_url: https://example.com/orderbook",
                "- target: reversal",
                "- markets: xrp",
                "- status: proposed",
            ]
        ),
        encoding="utf-8",
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert "Factor scout backlog already collected for you:" in prompt
    assert "orderbook imbalance persistence" in prompt
    assert "Use factor scout candidates as research leads, not as permission to edit experiment configs blindly." in prompt
    assert str(backlog) in prompt


def test_build_codex_cycle_prompt_includes_global_factor_inventory_not_just_current_brief(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    (root / "auto_research").mkdir(parents=True, exist_ok=True)
    (root / "auto_research" / "program.md").write_text(
        "# Demo Program\n\n- coins: `xrp`\n",
        encoding="utf-8",
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert "global factor inventory already extracted for you:" in prompt.lower()
    assert "family=strike" in prompt
    assert "up_move_remaining_per_minute" in prompt
    assert "family=cycle" in prompt
    assert "minutes_left_to_settle" in prompt
    assert "family=cross_asset" in prompt
    assert "rel_strength_15m" in prompt
    assert "treat the feature-family brief as a shortcut, not a whitelist." in prompt.lower()


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
        base_url="https://nimabo.cn/v1",
        api_key="fallback-key",
    )

    isolated_codex_dir = isolated_home / ".codex"
    assert payload["codex_dir"] == str(isolated_codex_dir)
    config_text = (isolated_codex_dir / "config.toml").read_text(encoding="utf-8")
    assert 'base_url = "https://nimabo.cn/v1"' in config_text
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
    Switching to fallback provider https://nimabo.cn/v1 for the next attempt.
    The previous attempt failed because /bin/bash: python: command not found
    """
    assert (
        is_transient_codex_provider_failure(
            output,
            base_url="https://nimabo.cn/v1",
        )
        is False
    )


def test_codex_background_loop_includes_secondary_nimabo_fallback_layer() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert "CODEX_SECONDARY_BASE_URL" in script_text
    assert "CODEX_SECONDARY_API_KEY" in script_text
    assert "CODEX_SECONDARY_HOME_DIR" in script_text
    assert "trying secondary provider first" in script_text
    assert "retrying with secondary fallback provider" in script_text
    assert script_text.index("trying secondary provider first") < script_text.index("trying official auth first")
    assert script_text.index("retrying with secondary fallback provider") < script_text.index(
        "retrying with fallback provider"
    )
    assert script_text.index("retrying with fallback provider") < script_text.index(
        "retrying with official auth fallback"
    )


def test_codex_background_loop_disables_official_auth_by_default() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert 'CODEX_OFFICIAL_PRIORITY="${CODEX_OFFICIAL_PRIORITY:-disabled}"' in script_text
    assert '[[ "$CODEX_OFFICIAL_PRIORITY" != "disabled" && -f "$CODEX_OFFICIAL_AUTH_PATH" ]]' in script_text
    assert '[[ "$primary_provider" != "secondary" ]]' in script_text


def test_codex_background_loop_loads_shared_fallback_env_before_per_track_override() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert "CODEX_SHARED_FALLBACK_ENV_PATH" in script_text
    assert script_text.index('if [[ -f "$CODEX_SHARED_FALLBACK_ENV_PATH" ]]') < script_text.index(
        'if [[ -f "$FALLBACK_ENV_PATH" ]]'
    )


def test_codex_background_loop_can_continue_after_provider_outage_failures() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert "CODEX_STOP_ON_CONSECUTIVE_FAILURES" in script_text
    assert 'CODEX_STOP_ON_CONSECUTIVE_FAILURES="${CODEX_STOP_ON_CONSECUTIVE_FAILURES:-0}"' in script_text
    assert 'if [[ "$RUN_ONCE_SHOULD_STOP" == "1" && "$CODEX_STOP_ON_CONSECUTIVE_FAILURES" == "1" ]]' in script_text


def test_codex_background_loop_terminates_full_attempt_process_group() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert "setsid" in script_text
    assert "kill -- -\"$attempt_pid\"" in script_text or "kill -TERM -- -\"$attempt_pid\"" in script_text
    assert "kill -9 -- -\"$attempt_pid\"" in script_text or "kill -KILL -- -\"$attempt_pid\"" in script_text


def test_codex_background_loop_stop_cleans_scoped_experiment_workers() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert "list_scoped_experiment_worker_pids()" in script_text
    assert "find_scoped_experiment_worker_pids" in script_text
    assert "PM15MIN_ALLOWED_QUEUE_MARKETS" in script_text
    assert "PM15MIN_MANAGED_EXPERIMENT_TRACK" in script_text
    assert "terminate_scoped_experiment_workers" in script_text.split("stop)")[1]


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


def test_codex_background_loop_runs_factor_scout_before_main_cycle() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert "run_factor_scout_if_due" in script_text
    assert "FACTOR_SCOUT_ENABLE" in script_text
    assert 'FACTOR_SCOUT_LOCK_STALE_SEC="${FACTOR_SCOUT_LOCK_STALE_SEC:-3600}"' in script_text
    assert "factor-scout.last-success" in script_text
    assert 'FACTOR_SCOUT_STAMP_PATH="${FACTOR_SCOUT_STAMP_PATH:-$ROOT_DIR/var/research/autorun/factor-scout.last-success}"' in script_text
    assert 'FACTOR_SCOUT_LOCK_DIR="${FACTOR_SCOUT_LOCK_DIR:-$ROOT_DIR/var/research/autorun/factor-scout.lock}"' in script_text
    assert "maybe_clear_stale_factor_scout_lock" in script_text
    assert "[factor_scout] stale_lock_removed" in script_text
    assert 'mkdir "$FACTOR_SCOUT_LOCK_DIR"' in script_text
    assert "auto_research/factor_scout.py" in script_text
    assert 'local LAST_PROMPT_PATH="$FACTOR_SCOUT_PROMPT_PATH"' in script_text
    assert script_text.index("run_factor_scout_if_due") < script_text.index("build_prompt > \"$LAST_PROMPT_PATH\"")


def test_research_readme_documents_secondary_nimabo_fallback_order() -> None:
    readme_text = Path("auto_research/README.md").read_text(encoding="utf-8")

    assert "CODEX_SECONDARY_BASE_URL" in readme_text
    assert "CODEX_SECONDARY_API_KEY" in readme_text
    assert "CODEX_OFFICIAL_NETWORK_PROXY_MODE" in readme_text
    assert "CODEX_OFFICIAL_PRIORITY=disabled|fallback|first" in readme_text
    assert "official login is disabled by default" in readme_text
    assert "secondary Nimabo key from `CODEX_SECONDARY_BASE_URL` + `CODEX_SECONDARY_API_KEY`" in readme_text
    assert "secondary Nimabo" in readme_text
    assert "optional backup provider" in readme_text
    assert "managed proxy env file" in readme_text
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
    assert "30 / 34 / 38 / 40 / 44 / 48 / 56" in direction_text
    assert "30 / 34 / 38 / 40 / 44 / 48 / 56" in reversal_text
    assert "one bucket per bounded cycle" in direction_text.lower()
    assert "one bucket per bounded cycle" in reversal_text.lower()
    assert "Profitable Offset Pool Gate" in direction_text
    assert "Profitable Offset Pool Gate" in reversal_text
    assert "Candidate Ranking" in direction_text
    assert "Candidate Ranking" in reversal_text
    assert "shared by both dense tracks" in direction_text
    assert "shared by both dense tracks" in reversal_text
    assert "2026-04-15" in direction_text and "2026-05-07" in direction_text
    assert "2026-04-15" in reversal_text and "2026-05-07" in reversal_text
    assert "<= 0.30" in direction_text
    assert "<= 0.30" in reversal_text
    assert "70%" in direction_text
    assert "70%" in reversal_text


def test_dense_start_wrappers_bind_distinct_program_and_autorun_dirs() -> None:
    direction_text = Path("auto_research/start_direction_dense.sh").read_text(encoding="utf-8")
    reversal_text = Path("auto_research/start_reversal_dense.sh").read_text(encoding="utf-8")

    assert 'PROGRAM_PATH="${PROGRAM_PATH:-$ROOT_DIR/auto_research/program_direction_dense.md}"' in direction_text
    assert 'PROGRAM_PATH="${PROGRAM_PATH:-$ROOT_DIR/auto_research/program_reversal_dense.md}"' in reversal_text
    assert 'SESSION_DIR="${SESSION_DIR:-$ROOT_DIR/sessions/deep_otm_baseline_direction_dense_autoresearch}"' in direction_text
    assert 'SESSION_DIR="${SESSION_DIR:-$ROOT_DIR/sessions/deep_otm_baseline_reversal_dense_autoresearch}"' in reversal_text
    assert 'AUTORUN_DIR="${AUTORUN_DIR:-$ROOT_DIR/var/research/autorun/direction_dense}"' in direction_text
    assert 'AUTORUN_DIR="${AUTORUN_DIR:-$ROOT_DIR/var/research/autorun/reversal_dense}"' in reversal_text
    assert 'CODEX_OFFICIAL_AUTH_PATH="${CODEX_OFFICIAL_AUTH_PATH:-$ROOT_DIR/var/research/autorun/codex-official-auth.json}"' in direction_text
    assert 'CODEX_OFFICIAL_AUTH_PATH="${CODEX_OFFICIAL_AUTH_PATH:-$ROOT_DIR/var/research/autorun/codex-official-auth.json}"' in reversal_text
    assert 'CODEX_NETWORK_PROXY_MODE="${CODEX_NETWORK_PROXY_MODE:-direct}"' in direction_text
    assert 'CODEX_NETWORK_PROXY_MODE="${CODEX_NETWORK_PROXY_MODE:-direct}"' in reversal_text
    assert 'CODEX_OFFICIAL_NETWORK_PROXY_MODE="${CODEX_OFFICIAL_NETWORK_PROXY_MODE:-managed}"' in direction_text
    assert 'CODEX_OFFICIAL_NETWORK_PROXY_MODE="${CODEX_OFFICIAL_NETWORK_PROXY_MODE:-managed}"' in reversal_text
    assert 'CODEX_OFFICIAL_PRIORITY="${CODEX_OFFICIAL_PRIORITY:-disabled}"' in direction_text
    assert 'CODEX_OFFICIAL_PRIORITY="${CODEX_OFFICIAL_PRIORITY:-disabled}"' in reversal_text
    assert 'MAX_LIVE_RUNS="${MAX_LIVE_RUNS:-5}"' in direction_text
    assert 'MAX_LIVE_RUNS="${MAX_LIVE_RUNS:-5}"' in reversal_text
    assert 'DEFAULT_TRACK_SLOT_CAPS_JSON=\'{"direction_dense":5,"reversal_dense":5}\'' in direction_text
    assert 'DEFAULT_TRACK_SLOT_CAPS_JSON=\'{"direction_dense":5,"reversal_dense":5}\'' in reversal_text
    assert 'TRACK_SLOT_CAPS_JSON="${TRACK_SLOT_CAPS_JSON:-$DEFAULT_TRACK_SLOT_CAPS_JSON}"' in direction_text
    assert 'TRACK_SLOT_CAPS_JSON="${TRACK_SLOT_CAPS_JSON:-$DEFAULT_TRACK_SLOT_CAPS_JSON}"' in reversal_text
    assert 'PM15MIN_FIXED_TRACK_SLOT_CAPS_JSON="${PM15MIN_FIXED_TRACK_SLOT_CAPS_JSON:-$TRACK_SLOT_CAPS_JSON}"' in direction_text
    assert 'PM15MIN_FIXED_TRACK_SLOT_CAPS_JSON="${PM15MIN_FIXED_TRACK_SLOT_CAPS_JSON:-$TRACK_SLOT_CAPS_JSON}"' in reversal_text
    assert 'PM15MIN_MANAGED_EXPERIMENT_TRACK="${PM15MIN_MANAGED_EXPERIMENT_TRACK:-direction_dense}"' in direction_text
    assert 'PM15MIN_MANAGED_EXPERIMENT_TRACK="${PM15MIN_MANAGED_EXPERIMENT_TRACK:-reversal_dense}"' in reversal_text
    assert 'LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-900}"' in direction_text
    assert 'LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-900}"' in reversal_text
    assert 'CODEX_MODEL="${CODEX_MODEL:-gpt-5.5}"' in direction_text
    assert 'CODEX_MODEL="${CODEX_MODEL:-gpt-5.5}"' in reversal_text
    assert 'CODEX_REASONING_EFFORT="${CODEX_REASONING_EFFORT:-xhigh}"' in direction_text
    assert 'CODEX_REASONING_EFFORT="${CODEX_REASONING_EFFORT:-xhigh}"' in reversal_text
    assert 'CODEX_ATTEMPT_TIMEOUT_SEC="${CODEX_ATTEMPT_TIMEOUT_SEC:-600}"' in direction_text
    assert 'CODEX_ATTEMPT_TIMEOUT_SEC="${CODEX_ATTEMPT_TIMEOUT_SEC:-600}"' in reversal_text
    assert 'MAX_CONSECUTIVE_FAILURES="${MAX_CONSECUTIVE_FAILURES:-12}"' in direction_text
    assert 'MAX_CONSECUTIVE_FAILURES="${MAX_CONSECUTIVE_FAILURES:-12}"' in reversal_text


def test_dense_xrp_programs_bind_xrp_specific_sessions() -> None:
    direction_text = Path("auto_research/program_direction_dense_xrp.md").read_text(encoding="utf-8")
    reversal_text = Path("auto_research/program_reversal_dense_xrp.md").read_text(encoding="utf-8")

    assert "deep_otm_baseline_direction_dense_xrp_autoresearch/session.md" in direction_text
    assert "deep_otm_baseline_direction_dense_xrp_autoresearch/results.tsv" in direction_text
    assert "deep_otm_baseline_reversal_dense_xrp_autoresearch/session.md" in reversal_text
    assert "deep_otm_baseline_reversal_dense_xrp_autoresearch/results.tsv" in reversal_text
    assert "- coins: `xrp`" in direction_text
    assert "- coins: `xrp`" in reversal_text


def test_dense_xrp_start_wrappers_bind_xrp_programs_and_sessions() -> None:
    direction_text = Path("auto_research/start_direction_dense_xrp.sh").read_text(encoding="utf-8")
    reversal_text = Path("auto_research/start_reversal_dense_xrp.sh").read_text(encoding="utf-8")

    assert 'PROGRAM_PATH="${PROGRAM_PATH:-$ROOT_DIR/auto_research/program_direction_dense_xrp.md}"' in direction_text
    assert 'SESSION_DIR="${SESSION_DIR:-$ROOT_DIR/sessions/deep_otm_baseline_direction_dense_xrp_autoresearch}"' in direction_text
    assert 'PM15MIN_ALLOWED_QUEUE_MARKETS="${PM15MIN_ALLOWED_QUEUE_MARKETS:-xrp}"' in direction_text
    assert 'exec "$ROOT_DIR/auto_research/start_direction_dense.sh" "$@"' in direction_text
    assert 'PROGRAM_PATH="${PROGRAM_PATH:-$ROOT_DIR/auto_research/program_reversal_dense_xrp.md}"' in reversal_text
    assert 'SESSION_DIR="${SESSION_DIR:-$ROOT_DIR/sessions/deep_otm_baseline_reversal_dense_xrp_autoresearch}"' in reversal_text
    assert 'PM15MIN_ALLOWED_QUEUE_MARKETS="${PM15MIN_ALLOWED_QUEUE_MARKETS:-xrp}"' in reversal_text
    assert 'exec "$ROOT_DIR/auto_research/start_reversal_dense.sh" "$@"' in reversal_text


def test_dense_sol_xrp_programs_bind_sol_xrp_specific_sessions() -> None:
    direction_text = Path("auto_research/program_direction_dense_sol_xrp.md").read_text(encoding="utf-8")
    reversal_text = Path("auto_research/program_reversal_dense_sol_xrp.md").read_text(encoding="utf-8")

    assert "deep_otm_baseline_direction_dense_sol_xrp_autoresearch/session.md" in direction_text
    assert "deep_otm_baseline_direction_dense_sol_xrp_autoresearch/results.tsv" in direction_text
    assert "deep_otm_baseline_reversal_dense_sol_xrp_autoresearch/session.md" in reversal_text
    assert "deep_otm_baseline_reversal_dense_sol_xrp_autoresearch/results.tsv" in reversal_text
    assert "- coins: `sol`, `xrp`" in direction_text
    assert "- coins: `sol`, `xrp`" in reversal_text
    for text in (direction_text, reversal_text):
        assert "Required Funnel Diagnosis" in text
        assert "density_bottleneck" in text
        assert "probability_gate" in text
        assert "entry_price_gate" in text
        assert "Forced Stagnation Escalation" in text
        assert "same-width, same-model, same-family retry" in text
        assert "SOL and XRP must be routed independently" in text
        assert "Strategy Lock" in text
        assert "do not change the strategy gates to create more trades" in text
        assert "failure-to-factor thesis" in text
        assert "probability-band experiment" not in text
        assert "bounded entry-band release" not in text
        assert "threshold/weight release" not in text


def test_dense_sol_xrp_start_wrappers_bind_sol_xrp_programs_and_sessions() -> None:
    direction_text = Path("auto_research/start_direction_dense_sol_xrp.sh").read_text(encoding="utf-8")
    reversal_text = Path("auto_research/start_reversal_dense_sol_xrp.sh").read_text(encoding="utf-8")

    assert 'PROGRAM_PATH="${PROGRAM_PATH:-$ROOT_DIR/auto_research/program_direction_dense_sol_xrp.md}"' in direction_text
    assert 'SESSION_DIR="${SESSION_DIR:-$ROOT_DIR/sessions/deep_otm_baseline_direction_dense_sol_xrp_autoresearch}"' in direction_text
    assert 'PM15MIN_ALLOWED_QUEUE_MARKETS="${PM15MIN_ALLOWED_QUEUE_MARKETS:-sol,xrp}"' in direction_text
    assert 'PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS="${PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS:-1}"' in direction_text
    assert 'PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY="${PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY:-5}"' in direction_text
    assert 'CODEX_ATTEMPT_TIMEOUT_SEC="${CODEX_ATTEMPT_TIMEOUT_SEC:-1800}"' in direction_text
    assert 'CODEX_HEAVY_ANALYSIS_TIMEOUT_SEC="${CODEX_HEAVY_ANALYSIS_TIMEOUT_SEC:-2700}"' in direction_text
    assert 'exec "$ROOT_DIR/auto_research/start_direction_dense.sh" "$@"' in direction_text
    assert 'PROGRAM_PATH="${PROGRAM_PATH:-$ROOT_DIR/auto_research/program_reversal_dense_sol_xrp.md}"' in reversal_text
    assert 'SESSION_DIR="${SESSION_DIR:-$ROOT_DIR/sessions/deep_otm_baseline_reversal_dense_sol_xrp_autoresearch}"' in reversal_text
    assert 'PM15MIN_ALLOWED_QUEUE_MARKETS="${PM15MIN_ALLOWED_QUEUE_MARKETS:-sol,xrp}"' in reversal_text
    assert 'PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS="${PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS:-1}"' in reversal_text
    assert 'PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY="${PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY:-5}"' in reversal_text
    assert 'CODEX_ATTEMPT_TIMEOUT_SEC="${CODEX_ATTEMPT_TIMEOUT_SEC:-1800}"' in reversal_text
    assert 'CODEX_HEAVY_ANALYSIS_TIMEOUT_SEC="${CODEX_HEAVY_ANALYSIS_TIMEOUT_SEC:-2700}"' in reversal_text
    assert 'exec "$ROOT_DIR/auto_research/start_reversal_dense.sh" "$@"' in reversal_text


def test_midprice_direction_programs_bind_btc_eth_independent_sessions() -> None:
    btc_text = Path("auto_research/program_direction_midprice_btc.md").read_text(encoding="utf-8")
    eth_text = Path("auto_research/program_direction_midprice_eth.md").read_text(encoding="utf-8")

    assert "deep_otm_midprice_direction_btc_autoresearch/session.md" in btc_text
    assert "deep_otm_midprice_direction_btc_autoresearch/results.tsv" in btc_text
    assert "deep_otm_midprice_direction_eth_autoresearch/session.md" in eth_text
    assert "deep_otm_midprice_direction_eth_autoresearch/results.tsv" in eth_text
    assert "- coin: `btc`" in btc_text
    assert "- coin: `eth`" in eth_text
    assert "full formal" in btc_text
    assert "full formal" in eth_text
    assert "do not use the shared SOL/XRP quick-screen queue" in btc_text
    assert "do not use the shared SOL/XRP quick-screen queue" in eth_text
    assert "compare the direction signal against the `0.50` midpoint" in btc_text
    assert "compare the direction signal against the `0.50` midpoint" in eth_text
    assert "`0.45-0.50`" in btc_text
    assert "`0.45-0.50`" in eth_text
    assert "`0.60`" in btc_text
    assert "`0.60`" in eth_text
    assert "Allowed Research Levers" in btc_text
    assert "Allowed Research Levers" in eth_text
    assert "change the feature-count bucket" in btc_text
    assert "change the feature-count bucket" in eth_text
    assert "change the model family or ensemble recipe" in btc_text
    assert "change the model family or ensemble recipe" in eth_text
    assert "only one primary lever per follow-up" in btc_text
    assert "only one primary lever per follow-up" in eth_text
    assert "baseline_midprice_direction_btc_2usd_5max_train0415_backtest0507_20260501" in btc_text
    assert "baseline_midprice_direction_eth_2usd_5max_train0415_backtest0507_20260501" in eth_text
    assert "filled-trades-per-offset formal judge fixed" in btc_text
    assert "filled-trades-per-offset formal judge fixed" in eth_text
    assert "20max" not in btc_text
    assert "20max" not in eth_text
    for text in (btc_text, eth_text):
        assert "decision / backtest window is `2026-04-15` through `2026-05-07`" in text
        assert "decision / backtest window is `2026-04-01` through `2026-05-07`" not in text
        assert "every completed sparse result must be classified by the dominant blocker" in text
        assert "10 trades is still sparse" in text
        assert "do not review ROI as a promotion signal while trades remain below `56`" in text
        assert "after `3` consecutive sparse completions below `56` trades" in text
        assert "same-width, same-model factor shuffle" in text
        assert "Strategy Lock" in text
        assert "do not change the midpoint entry policy" in text
        assert "do not change the `0.60` probability threshold" in text
        assert "do not change the `0.45-0.50` accepted entry band" in text
        assert "failure-to-factor thesis" in text


def test_build_codex_cycle_prompt_locks_strategy_and_requires_failure_to_factor_thesis(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_baseline_direction_dense_sol_xrp_autoresearch"
    session_dir.mkdir(parents=True)
    program_dir = root / "auto_research"
    program_dir.mkdir()
    program_path = program_dir / "program_direction_dense_sol_xrp.md"
    program_path.write_text(
        "\n".join(
            [
                "# Direction SOL/XRP",
                "- coins: `sol`, `xrp`",
                "- target fixed to `direction`",
                "- run SOL/XRP dense work through the shared queue in `quick_screen` mode only",
                "- profitable offset pool window: `2026-04-15` through `2026-05-07`, `2usd`",
            ]
        ),
        encoding="utf-8",
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)
    lower_prompt = prompt.lower()

    assert "strategy lock for this cycle" in lower_prompt
    assert "do not change the strategy gates to create more trades" in lower_prompt
    assert "treat probability_gate and entry_price_gate as diagnostics" in lower_prompt
    assert "failure-to-factor thesis" in lower_prompt
    assert "apply this to btc, eth, sol, and xrp sessions" in lower_prompt
    assert "do not alter execution thresholds, entry bands, stake sizing, max-trades caps" in lower_prompt


def test_train0415_backtest0507_suite_specs_use_post_train_backtest_window_and_fixed_5max_default() -> None:
    spec_dir = Path("research/experiments/suite_specs")
    for market in ("btc", "eth"):
        spec_path = spec_dir / f"baseline_midprice_direction_{market}_2usd_5max_train0415_backtest0507_20260501.json"
        payload = json.loads(spec_path.read_text(encoding="utf-8"))
        assert payload["suite_name"] == f"baseline_midprice_direction_{market}_2usd_5max_train0415_backtest0507_20260501"
        assert payload["window"]["end"] == "2026-04-15"
        assert payload["decision_start"] == "2026-04-15"
        assert payload["decision_end"] == "2026-05-07"
        assert payload["max_trades_per_market_values"] == [5]
        assert "max5" in payload["tags"]
        assert "max20" not in payload["tags"]


def test_midprice_direction_start_wrappers_disable_shared_queue_and_bind_sessions() -> None:
    btc_text = Path("auto_research/start_direction_midprice_btc.sh").read_text(encoding="utf-8")
    eth_text = Path("auto_research/start_direction_midprice_eth.sh").read_text(encoding="utf-8")

    assert 'PROGRAM_PATH="${PROGRAM_PATH:-$ROOT_DIR/auto_research/program_direction_midprice_btc.md}"' in btc_text
    assert 'SESSION_DIR="${SESSION_DIR:-$ROOT_DIR/sessions/deep_otm_midprice_direction_btc_autoresearch}"' in btc_text
    assert 'AUTORUN_DIR="${AUTORUN_DIR:-$ROOT_DIR/var/research/autorun/midprice_direction_btc}"' in btc_text
    assert 'START_QUEUE_SUPERVISOR="${START_QUEUE_SUPERVISOR:-0}"' in btc_text
    assert 'PM15MIN_ALLOWED_QUEUE_MARKETS="${PM15MIN_ALLOWED_QUEUE_MARKETS:-btc}"' in btc_text
    assert 'MAX_LIVE_RUNS="${MAX_LIVE_RUNS:-2}"' in btc_text
    assert 'PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY="${PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY:-2}"' in btc_text
    assert 'PM15MIN_MANAGED_EXPERIMENT_TRACK="${PM15MIN_MANAGED_EXPERIMENT_TRACK:-direction_dense}"' in btc_text
    assert 'LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-1800}"' in btc_text
    assert 'CODEX_MODEL="${CODEX_MODEL:-gpt-5.5}"' in btc_text
    assert 'CODEX_REASONING_EFFORT="${CODEX_REASONING_EFFORT:-xhigh}"' in btc_text
    assert 'CODEX_OFFICIAL_PRIORITY="${CODEX_OFFICIAL_PRIORITY:-disabled}"' in btc_text
    assert 'FACTOR_SCOUT_STAMP_PATH="${FACTOR_SCOUT_STAMP_PATH:-$AUTORUN_DIR/factor-scout.last-success}"' in btc_text
    assert 'exec "$ROOT_DIR/auto_research/codex_background_loop.sh" "$@"' in btc_text

    assert 'PROGRAM_PATH="${PROGRAM_PATH:-$ROOT_DIR/auto_research/program_direction_midprice_eth.md}"' in eth_text
    assert 'SESSION_DIR="${SESSION_DIR:-$ROOT_DIR/sessions/deep_otm_midprice_direction_eth_autoresearch}"' in eth_text
    assert 'AUTORUN_DIR="${AUTORUN_DIR:-$ROOT_DIR/var/research/autorun/midprice_direction_eth}"' in eth_text
    assert 'START_QUEUE_SUPERVISOR="${START_QUEUE_SUPERVISOR:-0}"' in eth_text
    assert 'PM15MIN_ALLOWED_QUEUE_MARKETS="${PM15MIN_ALLOWED_QUEUE_MARKETS:-eth}"' in eth_text
    assert 'MAX_LIVE_RUNS="${MAX_LIVE_RUNS:-2}"' in eth_text
    assert 'PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY="${PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY:-2}"' in eth_text
    assert 'PM15MIN_MANAGED_EXPERIMENT_TRACK="${PM15MIN_MANAGED_EXPERIMENT_TRACK:-direction_dense}"' in eth_text
    assert 'LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-1800}"' in eth_text
    assert 'CODEX_MODEL="${CODEX_MODEL:-gpt-5.5}"' in eth_text
    assert 'CODEX_REASONING_EFFORT="${CODEX_REASONING_EFFORT:-xhigh}"' in eth_text
    assert 'CODEX_OFFICIAL_PRIORITY="${CODEX_OFFICIAL_PRIORITY:-disabled}"' in eth_text
    assert 'FACTOR_SCOUT_STAMP_PATH="${FACTOR_SCOUT_STAMP_PATH:-$AUTORUN_DIR/factor-scout.last-success}"' in eth_text
    assert 'exec "$ROOT_DIR/auto_research/codex_background_loop.sh" "$@"' in eth_text


def test_midprice_direction_stack_starts_btc_and_eth_autoresearch() -> None:
    script_text = Path("auto_research/start_midprice_direction_stack.sh").read_text(encoding="utf-8")

    assert 'BTC_SCRIPT="$ROOT_DIR/auto_research/start_direction_midprice_btc.sh"' in script_text
    assert 'ETH_SCRIPT="$ROOT_DIR/auto_research/start_direction_midprice_eth.sh"' in script_text
    assert '"$BTC_SCRIPT" start' in script_text
    assert '"$ETH_SCRIPT" start' in script_text
    assert '"$BTC_SCRIPT" stop' in script_text
    assert '"$ETH_SCRIPT" stop' in script_text


def test_ht66_sync_excludes_runtime_custom_feature_registry() -> None:
    script_text = Path("scripts/maintenance/compare_hashes_ht66.sh").read_text(encoding="utf-8")
    include_block = script_text.split("include_files = {", maxsplit=1)[1].split("}", maxsplit=1)[0]
    exclude_block = script_text.split("exclude_files = {", maxsplit=1)[1].split("}", maxsplit=1)[0]

    assert "research/experiments/custom_feature_sets.json" not in include_block
    assert '"research/experiments/custom_feature_sets.json"' in exclude_block


def test_codex_background_loop_can_skip_queue_supervisor_for_standalone_lines() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert 'START_QUEUE_SUPERVISOR="${START_QUEUE_SUPERVISOR:-1}"' in script_text
    assert 'if [[ "$START_QUEUE_SUPERVISOR" == "1" && -x "$QUEUE_SUPERVISOR_SCRIPT" ]]; then' in script_text


def test_codex_background_loop_initializes_missing_session_files() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert "ensure_session_files()" in script_text
    assert 'mkdir -p "$SESSION_DIR"' in script_text
    assert '"cycle\tteam\tmetric\tstatus\tdescription\tfiles_changed\ttimestamp"' in script_text
    assert "ensure_session_files" in script_text.split('SESSION_DIR="$(resolve_session_dir)"', maxsplit=1)[1]


def test_prune_incomplete_runs_to_current_session_prefers_current_session_bootstrap_labels(tmp_path: Path) -> None:
    session_dir = tmp_path / "sessions" / "dense_xrp"
    bootstrap_dir = session_dir / "bootstrap"
    bootstrap_dir.mkdir(parents=True, exist_ok=True)
    (bootstrap_dir / "auto_xrp_current.log").write_text("ok", encoding="utf-8")

    pruned = control_plane._prune_incomplete_runs_to_current_session(
        [
            {"run_label": "auto_xrp_current", "suite_name": "suite_current"},
            {"run_label": "auto_xrp_old", "suite_name": "suite_old"},
        ],
        session_dir=session_dir,
        queue_items=[],
        formal_workers=[],
    )

    assert [item["run_label"] for item in pruned] == ["auto_xrp_current"]


def test_prune_incomplete_runs_to_current_session_uses_queue_and_live_worker_labels_when_bootstrap_empty(
    tmp_path: Path,
) -> None:
    session_dir = tmp_path / "sessions" / "dense_xrp"
    session_dir.mkdir(parents=True, exist_ok=True)

    pruned = control_plane._prune_incomplete_runs_to_current_session(
        [
            {"run_label": "auto_xrp_queue", "suite_name": "suite_queue"},
            {"run_label": "auto_xrp_live", "suite_name": "suite_live"},
            {"run_label": "auto_xrp_old", "suite_name": "suite_old"},
        ],
        session_dir=session_dir,
        queue_items=[{"run_label": "auto_xrp_queue"}],
        formal_workers=[{"run_label": "auto_xrp_live"}],
    )

    assert [item["run_label"] for item in pruned] == ["auto_xrp_queue", "auto_xrp_live"]


def test_codex_background_loop_preserves_attempt_timeout_marker_in_attempt_log() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert 'exec "${BUILT_ENV_PREFIX[@]}" "${CODEX_CMD[@]}" < "$LAST_PROMPT_PATH" >> "$output_log" 2>&1' in script_text
    assert 'setsid "${BUILT_ENV_PREFIX[@]}" "${CODEX_CMD[@]}" < "$LAST_PROMPT_PATH" >> "$output_log" 2>&1 &' in script_text


def test_run_one_experiment_supports_quick_screen_launch_mode() -> None:
    script_text = Path("auto_research/run_one_experiment.sh").read_text(encoding="utf-8")

    assert '--launch-mode)' in script_text
    assert 'PM15MIN_EXPERIMENT_LAUNCH_MODE="${PM15MIN_EXPERIMENT_LAUNCH_MODE:-formal}"' in script_text
    assert 'PM15MIN_QUICK_SCREEN_TOP_K="${PM15MIN_QUICK_SCREEN_TOP_K:-1}"' in script_text
    assert 'PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS="${PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS:-3}"' in script_text
    assert 'PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY="${PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY:-16}"' in script_text
    assert 'PM15MIN_EXPERIMENT_CPU_THREADS="${PM15MIN_EXPERIMENT_CPU_THREADS:-}"' in script_text
    assert 'OMP_NUM_THREADS="${OMP_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"' in script_text
    assert 'OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"' in script_text
    assert 'MKL_NUM_THREADS="${MKL_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"' in script_text
    assert 'NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"' in script_text
    assert 'MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-2}"' in script_text
    assert 'PYTHONMALLOC="${PYTHONMALLOC:-malloc}"' in script_text
    assert "run_quick_screen_suite.py" in script_text
    assert 'case "$LAUNCH_MODE" in' in script_text
    assert 'quick_screen)' in script_text


def test_run_one_experiment_waits_for_reserved_system_memory() -> None:
    script_text = Path("auto_research/run_one_experiment.sh").read_text(encoding="utf-8")

    assert 'PM15MIN_MEMORY_GUARD_ENABLE="${PM15MIN_MEMORY_GUARD_ENABLE:-1}"' in script_text
    assert 'PM15MIN_MIN_AVAILABLE_MEM_GB="${PM15MIN_MIN_AVAILABLE_MEM_GB:-1}"' in script_text
    assert "wait_for_memory_guard()" in script_text
    assert "MemAvailable:" in script_text
    assert "[run_one_experiment] memory_guard waiting" in script_text
    assert "wait_for_memory_guard" in script_text.split('echo "[run_one_experiment] quick_screen_train_parallel_workers=', maxsplit=1)[1]


def test_run_one_experiment_defaults_to_memory_lean_backtest_caches() -> None:
    script_text = Path("auto_research/run_one_experiment.sh").read_text(encoding="utf-8")

    assert 'PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES="${PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES:-0}"' in script_text
    assert 'PM15MIN_BACKTEST_SURFACE_RUNTIME_CACHE_MAX_ENTRIES="${PM15MIN_BACKTEST_SURFACE_RUNTIME_CACHE_MAX_ENTRIES:-0}"' in script_text


def test_quick_screen_batch_defaults_to_memory_lean_surface_cache() -> None:
    script_text = Path("auto_research/run_quick_screen_queue_batch.sh").read_text(encoding="utf-8")

    assert 'PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES="${PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES:-0}"' in script_text
    assert 'PM15MIN_BACKTEST_SURFACE_RUNTIME_CACHE_MAX_ENTRIES="${PM15MIN_BACKTEST_SURFACE_RUNTIME_CACHE_MAX_ENTRIES:-0}"' in script_text


def test_formal_run_completion_can_wake_matching_autoresearch_loop() -> None:
    run_text = Path("auto_research/run_one_experiment.sh").read_text(encoding="utf-8")
    loop_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")
    btc_text = Path("auto_research/start_direction_midprice_btc.sh").read_text(encoding="utf-8")
    eth_text = Path("auto_research/start_direction_midprice_eth.sh").read_text(encoding="utf-8")

    assert 'export WAKE_FLAG="${WAKE_FLAG:-$AUTORUN_DIR/wake.flag}"' in btc_text
    assert 'export WAKE_ON_IDLE_MARKET="${WAKE_ON_IDLE_MARKET:-btc}"' in btc_text
    assert 'export PM15MIN_AUTORESEARCH_WAKE_FLAG="${PM15MIN_AUTORESEARCH_WAKE_FLAG:-$WAKE_FLAG}"' in btc_text
    assert 'export WAKE_FLAG="${WAKE_FLAG:-$AUTORUN_DIR/wake.flag}"' in eth_text
    assert 'export WAKE_ON_IDLE_MARKET="${WAKE_ON_IDLE_MARKET:-eth}"' in eth_text
    assert 'export PM15MIN_AUTORESEARCH_WAKE_FLAG="${PM15MIN_AUTORESEARCH_WAKE_FLAG:-$WAKE_FLAG}"' in eth_text

    assert 'WAKE_FLAG="${WAKE_FLAG:-$AUTORUN_DIR/wake.flag}"' in loop_text
    assert 'WAKE_POLL_SLEEP_SEC="${WAKE_POLL_SLEEP_SEC:-5}"' in loop_text
    assert 'WAKE_ON_IDLE_MARKET="${WAKE_ON_IDLE_MARKET:-}"' in loop_text
    assert "sleep_until_next_cycle()" in loop_text
    assert "should_wake_for_idle_market()" in loop_text
    assert 'if [[ -f "$WAKE_FLAG" ]]; then' in loop_text
    assert 'rm -f "$WAKE_FLAG"' in loop_text
    assert 'if should_wake_for_idle_market; then' in loop_text
    assert "count_live_market_workers()" in loop_text
    assert 'local target="${MAX_LIVE_RUNS:-1}"' in loop_text
    assert 'local live_count' in loop_text
    assert '[[ "$live_count" -lt "$target" ]]' in loop_text
    assert "count_live_market_worker_groups()" in loop_text
    assert 'run_one_experiment.sh' in loop_text
    assert 'pm15min research experiment run-suite' in loop_text
    assert "run_labels.add(label)" in loop_text
    assert 'print(len(run_labels) + len(wrapper_pids_without_run_label))' in loop_text
    assert 'sleep_until_next_cycle' in loop_text.split("if ! run_once", maxsplit=1)[1]
    assert 'sleep "$LOOP_SLEEP_SEC"' not in loop_text

    assert "notify_autoresearch_wake()" in run_text
    assert '[[ "$LAUNCH_MODE" == "formal" || "$LAUNCH_MODE" == "quick_screen" ]]' in run_text
    assert '[[ -n "${PM15MIN_AUTORESEARCH_WAKE_FLAG:-}" ]]' in run_text
    assert 'touch "$PM15MIN_AUTORESEARCH_WAKE_FLAG"' in run_text


def test_dense_quick_screen_completion_can_wake_matching_autoresearch_loops() -> None:
    run_text = Path("auto_research/run_one_experiment.sh").read_text(encoding="utf-8")
    queue_text = Path("auto_research/experiment_queue.py").read_text(encoding="utf-8")
    direction_text = Path("auto_research/start_direction_dense_sol_xrp.sh").read_text(encoding="utf-8")
    reversal_text = Path("auto_research/start_reversal_dense_sol_xrp.sh").read_text(encoding="utf-8")

    assert 'export WAKE_FLAG="${WAKE_FLAG:-$AUTORUN_DIR/wake.flag}"' in direction_text
    assert 'export PM15MIN_AUTORESEARCH_WAKE_FLAG="${PM15MIN_AUTORESEARCH_WAKE_FLAG:-$WAKE_FLAG}"' in direction_text
    assert 'export WAKE_FLAG="${WAKE_FLAG:-$AUTORUN_DIR/wake.flag}"' in reversal_text
    assert 'export PM15MIN_AUTORESEARCH_WAKE_FLAG="${PM15MIN_AUTORESEARCH_WAKE_FLAG:-$WAKE_FLAG}"' in reversal_text
    assert '[[ "$LAUNCH_MODE" == "formal" || "$LAUNCH_MODE" == "quick_screen" ]]' in run_text
    assert "def _queue_wake_flag_for_item(" in queue_text
    assert '"direction_dense": "direction_dense_sol_xrp"' in queue_text
    assert '"reversal_dense": "reversal_dense_sol_xrp"' in queue_text
    assert 'env["PM15MIN_AUTORESEARCH_WAKE_FLAG"] = str(wake_flag)' in queue_text


def test_experiment_queue_supervisor_defaults_to_quick_screen_launch_mode() -> None:
    script_text = Path("auto_research/experiment_queue_supervisor.sh").read_text(encoding="utf-8")

    assert 'QUEUE_SUPERVISOR_LOOP_SLEEP_SEC="${QUEUE_SUPERVISOR_LOOP_SLEEP_SEC:-5}"' in script_text
    assert 'QUEUE_SUPERVISOR_PREWARM_ENABLE="${QUEUE_SUPERVISOR_PREWARM_ENABLE:-0}"' in script_text
    assert 'LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-5}"' not in script_text
    assert 'sleep "$QUEUE_SUPERVISOR_LOOP_SLEEP_SEC"' in script_text
    assert 'MAX_LIVE_RUNS="${MAX_LIVE_RUNS:-10}"' in script_text
    assert 'MAX_QUEUED_ITEMS="${MAX_QUEUED_ITEMS:-24}"' in script_text
    assert 'DEFAULT_TRACK_SLOT_CAPS_JSON=\'{"direction_dense":5,"reversal_dense":5}\'' in script_text
    assert 'TRACK_SLOT_CAPS_JSON="${TRACK_SLOT_CAPS_JSON:-$DEFAULT_TRACK_SLOT_CAPS_JSON}"' in script_text
    assert 'PM15MIN_FIXED_TRACK_SLOT_CAPS_JSON="${PM15MIN_FIXED_TRACK_SLOT_CAPS_JSON:-$TRACK_SLOT_CAPS_JSON}"' in script_text
    assert 'MIN_AVAILABLE_MEM_GB="${MIN_AVAILABLE_MEM_GB:-1}"' in script_text
    assert 'PM15MIN_QUEUE_QUICK_SCREEN_WORKER_MEM_GB="${PM15MIN_QUEUE_QUICK_SCREEN_WORKER_MEM_GB:-16}"' in script_text
    assert 'PM15MIN_ALLOWED_QUEUE_MARKETS="${PM15MIN_ALLOWED_QUEUE_MARKETS:-${MARKETS:-sol,xrp}}"' in script_text
    assert 'PREWARM_SCRIPT="$ROOT_DIR/auto_research/prewarm_profitable_offset_pools.sh"' in script_text
    assert 'if [[ "$QUEUE_SUPERVISOR_PREWARM_ENABLE" == "1" ]]; then' in script_text
    assert '"$PREWARM_SCRIPT" ensure' in script_text
    assert 'PM15MIN_EXPERIMENT_LAUNCH_MODE="quick_screen"' in script_text
    assert 'PM15MIN_QUICK_SCREEN_TOP_K="${PM15MIN_QUICK_SCREEN_TOP_K:-1}"' in script_text
    assert 'PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS="${PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS:-1}"' in script_text
    assert 'PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY="${PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY:-5}"' in script_text
    assert 'PM15MIN_QUEUE_QUICK_SCREEN_BATCH_SIZE="${PM15MIN_QUEUE_QUICK_SCREEN_BATCH_SIZE:-10}"' in script_text
    assert 'MAX_LAUNCHES_PER_PASS="${MAX_LAUNCHES_PER_PASS:-10}"' in script_text
    assert '--max-queued-items "$MAX_QUEUED_ITEMS"' in script_text
    assert '--max-launches-per-pass "$MAX_LAUNCHES_PER_PASS"' in script_text
    assert '--min-available-mem-gb "$MIN_AVAILABLE_MEM_GB"' in script_text
    assert '--quick-screen-worker-mem-gb "$PM15MIN_QUEUE_QUICK_SCREEN_WORKER_MEM_GB"' in script_text
    assert 'export PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY' in script_text
    assert 'export PM15MIN_ALLOWED_QUEUE_MARKETS' in script_text
    assert 'export QUEUE_SUPERVISOR_PREWARM_ENABLE' in script_text
    assert 'if ! run_once >> "$LOG_PATH" 2>&1; then' in script_text
    assert 'RUN_LOCK_PATH="$STATE_DIR/experiment-queue-supervisor.run.lock"' in script_text
    assert 'flock -n 9' in script_text
    assert 'queue supervisor pass skipped; another pass is running' in script_text
    assert 'queue supervisor pass failed' in script_text


def test_experiment_queue_cli_defaults_match_dense_ten_slot_capacity() -> None:
    script_text = Path("auto_research/experiment_queue.py").read_text(encoding="utf-8")

    assert 'supervise.add_argument("--max-live-runs", type=int, default=10)' in script_text
    assert 'default=\'{"direction_dense": 5, "reversal_dense": 5}\'' in script_text
    assert 'max(1, int(os.environ.get("MAX_LIVE_RUNS") or 10))' in script_text


def test_run_one_experiment_background_passes_explicit_quick_screen_controls() -> None:
    script_text = Path("auto_research/run_one_experiment_background.sh").read_text(encoding="utf-8")

    assert '--launch-mode)' in script_text
    assert '--quick-screen-top-k)' in script_text
    assert '--quick-screen-train-parallel-workers)' in script_text
    assert '--expected-concurrency)' in script_text
    assert 'cmd+=(--launch-mode "$LAUNCH_MODE")' in script_text
    assert 'cmd+=(--quick-screen-top-k "$QUICK_SCREEN_TOP_K")' in script_text
    assert 'cmd+=(--quick-screen-train-parallel-workers "$QUICK_SCREEN_TRAIN_PARALLEL_WORKERS")' in script_text
    assert 'cmd+=(--expected-concurrency "$EXPECTED_CONCURRENCY")' in script_text


def test_queue_batch_runner_and_supervisor_are_wired_for_dense_quick_screen_batches() -> None:
    supervisor_text = Path("auto_research/experiment_queue_supervisor.sh").read_text(encoding="utf-8")
    batch_wrapper_text = Path("auto_research/run_quick_screen_queue_batch.sh").read_text(encoding="utf-8")
    queue_text = Path("auto_research/experiment_queue.py").read_text(encoding="utf-8")
    runner_text = Path("scripts/research/run_quick_screen_queue_batch.py").read_text(encoding="utf-8")
    dense_stack_text = Path("auto_research/start_dense_stack.sh").read_text(encoding="utf-8")

    assert 'PM15MIN_QUEUE_QUICK_SCREEN_BATCH_SIZE="${PM15MIN_QUEUE_QUICK_SCREEN_BATCH_SIZE:-10}"' in supervisor_text
    assert "run_quick_screen_pool.py" in queue_text
    assert 'PM15MIN_QUICK_SCREEN_USE_POOL="${PM15MIN_QUICK_SCREEN_USE_POOL:-1}"' in supervisor_text
    assert 'PM15MIN_QUEUE_QUICK_SCREEN_WORKER_MEM_GB="${PM15MIN_QUEUE_QUICK_SCREEN_WORKER_MEM_GB:-16}"' in supervisor_text
    assert 'PM15MIN_MIN_AVAILABLE_MEM_GB="${PM15MIN_MIN_AVAILABLE_MEM_GB:-1}"' in batch_wrapper_text
    assert "--quick-screen-batch-size" in supervisor_text
    assert "--quick-screen-worker-mem-gb" in supervisor_text
    assert "launch_ready_queue_item_batches" in queue_text
    assert "run_quick_screen_queue_batch.py" in queue_text
    assert "run_quick_screen_suite" in runner_text
    assert "set-status" in runner_text
    assert "Unsupported feature_set" in runner_text
    assert 'pkill -f "$ROOT_DIR/scripts/research/run_quick_screen_queue_batch.py"' in dense_stack_text


def test_run_one_experiment_background_detaches_worker_process_group() -> None:
    script_text = Path("auto_research/run_one_experiment_background.sh").read_text(encoding="utf-8")

    assert 'if command -v setsid >/dev/null 2>&1; then' in script_text
    assert 'nohup setsid "${cmd[@]}" >"$STDOUT_PATH" 2>&1 &' in script_text
    assert 'nohup "${cmd[@]}" >"$STDOUT_PATH" 2>&1 &' in script_text


def test_experiment_queue_launcher_has_bounded_subprocess_timeout() -> None:
    script_text = Path("auto_research/experiment_queue.py").read_text(encoding="utf-8")

    assert "PM15MIN_QUEUE_LAUNCH_TIMEOUT_SEC" in script_text
    assert "timeout=launch_timeout_sec" in script_text
    assert 'pid_path = Path(artifact_paths["pid_path"])' in script_text
    assert "queue launch timed out" in script_text


def test_prewarm_profitable_offset_pool_script_targets_four_dense_markets() -> None:
    script_text = Path("auto_research/prewarm_profitable_offset_pools.sh").read_text(encoding="utf-8")
    cli_text = Path("scripts/research/prewarm_profitable_offset_pools.py").read_text(encoding="utf-8")

    assert 'MARKETS="${MARKETS:-btc,eth,sol,xrp}"' in script_text
    assert 'DECISION_START="${DECISION_START:-2026-04-15}"' in script_text
    assert 'PREWARM_STATUS_PATH="$STATE_DIR/profitable-offset-pool-prewarm.status.json"' in script_text
    assert 'PREWARM_STOP_FLAG="$STATE_DIR/profitable-offset-pool-prewarm.stop.flag"' in script_text
    assert 'scripts/research/prewarm_profitable_offset_pools.py' in script_text
    assert 'parser.add_argument("--decision-start", default="2026-04-15")' in cli_text


def test_dense_stack_start_script_does_not_block_autoresearch_on_prewarm_by_default() -> None:
    script_text = Path("auto_research/start_dense_stack.sh").read_text(encoding="utf-8")

    assert 'scripts/entrypoints/start_v2_orderbook_fleet.sh' in script_text
    assert 'ORDERBOOK_CYCLES="${ORDERBOOK_CYCLES:-15m,5m}"' in script_text
    assert 'PREWARM_SCRIPT="$ROOT_DIR/auto_research/prewarm_profitable_offset_pools.sh"' in script_text
    assert 'DENSE_STACK_PREWARM_ENABLE="${DENSE_STACK_PREWARM_ENABLE:-0}"' in script_text
    assert "start_optional_prewarm()" in script_text
    assert '"$PREWARM_SCRIPT" ensure' in script_text
    start_block = script_text.split("start)", maxsplit=1)[1].split(";;", maxsplit=1)[0]
    assert '"$QUEUE_SCRIPT" start' in start_block
    assert '"$DIRECTION_SCRIPT" start' in start_block
    assert '"$REVERSAL_SCRIPT" start' in start_block
    assert "start_optional_prewarm" in start_block
    assert start_block.index('"$QUEUE_SCRIPT" start') < start_block.index("start_optional_prewarm")
    assert 'MARKETS="$MARKETS" "$PREWARM_SCRIPT" ensure' not in start_block


def test_dense_stack_stop_cleans_experiment_workers() -> None:
    script_text = Path("auto_research/start_dense_stack.sh").read_text(encoding="utf-8")

    assert "stop_experiment_workers()" in script_text
    assert 'pkill -f "$ROOT_DIR/auto_research/run_one_experiment.sh"' in script_text
    assert 'pkill -f "$ROOT_DIR/scripts/research/run_quick_screen_suite.py"' in script_text
    assert 'pkill -f "research experiment run-suite"' in script_text
    assert 'pkill -f "$ROOT_DIR/auto_research/run_one_experiment_background.sh"' in script_text
    assert "stop_experiment_workers" in script_text.split("stop)")[1]


def test_quick_screen_suite_script_preserves_float_rank_precision() -> None:
    script_text = Path("scripts/research/run_quick_screen_suite.py").read_text(encoding="utf-8")

    assert "tuple(int(v) for v in item)" not in script_text
    assert "_sortable_rank_tuple" in script_text


def test_quick_screen_suite_script_clears_process_caches_after_each_case() -> None:
    script_text = Path("scripts/research/run_quick_screen_suite.py").read_text(encoding="utf-8")

    assert "clear_process_scoring_runtime_cache" in script_text
    assert "clear_process_backtest_runtime_cache" in script_text
    assert "gc.collect()" in script_text
    assert "finally:" in script_text
    assert "cleanup_between_cases" in script_text


def test_quick_screen_pool_keeps_process_caches_inside_pool() -> None:
    pool_text = Path("scripts/research/run_quick_screen_pool.py").read_text(encoding="utf-8")
    wrapper_text = Path("auto_research/run_quick_screen_pool.sh").read_text(encoding="utf-8")

    assert "ThreadPoolExecutor" in pool_text
    assert "cleanup_between_cases=False" in pool_text
    assert 'PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES="${PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES:-1}"' in wrapper_text
    assert 'PM15MIN_BACKTEST_SURFACE_RUNTIME_CACHE_MAX_ENTRIES="${PM15MIN_BACKTEST_SURFACE_RUNTIME_CACHE_MAX_ENTRIES:-2}"' in wrapper_text
    assert '--max-items "$PM15MIN_QUICK_SCREEN_POOL_MAX_ITEMS"' in wrapper_text


def test_status_dense_autorun_reads_both_dense_instances() -> None:
    script_text = Path("auto_research/status_dense_autorun.sh").read_text(encoding="utf-8")

    assert "direction_dense_sol_xrp" in script_text
    assert "reversal_dense_sol_xrp" in script_text
    assert "var/research/autorun/direction_dense_sol_xrp" in script_text
    assert "var/research/autorun/reversal_dense_sol_xrp" in script_text
    assert "status_autorun.sh" in script_text


def test_dense_stack_status_reads_sol_xrp_autoresearch_dirs() -> None:
    script_text = Path("auto_research/start_dense_stack.sh").read_text(encoding="utf-8")

    status_block = script_text.split("status)", maxsplit=1)[1].split(";;", maxsplit=1)[0]
    assert 'AUTORUN_DIR="$ROOT_DIR/var/research/autorun/direction_dense_sol_xrp"' in status_block
    assert 'AUTORUN_DIR="$ROOT_DIR/var/research/autorun/reversal_dense_sol_xrp"' in status_block


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
    assert 'CODEX_OFFICIAL_PRIORITY="${CODEX_OFFICIAL_PRIORITY:-disabled}"' in script_text
    assert 'PM15MIN_MANAGED_PROXY_ENV_FILE="${PM15MIN_MANAGED_PROXY_ENV_FILE:-${REAL_HOME:-$HOME}/.local/state/pm15min-managed-proxy/active_proxy.env}"' in script_text
    assert 'build_env_prefix "$CODEX_NETWORK_PROXY_MODE" "$home_root"' in script_text
    assert 'build_env_prefix "$CODEX_OFFICIAL_NETWORK_PROXY_MODE" "$home_root"' in script_text
    assert 'elif [[ "$proxy_mode" == "managed" ]]; then' in script_text
    assert 'source "$PM15MIN_MANAGED_PROXY_ENV_FILE"' in script_text
    assert 'trying official auth first' in script_text
    assert '[[ "$CODEX_OFFICIAL_PRIORITY" != "disabled" && -f "$CODEX_OFFICIAL_AUTH_PATH" ]]' in script_text
    assert '[[ "$CODEX_OFFICIAL_PRIORITY" != "first" ]]' in script_text


def test_codex_background_loop_defaults_to_cost_saver_codex_settings() -> None:
    script_text = Path("auto_research/codex_background_loop.sh").read_text(encoding="utf-8")

    assert 'LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-1800}"' in script_text
    assert 'CODEX_MODEL="${CODEX_MODEL:-gpt-5.5}"' in script_text
    assert 'CODEX_REASONING_EFFORT="${CODEX_REASONING_EFFORT:-xhigh}"' in script_text
    assert 'CODEX_PROMPT_BUDGET_MODE="${CODEX_PROMPT_BUDGET_MODE:-compact}"' in script_text
    assert '"$CODEX_REASONING_EFFORT"' in script_text
    assert '"$CODEX_PROMPT_BUDGET_MODE"' in script_text


def test_codex_exec_command_sets_reasoning_effort_config() -> None:
    command = build_codex_exec_command(
        project_root=Path("/tmp/demo"),
        output_path=Path("/tmp/demo/out.txt"),
        sandbox_mode="danger-full-access",
        model="gpt-5.5",
        reasoning_effort="high",
        codex_bin="/usr/bin/codex",
    )

    assert "--model" in command
    assert "gpt-5.5" in command
    assert "-c" in command
    assert 'model_reasoning_effort="high"' in command


def test_codex_cycle_prompt_compact_budget_omits_large_context_sections() -> None:
    source = Path("src/pm15min/research/automation/control_plane.py").read_text(encoding="utf-8")

    assert "prompt_budget_mode: str | None = None" in source
    assert "compact_prompt" in source
    assert "max_factors_per_family" in source
    assert "Global factor inventory already extracted for you:" in source
    assert "session / 'session.md'" in source
    assert "if not compact_prompt" in source


def test_autoresearch_start_wrappers_slow_codex_cycle_frequency_by_default() -> None:
    for script_name in ("start_direction_midprice_btc.sh", "start_direction_midprice_eth.sh"):
        script_text = Path("auto_research").joinpath(script_name).read_text(encoding="utf-8")
        assert 'LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-1800}"' in script_text
        assert 'CODEX_MODEL="${CODEX_MODEL:-gpt-5.5}"' in script_text
        assert 'CODEX_REASONING_EFFORT="${CODEX_REASONING_EFFORT:-xhigh}"' in script_text
        assert 'FACTOR_SCOUT_STAMP_PATH="${FACTOR_SCOUT_STAMP_PATH:-$AUTORUN_DIR/factor-scout.last-success}"' in script_text

    for script_name in ("start_direction_dense.sh", "start_reversal_dense.sh"):
        script_text = Path("auto_research").joinpath(script_name).read_text(encoding="utf-8")
        assert 'LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-900}"' in script_text
        assert 'CODEX_MODEL="${CODEX_MODEL:-gpt-5.5}"' in script_text
        assert 'CODEX_REASONING_EFFORT="${CODEX_REASONING_EFFORT:-xhigh}"' in script_text


def test_status_autorun_allows_status_path_override() -> None:
    script_text = Path("auto_research/status_autorun.sh").read_text(encoding="utf-8")

    assert 'AUTORUN_DIR="${AUTORUN_DIR:-$ROOT_DIR/var/research/autorun}"' in script_text
    assert "build_autorun_status_report(" in script_text
    assert "status_path=" in script_text


def test_build_codex_cycle_prompt_accepts_status_path_override() -> None:
    source = Path("src/pm15min/research/automation/control_plane.py").read_text(encoding="utf-8")

    assert "def build_codex_cycle_prompt(" in source
    assert "status_path: Path | None = None" in source
    assert "status_path=status_path" in source
    assert "log_tail_lines=2 if compact_prompt else 5" in source
    assert "max_incomplete_runs=3 if compact_prompt else 5" in source


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


def test_midprice_prompt_ignores_completed_runs_from_stale_decision_window(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_midprice_direction_btc_autoresearch"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_direction_midprice_btc.md"
    program_path.write_text(
        "\n".join(
            [
                "# BTC Midprice",
                "",
                "- coin: `btc`",
                "- target fixed to `direction`",
                "- suite seed: `btc_suite`",
                "- active session: `sessions/deep_otm_midprice_direction_btc_autoresearch/session.md`",
                "",
                "Run full formal experiments only; do not use the shared SOL/XRP quick-screen queue.",
                "Use auto_research/run_one_experiment_background.sh for detached formal work.",
            ]
        ),
        encoding="utf-8",
    )
    experiments_root = root / "research" / "experiments"
    suite_specs_dir = experiments_root / "suite_specs"
    suite_specs_dir.mkdir(parents=True, exist_ok=True)
    (suite_specs_dir / "btc_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "btc_suite",
                "markets": {
                    "btc": {
                        "groups": {
                            "midprice_direction": {
                                "runs": [
                                    {
                                        "run_name": "direction_entry45_50_prob60__max5",
                                        "target": "direction",
                                        "feature_set_variants": [
                                            {"label": "frontier", "feature_set": "focus_btc_44_v1"}
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

    def write_run(run_label: str, *, decision_start: str, trades: int, roi: float, mtime: int) -> None:
        run_dir = experiments_root / "runs" / "suite=btc_suite" / f"run={run_label}"
        (run_dir / "logs").mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "suite_name": "btc_suite",
                    "run_label": run_label,
                    "cases": 1,
                    "completed_cases": 1,
                    "failed_cases": 0,
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
                    "group_name": "midprice_direction",
                    "run_name": "direction_entry45_50_prob60__max5",
                    "target": "direction",
                    "variant_label": "default",
                    "roi_pct": str(roi),
                    "pnl_sum": str(roi),
                    "trades": str(trades),
                }
            )
        (run_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "object_type": "experiment_run",
                    "object_id": f"experiment_run:btc_suite:{run_label}",
                    "market": "btc",
                    "cycle": "15m",
                    "path": str(run_dir),
                    "created_at": "2026-05-02T00:00:00Z",
                    "spec": {
                        "suite_name": "btc_suite",
                        "run_label": run_label,
                        "markets": [
                            {
                                "market": "btc",
                                "decision_start": decision_start,
                                "decision_end": "2026-05-07",
                                "window": {"start": "2025-10-27", "end": "2026-04-15"},
                            }
                        ],
                    },
                    "inputs": [],
                    "outputs": [],
                    "metadata": {},
                    "schema_version": "pm15min.research.v1",
                }
            ),
            encoding="utf-8",
        )
        (run_dir / "logs" / "suite.jsonl").write_text(
            json.dumps({"event": "market_completed", "case_label": "btc/midprice_direction"}) + "\n",
            encoding="utf-8",
        )
        for path in (summary_path, run_dir / "manifest.json"):
            os.utime(path, (mtime, mtime))

    write_run("old_window_good", decision_start="2026-04-01", trades=12, roi=46.0, mtime=300)
    write_run("post0415_sparse", decision_start="2026-04-15", trades=5, roi=-22.0, mtime=200)

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert "old_window_good" not in prompt
    assert "best_trades=5" in prompt
    assert "recent_trades=5" in prompt
    assert "post0415_sparse" in prompt


def test_midprice_prompt_forces_branch_change_after_repeated_sparse_same_suite(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_midprice_direction_btc_autoresearch"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_direction_midprice_btc.md"
    program_path.write_text(
        "\n".join(
            [
                "# BTC Midprice",
                "",
                "- coin: `btc`",
                "- target fixed to `direction`",
                "- suite seed: `btc_suite`",
                "- active session: `sessions/deep_otm_midprice_direction_btc_autoresearch/session.md`",
                "",
                "Run full formal experiments only; do not use the shared SOL/XRP quick-screen queue.",
                "Use auto_research/run_one_experiment_background.sh for detached formal work.",
            ]
        ),
        encoding="utf-8",
    )
    experiments_root = root / "research" / "experiments"
    suite_specs_dir = experiments_root / "suite_specs"
    suite_specs_dir.mkdir(parents=True, exist_ok=True)
    (suite_specs_dir / "btc_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "btc_suite",
                "markets": {
                    "btc": {
                        "groups": {
                            "midprice_direction": {
                                "runs": [
                                    {
                                        "run_name": "direction_entry45_50_prob60__max5",
                                        "target": "direction",
                                        "feature_set_variants": [
                                            {"label": "frontier", "feature_set": "focus_btc_44_v1"}
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
    for index in range(3):
        run_label = f"same_suite_refill{index}"
        run_dir = experiments_root / "runs" / "suite=btc_suite" / f"run={run_label}"
        (run_dir / "logs").mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "suite_name": "btc_suite",
                    "run_label": run_label,
                    "cases": 1,
                    "completed_cases": 1,
                    "failed_cases": 0,
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
                    "group_name": "midprice_direction",
                    "run_name": "direction_entry45_50_prob60__max5",
                    "target": "direction",
                    "variant_label": "default",
                    "roi_pct": "-22",
                    "pnl_sum": "-11",
                    "trades": "5",
                }
            )
        (run_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "object_type": "experiment_run",
                    "object_id": f"experiment_run:btc_suite:{run_label}",
                    "market": "btc",
                    "cycle": "15m",
                    "path": str(run_dir),
                    "created_at": "2026-05-02T00:00:00Z",
                    "spec": {
                        "suite_name": "btc_suite",
                        "run_label": run_label,
                        "markets": [
                            {
                                "market": "btc",
                                "decision_start": "2026-04-15",
                                "decision_end": "2026-05-07",
                                "window": {"start": "2025-10-27", "end": "2026-04-15"},
                            }
                        ],
                    },
                    "inputs": [],
                    "outputs": [],
                    "metadata": {},
                    "schema_version": "pm15min.research.v1",
                }
            ),
            encoding="utf-8",
        )
        (run_dir / "logs" / "suite.jsonl").write_text(
            json.dumps({"event": "market_completed", "case_label": "btc/midprice_direction"}) + "\n",
            encoding="utf-8",
        )
        os.utime(summary_path, (300 - index, 300 - index))

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)
    lower_prompt = prompt.lower()

    assert "recommendation=heavy_rework" in prompt
    assert "recent_trades=5,5,5" in prompt
    assert "same suite sparse streak=3" in lower_prompt
    assert "do not launch another run of the same suite" in lower_prompt


def test_run_one_experiment_refuses_to_reuse_completed_run_label() -> None:
    script_text = Path("auto_research/run_one_experiment.sh").read_text(encoding="utf-8")

    assert "refusing to reuse completed run label" in script_text
    assert 'if [[ -e "$RUN_DIR/summary.json" || -e "$RUN_DIR/quick_screen_summary.json" ]]; then' in script_text


def test_codex_prompt_includes_structured_search_policy_not_only_free_text(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_midprice_direction_btc_autoresearch"
    session_dir.mkdir(parents=True)
    program_dir = root / "auto_research"
    program_dir.mkdir()
    program_path = program_dir / "program_direction_midprice_btc.md"
    program_path.write_text(
        "\n".join(
            [
                "# BTC",
                "- coin: `btc`",
                "- target fixed to `direction`",
                "- suite seed: `btc_suite`",
                "- active session: `sessions/deep_otm_midprice_direction_btc_autoresearch/session.md`",
                "Run full formal experiments only; do not use the shared SOL/XRP quick-screen queue.",
            ]
        ),
        encoding="utf-8",
    )
    specs_dir = root / "research" / "experiments" / "suite_specs"
    specs_dir.mkdir(parents=True)
    (specs_dir / "btc_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "btc_suite",
                "window": {"start": "2025-10-27", "end": "2026-04-15"},
                "decision_start": "2026-04-15",
                "decision_end": "2026-05-07",
                "markets": {
                    "btc": {
                        "groups": {
                            "direction": {
                                "runs": [
                                    {
                                        "target": "direction",
                                        "model_family": "deep_otm",
                                        "feature_set_variants": [{"label": "a", "feature_set": "focus_btc_40_v1"}],
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
    experiments_dir = root / "research" / "experiments"
    experiments_dir.mkdir(parents=True, exist_ok=True)
    (experiments_dir / "custom_feature_sets.json").write_text(
        json.dumps({"focus_btc_40_v1": {"market": "btc", "width": 40, "columns": ["ret_from_cycle_open"]}}),
        encoding="utf-8",
    )
    for idx in range(3):
        run_dir = root / "research" / "experiments" / "runs" / "suite=btc_suite" / f"run=run{idx}"
        run_dir.mkdir(parents=True)
        summary_path = run_dir / "summary.json"
        summary_path.write_text(
            json.dumps({"suite_name": "btc_suite", "run_label": f"run{idx}", "cases": 1, "completed_cases": 1, "failed_cases": 0}),
            encoding="utf-8",
        )
        (run_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "spec": {
                        "markets": [
                            {
                                "market": "btc",
                                "decision_start": "2026-04-15",
                                "decision_end": "2026-05-07",
                                "window": {"start": "2025-10-27", "end": "2026-04-15"},
                            }
                        ]
                    }
                }
            ),
            encoding="utf-8",
        )
        with (run_dir / "leaderboard.csv").open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["market", "target", "feature_set", "trades", "roi_pct"])
            writer.writeheader()
            writer.writerow({"market": "btc", "target": "direction", "feature_set": "focus_btc_40_v1", "trades": "5", "roi_pct": "-1"})
        os.utime(summary_path, (300 - idx, 300 - idx))

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert "attempt: btc: suite=btc_suite" in prompt
    assert "policy: btc: required_next_lever=feature_width" in prompt


def test_dense_sol_xrp_prompt_requires_filling_all_open_quick_screen_slots(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_baseline_direction_dense_sol_xrp_autoresearch"
    session_dir.mkdir(parents=True)
    program_dir = root / "auto_research"
    program_dir.mkdir()
    program_path = program_dir / "program_direction_dense_sol_xrp.md"
    program_path.write_text(
        "\n".join(
            [
                "# Direction SOL/XRP",
                "- coins: `sol`, `xrp`",
                "- target fixed to `direction`",
                "- run SOL/XRP dense work through the shared queue in `quick_screen` mode only",
            ]
        ),
        encoding="utf-8",
    )
    queue_state = root / "var" / "research" / "autorun"
    queue_state.mkdir(parents=True)
    (queue_state / "experiment-queue.json").write_text(
        json.dumps(
            {
                "version": 1,
                "max_live_runs": 10,
                "max_queued_items": 24,
                "track_slot_caps": {"direction_dense": 5, "reversal_dense": 5},
                "items": [],
            }
        ),
        encoding="utf-8",
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert "Current-track gap is 5" in prompt
    assert "queue 5 additional distinct quick-screen branches now" in prompt
    assert "must queue enough distinct quick-screen branches to fill all 5 open current-track slots" in prompt
    assert "Do not end with only 3 queued or launched branches when 5 slots are open" in prompt


def test_dense_sol_xrp_prompt_keeps_compact_mode_when_queue_slots_are_empty(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "deep_otm_baseline_direction_dense_sol_xrp_autoresearch"
    session_dir.mkdir(parents=True, exist_ok=True)
    auto_research_dir = root / "auto_research"
    auto_research_dir.mkdir(parents=True, exist_ok=True)
    program_path = auto_research_dir / "program_direction_dense_sol_xrp.md"
    program_path.write_text(
        "\n".join(
            [
                "# SOL/XRP Dense Direction",
                "- target fixed to `direction`",
                "- coins: `sol`, `xrp`",
                "- active session: `sessions/deep_otm_baseline_direction_dense_sol_xrp_autoresearch/session.md`",
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
                "updated_at": "2026-05-12T08:20:00Z",
                "max_live_runs": 10,
                "max_queued_items": 24,
                "track_slot_caps": {"direction_dense": 5, "reversal_dense": 5},
                "items": [],
            }
        ),
        encoding="utf-8",
    )
    experiments_root = root / "research" / "experiments"
    suite_specs_dir = experiments_root / "suite_specs"
    suite_specs_dir.mkdir(parents=True, exist_ok=True)
    (experiments_root / "custom_feature_sets.json").write_text(
        json.dumps(
            {
                "focus_sol_56_v1r1": {"market": "sol", "width": 56, "columns": ["ret_15m"]},
                "focus_xrp_56_v1r1": {"market": "xrp", "width": 56, "columns": ["ret_15m"]},
            }
        ),
        encoding="utf-8",
    )

    for market in ("sol", "xrp"):
        suite_name = f"{market}_direction_suite"
        (suite_specs_dir / f"{suite_name}.json").write_text(
            json.dumps(
                {
                    "suite_name": suite_name,
                    "targets": ["direction"],
                    "markets": {
                        market: {
                            "groups": {
                                "focus_search": {
                                    "runs": [
                                        {
                                            "run_name": "focus_search",
                                            "feature_set_variants": [
                                                {"label": "frontier", "feature_set": f"focus_{market}_56_v1r1"}
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
        for idx, trades in enumerate((3, 2, 1), start=1):
            run_label = f"auto_{market}_direction_r{idx}"
            run_dir = experiments_root / "runs" / f"suite={suite_name}" / f"run={run_label}"
            (run_dir / "logs").mkdir(parents=True, exist_ok=True)
            summary_path = run_dir / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "suite_name": suite_name,
                        "run_label": run_label,
                        "cases": 1,
                        "completed_cases": 1,
                        "failed_cases": 0,
                        "markets": [market],
                    }
                ),
                encoding="utf-8",
            )
            with (run_dir / "leaderboard.csv").open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(
                    fh,
                    fieldnames=[
                        "market",
                        "group_name",
                        "run_name",
                        "target",
                        "variant_label",
                        "roi_pct",
                        "pnl_sum",
                        "trades",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market": market,
                        "group_name": "focus_search",
                        "run_name": "focus_search",
                        "target": "direction",
                        "variant_label": "default",
                        "roi_pct": "1.0",
                        "pnl_sum": "1.0",
                        "trades": str(trades),
                    }
                )
            (run_dir / "logs" / "suite.jsonl").write_text(
                json.dumps({"event": "market_completed", "case_label": f"{market}/focus_search"}) + "\n",
                encoding="utf-8",
            )
            os.utime(summary_path, (100 + idx, 100 + idx))

    prompt = build_codex_cycle_prompt(
        project_root=root,
        session_dir=session_dir,
        program_path=program_path,
        prompt_budget_mode="compact",
    )
    lower_prompt = prompt.lower()

    assert "decision_mode=heavy_analysis" in lower_prompt
    assert "track_gap=5" in lower_prompt
    assert "queue 5 additional distinct quick-screen branches now" in prompt
    assert "Global factor inventory already extracted for you:" in prompt
    assert f"- {session_dir / 'session.md'}" not in prompt
    assert len(prompt) < 25000


def test_experiment_queue_enqueue_preserves_research_metadata(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    program_path = root / "auto_research" / "program_direction_dense_sol_xrp.md"
    program_path.parent.mkdir(parents=True)
    program_path.write_text("# program\n", encoding="utf-8")
    session_dir = root / "sessions" / "dense"
    session_dir.mkdir(parents=True)

    env = dict(os.environ)
    src_path = str((Path.cwd() / "src").resolve())
    env["PYTHONPATH"] = src_path + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")

    result = subprocess.run(
        [
            sys.executable,
            "auto_research/experiment_queue.py",
            "--root",
            str(root),
            "enqueue",
            "--suite",
            "suite_sol",
            "--run-label",
            "run_sol",
            "--market",
            "sol",
            "--action",
            "launch",
            "--track",
            "direction_dense",
            "--session-dir",
            str(session_dir),
            "--program-path",
            str(program_path),
            "--primary-lever",
            "feature_width",
            "--feature-width",
            "56",
            "--model-family",
            "catboost",
            "--feature-set",
            "focus_sol_56_v1",
            "--factor-family-change",
            "add_cross_asset_drop_short_returns",
            "--expected-trade-count-effect",
            "increase SOL dense trades from sparse to on-target",
            "--difference-from-recent-failures",
            "changes width and family after repeated sparse same-family runs",
        ],
        cwd=Path.cwd(),
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(result.stdout)
    item = payload["items"][0]
    assert item["research_meta"] == {
        "primary_lever": "feature_width",
        "feature_width": "56",
        "model_family": "catboost",
        "feature_set": "focus_sol_56_v1",
        "factor_family_change": "add_cross_asset_drop_short_returns",
        "expected_trade_count_effect": "increase SOL dense trades from sparse to on-target",
        "difference_from_recent_failures": "changes width and family after repeated sparse same-family runs",
    }
    assert item["research_candidate_gate"]["passed"] is True


def test_build_codex_cycle_prompt_requires_candidate_metadata_for_queue_launches(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    session_dir.mkdir(parents=True, exist_ok=True)
    program_path = root / "auto_research" / "program_direction_dense_sol_xrp.md"
    program_path.parent.mkdir(parents=True, exist_ok=True)
    program_path.write_text("# dense direction\n- coins: sol,xrp\n- target fixed to `direction`\n", encoding="utf-8")

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir, program_path=program_path)

    assert "--factor-family-change ..." in prompt
    assert "--expected-trade-count-effect ..." in prompt
    assert "--difference-from-recent-failures ..." in prompt
    assert "Every new launch queue item should carry candidate metadata" in prompt


def test_active_autoresearch_programs_do_not_reference_stale_april_windows() -> None:
    active_programs = [
        Path("auto_research/program_direction_dense_sol_xrp.md"),
        Path("auto_research/program_reversal_dense_sol_xrp.md"),
        Path("auto_research/program_direction_midprice_btc.md"),
        Path("auto_research/program_direction_midprice_eth.md"),
    ]
    for path in active_programs:
        text = path.read_text(encoding="utf-8")
        assert "2026-04-15" in text
        assert "2026-05-07" in text
        assert "2026-04-01` through `2026-04-23" not in text
        assert "2026-04-01 through 2026-04-23" not in text
