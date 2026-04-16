from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from pm15min.research.automation import build_autorun_status_report, build_codex_cycle_prompt
from pm15min.research.automation.queue_state import (
    build_queue_item,
    ensure_running_queue_items,
    launch_ready_queue_items,
    load_experiment_queue,
    reconcile_queue_with_live_workers,
    select_launchable_queue_items,
    set_queue_item_status,
    upsert_queue_item,
)


def test_load_experiment_queue_defaults_to_four_live_runs(tmp_path: Path) -> None:
    root = tmp_path / "repo"

    state = load_experiment_queue(root)

    assert state["max_live_runs"] == 4


def test_upsert_queue_item_replaces_older_normal_candidate_for_same_market(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    older = build_queue_item(
        market="btc",
        suite_name="btc_suite_old",
        run_label="btc_old",
        action="launch",
        reason="older idea",
    )
    newer = build_queue_item(
        market="btc",
        suite_name="btc_suite_new",
        run_label="btc_new",
        action="launch",
        reason="newer idea",
    )

    upsert_queue_item(root, older)
    state = upsert_queue_item(root, newer)

    queued = [item for item in state["items"] if item["status"] == "queued"]
    assert len(queued) == 1
    assert queued[0]["suite_name"] == "btc_suite_new"
    assert queued[0]["run_label"] == "btc_new"


def test_upsert_queue_item_keeps_one_normal_candidate_per_market_per_track(tmp_path: Path) -> None:
    root = tmp_path
    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_direction_old",
            run_label="btc_direction_old",
            action="launch",
            status="queued",
            track="direction_dense",
        ),
    )
    state = upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_reversal_new",
            run_label="btc_reversal_new",
            action="launch",
            status="queued",
            track="reversal_dense",
        ),
    )

    queued = [item for item in state["items"] if item["status"] == "queued"]
    assert {item["track"] for item in queued} == {"direction_dense", "reversal_dense"}


def test_upsert_queue_item_keeps_repair_item_when_new_normal_candidate_arrives(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    repair = build_queue_item(
        market="eth",
        suite_name="eth_suite_repair",
        run_label="eth_repair",
        action="repair",
        status="repair",
        reason="repair first",
    )
    launch = build_queue_item(
        market="eth",
        suite_name="eth_suite_launch",
        run_label="eth_launch",
        action="launch",
        reason="new branch",
    )

    upsert_queue_item(root, repair)
    state = upsert_queue_item(root, launch)

    assert {item["run_label"] for item in state["items"]} == {"eth_repair", "eth_launch"}
    repair_item = next(item for item in state["items"] if item["run_label"] == "eth_repair")
    launch_item = next(item for item in state["items"] if item["run_label"] == "eth_launch")
    assert repair_item["status"] == "repair"
    assert launch_item["status"] == "queued"


def test_select_launchable_queue_items_prioritizes_repair_and_respects_live_markets(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    upsert_queue_item(
        root,
        build_queue_item(
            market="eth",
            suite_name="eth_suite_repair",
            run_label="eth_repair",
            action="repair",
            status="queued",
            reason="repair first",
        ),
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="xrp",
            suite_name="xrp_suite_resume",
            run_label="xrp_resume",
            action="resume",
            status="queued",
            reason="resume second",
        ),
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_suite_launch",
            run_label="btc_launch",
            action="launch",
            status="queued",
            reason="launch third",
        ),
    )

    state = load_experiment_queue(root)
    selected = select_launchable_queue_items(
        state,
        max_live_runs=3,
        live_workers=[{"market": "eth", "run_label": "eth_live"}],
    )

    assert [item["run_label"] for item in selected] == ["xrp_resume", "btc_launch"]


def test_select_launchable_queue_items_respects_track_slot_caps(tmp_path: Path) -> None:
    root = tmp_path
    for market in ("btc", "eth", "sol", "xrp"):
        upsert_queue_item(
            root,
            build_queue_item(
                market=market,
                suite_name=f"{market}_direction",
                run_label=f"{market}_direction",
                action="launch",
                status="queued",
                track="direction_dense",
            ),
        )
    for market in ("btc", "eth", "sol", "xrp"):
        upsert_queue_item(
            root,
            build_queue_item(
                market=market,
                suite_name=f"{market}_reversal",
                run_label=f"{market}_reversal",
                action="launch",
                status="queued",
                track="reversal_dense",
            ),
        )

    payload = load_experiment_queue(root)
    payload["max_live_runs"] = 4
    payload["track_slot_caps"] = {"direction_dense": 2, "reversal_dense": 2}
    selected = select_launchable_queue_items(payload, max_live_runs=4, live_workers=[])

    counts = {}
    for item in selected:
        counts[item["track"]] = counts.get(item["track"], 0) + 1
    assert counts == {"direction_dense": 2, "reversal_dense": 2}


def test_seeded_live_worker_without_explicit_track_still_counts_against_inferred_track_cap(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    for market in ("eth", "sol"):
        upsert_queue_item(
            root,
            build_queue_item(
                market=market,
                suite_name=f"{market}_direction_dense_suite",
                run_label=f"{market}_direction_dense_run",
                action="launch",
                status="queued",
                track="direction_dense",
            ),
        )
    for market in ("btc", "xrp"):
        upsert_queue_item(
            root,
            build_queue_item(
                market=market,
                suite_name=f"{market}_reversal_dense_suite",
                run_label=f"{market}_reversal_dense_run",
                action="launch",
                status="queued",
                track="reversal_dense",
            ),
        )

    seeded = ensure_running_queue_items(
        root,
        live_workers=[
            {
                "market": "ada",
                "suite_name": "ada_direction_dense_suite",
                "run_label": "ada_direction_dense_run",
            }
        ],
    )

    running = next(item for item in seeded["items"] if item["run_label"] == "ada_direction_dense_run")
    assert running["track"] == "direction_dense"

    payload = load_experiment_queue(root)
    payload["max_live_runs"] = 4
    payload["track_slot_caps"] = {"direction_dense": 2, "reversal_dense": 2}
    selected = select_launchable_queue_items(payload, max_live_runs=4, live_workers=[])

    counts = {}
    for item in selected:
        counts[item["track"]] = counts.get(item["track"], 0) + 1
    assert counts == {"direction_dense": 1, "reversal_dense": 2}


def test_set_queue_item_status_rejects_ambiguous_suite_and_run_across_tracks(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    for track in ("direction_dense", "reversal_dense"):
        upsert_queue_item(
            root,
            build_queue_item(
                market="btc",
                suite_name="shared_suite",
                run_label="shared_run",
                action="launch",
                status="queued",
                track=track,
            ),
        )

    with pytest.raises(ValueError, match="ambiguous"):
        set_queue_item_status(
            root,
            suite_name="shared_suite",
            run_label="shared_run",
            status="done",
        )

    state = set_queue_item_status(
        root,
        suite_name="shared_suite",
        run_label="shared_run",
        track="direction_dense",
        status="done",
    )

    statuses = {item["track"]: item["status"] for item in state["items"]}
    assert statuses == {"direction_dense": "done", "reversal_dense": "queued"}


def test_cli_enqueue_requires_explicit_track_session_and_program(tmp_path: Path) -> None:
    workspace_root = Path(__file__).resolve().parents[1]
    root = tmp_path / "repo"
    result = subprocess.run(
        [
            sys.executable,
            str(workspace_root / "auto_research" / "experiment_queue.py"),
            "--root",
            str(root),
            "enqueue",
            "--suite",
            "btc_suite",
            "--run-label",
            "btc_run",
            "--market",
            "btc",
            "--action",
            "launch",
        ],
        cwd=workspace_root,
        env={**os.environ, "PYTHONPATH": str(workspace_root / "src")},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "--track" in result.stderr
    assert "--session-dir" in result.stderr
    assert "--program-path" in result.stderr


def test_reconcile_queue_with_live_workers_marks_missing_nonterminal_run_as_repair(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    running = build_queue_item(
        market="btc",
        suite_name="btc_suite",
        run_label="btc_run",
        action="launch",
        status="running",
        reason="running",
    )
    upsert_queue_item(root, running)

    state = reconcile_queue_with_live_workers(
        root,
        live_workers=[],
        inspect_run=lambda _run_dir: {"state": "checkpointed", "last_event": "market_cache_resolved"},
        max_repair_attempts=3,
    )

    item = next(entry for entry in state["items"] if entry["run_label"] == "btc_run")
    assert item["status"] == "repair"
    assert item["action"] == "repair"
    assert item["retry_count"] == 1


def test_launch_ready_queue_items_relaunches_repair_status_item(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    running = build_queue_item(
        market="sol",
        suite_name="sol_suite",
        run_label="sol_run",
        action="launch",
        status="running",
        reason="running",
        track="direction_dense",
        session_dir=root / "sessions" / "direction_dense",
        program_path=root / "auto_research" / "program_direction_dense.md",
    )
    upsert_queue_item(root, running)

    reconciled = reconcile_queue_with_live_workers(
        root,
        live_workers=[],
        inspect_run=lambda _run_dir: {"state": "checkpointed", "last_event": "market_cache_resolved"},
        max_repair_attempts=3,
    )
    repair_item = next(entry for entry in reconciled["items"] if entry["run_label"] == "sol_run")
    assert repair_item["status"] == "repair"

    launched: list[str] = []
    relaunched_state, launched_items = launch_ready_queue_items(
        root,
        live_workers=[],
        launcher=lambda item: launched.append(str(item["run_label"])) or {"pid": 456},
    )

    assert [item["run_label"] for item in launched_items] == ["sol_run"]
    assert launched == ["sol_run"]
    relaunched_item = next(entry for entry in relaunched_state["items"] if entry["run_label"] == "sol_run")
    assert relaunched_item["status"] == "running"
    assert relaunched_item["action"] == "repair"


def test_launch_ready_queue_items_marks_unknown_repair_dead_without_blocking_healthy_work(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    ensure_running_queue_items(
        root,
        live_workers=[
            {
                "market": "ada",
                "suite_name": "ada_manual_suite",
                "run_label": "ada_unknown_run",
            }
        ],
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_direction_dense_suite",
            run_label="btc_direction_dense_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=root / "sessions" / "direction_dense",
            program_path=root / "auto_research" / "program_direction_dense.md",
        ),
    )

    reconciled = reconcile_queue_with_live_workers(
        root,
        live_workers=[],
        inspect_run=lambda _run_dir: {"state": "checkpointed", "last_event": "market_cache_resolved"},
        max_repair_attempts=3,
    )
    repair_item = next(entry for entry in reconciled["items"] if entry["run_label"] == "ada_unknown_run")
    assert repair_item["status"] == "repair"
    assert repair_item["track"] == "unknown"

    launched: list[str] = []
    state, launched_items = launch_ready_queue_items(
        root,
        live_workers=[],
        launcher=lambda item: launched.append(str(item["run_label"])) or {"pid": 999},
        max_live_runs=2,
    )

    assert [item["run_label"] for item in launched_items] == ["btc_direction_dense_run"]
    assert launched == ["btc_direction_dense_run"]
    terminal_item = next(entry for entry in state["items"] if entry["run_label"] == "ada_unknown_run")
    assert terminal_item["status"] == "dead"
    assert terminal_item["action"] == "blocked"
    assert "unlaunchable_repair" in str(terminal_item["last_error"])


def test_launch_ready_queue_items_marks_invalid_explicit_track_repair_dead_and_nonblocking(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    upsert_queue_item(
        root,
        build_queue_item(
            market="ada",
            suite_name="ada_direction_dense_suite",
            run_label="ada_invalid_track_run",
            action="repair",
            status="repair",
            track="bogus_track",
            session_dir=root / "sessions" / "direction_dense",
            program_path=root / "auto_research" / "program_direction_dense.md",
        ),
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_direction_dense_suite",
            run_label="btc_direction_dense_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=root / "sessions" / "direction_dense",
            program_path=root / "auto_research" / "program_direction_dense.md",
        ),
    )

    launched: list[str] = []
    state, launched_items = launch_ready_queue_items(
        root,
        live_workers=[],
        launcher=lambda item: launched.append(str(item["run_label"])) or {"pid": 321},
        max_live_runs=2,
    )

    assert [item["run_label"] for item in launched_items] == ["btc_direction_dense_run"]
    assert launched == ["btc_direction_dense_run"]
    blocked_item = next(entry for entry in state["items"] if entry["run_label"] == "ada_invalid_track_run")
    assert blocked_item["track"] == "unknown"
    assert blocked_item["status"] == "dead"
    assert blocked_item["action"] == "blocked"
    assert "unlaunchable_repair" in str(blocked_item["last_error"])
    assert "track" in str(blocked_item["last_error"])


def test_launch_ready_queue_items_does_not_mark_running_repair_dead_when_not_selected(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    upsert_queue_item(
        root,
        build_queue_item(
            market="ada",
            suite_name="ada_running_repair_suite",
            run_label="ada_running_repair_run",
            action="repair",
            status="running",
            reason="already running repair",
        ),
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_direction_dense_suite",
            run_label="btc_direction_dense_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=root / "sessions" / "direction_dense",
            program_path=root / "auto_research" / "program_direction_dense.md",
        ),
    )

    launched: list[str] = []
    state, launched_items = launch_ready_queue_items(
        root,
        live_workers=[],
        launcher=lambda item: launched.append(str(item["run_label"])) or {"pid": 432},
        max_live_runs=2,
    )

    assert [item["run_label"] for item in launched_items] == ["btc_direction_dense_run"]
    assert launched == ["btc_direction_dense_run"]
    running_item = next(entry for entry in state["items"] if entry["run_label"] == "ada_running_repair_run")
    assert running_item["status"] == "running"
    assert running_item["action"] == "repair"


def test_launch_ready_queue_items_terminalizes_unlaunchable_resume_and_launches_healthy_item(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    upsert_queue_item(
        root,
        build_queue_item(
            market="ada",
            suite_name="ada_resume_suite",
            run_label="ada_resume_run",
            action="resume",
            status="queued",
            track="direction_dense",
        ),
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_direction_dense_suite",
            run_label="btc_direction_dense_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=root / "sessions" / "direction_dense",
            program_path=root / "auto_research" / "program_direction_dense.md",
        ),
    )

    launched: list[str] = []
    state, launched_items = launch_ready_queue_items(
        root,
        live_workers=[],
        launcher=lambda item: launched.append(str(item["run_label"])) or {"pid": 654},
        max_live_runs=2,
    )

    assert [item["run_label"] for item in launched_items] == ["btc_direction_dense_run"]
    assert launched == ["btc_direction_dense_run"]
    blocked_item = next(entry for entry in state["items"] if entry["run_label"] == "ada_resume_run")
    assert blocked_item["status"] == "dead"
    assert blocked_item["action"] == "blocked"
    assert "session_dir" in str(blocked_item["last_error"])
    assert "program_path" in str(blocked_item["last_error"])


def test_launch_ready_queue_items_terminalizes_launch_error_and_launches_healthy_item(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    upsert_queue_item(
        root,
        build_queue_item(
            market="ada",
            suite_name="ada_repair_suite",
            run_label="ada_repair_run",
            action="repair",
            status="repair",
            track="direction_dense",
            session_dir=root / "sessions" / "direction_dense",
            program_path=root / "auto_research" / "program_direction_dense.md",
        ),
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_direction_dense_suite",
            run_label="btc_direction_dense_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=root / "sessions" / "direction_dense",
            program_path=root / "auto_research" / "program_direction_dense.md",
        ),
    )

    def launcher(item: dict[str, object]) -> dict[str, object]:
        if str(item["run_label"]) == "ada_repair_run":
            raise RuntimeError("launcher exploded")
        return {"pid": 777}

    state, launched_items = launch_ready_queue_items(
        root,
        live_workers=[],
        launcher=launcher,
        max_live_runs=1,
    )

    assert [item["run_label"] for item in launched_items] == ["btc_direction_dense_run"]
    failed_item = next(entry for entry in state["items"] if entry["run_label"] == "ada_repair_run")
    assert failed_item["status"] == "dead"
    assert failed_item["action"] == "blocked"
    assert "launch_error" in str(failed_item["last_error"])


def test_reconcile_queue_with_live_workers_does_not_keep_cross_track_running_items_alive_via_ambiguous_fallback(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    for track in ("direction_dense", "reversal_dense"):
        upsert_queue_item(
            root,
            build_queue_item(
                market="btc",
                suite_name="shared_suite",
                run_label="shared_run",
                action="launch",
                status="running",
                track=track,
                session_dir=root / "sessions" / track,
                program_path=root / "auto_research" / f"program_{track}.md",
            ),
        )

    inspect_calls: list[str] = []

    def inspect_run(run_dir: Path) -> dict[str, object]:
        inspect_calls.append(str(run_dir))
        return {"state": "checkpointed", "last_event": "market_cache_resolved"}

    state = reconcile_queue_with_live_workers(
        root,
        live_workers=[
            {
                "market": "btc",
                "suite_name": "shared_suite",
                "run_label": "shared_run",
            }
        ],
        inspect_run=inspect_run,
        max_repair_attempts=3,
    )

    statuses = {item["track"]: item["status"] for item in state["items"]}
    assert statuses == {"direction_dense": "repair", "reversal_dense": "repair"}
    assert inspect_calls == []


def test_set_queue_item_status_accepts_unique_legacy_three_part_id_and_rejects_ambiguous_one(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    unique_state = upsert_queue_item(
        root,
        build_queue_item(
            market="sol",
            suite_name="sol_suite",
            run_label="sol_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=root / "sessions" / "direction_dense",
            program_path=root / "auto_research" / "program_direction_dense.md",
        ),
    )
    unique_item = unique_state["items"][0]

    updated = set_queue_item_status(
        root,
        item_id="sol:sol_suite:sol_run",
        status="done",
    )
    assert updated["items"][0]["status"] == "done"
    assert updated["items"][0]["id"] == unique_item["id"]

    for track in ("direction_dense", "reversal_dense"):
        upsert_queue_item(
            root,
            build_queue_item(
                market="btc",
                suite_name="btc_suite",
                run_label="btc_run",
                action="launch",
                status="queued",
                track=track,
                session_dir=root / "sessions" / track,
                program_path=root / "auto_research" / f"program_{track}.md",
            ),
        )

    with pytest.raises(ValueError, match="ambiguous"):
        set_queue_item_status(
            root,
            item_id="btc:btc_suite:btc_run",
            status="done",
        )


def test_reconcile_queue_with_live_workers_marks_terminal_run_done(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    running = build_queue_item(
        market="sol",
        suite_name="sol_suite",
        run_label="sol_run",
        action="launch",
        status="running",
        reason="running",
    )
    upsert_queue_item(root, running)

    state = reconcile_queue_with_live_workers(
        root,
        live_workers=[],
        inspect_run=lambda _run_dir: {"state": "completed", "last_event": "execution_group_completed"},
        max_repair_attempts=3,
    )

    item = next(entry for entry in state["items"] if entry["run_label"] == "sol_run")
    assert item["status"] == "done"
    assert item["retry_count"] == 0


def test_reconcile_queue_with_live_workers_marks_item_dead_after_repeated_repairs(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    running = build_queue_item(
        market="xrp",
        suite_name="xrp_suite",
        run_label="xrp_run",
        action="repair",
        status="running",
        reason="repair rerun",
        retry_count=2,
    )
    upsert_queue_item(root, running)

    state = reconcile_queue_with_live_workers(
        root,
        live_workers=[],
        inspect_run=lambda _run_dir: {"state": "checkpointed", "last_event": "market_cache_resolved"},
        max_repair_attempts=3,
    )

    item = next(entry for entry in state["items"] if entry["run_label"] == "xrp_run")
    assert item["status"] == "dead"
    assert item["retry_count"] == 3


def test_launch_ready_queue_items_marks_selected_items_running(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    upsert_queue_item(
        root,
        build_queue_item(
            market="eth",
            suite_name="eth_repair_suite",
            run_label="eth_repair",
            action="repair",
            status="queued",
            reason="repair first",
            track="direction_dense",
            session_dir=root / "sessions" / "direction_dense",
            program_path=root / "auto_research" / "program_direction_dense.md",
        ),
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_launch_suite",
            run_label="btc_launch",
            action="launch",
            status="queued",
            reason="launch second",
            track="reversal_dense",
            session_dir=root / "sessions" / "reversal_dense",
            program_path=root / "auto_research" / "program_reversal_dense.md",
        ),
    )

    launched: list[str] = []

    state, launched_items = launch_ready_queue_items(
        root,
        live_workers=[{"market": "xrp", "run_label": "xrp_live"}],
        launcher=lambda item: launched.append(str(item["run_label"])) or {"pid": 123},
    )

    assert [item["run_label"] for item in launched_items] == ["eth_repair", "btc_launch"]
    assert launched == ["eth_repair", "btc_launch"]
    running = {item["run_label"] for item in state["items"] if item["status"] == "running"}
    assert running == {"eth_repair", "btc_launch"}


def test_ensure_running_queue_items_seeds_orphan_live_workers(tmp_path: Path) -> None:
    root = tmp_path / "repo"

    state = ensure_running_queue_items(
        root,
        live_workers=[
            {
                "market": "sol",
                "suite_name": "sol_suite",
                "run_label": "sol_live",
            }
        ],
    )

    assert len(state["items"]) == 1
    item = state["items"][0]
    assert item["market"] == "sol"
    assert item["suite_name"] == "sol_suite"
    assert item["run_label"] == "sol_live"
    assert item["status"] == "running"
    assert item["action"] == "resume"


def test_build_autorun_status_report_includes_queue_items(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    autorun_dir = root / "var" / "research" / "autorun"
    autorun_dir.mkdir(parents=True, exist_ok=True)
    (autorun_dir / "codex-background.status.json").write_text('{"state":"idle","iteration":1}', encoding="utf-8")
    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_suite",
            run_label="btc_launch",
            action="launch",
            reason="queued from codex",
        ),
    )

    payload = build_autorun_status_report(root)

    assert payload["queue"]["queue_path"].endswith("experiment-queue.json")
    assert len(payload["queue"]["items"]) == 1
    assert payload["queue"]["items"][0]["run_label"] == "btc_launch"


def test_build_codex_cycle_prompt_includes_queue_snapshot_and_queue_instruction(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "demo"
    session_dir.mkdir(parents=True, exist_ok=True)
    (root / "auto_research").mkdir(parents=True, exist_ok=True)
    (root / "auto_research" / "program.md").write_text("# Demo\n\n- coins: btc, eth, sol, xrp\n", encoding="utf-8")
    (session_dir / "results.tsv").write_text(
        "cycle\tteam\tmetric\tstatus\tdescription\tfiles_changed\ttimestamp\n",
        encoding="utf-8",
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="eth",
            suite_name="eth_suite_repair",
            run_label="eth_repair",
            action="repair",
            status="repair",
            reason="fix before new branch",
        ),
    )

    prompt = build_codex_cycle_prompt(project_root=root, session_dir=session_dir)

    assert "queue snapshot already collected for you:" in prompt.lower()
    assert "eth_repair" in prompt
    assert "queue formal launches and repairs instead of directly filling all slots yourself" in prompt.lower()
