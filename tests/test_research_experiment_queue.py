from __future__ import annotations

from pathlib import Path

from pm15min.research.automation import build_autorun_status_report, build_codex_cycle_prompt
from pm15min.research.automation.queue_state import (
    build_queue_item,
    ensure_running_queue_items,
    launch_ready_queue_items,
    load_experiment_queue,
    reconcile_queue_with_live_workers,
    select_launchable_queue_items,
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
