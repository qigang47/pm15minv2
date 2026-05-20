from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from pm15min.research.automation import build_autorun_status_report, build_codex_cycle_prompt
from pm15min.research.automation.queue_state import (
    build_queue_item,
    ensure_running_queue_items,
    launch_ready_queue_item_batches,
    launch_ready_queue_items,
    load_experiment_queue,
    reconcile_queue_with_live_workers,
    reseed_empty_tracks_from_recent_done,
    save_experiment_queue,
    select_launchable_queue_items,
    set_queue_item_status,
    upsert_queue_item,
)


def _load_experiment_queue_cli_module():
    import importlib.util

    workspace_root = Path(__file__).resolve().parents[1]
    module_path = workspace_root / "auto_research" / "experiment_queue.py"
    spec = importlib.util.spec_from_file_location("experiment_queue_cli_under_test", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_quick_screen_pool_launcher_sets_shared_surface_env_and_pool_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_cli = _load_experiment_queue_cli_module()
    root = tmp_path / "repo"
    (root / "auto_research").mkdir(parents=True)
    (root / "auto_research" / "run_quick_screen_pool.sh").write_text("#!/bin/bash\n", encoding="utf-8")
    item = build_queue_item(
        market="sol",
        suite_name="sol_suite",
        run_label="sol_run",
        action="launch",
        status="queued",
        track="direction_dense",
        session_dir=root / "sessions" / "direction_dense",
        program_path=root / "auto_research" / "program_direction_dense.md",
    )
    captured: dict[str, object] = {}

    class _FakeProcess:
        pid = 24680
        returncode = None

        def wait(self, timeout=None):
            raise subprocess.TimeoutExpired(cmd="pool", timeout=timeout or 0)

    def _fake_popen(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = dict(kwargs.get("env") or {})
        captured["cwd"] = kwargs.get("cwd")
        return _FakeProcess()

    monkeypatch.setattr(queue_cli.subprocess, "Popen", _fake_popen)

    result = queue_cli._queue_pool_launcher(root)([item])

    assert result["pid"] == 24680
    assert result["batch_id"].startswith("quick_screen_pool_direction_dense_")
    assert captured["cwd"] == root
    assert captured["cmd"][0] == str(root / "auto_research" / "run_quick_screen_pool.sh")
    env = captured["env"]
    assert env["PM15MIN_QUICK_SCREEN_SHARED_SURFACES"] == "1"
    assert env["PM15MIN_QUICK_SCREEN_POOL_WORKERS"] == "1"


def test_load_experiment_queue_defaults_to_ten_live_runs_and_twenty_four_queued_items(tmp_path: Path) -> None:
    root = tmp_path / "repo"

    state = load_experiment_queue(root)

    assert state["max_live_runs"] == 10
    assert state["max_queued_items"] == 24
    assert state["track_slot_caps"] == {"direction_dense": 5, "reversal_dense": 5}


def test_memory_gate_reserves_system_memory_and_live_worker_ramp_budget(tmp_path: Path) -> None:
    queue_cli = _load_experiment_queue_cli_module()
    meminfo_path = tmp_path / "meminfo"
    meminfo_path.write_text(
        "\n".join(
            [
                "MemTotal:       67108864 kB",
                "MemAvailable:  37748736 kB",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    gate = queue_cli._memory_gate_payload(
        min_available_mem_gb=4,
        meminfo_path=str(meminfo_path),
        live_workers=[{"pid": 100, "track": "direction_dense", "rss_kb": 4 * 1024 * 1024}],
        launch_mem_gb=16,
    )

    assert gate["state"] == "open"
    assert gate["required_kb"] == 4 * 1024 * 1024
    assert gate["launch_budget_kb"] == 16 * 1024 * 1024
    assert gate["live_worker_reservation_gap_kb"] == 12 * 1024 * 1024
    assert gate["launch_capacity"] == 1


def test_memory_gate_deduplicates_batch_workers_by_pid(tmp_path: Path) -> None:
    queue_cli = _load_experiment_queue_cli_module()
    meminfo_path = tmp_path / "meminfo"
    meminfo_path.write_text(
        "\n".join(
            [
                "MemTotal:       67108864 kB",
                "MemAvailable:  37748736 kB",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    gate = queue_cli._memory_gate_payload(
        min_available_mem_gb=4,
        meminfo_path=str(meminfo_path),
        live_workers=[
            {
                "pid": 100,
                "batch_id": "batch-a",
                "track": "direction_dense",
                "rss_kb": 4 * 1024 * 1024,
                "run_label": "sol",
            },
            {
                "pid": 100,
                "batch_id": "batch-a",
                "track": "direction_dense",
                "rss_kb": 4 * 1024 * 1024,
                "run_label": "xrp",
            },
        ],
        launch_mem_gb=16,
    )

    assert gate["live_worker_count_for_budget"] == 1
    assert gate["live_worker_reservation_gap_kb"] == 12 * 1024 * 1024
    assert gate["launch_capacity"] == 1


def test_memory_gate_counts_new_worker_budget_in_capacity(tmp_path: Path) -> None:
    queue_cli = _load_experiment_queue_cli_module()
    meminfo_path = tmp_path / "meminfo"
    meminfo_path.write_text(
        "\n".join(
            [
                "MemTotal:       67108864 kB",
                "MemAvailable:  10485760 kB",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    gate = queue_cli._memory_gate_payload(
        min_available_mem_gb=1,
        meminfo_path=str(meminfo_path),
        live_workers=[],
        launch_mem_gb=12,
    )

    assert gate["state"] == "blocked"
    assert gate["launch_capacity"] == 0
    assert gate["required_with_next_launch_kb"] == 13 * 1024 * 1024


def test_supervise_once_default_worker_budget_allows_quick_screen_during_two_formals(
    tmp_path: Path,
) -> None:
    queue_cli = _load_experiment_queue_cli_module()
    args = queue_cli._build_parser().parse_args(["supervise-once"])
    meminfo_path = tmp_path / "meminfo"
    meminfo_path.write_text(
        "\n".join(
            [
                "MemTotal:       67108864 kB",
                "MemAvailable:  55574528 kB",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    gate = queue_cli._memory_gate_payload(
        min_available_mem_gb=args.min_available_mem_gb,
        meminfo_path=str(meminfo_path),
        live_workers=[
            {"pid": 100, "rss_kb": 2 * 1024 * 1024, "run_label": "btc_formal"},
            {"pid": 200, "rss_kb": 2 * 1024 * 1024, "run_label": "eth_formal"},
        ],
        launch_mem_gb=args.quick_screen_worker_mem_gb,
    )

    assert args.quick_screen_worker_mem_gb == 16.0
    assert gate["state"] == "open"
    assert gate["live_worker_count_for_budget"] == 0
    assert gate["live_worker_reservation_gap_kb"] == 0
    assert gate["launch_capacity"] == 3


def test_memory_gate_reserves_ramp_budget_only_for_quick_screen_workers(tmp_path: Path) -> None:
    queue_cli = _load_experiment_queue_cli_module()
    meminfo_path = tmp_path / "meminfo"
    meminfo_path.write_text(
        "\n".join(
            [
                "MemTotal:       67108864 kB",
                "MemAvailable:  55574528 kB",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    gate = queue_cli._memory_gate_payload(
        min_available_mem_gb=1,
        meminfo_path=str(meminfo_path),
        live_workers=[
            {"pid": 100, "rss_kb": 2 * 1024 * 1024, "run_label": "btc_formal"},
            {"pid": 200, "rss_kb": 2 * 1024 * 1024, "run_label": "eth_formal"},
            {
                "pid": 300,
                "batch_id": "batch-a",
                "track": "reversal_dense",
                "rss_kb": 4 * 1024 * 1024,
                "run_label": "sol_reversal",
            },
        ],
        launch_mem_gb=24,
    )

    assert gate["live_worker_count_for_budget"] == 1
    assert gate["live_worker_reservation_gap_kb"] == 20 * 1024 * 1024
    assert gate["launch_capacity"] == 1


def test_cli_supervise_once_skips_launch_when_available_memory_is_low(tmp_path: Path) -> None:
    workspace_root = Path(__file__).resolve().parents[1]
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    upsert_queue_item(
        root,
        build_queue_item(
            market="sol",
            suite_name="sol_suite",
            run_label="sol_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
    )

    result = subprocess.run(
        [
            sys.executable,
            str(workspace_root / "auto_research" / "experiment_queue.py"),
            "--root",
            str(root),
            "supervise-once",
            "--max-live-runs",
            "1",
            "--min-available-mem-gb",
            "32",
            "--meminfo-path",
            str(tmp_path / "meminfo"),
        ],
        cwd=workspace_root,
        env={**os.environ, "PYTHONPATH": str(workspace_root / "src")},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["memory_gate"]["state"] == "blocked"
    assert payload["launched"] == []
    assert payload["memory_gate"]["required_kb"] == 32 * 1024 * 1024
    state = load_experiment_queue(root)
    item = next(entry for entry in state["items"] if entry["run_label"] == "sol_run")
    assert item["status"] == "queued"


def test_cli_supervise_once_marks_unsupported_feature_set_dead_before_launch(tmp_path: Path) -> None:
    workspace_root = Path(__file__).resolve().parents[1]
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "reversal_dense"
    program_path = root / "auto_research" / "program_reversal_dense.md"
    suite_dir = root / "research" / "experiments" / "suite_specs"
    suite_dir.mkdir(parents=True, exist_ok=True)
    (suite_dir / "bad_suite.json").write_text(
        json.dumps(
            {
                "suite_name": "bad_suite",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "missing_feature_set",
                "label_set": "truth",
                "target": "reversal",
                "offsets": [7, 8, 9],
                "window": {"start": "2026-04-01", "end": "2026-04-02"},
                "markets": ["sol"],
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "meminfo").write_text(
        "MemTotal:       67108864 kB\nMemAvailable:  62914560 kB\n",
        encoding="utf-8",
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="sol",
            suite_name="bad_suite",
            run_label="bad_run",
            action="repair",
            status="repair",
            track="reversal_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
    )

    result = subprocess.run(
        [
            sys.executable,
            str(workspace_root / "auto_research" / "experiment_queue.py"),
            "--root",
            str(root),
            "supervise-once",
            "--max-live-runs",
            "8",
            "--max-launches-per-pass",
            "8",
            "--min-available-mem-gb",
            "4",
            "--quick-screen-worker-mem-gb",
            "16",
            "--meminfo-path",
            str(tmp_path / "meminfo"),
        ],
        cwd=workspace_root,
        env={**os.environ, "PYTHONPATH": str(workspace_root / "src")},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["launched"] == []
    state = load_experiment_queue(root)
    item = next(entry for entry in state["items"] if entry["run_label"] == "bad_run")
    assert item["status"] == "dead"
    assert item["action"] == "blocked"
    assert item["reason"] == "launch_preflight_failed"
    assert "Unsupported feature_set" in item["last_error"]


def test_cli_supervise_once_reports_pending_queue_separately_from_history(tmp_path: Path) -> None:
    workspace_root = Path(__file__).resolve().parents[1]
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    for status, run_label in (
        ("queued", "sol_pending"),
        ("done", "sol_done"),
        ("dead", "sol_dead"),
    ):
        upsert_queue_item(
            root,
            build_queue_item(
                market="sol",
                suite_name=f"sol_{status}",
                run_label=run_label,
                action="launch",
                status=status,
                track="direction_dense",
                session_dir=session_dir,
                program_path=program_path,
            ),
        )

    result = subprocess.run(
        [
            sys.executable,
            str(workspace_root / "auto_research" / "experiment_queue.py"),
            "--root",
            str(root),
            "supervise-once",
            "--max-live-runs",
            "1",
            "--min-available-mem-gb",
            "32",
            "--meminfo-path",
            str(tmp_path / "meminfo"),
        ],
        cwd=workspace_root,
        env={**os.environ, "PYTHONPATH": str(workspace_root / "src")},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["queue_items"] == 1
    assert payload["pending_queue_items"] == 1
    assert payload["total_queue_items"] == 3
    assert payload["queue_status_counts"] == {"dead": 1, "done": 1, "queued": 1}


def test_upsert_queue_item_prunes_low_priority_pending_items_beyond_queue_cap(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    state = load_experiment_queue(root)
    state["max_queued_items"] = 2
    from pm15min.research.automation.queue_state import save_experiment_queue

    save_experiment_queue(root, state)

    upsert_queue_item(
        root,
        build_queue_item(
            market="btc",
            suite_name="btc_suite",
            run_label="btc_run",
            action="launch",
            status="queued",
            priority=100,
        ),
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="eth",
            suite_name="eth_suite",
            run_label="eth_run",
            action="launch",
            status="queued",
            priority=90,
        ),
    )
    state = upsert_queue_item(
        root,
        build_queue_item(
            market="sol",
            suite_name="sol_suite",
            run_label="sol_run",
            action="launch",
            status="queued",
            priority=10,
        ),
    )

    queued = [item["run_label"] for item in state["items"] if item["status"] in {"queued", "repair"}]
    assert queued == ["btc_run", "eth_run"]


def test_upsert_queue_item_keeps_distinct_normal_candidates_for_same_market_and_track(tmp_path: Path) -> None:
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
    assert {(item["suite_name"], item["run_label"]) for item in queued} == {
        ("btc_suite_old", "btc_old"),
        ("btc_suite_new", "btc_new"),
    }


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


def test_select_launchable_queue_items_allows_multiple_branches_for_same_market_and_track(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    for suffix, priority in (("a", 100), ("b", 90), ("c", 80)):
        upsert_queue_item(
            root,
            build_queue_item(
                market="btc",
                suite_name=f"btc_direction_{suffix}",
                run_label=f"btc_direction_{suffix}",
                action="launch",
                status="queued",
                track="direction_dense",
                priority=priority,
            ),
        )

    payload = load_experiment_queue(root)
    payload["max_live_runs"] = 3
    payload["track_slot_caps"] = {"direction_dense": 3, "reversal_dense": 0}
    selected = select_launchable_queue_items(payload, max_live_runs=3, live_workers=[])

    assert [item["run_label"] for item in selected] == [
        "btc_direction_a",
        "btc_direction_b",
        "btc_direction_c",
    ]


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


def test_select_launchable_queue_items_does_not_let_cross_track_shared_runs_block_market(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "dense"
    direction_program = root / "auto_research" / "program_direction_dense_sol_xrp.md"
    reversal_program = root / "auto_research" / "program_reversal_dense_sol_xrp.md"
    payload = load_experiment_queue(root)
    payload["max_live_runs"] = 10
    payload["track_slot_caps"] = {"direction_dense": 5, "reversal_dense": 5}
    payload["items"] = [
        build_queue_item(
            market="sol",
            suite_name="sol_reversal_running",
            run_label="sol_reversal_running",
            action="repair",
            status="running",
            track="reversal_dense",
            session_dir=session_dir,
            program_path=reversal_program,
        ),
        build_queue_item(
            market="xrp",
            suite_name="xrp_reversal_running",
            run_label="xrp_reversal_running",
            action="repair",
            status="running",
            track="reversal_dense",
            session_dir=session_dir,
            program_path=reversal_program,
        ),
        build_queue_item(
            market="sol",
            suite_name="sol_direction_queued",
            run_label="sol_direction_queued",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=direction_program,
        ),
        build_queue_item(
            market="xrp",
            suite_name="xrp_direction_queued",
            run_label="xrp_direction_queued",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=direction_program,
        ),
    ]

    selected = select_launchable_queue_items(payload, max_live_runs=10, live_workers=[])

    assert [item["run_label"] for item in selected] == [
        "sol_direction_queued",
        "xrp_direction_queued",
    ]


def test_select_launchable_queue_items_allows_distinct_same_market_branches_when_track_has_room(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "dense"
    program_path = root / "auto_research" / "program_direction_dense_sol_xrp.md"
    payload = load_experiment_queue(root)
    payload["max_live_runs"] = 10
    payload["track_slot_caps"] = {"direction_dense": 5, "reversal_dense": 5}
    payload["items"] = [
        build_queue_item(
            market="xrp",
            suite_name="xrp_direction_running",
            run_label="xrp_direction_running",
            action="launch",
            status="running",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="xrp",
            suite_name="xrp_direction_followup_a",
            run_label="xrp_direction_followup_a",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="xrp",
            suite_name="xrp_direction_followup_b",
            run_label="xrp_direction_followup_b",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
    ]

    selected = select_launchable_queue_items(payload, max_live_runs=10, live_workers=[])

    assert [item["run_label"] for item in selected] == [
        "xrp_direction_followup_a",
        "xrp_direction_followup_b",
    ]


def test_fixed_track_slot_caps_env_overrides_queue_payload(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "repo"
    monkeypatch.setenv(
        "PM15MIN_FIXED_TRACK_SLOT_CAPS_JSON",
        '{"direction_dense":8,"reversal_dense":8}',
    )

    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 16, "reversal_dense": 0}
    save_experiment_queue(root, payload)

    state = load_experiment_queue(root)

    assert state["track_slot_caps"] == {"direction_dense": 8, "reversal_dense": 8}


def test_allowed_queue_markets_env_filters_queue_items_and_blocks_disallowed_upserts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "repo"
    monkeypatch.setenv("PM15MIN_ALLOWED_QUEUE_MARKETS", "xrp")

    upsert_queue_item(
        root,
        build_queue_item(
            market="xrp",
            suite_name="xrp_direction",
            run_label="xrp_direction",
            action="launch",
            status="queued",
            track="direction_dense",
        ),
    )
    upsert_queue_item(
        root,
        build_queue_item(
            market="sol",
            suite_name="sol_direction",
            run_label="sol_direction",
            action="launch",
            status="queued",
            track="direction_dense",
        ),
    )

    payload = load_experiment_queue(root)

    assert [item["market"] for item in payload["items"]] == ["xrp"]


def test_select_launchable_queue_items_ignores_live_workers_outside_allowed_markets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "repo"
    monkeypatch.setenv("PM15MIN_ALLOWED_QUEUE_MARKETS", "sol,xrp")

    upsert_queue_item(
        root,
        build_queue_item(
            market="sol",
            suite_name="sol_direction",
            run_label="sol_direction",
            action="launch",
            status="queued",
            track="direction_dense",
        ),
    )
    payload = load_experiment_queue(root)
    payload["max_live_runs"] = 1

    selected = select_launchable_queue_items(
        payload,
        max_live_runs=1,
        live_workers=[
            {
                "market": "btc",
                "suite_name": "btc_formal",
                "run_label": "btc_formal",
                "track": "direction_midprice",
            }
        ],
    )

    assert [item["run_label"] for item in selected] == ["sol_direction"]


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


def test_reconcile_queue_with_live_workers_marks_finished_quick_screen_run_done(tmp_path: Path) -> None:
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

    run_dir = root / "research" / "experiments" / "runs" / "suite=btc_suite" / "run=btc_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "quick_screen_summary.json").write_text(
        '{"suite_name":"btc_suite","run_label":"btc_run","rows":4,"selected_rows":1,"markets":["btc"]}',
        encoding="utf-8",
    )

    state = reconcile_queue_with_live_workers(
        root,
        live_workers=[],
    )

    item = next(entry for entry in state["items"] if entry["run_label"] == "btc_run")
    assert item["status"] == "done"


def test_dense_quick_screen_selects_sol_xrp_runs_independently(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 4, "reversal_dense": 0}
    payload["items"] = [
        build_queue_item(
            market="sol",
            suite_name="shared_dense_suite",
            run_label="shared_dense_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="xrp",
            suite_name="shared_dense_suite",
            run_label="shared_dense_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
    ]

    selected = select_launchable_queue_items(payload, max_live_runs=4, live_workers=[])

    assert [item["market"] for item in selected] == ["sol", "xrp"]


def test_launch_ready_queue_items_launches_sol_xrp_runs_independently(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 4, "reversal_dense": 0}
    payload["items"] = [
        build_queue_item(
            market="sol",
            suite_name="shared_dense_suite",
            run_label="shared_dense_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="xrp",
            suite_name="shared_dense_suite",
            run_label="shared_dense_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
    ]
    save_experiment_queue(root, payload)
    launched: list[dict[str, object]] = []

    def launcher(item: dict[str, object]) -> dict[str, object]:
        launched.append(dict(item))
        return {"pid": 12345}

    state, launched_items = launch_ready_queue_items(
        root,
        live_workers=[],
        launcher=launcher,
        max_live_runs=4,
    )

    assert [item["market"] for item in launched] == ["sol", "xrp"]
    assert {item["market"] for item in launched_items} == {"sol", "xrp"}
    statuses = {item["market"]: item["status"] for item in state["items"]}
    assert statuses == {"sol": "running", "xrp": "running"}
    assert {item["market"]: item.get("pid") for item in state["items"]} == {"sol": 12345, "xrp": 12345}


def test_launch_ready_queue_item_batches_marks_dense_candidates_running_with_one_process(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 5, "reversal_dense": 0}
    payload["items"] = [
        build_queue_item(
            market=market,
            suite_name=f"{market}_suite_{index}",
            run_label=f"{market}_run_{index}",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        )
        for index, market in enumerate(("sol", "xrp", "sol"), start=1)
    ]
    save_experiment_queue(root, payload)
    launched_batches: list[list[dict[str, object]]] = []

    def batch_launcher(items: list[dict[str, object]]) -> dict[str, object]:
        launched_batches.append([dict(item) for item in items])
        return {"pid": 12345, "batch_id": "batch-abc", "manifest_path": str(root / "manifest.json")}

    state, launched_items = launch_ready_queue_item_batches(
        root,
        live_workers=[],
        batch_launcher=batch_launcher,
        max_live_runs=5,
        max_new_launches=1,
        quick_screen_batch_size=3,
    )

    assert {item["run_label"] for batch in launched_batches for item in batch} == {
        "sol_run_1",
        "xrp_run_2",
        "sol_run_3",
    }
    assert {item["run_label"] for item in launched_items} == {"sol_run_1", "xrp_run_2", "sol_run_3"}
    assert {item["status"] for item in state["items"]} == {"running"}
    assert {item.get("pid") for item in state["items"]} == {12345}
    assert {item.get("batch_id") for item in state["items"]} == {"batch-abc"}
    assert {item.get("batch_manifest_path") for item in state["items"]} == {str(root / "manifest.json")}


def test_launch_ready_queue_item_batches_ignores_disallowed_btc_eth_live_workers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    monkeypatch.setenv("PM15MIN_ALLOWED_QUEUE_MARKETS", "sol,xrp")
    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 5, "reversal_dense": 0}
    payload["items"] = [
        build_queue_item(
            market="sol",
            suite_name="sol_suite",
            run_label="sol_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        )
    ]
    save_experiment_queue(root, payload)
    launched_batches: list[list[dict[str, object]]] = []

    def batch_launcher(items: list[dict[str, object]]) -> dict[str, object]:
        launched_batches.append([dict(item) for item in items])
        return {"pid": 12345, "batch_id": "batch-sol"}

    _state, launched_items = launch_ready_queue_item_batches(
        root,
        live_workers=[
            {"market": "btc", "suite_name": "btc_suite", "run_label": "btc_run", "track": "direction_dense"},
            {"market": "eth", "suite_name": "eth_suite", "run_label": "eth_run", "track": "direction_dense"},
        ],
        batch_launcher=batch_launcher,
        max_live_runs=2,
        max_new_launches=1,
        quick_screen_batch_size=5,
    )

    assert [item["run_label"] for batch in launched_batches for item in batch] == ["sol_run"]
    assert [item["run_label"] for item in launched_items] == ["sol_run"]


def test_reconcile_queue_with_live_workers_reconciles_sol_xrp_runs_independently(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    for market in ("sol", "xrp"):
        upsert_queue_item(
            root,
            build_queue_item(
                market=market,
                suite_name="shared_dense_suite",
                run_label="shared_dense_run",
                action="launch",
                status="running",
                track="direction_dense",
                session_dir=session_dir,
                program_path=program_path,
            ),
        )

    state = reconcile_queue_with_live_workers(
        root,
        live_workers=[
            {
                "market": "sol",
                "suite_name": "shared_dense_suite",
                "run_label": "shared_dense_run",
                "track": "direction_dense",
            }
        ],
    )

    assert {item["market"]: item["status"] for item in state["items"]} == {"sol": "running", "xrp": "repair"}


def test_reconcile_queue_with_live_workers_keeps_batch_items_running(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    for index, market in enumerate(("sol", "xrp"), start=1):
        item = build_queue_item(
            market=market,
            suite_name=f"{market}_suite_{index}",
            run_label=f"{market}_run_{index}",
            action="launch",
            status="running",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        )
        item["batch_id"] = "batch-abc"
        upsert_queue_item(root, item)

    state = reconcile_queue_with_live_workers(
        root,
        live_workers=[
            {
                "suite_name": "sol_suite_1",
                "run_label": "sol_run_1",
                "market": "sol",
                "track": "direction_dense",
                "batch_id": "batch-abc",
            }
        ],
    )

    assert {item["run_label"]: item["status"] for item in state["items"]} == {
        "sol_run_1": "running",
        "xrp_run_2": "running",
    }


def test_batch_selection_skips_live_batch_item_but_fills_remaining_track_capacity(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "reversal_dense"
    program_path = root / "auto_research" / "program_reversal_dense.md"
    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 0, "reversal_dense": 4}
    payload["items"] = [
        build_queue_item(
            market="sol",
            suite_name="sol_repair_suite",
            run_label="sol_repair_run",
            action="repair",
            status="repair",
            track="reversal_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="xrp",
            suite_name="xrp_fresh_suite",
            run_label="xrp_fresh_run",
            action="launch",
            status="queued",
            track="reversal_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
    ]
    payload["items"][0]["batch_id"] = "batch-live"
    save_experiment_queue(root, payload)
    launched_batches: list[list[dict[str, object]]] = []

    def batch_launcher(items: list[dict[str, object]]) -> dict[str, object]:
        launched_batches.append([dict(item) for item in items])
        return {"pid": 22222, "batch_id": "batch-new"}

    _state, launched_items = launch_ready_queue_item_batches(
        root,
        live_workers=[
            {
                "batch_id": "batch-live",
                "market": "sol",
                "suite_name": "sol_repair_suite",
                "run_label": "sol_repair_run",
                "track": "reversal_dense",
            }
        ],
        batch_launcher=batch_launcher,
        max_live_runs=4,
        max_new_launches=1,
        quick_screen_batch_size=4,
    )

    assert [[item["run_label"] for item in batch] for batch in launched_batches] == [["xrp_fresh_run"]]
    assert [item["run_label"] for item in launched_items] == ["xrp_fresh_run"]
    statuses = {item["run_label"]: item["status"] for item in _state["items"]}
    assert statuses == {"sol_repair_run": "repair", "xrp_fresh_run": "running"}


def test_batch_selection_allows_dense_track_to_fill_remaining_batch_slots(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "reversal_dense"
    program_path = root / "auto_research" / "program_reversal_dense.md"
    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 0, "reversal_dense": 4}
    payload["items"] = [
        build_queue_item(
            market="sol",
            suite_name="sol_live_suite",
            run_label="sol_live_run",
            action="repair",
            status="running",
            track="reversal_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="xrp",
            suite_name="xrp_wait_suite",
            run_label="xrp_wait_run",
            action="launch",
            status="queued",
            track="reversal_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
    ]
    payload["items"][0]["batch_id"] = "batch-live"
    save_experiment_queue(root, payload)
    launched_batches: list[list[dict[str, object]]] = []

    def batch_launcher(items: list[dict[str, object]]) -> dict[str, object]:
        launched_batches.append([dict(item) for item in items])
        return {"pid": 22222, "batch_id": "batch-new"}

    _state, launched_items = launch_ready_queue_item_batches(
        root,
        live_workers=[
            {
                "batch_id": "batch-live",
                "market": "sol",
                "suite_name": "sol_live_suite",
                "run_label": "sol_live_run",
                "track": "reversal_dense",
            }
        ],
        batch_launcher=batch_launcher,
        max_live_runs=4,
        max_new_launches=1,
        quick_screen_batch_size=4,
    )

    assert [[item["run_label"] for item in batch] for batch in launched_batches] == [["xrp_wait_run"]]
    assert [item["run_label"] for item in launched_items] == ["xrp_wait_run"]
    statuses = {item["run_label"]: item["status"] for item in _state["items"]}
    assert statuses == {"sol_live_run": "running", "xrp_wait_run": "running"}


def test_pool_mode_allows_other_market_pool_when_same_track_pool_is_live(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 5, "reversal_dense": 5}
    payload["items"] = [
        build_queue_item(
            market="sol",
            suite_name="sol_live_suite",
            run_label="sol_live_run",
            action="launch",
            status="running",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="xrp",
            suite_name="xrp_wait_suite",
            run_label="xrp_wait_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="xrp",
            suite_name="xrp_reversal_suite",
            run_label="xrp_reversal_run",
            action="launch",
            status="queued",
            track="reversal_dense",
            session_dir=root / "sessions" / "reversal_dense",
            program_path=root / "auto_research" / "program_reversal_dense.md",
        ),
    ]
    payload["items"][0]["batch_id"] = "direction-pool-live"
    save_experiment_queue(root, payload)
    launched_batches: list[list[dict[str, object]]] = []

    def batch_launcher(items: list[dict[str, object]]) -> dict[str, object]:
        launched_batches.append([dict(item) for item in items])
        return {"pid": 22222, "batch_id": "reversal-pool-new"}

    _state, launched_items = launch_ready_queue_item_batches(
        root,
        live_workers=[
            {
                "batch_id": "direction-pool-live",
                "market": "sol",
                "suite_name": "sol_live_suite",
                "run_label": "sol_live_run",
                "track": "direction_dense",
                "cmd": "python scripts/research/run_quick_screen_pool.py --batch-id direction-pool-live",
            }
        ],
        batch_launcher=batch_launcher,
        max_live_runs=10,
        max_new_launches=10,
        quick_screen_batch_size=10,
        single_pool_per_track=True,
    )

    assert [[item["run_label"] for item in batch] for batch in launched_batches] == [
        ["xrp_reversal_run"],
        ["xrp_wait_run"],
    ]
    assert [item["run_label"] for item in launched_items] == ["xrp_reversal_run", "xrp_wait_run"]
    statuses = {item["run_label"]: item["status"] for item in _state["items"]}
    assert statuses["sol_live_run"] == "running"
    assert statuses["xrp_wait_run"] == "running"
    assert statuses["xrp_reversal_run"] == "running"


def test_pool_mode_isolates_sol_xrp_batches_by_market(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 5, "reversal_dense": 0}
    payload["items"] = [
        build_queue_item(
            market=market,
            suite_name=f"{market}_suite_{index}",
            run_label=f"{market}_run_{index}",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        )
        for index, market in enumerate(("sol", "xrp", "sol", "xrp"), start=1)
    ]
    save_experiment_queue(root, payload)
    launched_batches: list[list[dict[str, object]]] = []

    def batch_launcher(items: list[dict[str, object]]) -> dict[str, object]:
        launched_batches.append([dict(item) for item in items])
        market = str(items[0]["market"])
        return {
            "pid": 10000 + len(launched_batches),
            "batch_id": f"quick_screen_pool_direction_dense_{market}",
            "manifest_path": str(root / f"{market}.manifest.json"),
        }

    state, launched_items = launch_ready_queue_item_batches(
        root,
        live_workers=[],
        batch_launcher=batch_launcher,
        max_live_runs=10,
        max_new_launches=10,
        quick_screen_batch_size=10,
        single_pool_per_track=True,
    )

    assert [[item["market"] for item in batch] for batch in launched_batches] == [
        ["sol", "sol"],
        ["xrp", "xrp"],
    ]
    assert {item["run_label"] for item in launched_items} == {
        "sol_run_1",
        "sol_run_3",
        "xrp_run_2",
        "xrp_run_4",
    }
    items_by_label = {str(item["run_label"]): item for item in state["items"]}
    assert items_by_label["sol_run_1"]["batch_id"] == "quick_screen_pool_direction_dense_sol"
    assert items_by_label["sol_run_3"]["batch_id"] == "quick_screen_pool_direction_dense_sol"
    assert items_by_label["xrp_run_2"]["batch_id"] == "quick_screen_pool_direction_dense_xrp"
    assert items_by_label["xrp_run_4"]["batch_id"] == "quick_screen_pool_direction_dense_xrp"


def test_pool_mode_allows_other_market_when_same_track_market_pool_is_live(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 5, "reversal_dense": 0}
    payload["items"] = [
        build_queue_item(
            market="sol",
            suite_name="sol_live_suite",
            run_label="sol_live_run",
            action="launch",
            status="running",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="sol",
            suite_name="sol_wait_suite",
            run_label="sol_wait_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="xrp",
            suite_name="xrp_wait_suite",
            run_label="xrp_wait_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
    ]
    payload["items"][0]["batch_id"] = "quick_screen_pool_direction_dense_sol"
    save_experiment_queue(root, payload)
    launched_batches: list[list[dict[str, object]]] = []

    def batch_launcher(items: list[dict[str, object]]) -> dict[str, object]:
        launched_batches.append([dict(item) for item in items])
        return {"pid": 22222, "batch_id": "quick_screen_pool_direction_dense_xrp"}

    _state, launched_items = launch_ready_queue_item_batches(
        root,
        live_workers=[
            {
                "batch_id": "quick_screen_pool_direction_dense_sol",
                "market": "sol",
                "suite_name": "sol_live_suite",
                "run_label": "sol_live_run",
                "track": "direction_dense",
                "cmd": "python scripts/research/run_quick_screen_pool.py --batch-id quick_screen_pool_direction_dense_sol",
            }
        ],
        batch_launcher=batch_launcher,
        max_live_runs=10,
        max_new_launches=10,
        quick_screen_batch_size=10,
        single_pool_per_track=True,
    )

    assert [[item["run_label"] for item in batch] for batch in launched_batches] == [["xrp_wait_run"]]
    assert [item["run_label"] for item in launched_items] == ["xrp_wait_run"]
    statuses = {item["run_label"]: item["status"] for item in _state["items"]}
    assert statuses["sol_live_run"] == "running"
    assert statuses["sol_wait_run"] == "queued"
    assert statuses["xrp_wait_run"] == "running"


def test_reseed_empty_tracks_from_recent_done_repairs_recent_completed_items(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 2, "reversal_dense": 0}

    items = [
        build_queue_item(
            market="btc",
            suite_name="btc_suite_new",
            run_label="btc_run_new",
            action="launch",
            status="done",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="eth",
            suite_name="eth_suite_new",
            run_label="eth_run_new",
            action="launch",
            status="done",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="btc",
            suite_name="btc_suite_old",
            run_label="btc_run_old",
            action="launch",
            status="done",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
    ]
    items[0]["updated_at"] = items[0]["created_at"] = "2026-04-17T13:00:00Z"
    items[1]["updated_at"] = items[1]["created_at"] = "2026-04-17T12:59:00Z"
    items[2]["updated_at"] = items[2]["created_at"] = "2026-04-17T12:00:00Z"
    payload["items"] = items
    save_experiment_queue(root, payload)

    for suite_name, run_label in (
        ("btc_suite_new", "btc_run_new"),
        ("eth_suite_new", "eth_run_new"),
        ("btc_suite_old", "btc_run_old"),
    ):
        run_dir = root / "research" / "experiments" / "runs" / f"suite={suite_name}" / f"run={run_label}"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "quick_screen_summary.json").write_text(
            json.dumps(
                {
                    "suite_name": suite_name,
                    "run_label": run_label,
                    "rows": 4,
                    "selected_rows": 1,
                    "markets": ["btc" if suite_name.startswith("btc") else "eth"],
                }
            ),
            encoding="utf-8",
        )

    state, reseeded = reseed_empty_tracks_from_recent_done(root, live_workers=[])

    assert {item["run_label"] for item in reseeded} == {"btc_run_new", "eth_run_new"}
    statuses = {item["run_label"]: item["status"] for item in state["items"]}
    assert statuses["btc_run_new"] == "repair"
    assert statuses["eth_run_new"] == "repair"
    assert statuses["btc_run_old"] == "done"


def test_reseed_empty_tracks_from_recent_done_skips_completed_auto_refill_repairs(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 1, "reversal_dense": 0}
    item = build_queue_item(
        market="sol",
        suite_name="sol_suite",
        run_label="sol_run",
        action="repair",
        status="done",
        reason="auto_refill_underfilled_track_from_recent_done",
        track="direction_dense",
        session_dir=session_dir,
        program_path=program_path,
    )
    payload["items"] = [item]
    save_experiment_queue(root, payload)

    run_dir = root / "research" / "experiments" / "runs" / "suite=sol_suite" / "run=sol_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "quick_screen_summary.json").write_text(
        '{"suite_name":"sol_suite","run_label":"sol_run","rows":4,"selected_rows":1,"markets":["sol"]}',
        encoding="utf-8",
    )

    state, reseeded = reseed_empty_tracks_from_recent_done(root, live_workers=[])

    assert reseeded == []
    saved_item = next(entry for entry in state["items"] if entry["run_label"] == "sol_run")
    assert saved_item["status"] == "done"


def test_reseed_empty_tracks_from_recent_done_skips_sparse_recent_failures(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 1, "reversal_dense": 0}
    item = build_queue_item(
        market="sol",
        suite_name="sol_sparse_suite",
        run_label="sol_sparse_run",
        action="launch",
        status="done",
        reason="reject_sparse_9_trades_below_dense_floor_width_move_required",
        track="direction_dense",
        session_dir=session_dir,
        program_path=program_path,
    )
    item["research_meta"] = {
        "feature_width": "56",
        "primary_lever": "factor_family_rework",
        "model_family": "deep_otm",
    }
    payload["items"] = [item]
    save_experiment_queue(root, payload)

    state, reseeded = reseed_empty_tracks_from_recent_done(
        root,
        live_workers=[],
        inspect_run=lambda _run_dir: {"state": "completed", "top_case": {"trades": 9}},
    )

    assert reseeded == []
    saved_item = next(entry for entry in state["items"] if entry["run_label"] == "sol_sparse_run")
    assert saved_item["status"] == "done"


def test_upsert_queue_item_blocks_same_route_after_repeated_sparse_attempts(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    suite_dir = root / "research" / "experiments" / "suite_specs"
    run_root = root / "research" / "experiments" / "runs"
    suite_dir.mkdir(parents=True, exist_ok=True)
    custom_sets = root / "research" / "experiments" / "custom_feature_sets.json"
    custom_sets.parent.mkdir(parents=True, exist_ok=True)
    custom_sets.write_text(
        json.dumps(
            {
                "focus_xrp_40_old": {
                    "width": 40,
                    "columns": ["ret_from_cycle_open", "ret_from_strike", "move_z"],
                },
                "focus_xrp_40_new": {
                    "width": 40,
                    "columns": ["ret_from_cycle_open", "ret_from_strike", "move_z"],
                },
                "focus_xrp_44_new": {
                    "width": 44,
                    "columns": ["ret_from_cycle_open", "ret_from_strike", "move_z", "basis_bp"],
                },
            }
        ),
        encoding="utf-8",
    )

    def write_suite(suite_name: str, feature_set: str) -> None:
        (suite_dir / f"{suite_name}.json").write_text(
            json.dumps(
                {
                    "suite_name": suite_name,
                    "window": {"start": "2025-10-27", "end": "2026-04-15"},
                    "decision_start": "2026-04-15",
                    "decision_end": "2026-05-07",
                    "markets": {
                        "xrp": {
                            "groups": {
                                "direction": {
                                    "runs": [
                                        {
                                            "run_name": "xrp",
                                            "target": "direction",
                                            "model_family": "deep_otm",
                                            "feature_set_variants": [
                                                {"label": "candidate", "feature_set": feature_set}
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

    def write_completed_run(index: int) -> None:
        suite_name = f"xrp_sparse_{index}"
        run_label = f"xrp_sparse_run_{index}"
        write_suite(suite_name, "focus_xrp_40_old")
        run_dir = run_root / f"suite={suite_name}" / f"run={run_label}"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "quick_screen_summary.json").write_text(
            json.dumps({"suite_name": suite_name, "run_label": run_label, "rows": 1, "markets": ["xrp"]}),
            encoding="utf-8",
        )
        (run_dir / "quick_screen_leaderboard.csv").write_text(
            "feature_set,trades,profitable_pool_capture_rows,profitable_pool_correct_side_rows,density_bottleneck\n"
            'focus_xrp_40_old,18,0,2,"{""primary_bottleneck"": ""low_trade_density""}"\n',
            encoding="utf-8",
        )
        stamp = 1_800_000_000 + index
        os.utime(run_dir / "quick_screen_summary.json", (stamp, stamp))

    for index in range(3):
        write_completed_run(index)

    write_suite("xrp_same_route", "focus_xrp_40_new")
    upsert_queue_item(
        root,
        build_queue_item(
            market="xrp",
            suite_name="xrp_same_route",
            run_label="xrp_same_route_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
    )
    state = load_experiment_queue(root)
    blocked = next(item for item in state["items"] if item["run_label"] == "xrp_same_route_run")
    assert blocked["status"] == "dead"
    assert blocked["action"] == "blocked"
    assert "required feature_width" in str(blocked["last_error"])

    write_suite("xrp_wider_route", "focus_xrp_44_new")
    upsert_queue_item(
        root,
        build_queue_item(
            market="xrp",
            suite_name="xrp_wider_route",
            run_label="xrp_wider_route_run",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
    )
    state = load_experiment_queue(root)
    allowed = next(item for item in state["items"] if item["run_label"] == "xrp_wider_route_run")
    assert allowed["status"] == "queued"


def test_reseed_empty_tracks_from_recent_done_skips_track_with_pending_or_live_work(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    session_dir = root / "sessions" / "direction_dense"
    program_path = root / "auto_research" / "program_direction_dense.md"
    payload = load_experiment_queue(root)
    payload["track_slot_caps"] = {"direction_dense": 2, "reversal_dense": 1}
    payload["items"] = [
        build_queue_item(
            market="btc",
            suite_name="btc_done",
            run_label="btc_done",
            action="launch",
            status="done",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="eth",
            suite_name="eth_pending",
            run_label="eth_pending",
            action="launch",
            status="queued",
            track="direction_dense",
            session_dir=session_dir,
            program_path=program_path,
        ),
        build_queue_item(
            market="sol",
            suite_name="sol_done",
            run_label="sol_done",
            action="launch",
            status="done",
            track="reversal_dense",
            session_dir=root / "sessions" / "reversal_dense",
            program_path=root / "auto_research" / "program_reversal_dense.md",
        ),
    ]
    save_experiment_queue(root, payload)

    for suite_name, run_label in (("btc_done", "btc_done"), ("sol_done", "sol_done")):
        run_dir = root / "research" / "experiments" / "runs" / f"suite={suite_name}" / f"run={run_label}"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "quick_screen_summary.json").write_text(
            '{"suite_name":"demo","run_label":"demo","rows":4,"selected_rows":1,"markets":["btc"]}',
            encoding="utf-8",
        )

    state, reseeded = reseed_empty_tracks_from_recent_done(
        root,
        live_workers=[{"market": "sol", "suite_name": "sol_live", "run_label": "sol_live", "track": "reversal_dense"}],
    )

    assert reseeded == []
    statuses = {item["run_label"]: item["status"] for item in state["items"]}
    assert statuses["btc_done"] == "done"
    assert statuses["eth_pending"] == "queued"
    assert statuses["sol_done"] == "done"


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


def test_launch_ready_queue_items_can_limit_new_launches_per_pass(tmp_path: Path) -> None:
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
        live_workers=[],
        launcher=lambda item: launched.append(str(item["run_label"])) or {"pid": 123},
        max_live_runs=4,
        max_new_launches=1,
    )

    assert [item["run_label"] for item in launched_items] == ["eth_repair"]
    assert launched == ["eth_repair"]
    statuses = {item["run_label"]: item["status"] for item in state["items"]}
    assert statuses["eth_repair"] == "running"
    assert statuses["btc_launch"] == "queued"


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
    assert payload["queue"]["summary"]["total_items"] == 1
    assert payload["queue"]["summary"]["pending_items"] == 1


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
