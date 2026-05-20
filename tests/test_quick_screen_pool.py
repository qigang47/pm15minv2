from __future__ import annotations

import importlib.util
import json
import threading
import time
from pathlib import Path

from pm15min.research.automation.queue_state import build_queue_item, save_experiment_queue


def _load_pool_module():
    workspace_root = Path(__file__).resolve().parents[1]
    module_path = workspace_root / "scripts" / "research" / "run_quick_screen_pool.py"
    spec = importlib.util.spec_from_file_location("quick_screen_pool_under_test", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_quick_screen_pool_runs_manifest_items_concurrently(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_pool_module()
    manifest_path = tmp_path / "pool.manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "items": [
                    {"id": "item-1", "suite_name": "suite_a", "run_label": "run_a", "track": "direction_dense"},
                    {"id": "item-2", "suite_name": "suite_b", "run_label": "run_b", "track": "direction_dense"},
                    {"id": "item-3", "suite_name": "suite_c", "run_label": "run_c", "track": "direction_dense"},
                ]
            }
        ),
        encoding="utf-8",
    )
    lock = threading.Lock()
    active = 0
    max_active = 0
    calls: list[tuple[str, str]] = []
    status_updates: list[tuple[str, str]] = []

    def fake_run_quick_screen_suite(
        *,
        suite_name: str,
        run_label: str,
        top_k: int,
        cleanup_between_cases: bool = True,
    ):
        nonlocal active, max_active
        assert cleanup_between_cases is False
        with lock:
            active += 1
            max_active = max(max_active, active)
            calls.append((suite_name, run_label))
        time.sleep(0.05)
        with lock:
            active -= 1
        return {"suite_name": suite_name, "run_label": run_label, "top_k": top_k}

    def fake_set_queue_status(root: Path, item: dict[str, object], *, status: str, reason: str, last_error=None):
        del root, reason, last_error
        status_updates.append((str(item["id"]), status))

    monkeypatch.setattr(module, "run_quick_screen_suite", fake_run_quick_screen_suite, raising=False)
    monkeypatch.setattr(module, "_set_queue_status", fake_set_queue_status, raising=False)
    monkeypatch.setattr(module, "_cleanup_after_item", lambda: None, raising=False)

    exit_code = module.run_pool(
        root=tmp_path,
        manifest_path=manifest_path,
        batch_id="pool-test",
        top_k=2,
        workers=2,
        max_items=10,
        memory_report_interval_sec=3600,
    )

    assert exit_code == 0
    assert max_active >= 2
    assert sorted(calls) == [("suite_a", "run_a"), ("suite_b", "run_b"), ("suite_c", "run_c")]
    assert sorted(status_updates) == [("item-1", "done"), ("item-2", "done"), ("item-3", "done")]


def test_quick_screen_pool_refills_from_same_track_queue(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_pool_module()
    manifest_path = tmp_path / "pool.manifest.json"
    first_item = build_queue_item(
        market="xrp",
        suite_name="suite_a",
        run_label="run_a",
        action="launch",
        status="running",
        track="reversal_dense",
        session_dir=tmp_path / "sessions" / "reversal",
        program_path=tmp_path / "auto_research" / "program_reversal.md",
    )
    queued_items = [
        build_queue_item(
            market="xrp",
            suite_name=f"suite_{suffix}",
            run_label=f"run_{suffix}",
            action="launch",
            status="queued",
            track="reversal_dense",
            session_dir=tmp_path / "sessions" / "reversal",
            program_path=tmp_path / "auto_research" / "program_reversal.md",
        )
        for suffix in ("b", "c")
    ]
    manifest_path.write_text(json.dumps({"items": [first_item]}), encoding="utf-8")
    save_experiment_queue(
        tmp_path,
        {
            "version": 1,
            "max_live_runs": 10,
            "max_queued_items": 24,
            "track_slot_caps": {"direction_dense": 5, "reversal_dense": 5},
            "items": [first_item, *queued_items],
        },
    )

    lock = threading.Lock()
    active = 0
    max_active = 0
    calls: list[tuple[str, str]] = []
    status_updates: list[tuple[str, str]] = []

    def fake_run_quick_screen_suite(
        *,
        suite_name: str,
        run_label: str,
        top_k: int,
        cleanup_between_cases: bool = True,
    ):
        nonlocal active, max_active
        assert cleanup_between_cases is False
        with lock:
            active += 1
            max_active = max(max_active, active)
            calls.append((suite_name, run_label))
        time.sleep(0.05)
        with lock:
            active -= 1
        return {"suite_name": suite_name, "run_label": run_label, "top_k": top_k}

    def fake_set_queue_status(root: Path, item: dict[str, object], *, status: str, reason: str, last_error=None):
        del root, reason, last_error
        status_updates.append((str(item["id"]), status))

    monkeypatch.setattr(module, "run_quick_screen_suite", fake_run_quick_screen_suite, raising=False)
    monkeypatch.setattr(module, "_set_queue_status", fake_set_queue_status, raising=False)
    monkeypatch.setattr(module, "_cleanup_after_item", lambda: None, raising=False)

    exit_code = module.run_pool(
        root=tmp_path,
        manifest_path=manifest_path,
        batch_id="quick_screen_pool_reversal_dense_test",
        top_k=1,
        workers=3,
        max_items=3,
        memory_report_interval_sec=3600,
    )

    assert exit_code == 0
    assert max_active >= 2
    assert sorted(calls) == [("suite_a", "run_a"), ("suite_b", "run_b"), ("suite_c", "run_c")]
    assert sorted(status for _, status in status_updates) == ["done", "done", "done"]


def test_quick_screen_pool_refills_only_same_market_queue_items(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_pool_module()
    manifest_path = tmp_path / "pool.manifest.json"
    first_item = build_queue_item(
        market="sol",
        suite_name="suite_a",
        run_label="run_a",
        action="launch",
        status="running",
        track="direction_dense",
        session_dir=tmp_path / "sessions" / "direction",
        program_path=tmp_path / "auto_research" / "program_direction.md",
    )
    queued_sol = build_queue_item(
        market="sol",
        suite_name="suite_b",
        run_label="run_b",
        action="launch",
        status="queued",
        track="direction_dense",
        session_dir=tmp_path / "sessions" / "direction",
        program_path=tmp_path / "auto_research" / "program_direction.md",
    )
    queued_xrp = build_queue_item(
        market="xrp",
        suite_name="suite_c",
        run_label="run_c",
        action="launch",
        status="queued",
        track="direction_dense",
        session_dir=tmp_path / "sessions" / "direction",
        program_path=tmp_path / "auto_research" / "program_direction.md",
    )
    manifest_path.write_text(json.dumps({"items": [first_item]}), encoding="utf-8")
    save_experiment_queue(
        tmp_path,
        {
            "version": 1,
            "max_live_runs": 10,
            "max_queued_items": 24,
            "track_slot_caps": {"direction_dense": 5, "reversal_dense": 5},
            "items": [first_item, queued_sol, queued_xrp],
        },
    )

    calls: list[tuple[str, str]] = []
    status_updates: list[tuple[str, str]] = []

    def fake_run_quick_screen_suite(
        *,
        suite_name: str,
        run_label: str,
        top_k: int,
        cleanup_between_cases: bool = True,
    ):
        del top_k
        assert cleanup_between_cases is False
        calls.append((suite_name, run_label))
        return {"suite_name": suite_name, "run_label": run_label}

    def fake_set_queue_status(root: Path, item: dict[str, object], *, status: str, reason: str, last_error=None):
        del root, reason, last_error
        status_updates.append((str(item["id"]), status))

    monkeypatch.setattr(module, "run_quick_screen_suite", fake_run_quick_screen_suite, raising=False)
    monkeypatch.setattr(module, "_set_queue_status", fake_set_queue_status, raising=False)
    monkeypatch.setattr(module, "_cleanup_after_item", lambda: None, raising=False)

    exit_code = module.run_pool(
        root=tmp_path,
        manifest_path=manifest_path,
        batch_id="quick_screen_pool_direction_dense_sol_test",
        top_k=1,
        workers=2,
        max_items=3,
        memory_report_interval_sec=3600,
    )

    assert exit_code == 0
    assert sorted(calls) == [("suite_a", "run_a"), ("suite_b", "run_b")]
    assert sorted(status for _, status in status_updates) == ["done", "done"]
    queue_payload = json.loads((tmp_path / "var" / "research" / "autorun" / "experiment-queue.json").read_text())
    xrp_item = next(item for item in queue_payload["items"] if item["market"] == "xrp")
    assert xrp_item["status"] == "queued"
    assert not xrp_item.get("batch_id")


def test_quick_screen_pool_polls_for_late_queue_refills(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_pool_module()
    manifest_path = tmp_path / "pool.manifest.json"
    first_item = build_queue_item(
        market="sol",
        suite_name="suite_a",
        run_label="run_a",
        action="launch",
        status="running",
        track="direction_dense",
        session_dir=tmp_path / "sessions" / "direction",
        program_path=tmp_path / "auto_research" / "program_direction.md",
    )
    late_item = build_queue_item(
        market="sol",
        suite_name="suite_b",
        run_label="run_b",
        action="launch",
        status="queued",
        track="direction_dense",
        session_dir=tmp_path / "sessions" / "direction",
        program_path=tmp_path / "auto_research" / "program_direction.md",
    )
    manifest_path.write_text(json.dumps({"items": [first_item]}), encoding="utf-8")
    save_experiment_queue(
        tmp_path,
        {
            "version": 1,
            "max_live_runs": 10,
            "max_queued_items": 24,
            "track_slot_caps": {"direction_dense": 5, "reversal_dense": 5},
            "items": [first_item],
        },
    )

    release_first = threading.Event()
    late_item_queued = threading.Event()
    calls: list[tuple[str, str]] = []
    status_updates: list[tuple[str, str]] = []

    def fake_run_quick_screen_suite(
        *,
        suite_name: str,
        run_label: str,
        top_k: int,
        cleanup_between_cases: bool = True,
    ):
        del top_k
        assert cleanup_between_cases is False
        calls.append((suite_name, run_label))
        if run_label == "run_a":
            late_item_queued.wait(timeout=2)
            release_first.wait(timeout=2)
        if run_label == "run_b":
            release_first.set()
        return {"suite_name": suite_name, "run_label": run_label}

    def fake_set_queue_status(root: Path, item: dict[str, object], *, status: str, reason: str, last_error=None):
        del root, reason, last_error
        status_updates.append((str(item["id"]), status))

    real_claim_refill_items = module._claim_refill_items

    claim_calls = 0

    def claim_refill_with_late_enqueue(*args, **kwargs):
        nonlocal claim_calls
        claim_calls += 1
        if claim_calls >= 2 and not late_item_queued.is_set():
            save_experiment_queue(
                tmp_path,
                {
                    "version": 1,
                    "max_live_runs": 10,
                    "max_queued_items": 24,
                    "track_slot_caps": {"direction_dense": 5, "reversal_dense": 5},
                    "items": [first_item, late_item],
                },
            )
            late_item_queued.set()
        return real_claim_refill_items(*args, **kwargs)

    monkeypatch.setattr(module, "_REFILL_POLL_SEC", 0.01, raising=False)
    monkeypatch.setattr(module, "run_quick_screen_suite", fake_run_quick_screen_suite, raising=False)
    monkeypatch.setattr(module, "_set_queue_status", fake_set_queue_status, raising=False)
    monkeypatch.setattr(module, "_cleanup_after_item", lambda: None, raising=False)
    monkeypatch.setattr(module, "_claim_refill_items", claim_refill_with_late_enqueue, raising=False)

    try:
        exit_code = module.run_pool(
            root=tmp_path,
            manifest_path=manifest_path,
            batch_id="quick_screen_pool_direction_dense_test",
            top_k=1,
            workers=2,
            max_items=2,
            memory_report_interval_sec=3600,
        )
    finally:
        release_first.set()

    assert exit_code == 0
    assert sorted(calls) == [("suite_a", "run_a"), ("suite_b", "run_b")]
    assert sorted(status for _, status in status_updates) == ["done", "done"]
