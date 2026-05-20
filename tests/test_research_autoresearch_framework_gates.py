from __future__ import annotations

import csv
import json
from pathlib import Path

from pm15min.research.automation import control_plane
from pm15min.research.automation.framework_gates import (
    build_framework_status_matrix,
    quick_screen_formal_confirmation_gate,
    validate_research_candidate_meta,
)
from pm15min.research.automation.queue_state import build_queue_item, upsert_queue_item


def test_validate_research_candidate_meta_requires_material_change_fields() -> None:
    result = validate_research_candidate_meta({"primary_lever": "feature_width"})

    assert result["passed"] is False
    assert result["failures"] == [
        "missing_feature_width",
        "missing_model_family",
        "missing_feature_set",
        "missing_factor_family_change",
        "missing_expected_trade_count_effect",
        "missing_difference_from_recent_failures",
    ]


def test_upsert_queue_item_blocks_launch_without_research_candidate_gate(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    item = build_queue_item(
        market="sol",
        suite_name="suite_sol",
        run_label="run_sol",
        action="launch",
        status="queued",
        track="direction_dense",
        session_dir=root / "sessions" / "direction_dense",
        program_path=root / "auto_research" / "program_direction_dense.md",
    )
    item["research_meta"] = {"primary_lever": "feature_width"}

    state = upsert_queue_item(root, item)

    saved = state["items"][0]
    assert saved["status"] == "dead"
    assert saved["action"] == "blocked"
    assert saved["reason"] == "research_candidate_quality_gate_failed"
    assert saved["research_candidate_gate"]["passed"] is False
    assert "missing_expected_trade_count_effect" in saved["last_error"]


def test_quick_screen_formal_confirmation_gate_requires_formal_before_frontier() -> None:
    selected = {
        "suite_name": "suite_xrp",
        "run_label": "quick_xrp",
        "top_case": {
            "selected_for_formal": True,
            "trade_rows": 180,
            "profitable_pool_capture_rows": 126,
            "profitable_pool_coverage_ratio": 0.7,
        },
    }

    gate = quick_screen_formal_confirmation_gate(selected, formal_run=None)

    assert gate == {
        "passed": False,
        "state": "formal_required",
        "reason": "quick_screen_selected_candidate_requires_formal_confirmation",
        "quick_screen_run_label": "quick_xrp",
        "formal_run_label": "",
    }


def test_quick_screen_top_case_marks_selected_rows_as_formal_required(tmp_path: Path) -> None:
    run_dir = tmp_path / "research" / "experiments" / "runs" / "suite=suite_xrp" / "run=quick_xrp"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "quick_screen_summary.json").write_text(
        json.dumps(
            {
                "suite_name": "suite_xrp",
                "run_label": "quick_xrp",
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
                "feature_set",
                "trade_rows",
                "profitable_pool_capture_rows",
                "profitable_pool_coverage_ratio",
                "selected_for_formal",
                "rank",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "market": "xrp",
                "feature_set": "focus_xrp_56_v9",
                "trade_rows": "180",
                "profitable_pool_capture_rows": "126",
                "profitable_pool_coverage_ratio": "0.7",
                "selected_for_formal": "True",
                "rank": "1",
            }
        )

    payload = control_plane.summarize_experiment_run(run_dir)

    assert payload["top_case"]["selected_for_formal"] is True
    assert payload["top_case"]["promotion_state"] == "formal_required"


def test_build_framework_status_matrix_aggregates_track_market_slots() -> None:
    report = {
        "queue": {
            "items": [
                {"market": "sol", "track": "direction_dense", "status": "queued"},
                {"market": "xrp", "track": "reversal_dense", "status": "running"},
            ]
        },
        "formal_workers": [
            {"market": "xrp", "track": "reversal_dense", "run_label": "xrp_run"},
        ],
        "completed_runs": [
            {
                "markets": ["sol"],
                "suite_name": "suite_sol",
                "run_label": "sol_quick",
                "top_case": {
                    "selected_for_formal": True,
                    "trade_rows": 160,
                    "profitable_pool_capture_rows": 100,
                },
            }
        ],
    }

    matrix = build_framework_status_matrix(report, markets=["sol", "xrp"], tracks=["direction_dense", "reversal_dense"])

    assert matrix["totals"] == {
        "queued": 1,
        "running": 1,
        "live_workers": 1,
        "formal_required": 1,
    }
    assert matrix["slots"]["direction_dense"]["sol"]["queue_status_counts"] == {"queued": 1}
    assert matrix["slots"]["direction_dense"]["sol"]["promotion_state"] == "formal_required"
    assert matrix["slots"]["reversal_dense"]["xrp"]["live_workers"] == 1
