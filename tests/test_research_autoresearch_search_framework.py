from __future__ import annotations

import json
from pathlib import Path

from pm15min.research.automation.search_ledger import build_attempt_record
from pm15min.research.automation.search_policy import choose_required_next_lever
from pm15min.research.automation.window_contract import (
    CANONICAL_DECISION_END,
    CANONICAL_DECISION_START,
    CANONICAL_TRAIN_END,
    audit_suite_spec_windows,
    extract_suite_spec_window,
    suite_spec_uses_canonical_window,
)


def test_extract_suite_spec_window_reads_train_and_decision_bounds(tmp_path: Path) -> None:
    spec_path = tmp_path / "suite.json"
    spec_path.write_text(
        json.dumps(
            {
                "suite_name": "suite",
                "window": {"start": "2025-10-27", "end": "2026-04-15"},
                "decision_start": "2026-04-15",
                "decision_end": "2026-05-07",
            }
        ),
        encoding="utf-8",
    )

    window = extract_suite_spec_window(spec_path)

    assert window.train_end == CANONICAL_TRAIN_END
    assert window.decision_start == CANONICAL_DECISION_START
    assert window.decision_end == CANONICAL_DECISION_END
    assert suite_spec_uses_canonical_window(spec_path)


def test_audit_suite_spec_windows_reports_stale_parseable_specs(tmp_path: Path) -> None:
    specs_dir = tmp_path / "research" / "experiments" / "suite_specs"
    specs_dir.mkdir(parents=True)
    (specs_dir / "good.json").write_text(
        json.dumps(
            {
                "suite_name": "good",
                "window": {"start": "2025-10-27", "end": "2026-04-15"},
                "decision_start": "2026-04-15",
                "decision_end": "2026-05-07",
            }
        ),
        encoding="utf-8",
    )
    (specs_dir / "stale.json").write_text(
        json.dumps(
            {
                "suite_name": "stale",
                "window": {"start": "2025-10-27", "end": "2026-03-31"},
                "decision_start": "2026-04-01",
                "decision_end": "2026-04-23",
            }
        ),
        encoding="utf-8",
    )
    (specs_dir / "._metadata.json").write_text("not json", encoding="utf-8")

    audit = audit_suite_spec_windows(tmp_path)

    assert audit["parseable_specs"] == 2
    assert audit["canonical_specs"] == 1
    assert audit["stale_specs"] == ["stale.json"]
    assert audit["ignored_files"] == ["._metadata.json"]


def test_build_attempt_record_extracts_feature_width_model_and_bottleneck(tmp_path: Path) -> None:
    root = tmp_path
    specs_dir = root / "research" / "experiments" / "suite_specs"
    specs_dir.mkdir(parents=True)
    (root / "research" / "experiments").mkdir(parents=True, exist_ok=True)
    (root / "research" / "experiments" / "custom_feature_sets.json").write_text(
        json.dumps(
            {
                "focus_xrp_56_v1": {
                    "market": "xrp",
                    "width": 56,
                    "columns": ["ret_from_cycle_open", "ret_from_strike", "move_z"],
                }
            }
        ),
        encoding="utf-8",
    )
    (specs_dir / "suite_xrp.json").write_text(
        json.dumps(
            {
                "suite_name": "suite_xrp",
                "window": {"start": "2025-10-27", "end": "2026-04-15"},
                "decision_start": "2026-04-15",
                "decision_end": "2026-05-07",
                "markets": {
                    "xrp": {
                        "groups": {
                            "direction": {
                                "runs": [
                                    {
                                        "run_name": "r1",
                                        "target": "direction",
                                        "model_family": "catboost",
                                        "feature_set_variants": [
                                            {"label": "frontier", "feature_set": "focus_xrp_56_v1"}
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
    run_payload = {
        "suite_name": "suite_xrp",
        "run_label": "run_xrp",
        "market": "xrp",
        "decision_start": "2026-04-15",
        "decision_end": "2026-05-07",
        "train_end": "2026-04-15",
        "top_case": {
            "trades": 18,
            "roi_pct": -12.5,
            "feature_set": "focus_xrp_56_v1",
            "density_bottleneck": {"primary_bottleneck": "probability_gate"},
        },
    }

    attempt = build_attempt_record(root, run_payload, track="direction_dense")

    assert attempt["market"] == "xrp"
    assert attempt["track"] == "direction_dense"
    assert attempt["feature_sets"] == ["focus_xrp_56_v1"]
    assert attempt["widths"] == [56]
    assert attempt["model_families"] == ["catboost"]
    assert attempt["primary_bottleneck"] == "probability_gate"
    assert attempt["outcome"] == "sparse"


def test_choose_required_next_lever_forces_width_after_same_width_sparse_loop() -> None:
    attempts = [
        {"outcome": "sparse", "widths": [40], "model_families": ["deep_otm"], "primary_bottleneck": "low_trade_density"},
        {"outcome": "sparse", "widths": [40], "model_families": ["deep_otm"], "primary_bottleneck": "low_trade_density"},
        {"outcome": "sparse", "widths": [40], "model_families": ["deep_otm"], "primary_bottleneck": "low_trade_density"},
    ]

    decision = choose_required_next_lever(attempts)

    assert decision["required_lever"] == "feature_width"
    assert decision["forbid_same_width"] is True
    assert decision["forbid_same_model"] is False


def test_choose_required_next_lever_forces_model_after_probability_gate_loop() -> None:
    attempts = [
        {"outcome": "sparse", "widths": [56], "model_families": ["deep_otm"], "primary_bottleneck": "probability_gate"},
        {"outcome": "sparse", "widths": [56], "model_families": ["deep_otm"], "primary_bottleneck": "probability_gate"},
        {"outcome": "sparse", "widths": [56], "model_families": ["deep_otm"], "primary_bottleneck": "probability_gate"},
    ]

    decision = choose_required_next_lever(attempts)

    assert decision["required_lever"] == "model_family"
    assert decision["forbid_same_width"] is False
    assert decision["forbid_same_model"] is True


def test_choose_required_next_lever_escalates_after_extended_sparse_loop() -> None:
    attempts = [
        {"outcome": "sparse", "widths": [56], "model_families": ["deep_otm"], "primary_bottleneck": "probability_gate"},
        {"outcome": "sparse", "widths": [48], "model_families": ["deep_otm"], "primary_bottleneck": "probability_gate"},
        {"outcome": "sparse", "widths": [44], "model_families": ["logreg"], "primary_bottleneck": "probability_gate"},
        {"outcome": "sparse", "widths": [56], "model_families": ["catboost"], "primary_bottleneck": "probability_gate"},
    ]

    decision = choose_required_next_lever(attempts)

    assert decision["required_lever"] == "search_space_rework"
    assert decision["forbid_same_width"] is True
    assert decision["forbid_same_model"] is True
    assert decision["forbid_same_family"] is True
