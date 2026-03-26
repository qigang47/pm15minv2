from __future__ import annotations

import json
from pathlib import Path

from pm15min.research.experiments.specs import load_suite_definition


def test_load_suite_definition_expands_mapping_groups_runs_with_defaults_inheritance(tmp_path: Path) -> None:
    path = tmp_path / "suite.json"
    path.write_text(
        json.dumps(
            {
                "suite_name": "matrix_suite",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "offsets": [7, 8],
                "window": {"start": "2026-03-01", "end": "2026-03-03"},
                "tags": ["suite"],
                "parity": {
                    "regime_enabled": True,
                    "liquidity_proxy_mode": "spot_kline_mirror",
                },
                "markets": {
                    "sol": {
                        "feature_set": "deep_otm_sol_v2",
                        "tags": ["market"],
                        "groups": {
                            "core": {
                                "tags": ["group"],
                                "runs": [
                                    {
                                        "run_name": "baseline",
                                        "tags": ["run"],
                                        "hybrid_secondary_target": "reversal",
                                        "hybrid_secondary_offsets": [7],
                                        "hybrid_fallback_reasons": ["direction_prob"],
                                        "parity": {
                                            "regime_defense_max_trades_per_market": 2,
                                        },
                                    }
                                ],
                            }
                        },
                    }
                },
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    suite = load_suite_definition(path)

    assert suite.suite_name == "matrix_suite"
    assert len(suite.markets) == 1
    spec = suite.markets[0]
    assert spec.market == "sol"
    assert spec.group_name == "core"
    assert spec.run_name == "baseline"
    assert spec.feature_set == "deep_otm_sol_v2"
    assert spec.offsets == (7, 8)
    assert spec.window.start == "2026-03-01"
    assert spec.window.end == "2026-03-03"
    assert spec.hybrid_secondary_target == "reversal"
    assert spec.hybrid_secondary_offsets == (7,)
    assert spec.hybrid_fallback_reasons == ("direction_prob",)
    assert spec.tags == ("suite", "market", "group", "run")
    assert spec.parity.regime_enabled is True
    assert spec.parity.liquidity_proxy_mode == "spot_kline_mirror"
    assert spec.parity.regime_defense_max_trades_per_market == 2


def test_load_suite_definition_expands_backtest_variants_across_run_and_suite_defaults(tmp_path: Path) -> None:
    path = tmp_path / "suite.json"
    path.write_text(
        json.dumps(
            {
                "suite_name": "variant_suite",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "offsets": [7, 8],
                "window": {"start": "2026-03-01", "end": "2026-03-01"},
                "backtest_variants": [
                    {
                        "label": "tight",
                        "notes": "suite-tight",
                        "parity": {"regime_defense_max_trades_per_market": 1},
                    },
                    {
                        "label": "loose",
                        "notes": "suite-loose",
                        "parity": {"regime_defense_max_trades_per_market": 3},
                    },
                ],
                "markets": {
                    "sol": {
                        "groups": {
                            "core": {
                                "runs": [
                                    {
                                        "run_name": "base",
                                    },
                                    {
                                        "run_name": "shadow",
                                        "backtest_variants": [
                                            {
                                                "label": "shadow_only",
                                                "notes": "run-override",
                                                "target": "reversal",
                                                "parity": {"regime_enabled": False},
                                            }
                                        ],
                                    },
                                ],
                            }
                        },
                    }
                },
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    suite = load_suite_definition(path)

    specs = {(spec.run_name, spec.variant_label): spec for spec in suite.markets}
    assert set(specs) == {
        ("base", "tight"),
        ("base", "loose"),
        ("shadow", "shadow_only"),
    }
    assert specs[("base", "tight")].variant_notes == "suite-tight"
    assert specs[("base", "tight")].target == "direction"
    assert specs[("base", "tight")].parity.regime_defense_max_trades_per_market == 1
    assert specs[("base", "loose")].parity.regime_defense_max_trades_per_market == 3
    assert specs[("shadow", "shadow_only")].variant_notes == "run-override"
    assert specs[("shadow", "shadow_only")].target == "reversal"
    assert specs[("shadow", "shadow_only")].parity.regime_enabled is False


def test_load_suite_definition_expands_stake_matrix_into_execution_run_names(tmp_path: Path) -> None:
    path = tmp_path / "suite.json"
    path.write_text(
        json.dumps(
            {
                "suite_name": "stake_matrix_suite",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "offsets": [7, 8],
                "window": {"start": "2026-03-01", "end": "2026-03-01"},
                "stakes": [1.0, 5.0],
                "markets": {
                    "sol": {
                        "groups": {
                            "core": {
                                "runs": [
                                    {
                                        "run_name": "base",
                                        "max_notional_usd": 8.0,
                                    }
                                ]
                            }
                        }
                    }
                },
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    suite = load_suite_definition(path)

    specs = sorted(suite.markets, key=lambda spec: float(spec.stake_usd or 0.0))
    assert [spec.run_name for spec in specs] == [
        "base__stake_1usd__max_8usd",
        "base__stake_5usd__max_8usd",
    ]
    assert [spec.matrix_parent_run_name for spec in specs] == ["base", "base"]
    assert [spec.matrix_stake_label for spec in specs] == [
        "stake_1usd__max_8usd",
        "stake_5usd__max_8usd",
    ]
    assert [spec.stake_usd for spec in specs] == [1.0, 5.0]
    assert all(spec.max_notional_usd == 8.0 for spec in specs)


def test_load_suite_definition_expands_max_trades_and_stake_matrix_into_execution_run_names(tmp_path: Path) -> None:
    path = tmp_path / "suite.json"
    path.write_text(
        json.dumps(
            {
                "suite_name": "backtest_grid_suite",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "offsets": [7, 8, 9],
                "window": {"start": "2026-03-15", "end": "2026-03-21"},
                "parity": {
                    "regime_defense_max_trades_per_market": 9,
                },
                "stakes": [1.0, 5.0],
                "markets": {
                    "xrp": {
                        "groups": {
                            "core": {
                                "runs": [
                                    {
                                        "run_name": "deep_grid",
                                        "max_trades_per_market_values": [1, 3, "unlimited"],
                                    }
                                ]
                            }
                        }
                    }
                },
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    suite = load_suite_definition(path)

    specs = sorted(
        suite.markets,
        key=lambda spec: (
            spec.run_name,
            float(spec.stake_usd or 0.0),
        ),
    )
    assert [spec.run_name for spec in specs] == [
        "deep_grid__max1__stake_1usd",
        "deep_grid__max1__stake_5usd",
        "deep_grid__max3__stake_1usd",
        "deep_grid__max3__stake_5usd",
        "deep_grid__maxu__stake_1usd",
        "deep_grid__maxu__stake_5usd",
    ]
    assert [spec.matrix_parent_run_name for spec in specs] == [
        "deep_grid__max1",
        "deep_grid__max1",
        "deep_grid__max3",
        "deep_grid__max3",
        "deep_grid__maxu",
        "deep_grid__maxu",
    ]
    assert [spec.matrix_stake_label for spec in specs] == [
        "stake_1usd",
        "stake_5usd",
        "stake_1usd",
        "stake_5usd",
        "stake_1usd",
        "stake_5usd",
    ]
    assert [spec.stake_usd for spec in specs] == [1.0, 5.0, 1.0, 5.0, 1.0, 5.0]
    assert [spec.parity.regime_defense_max_trades_per_market for spec in specs] == [1, 1, 3, 3, None, None]
    assert all("max_trades_per_market:" in "|".join(spec.tags) for spec in specs)


def test_load_suite_definition_parses_backtest_decision_window_fields(tmp_path: Path) -> None:
    path = tmp_path / "suite.json"
    path.write_text(
        json.dumps(
            {
                "suite_name": "decision_window_suite",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "offsets": [7, 8, 9],
                "window": {"start": "2025-10-27", "end": "2026-03-15"},
                "decision_start": "2026-03-15",
                "backtest_decision_end": "2026-03-21",
                "markets": ["xrp"],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    suite = load_suite_definition(path)
    spec = suite.markets[0]

    assert spec.decision_start == "2026-03-15"
    assert spec.decision_end == "2026-03-21"


def test_load_suite_definition_parses_runtime_policy_with_legacy_bool_aliases(tmp_path: Path) -> None:
    path = tmp_path / "suite.json"
    path.write_text(
        json.dumps(
            {
                "suite_name": "runtime_policy_suite",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "offsets": [7, 8],
                "window": {"start": "2026-03-01", "end": "2026-03-01"},
                "runtime_policy": {
                    "resume": False,
                    "rerun_failed_cases": False,
                    "parallel_workers": 3,
                },
                "markets": ["sol"],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    suite = load_suite_definition(path)

    assert suite.runtime_policy.completed_cases == "rerun"
    assert suite.runtime_policy.failed_cases == "skip"
    assert suite.runtime_policy.parallel_case_workers == 3
    assert suite.to_dict()["runtime_policy"] == {
        "completed_cases": "rerun",
        "failed_cases": "skip",
        "parallel_case_workers": 3,
    }
