from __future__ import annotations

import json
from pathlib import Path

from pm15min.research._contracts_runs import BacktestParitySpec
from pm15min.research.backtests.build_signature import backtest_build_signature
from pm15min.research.backtests.grouped_grid import (
    _load_existing_case_row,
    expand_grouped_backtest_groups,
    load_grouped_backtest_grid_spec,
)
from pm15min.research.config import ResearchConfig


def test_load_grouped_backtest_grid_spec_expands_groups_and_case_labels(tmp_path: Path) -> None:
    path = tmp_path / "grid.json"
    path.write_text(
        json.dumps(
            {
                "run_label": "grid_run",
                "cycle": "15m",
                "decision_start": "2026-03-15",
                "decision_end": "2026-03-21",
                "parity": {"disable_ret_30m_direction_guard": True},
                "stake_usd_values": [1, 5],
                "max_trades_per_market_values": [1, "unlimited"],
                "bundles": [
                    {
                        "market": "xrp",
                        "profile": "deep_otm",
                        "bundle_label": "bundle_a",
                    }
                ],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    spec = load_grouped_backtest_grid_spec(path)
    groups = expand_grouped_backtest_groups(spec)

    assert len(groups) == 2
    assert groups[0].group_label == "xrp/bundle_a/max1"
    assert groups[0].max_trades_per_market == 1
    assert groups[0].stake_usd_values == (1.0, 5.0)
    assert groups[0].parity.disable_ret_30m_direction_guard is True
    assert all("grid_run-xrp-bundle_a-max1-" in label for label in groups[0].case_run_labels)
    assert groups[1].group_label == "xrp/bundle_a/maxu"
    assert groups[1].max_trades_per_market is None
    assert groups[1].parity.disable_ret_30m_direction_guard is True
    assert all("grid_run-xrp-bundle_a-maxu-" in label for label in groups[1].case_run_labels)


def test_grouped_grid_existing_case_requires_matching_manifest_spec(tmp_path: Path) -> None:
    path = tmp_path / "grid.json"
    path.write_text(
        json.dumps(
            {
                "run_label": "grid_run",
                "cycle": "15m",
                "decision_start": "2026-03-15",
                "decision_end": "2026-03-21",
                "parity": {"disable_ret_30m_direction_guard": True},
                "stake_usd_values": [5],
                "max_trades_per_market_values": [1],
                "bundles": [
                    {
                        "market": "xrp",
                        "profile": "deep_otm",
                        "bundle_label": "bundle_a",
                    }
                ],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    spec = load_grouped_backtest_grid_spec(path)
    group = expand_grouped_backtest_groups(spec)[0]
    run_label = group.case_run_labels[0]
    cfg = ResearchConfig.build(
        market=group.market,
        cycle=group.cycle,
        profile=group.profile,
        source_surface="backtest",
        feature_set=group.feature_set,
        label_set=group.label_set,
        target=group.target,
        model_family=group.model_family,
        root=tmp_path / "v2",
    )
    run_dir = cfg.layout.backtest_run_dir(
        profile=group.profile,
        spec_name=group.spec_name,
        run_label_text=run_label,
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    summary_payload = {
        "market": group.market,
        "cycle": group.cycle,
        "profile": group.profile,
        "spec_name": group.spec_name,
        "target": group.target,
        "bundle_label": group.bundle_label,
        "feature_set": group.feature_set,
        "label_set": group.label_set,
        "run_label": run_label,
        "stake_usd": 5.0,
        "parity": {
            **group.parity.to_dict(),
            "regime_defense_max_trades_per_market": 1,
        },
        "backtest_build_signature": backtest_build_signature(),
        "trades": 0,
    }
    (run_dir / "summary.json").write_text(json.dumps(summary_payload), encoding="utf-8")
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "market": group.market,
                "cycle": group.cycle,
                "spec": {
                    "profile": group.profile,
                    "spec_name": group.spec_name,
                    "run_label": run_label,
                    "target": "other_target",
                    "decision_start": group.decision_start,
                    "decision_end": group.decision_end,
                    "bundle_label": group.bundle_label,
                    "stake_usd": 5.0,
                    "parity": {
                        **group.parity.to_dict(),
                        "regime_defense_max_trades_per_market": 1,
                    },
                    "feature_set": group.feature_set,
                    "label_set": group.label_set,
                },
            }
        ),
        encoding="utf-8",
    )

    assert (
        _load_existing_case_row(
            cfg=cfg,
            group=group,
            stake_usd=5.0,
            run_label=run_label,
        )
        is None
    )

    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "market": group.market,
                "cycle": group.cycle,
                "spec": {
                    "profile": group.profile,
                    "spec_name": group.spec_name,
                    "run_label": run_label,
                    "target": group.target,
                    "decision_start": group.decision_start,
                    "decision_end": group.decision_end,
                    "bundle_label": group.bundle_label,
                    "stake_usd": 5.0,
                    "parity": {
                        **group.parity.to_dict(),
                        "regime_defense_max_trades_per_market": 1,
                    },
                    "feature_set": group.feature_set,
                    "label_set": group.label_set,
                },
            }
        ),
        encoding="utf-8",
    )

    reused = _load_existing_case_row(
        cfg=cfg,
        group=group,
        stake_usd=5.0,
        run_label=run_label,
    )
    assert reused is not None
    assert reused["reused_existing"] is True


def test_grouped_grid_existing_case_requires_matching_build_signature(tmp_path: Path) -> None:
    path = tmp_path / "grid.json"
    path.write_text(
        json.dumps(
            {
                "run_label": "grid_run",
                "cycle": "15m",
                "decision_start": "2026-03-15",
                "decision_end": "2026-03-21",
                "parity": {"disable_ret_30m_direction_guard": True},
                "stake_usd_values": [5],
                "max_trades_per_market_values": [1],
                "bundles": [
                    {
                        "market": "btc",
                        "profile": "deep_otm_baseline",
                        "bundle_label": "bundle_a",
                    }
                ],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    spec = load_grouped_backtest_grid_spec(path)
    group = expand_grouped_backtest_groups(spec)[0]
    run_label = group.case_run_labels[0]
    cfg = ResearchConfig.build(
        market=group.market,
        cycle=group.cycle,
        profile=group.profile,
        source_surface="backtest",
        feature_set=group.feature_set,
        label_set=group.label_set,
        target=group.target,
        model_family=group.model_family,
        root=tmp_path / "v2",
    )
    run_dir = cfg.layout.backtest_run_dir(
        profile=group.profile,
        spec_name=group.spec_name,
        run_label_text=run_label,
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "market": group.market,
                "cycle": group.cycle,
                "profile": group.profile,
                "spec_name": group.spec_name,
                "target": group.target,
                "bundle_label": group.bundle_label,
                "feature_set": group.feature_set,
                "label_set": group.label_set,
                "run_label": run_label,
                "stake_usd": 5.0,
                "parity": {
                    **group.parity.to_dict(),
                    "regime_defense_max_trades_per_market": 1,
                },
                "backtest_build_signature": "legacy_signature",
                "trades": 0,
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "market": group.market,
                "cycle": group.cycle,
                "spec": {
                    "profile": group.profile,
                    "spec_name": group.spec_name,
                    "run_label": run_label,
                    "target": group.target,
                    "decision_start": group.decision_start,
                    "decision_end": group.decision_end,
                    "bundle_label": group.bundle_label,
                    "stake_usd": 5.0,
                    "parity": {
                        **group.parity.to_dict(),
                        "regime_defense_max_trades_per_market": 1,
                    },
                    "feature_set": group.feature_set,
                    "label_set": group.label_set,
                },
            }
        ),
        encoding="utf-8",
    )

    assert (
        _load_existing_case_row(
            cfg=cfg,
            group=group,
            stake_usd=5.0,
            run_label=run_label,
        )
        is None
    )
