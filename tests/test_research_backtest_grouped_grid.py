from __future__ import annotations

import json
from pathlib import Path

from pm15min.research.backtests.grouped_grid import expand_grouped_backtest_groups, load_grouped_backtest_grid_spec


def test_load_grouped_backtest_grid_spec_expands_groups_and_case_labels(tmp_path: Path) -> None:
    path = tmp_path / "grid.json"
    path.write_text(
        json.dumps(
            {
                "run_label": "grid_run",
                "cycle": "15m",
                "decision_start": "2026-03-15",
                "decision_end": "2026-03-21",
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
    assert all("grid_run-xrp-bundle_a-max1-" in label for label in groups[0].case_run_labels)
    assert groups[1].group_label == "xrp/bundle_a/maxu"
    assert groups[1].max_trades_per_market is None
    assert all("grid_run-xrp-bundle_a-maxu-" in label for label in groups[1].case_run_labels)
