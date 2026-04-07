from __future__ import annotations

import json
from pathlib import Path

from pm15min.research.features.registry import feature_set_columns


def test_feature_set_columns_reads_custom_feature_sets_from_repo_registry(
    monkeypatch,
    tmp_path: Path,
) -> None:
    experiments_root = tmp_path / "research" / "experiments"
    experiments_root.mkdir(parents=True, exist_ok=True)
    (experiments_root / "custom_feature_sets.json").write_text(
        json.dumps(
            {
                "focus_btc_12_v1": {
                    "columns": [
                        "q_bs_up_strike",
                        "first_half_ret",
                        "bb_pos_20",
                        "ret_from_cycle_open",
                    ],
                    "notes": "btc focus set",
                }
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("pm15min.research.features.registry.rewrite_root", lambda: tmp_path)

    assert feature_set_columns("focus_btc_12_v1") == (
        "q_bs_up_strike",
        "first_half_ret",
        "bb_pos_20",
        "ret_from_cycle_open",
    )

