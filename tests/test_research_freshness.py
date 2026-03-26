from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.config import ResearchConfig
from pm15min.research.freshness import ensure_research_artifacts_aligned


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def test_ensure_research_artifacts_aligned_rebuilds_stale_chain(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = ResearchConfig.build(market="sol", cycle="15m", source_surface="backtest", root=root)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(pd.DataFrame([{"market_id": "m-1"}]), data_cfg.layout.market_catalog_table_path)
    write_parquet_atomic(
        pd.DataFrame([{"asset": "sol", "cycle_start_ts": 1, "cycle_end_ts": 2, "price_to_beat": 100.0}]),
        data_cfg.layout.direct_oracle_source_path,
    )
    write_parquet_atomic(
        pd.DataFrame([{"asset": "sol", "extra_ts": 2, "price": 101.0}]),
        data_cfg.layout.streams_partition_path(2026, 3),
    )
    write_parquet_atomic(
        pd.DataFrame([{"asset": "sol", "cycle_start_ts": 1, "cycle_end_ts": 2, "winner_side": "UP", "full_truth": True}]),
        data_cfg.layout.settlement_truth_source_path,
    )

    write_parquet_atomic(pd.DataFrame([{"stale": True}]), data_cfg.layout.oracle_prices_table_path)
    write_parquet_atomic(pd.DataFrame([{"stale": True}]), data_cfg.layout.truth_table_path)
    write_parquet_atomic(
        pd.DataFrame([{"stale": True}]),
        cfg.layout.feature_frame_path(cfg.feature_set, source_surface=cfg.source_surface),
    )
    write_parquet_atomic(pd.DataFrame([{"stale": True}]), cfg.layout.label_frame_path(cfg.label_set))

    for path in (
        data_cfg.layout.oracle_prices_table_path,
        data_cfg.layout.truth_table_path,
        cfg.layout.feature_frame_path(cfg.feature_set, source_surface=cfg.source_surface),
        cfg.layout.label_frame_path(cfg.label_set),
    ):
        os.utime(path, (1, 1))

    calls: list[str] = []

    def _fake_oracle(target_cfg):
        calls.append("oracle")
        write_parquet_atomic(
            pd.DataFrame(
                [
                    {
                        "asset": "sol",
                        "cycle_start_ts": 1,
                        "cycle_end_ts": 2,
                        "price_to_beat": 100.0,
                        "final_price": 101.0,
                        "has_price_to_beat": True,
                        "has_final_price": True,
                        "has_both": True,
                    }
                ]
            ),
            target_cfg.layout.oracle_prices_table_path,
        )
        return {"dataset": "oracle_prices_15m"}

    def _fake_truth(target_cfg):
        calls.append("truth")
        write_parquet_atomic(
            pd.DataFrame(
                [
                    {
                        "asset": "sol",
                        "cycle_start_ts": 1,
                        "cycle_end_ts": 2,
                        "winner_side": "UP",
                        "resolved": True,
                        "truth_source": "settlement_truth",
                        "full_truth": True,
                    }
                ]
            ),
            target_cfg.layout.truth_table_path,
        )
        return {"dataset": "truth_15m"}

    def _fake_feature_frame(target_cfg, *, skip_freshness=False):
        calls.append("feature")
        write_parquet_atomic(
            pd.DataFrame([{"decision_ts": "2026-03-20T00:08:00Z", "offset": 7}]),
            target_cfg.layout.feature_frame_path(target_cfg.feature_set, source_surface=target_cfg.source_surface),
        )
        return {"dataset": "feature_frame"}

    def _fake_label_frame(target_cfg, *, skip_freshness=False):
        calls.append("label")
        write_parquet_atomic(
            pd.DataFrame([{"cycle_start_ts": 1, "resolved": True, "winner_side": "UP"}]),
            target_cfg.layout.label_frame_path(target_cfg.label_set),
        )
        return {"dataset": "label_frame"}

    monkeypatch.setattr("pm15min.research.freshness.build_oracle_prices_15m", _fake_oracle)
    monkeypatch.setattr("pm15min.research.freshness.build_truth_15m", _fake_truth)
    monkeypatch.setattr("pm15min.research.datasets.feature_frames.build_feature_frame_dataset", _fake_feature_frame)
    monkeypatch.setattr("pm15min.research.labels.datasets.build_label_frame_dataset", _fake_label_frame)

    summary = ensure_research_artifacts_aligned(
        cfg,
        feature_set=cfg.feature_set,
        label_set=cfg.label_set,
    )

    assert calls == ["oracle", "truth", "feature", "label"]
    assert summary["oracle_prices_table"]["status"] == "rebuilt"
    assert summary["truth_table"]["status"] == "rebuilt"
    assert summary["feature_frame"]["status"] == "rebuilt"
    assert summary["label_frame"]["status"] == "rebuilt"

    calls.clear()
    fresh_summary = ensure_research_artifacts_aligned(
        cfg,
        feature_set=cfg.feature_set,
        label_set=cfg.label_set,
    )

    assert calls == []
    assert fresh_summary["oracle_prices_table"]["status"] == "fresh"
    assert fresh_summary["truth_table"]["status"] == "fresh"
    assert fresh_summary["feature_frame"]["status"] == "fresh"
    assert fresh_summary["label_frame"]["status"] == "fresh"


def test_ensure_research_artifacts_aligned_rebuilds_feature_frame_when_helper_module_changes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = ResearchConfig.build(market="sol", cycle="15m", source_surface="backtest", root=root)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(pd.DataFrame([{"open_time": 1, "close": 100.0}]), data_cfg.layout.binance_klines_path())
    write_parquet_atomic(
        pd.DataFrame([{"asset": "sol", "cycle_start_ts": 1, "cycle_end_ts": 2, "price_to_beat": 100.0, "final_price": 101.0}]),
        data_cfg.layout.oracle_prices_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame([{"decision_ts": "2026-03-20T00:08:00Z", "offset": 7}]),
        cfg.layout.feature_frame_path(cfg.feature_set, source_surface=cfg.source_surface),
    )

    feature_path = cfg.layout.feature_frame_path(cfg.feature_set, source_surface=cfg.source_surface)
    helper_path = tmp_path / "feature_helper.py"
    helper_path.write_text("helper = 1\n", encoding="utf-8")
    os.utime(feature_path, (10, 10))
    os.utime(helper_path, (20, 20))

    calls: list[str] = []

    def _fake_feature_frame(target_cfg, *, skip_freshness=False):
        calls.append("feature")
        write_parquet_atomic(
            pd.DataFrame([{"decision_ts": "2026-03-20T00:09:00Z", "offset": 8}]),
            target_cfg.layout.feature_frame_path(target_cfg.feature_set, source_surface=target_cfg.source_surface),
        )
        return {"dataset": "feature_frame"}

    monkeypatch.setattr(
        "pm15min.research.freshness._feature_frame_code_dependencies",
        lambda: [helper_path],
    )
    monkeypatch.setattr("pm15min.research.datasets.feature_frames.build_feature_frame_dataset", _fake_feature_frame)

    summary = ensure_research_artifacts_aligned(
        cfg,
        feature_set=cfg.feature_set,
    )

    assert calls == ["feature"]
    assert summary["feature_frame"]["status"] == "rebuilt"
    assert f"dependency_newer:{helper_path.name}" in summary["feature_frame"]["reasons"]
