from __future__ import annotations

from pathlib import Path

from pm15min.data.config import DataConfig
from pm15min.research.config import ResearchConfig
from pm15min.research.workflows.backfill_followups import rebuild_label_frame_after_backfill


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}", encoding="utf-8")


def test_rebuild_label_frame_after_backfill_rebuilds_existing_feature_frames(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        root=root,
    )
    _touch(cfg.layout.feature_frame_path("bs_q_replace_direction", source_surface="backtest"))
    _touch(cfg.layout.feature_frame_manifest_path("bs_q_replace_direction", source_surface="backtest"))
    _touch(cfg.layout.feature_frame_path("alpha_search_direction_live", source_surface="backtest"))

    feature_calls: list[str] = []
    label_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        "pm15min.research.workflows.backfill_followups.build_feature_frame_dataset",
        lambda cfg, **kwargs: feature_calls.append(cfg.feature_set) or {
            "dataset": "feature_frame",
            "market": cfg.asset.slug,
            "feature_set": cfg.feature_set,
            "source_surface": cfg.source_surface,
            **kwargs,
        },
    )
    monkeypatch.setattr(
        "pm15min.research.workflows.backfill_followups.build_label_frame_dataset",
        lambda cfg, **kwargs: label_calls.append((cfg.asset.slug, cfg.label_set)) or {
            "dataset": "label_frame",
            "market": cfg.asset.slug,
            "label_set": cfg.label_set,
            **kwargs,
        },
    )

    payload = rebuild_label_frame_after_backfill(
        cfg,
        skip_freshness=True,
        dependency_mode="fail_fast",
    )

    assert feature_calls == ["alpha_search_direction_live", "bs_q_replace_direction"]
    assert label_calls == [("sol", "truth")]
    assert payload["artifact"] == "label_frame"
    assert payload["feature_frame_count"] == 2
    assert [item["feature_set"] for item in payload["feature_frames"]] == [
        "alpha_search_direction_live",
        "bs_q_replace_direction",
    ]
    assert payload["summary"]["label_set"] == "truth"


def test_rebuild_label_frame_after_backfill_accepts_data_config(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    research_cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        root=root,
    )
    _touch(research_cfg.layout.feature_frame_path("bs_q_replace_direction", source_surface="backtest"))

    seen = {"feature_market": None, "label_surface": None}

    monkeypatch.setattr(
        "pm15min.research.workflows.backfill_followups.build_feature_frame_dataset",
        lambda cfg, **kwargs: seen.__setitem__("feature_market", cfg.asset.slug) or {"dataset": "feature_frame", **kwargs},
    )
    monkeypatch.setattr(
        "pm15min.research.workflows.backfill_followups.build_label_frame_dataset",
        lambda cfg, **kwargs: seen.__setitem__("label_surface", cfg.source_surface) or {"dataset": "label_frame", **kwargs},
    )

    payload = rebuild_label_frame_after_backfill(data_cfg, skip_freshness=True)

    assert payload["market"] == "sol"
    assert payload["source_surface"] == "backtest"
    assert seen == {
        "feature_market": "sol",
        "label_surface": "backtest",
    }
