from __future__ import annotations

from pathlib import Path
from typing import Iterable

from pm15min.data.config import DataConfig
from pm15min.research.datasets.feature_frames import build_feature_frame_dataset
from pm15min.research.config import ResearchConfig
from pm15min.research.labels.datasets import build_label_frame_dataset


def rebuild_label_frame_after_backfill(
    cfg: ResearchConfig | DataConfig,
    *,
    skip_freshness: bool = False,
    dependency_mode: str = "auto_repair",
) -> dict[str, object]:
    research_cfg = _coerce_research_cfg(cfg)
    feature_frames = rebuild_feature_frames_after_backfill(
        research_cfg,
        skip_freshness=skip_freshness,
        dependency_mode=dependency_mode,
    )
    summary = build_label_frame_dataset(
        research_cfg,
        skip_freshness=skip_freshness,
        dependency_mode=dependency_mode,
    )
    payload = {
        "workflow": "research_backfill_followup",
        "artifact": "label_frame",
        "market": research_cfg.asset.slug,
        "cycle": research_cfg.cycle,
        "source_surface": research_cfg.source_surface,
        "label_set": research_cfg.label_set,
        "skip_freshness": bool(skip_freshness),
        "summary": summary,
        "feature_frames": feature_frames,
        "feature_frame_count": int(len(feature_frames)),
    }
    return payload


def rebuild_feature_frames_after_backfill(
    cfg: ResearchConfig | DataConfig,
    *,
    skip_freshness: bool = False,
    dependency_mode: str = "auto_repair",
) -> list[dict[str, object]]:
    research_cfg = _coerce_research_cfg(cfg)
    feature_sets = _discover_backfill_feature_sets(research_cfg)
    results: list[dict[str, object]] = []
    for feature_set in feature_sets:
        feature_cfg = ResearchConfig.build(
            market=research_cfg.asset.slug,
            cycle=research_cfg.cycle,
            source_surface=research_cfg.source_surface,
            feature_set=feature_set,
            label_set=research_cfg.label_set,
            profile=research_cfg.profile,
            target=research_cfg.target,
            model_family=research_cfg.model_family,
            run_prefix=research_cfg.run_prefix,
            root=research_cfg.layout.storage.rewrite_root,
        )
        summary = build_feature_frame_dataset(
            feature_cfg,
            skip_freshness=skip_freshness,
            dependency_mode=dependency_mode,
        )
        results.append(
            {
                "workflow": "research_backfill_followup",
                "artifact": "feature_frame",
                "market": feature_cfg.asset.slug,
                "cycle": feature_cfg.cycle,
                "source_surface": feature_cfg.source_surface,
                "feature_set": feature_cfg.feature_set,
                "skip_freshness": bool(skip_freshness),
                "summary": summary,
            }
        )
    return results


def run_research_backfill_followups(
    *,
    markets: list[str],
    cycle: str = "15m",
    source_surface: str = "backtest",
    label_set: str = "truth",
    skip_freshness: bool = False,
    dependency_mode: str = "auto_repair",
    root: Path | None = None,
) -> dict[str, object]:
    results: list[dict[str, object]] = []
    for market in markets:
        cfg = ResearchConfig.build(
            market=market,
            cycle=cycle,
            source_surface=source_surface,
            label_set=label_set,
            root=root,
        )
        results.append(
            rebuild_label_frame_after_backfill(
                cfg,
                skip_freshness=skip_freshness,
                dependency_mode=dependency_mode,
            )
        )
    return {
        "dataset": "research_backfill_followups",
        "markets": [str(market) for market in markets],
        "cycle": str(cycle),
        "source_surface": str(source_surface),
        "label_set": str(label_set),
        "skip_freshness": bool(skip_freshness),
        "results": results,
    }


def _coerce_research_cfg(cfg: ResearchConfig | DataConfig) -> ResearchConfig:
    if isinstance(cfg, ResearchConfig):
        return cfg
    return ResearchConfig.build(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        source_surface=cfg.surface,
        root=cfg.layout.storage.rewrite_root,
    )


def _discover_backfill_feature_sets(cfg: ResearchConfig) -> list[str]:
    feature_sets: set[str] = set()
    feature_surface = str(cfg.source_surface)
    for path in _iter_existing_feature_surface_dirs(cfg):
        if path.name != f"source_surface={feature_surface}":
            continue
        feature_token = path.parent.name.partition("=")[2].strip()
        if feature_token:
            feature_sets.add(feature_token)
    for path in cfg.layout.training_sets_root.glob("feature_set=*"):
        feature_token = path.name.partition("=")[2].strip()
        if feature_token:
            feature_sets.add(feature_token)
    return sorted(feature_sets)


def _iter_existing_feature_surface_dirs(cfg: ResearchConfig) -> Iterable[Path]:
    root = cfg.layout.feature_frames_root
    if not root.exists():
        return ()
    return (
        path
        for path in root.glob("feature_set=*/source_surface=*")
        if path.is_dir() and ((path / "data.parquet").exists() or (path / "manifest.json").exists())
    )
