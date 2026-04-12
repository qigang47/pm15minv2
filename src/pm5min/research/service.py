from __future__ import annotations


def describe_research_runtime(cfg) -> dict[str, object]:
    return {
        "domain": "research",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "profile": cfg.profile,
        "source_surface": cfg.source_surface,
        "feature_set": cfg.feature_set,
        "label_set": cfg.label_set,
        "target": cfg.target,
        "model_family": cfg.model_family,
        "run_prefix": cfg.run_prefix,
        "layout": cfg.layout.to_dict(),
        "research_root": str(cfg.layout.storage.research_root),
        "market_training_runs_root": str(cfg.layout.training_runs_root),
        "market_model_bundles_root": str(cfg.layout.model_bundles_root),
        "market_active_bundles_root": str(cfg.layout.active_bundles_root),
        "active_bundle_selection_path": str(
            cfg.layout.active_bundle_selection_path(profile=cfg.profile, target=cfg.target)
        ),
        "suite_specs_root": str(cfg.layout.storage.suite_specs_root),
        "evaluations_root": str(cfg.layout.storage.evaluations_root),
    }
