from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pm15min.core.assets import AssetSpec, resolve_asset
from pm15min.research.labels.sources import normalize_label_set
from pm15min.research.layout import (
    MarketResearchLayout,
    ResearchLayout,
    normalize_cycle,
    normalize_source_surface,
    normalize_target,
    slug_token,
)


@dataclass(frozen=True)
class ResearchConfig:
    asset: AssetSpec
    cycle: str
    profile: str
    source_surface: str
    feature_set: str
    label_set: str
    target: str
    model_family: str
    run_prefix: str | None
    layout: MarketResearchLayout

    @classmethod
    def build(
        cls,
        *,
        market: str,
        cycle: str | int = "15m",
        profile: str = "default",
        source_surface: str = "backtest",
        feature_set: str = "deep_otm_v1",
        label_set: str = "truth",
        target: str = "direction",
        model_family: str = "deep_otm",
        run_prefix: str | None = None,
        root: Path | None = None,
    ) -> "ResearchConfig":
        asset = resolve_asset(market)
        storage = ResearchLayout.discover(root=root)
        storage.ensure_base_dirs()
        cycle_slug = normalize_cycle(cycle)
        return cls(
            asset=asset,
            cycle=cycle_slug,
            profile=slug_token(profile),
            source_surface=normalize_source_surface(source_surface),
            feature_set=slug_token(feature_set),
            label_set=normalize_label_set(label_set),
            target=normalize_target(target),
            model_family=slug_token(model_family),
            run_prefix=slug_token(run_prefix) if run_prefix else None,
            layout=storage.for_market(asset, cycle=cycle_slug),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "market": self.asset.slug,
            "asset_name": self.asset.asset_name,
            "binance_symbol": self.asset.binance_symbol,
            "cycle": self.cycle,
            "profile": self.profile,
            "source_surface": self.source_surface,
            "feature_set": self.feature_set,
            "label_set": self.label_set,
            "target": self.target,
            "model_family": self.model_family,
            "run_prefix": self.run_prefix,
            "layout": self.layout.to_dict(),
        }
