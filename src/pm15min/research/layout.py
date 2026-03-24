from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pm15min.core.assets import AssetSpec, resolve_asset
from pm15min.core.layout import rewrite_root
from pm15min.research.layout_helpers import (
    date_text,
    normalize_cycle,
    normalize_source_surface,
    normalize_target,
    slug_token,
    utc_run_label,
    window_label,
)


@dataclass(frozen=True)
class ResearchLayout:
    rewrite_root: Path

    @classmethod
    def discover(cls, root: Path | None = None) -> "ResearchLayout":
        return cls(rewrite_root=Path(root) if root is not None else rewrite_root())

    @property
    def research_root(self) -> Path:
        return self.rewrite_root / "research"

    @property
    def feature_frames_root(self) -> Path:
        return self.research_root / "feature_frames"

    @property
    def label_frames_root(self) -> Path:
        return self.research_root / "label_frames"

    @property
    def training_sets_root(self) -> Path:
        return self.research_root / "training_sets"

    @property
    def training_runs_root(self) -> Path:
        return self.research_root / "training_runs"

    @property
    def model_bundles_root(self) -> Path:
        return self.research_root / "model_bundles"

    @property
    def active_bundles_root(self) -> Path:
        return self.research_root / "active_bundles"

    @property
    def backtests_root(self) -> Path:
        return self.research_root / "backtests"

    @property
    def experiments_root(self) -> Path:
        return self.research_root / "experiments"

    @property
    def suite_specs_root(self) -> Path:
        return self.experiments_root / "suite_specs"

    @property
    def experiment_runs_root(self) -> Path:
        return self.experiments_root / "runs"

    @property
    def evaluations_root(self) -> Path:
        return self.research_root / "evaluations"

    @property
    def var_root(self) -> Path:
        return self.rewrite_root / "var" / "research"

    @property
    def cache_root(self) -> Path:
        return self.var_root / "cache"

    @property
    def locks_root(self) -> Path:
        return self.var_root / "locks"

    @property
    def logs_root(self) -> Path:
        return self.var_root / "logs"

    @property
    def tmp_root(self) -> Path:
        return self.var_root / "tmp"

    def ensure_base_dirs(self) -> None:
        for path in (
            self.feature_frames_root,
            self.label_frames_root,
            self.training_sets_root,
            self.training_runs_root,
            self.model_bundles_root,
            self.active_bundles_root,
            self.backtests_root,
            self.suite_specs_root,
            self.experiment_runs_root,
            self.evaluations_root,
            self.cache_root,
            self.locks_root,
            self.logs_root,
            self.tmp_root,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def for_market(self, market: str | AssetSpec, cycle: str | int = "15m") -> "MarketResearchLayout":
        asset = market if isinstance(market, AssetSpec) else resolve_asset(str(market))
        return MarketResearchLayout(
            storage=self,
            asset=asset,
            cycle=normalize_cycle(cycle),
        )

    def suite_spec_path(self, suite_name: str) -> Path:
        return self.suite_specs_root / f"{slug_token(suite_name)}.json"

    def experiment_run_dir(self, suite_name: str, run_label_text: str) -> Path:
        return (
            self.experiment_runs_root
            / f"suite={slug_token(suite_name)}"
            / f"run={slug_token(run_label_text, default='planned')}"
        )

    def evaluation_run_dir(
        self,
        category: str,
        *,
        asset: str | AssetSpec,
        scope_label: str,
        run_label_text: str,
    ) -> Path:
        asset_spec = asset if isinstance(asset, AssetSpec) else resolve_asset(str(asset))
        return (
            self.evaluations_root
            / slug_token(category)
            / f"asset={asset_spec.slug}"
            / f"scope={slug_token(scope_label)}"
            / f"run={slug_token(run_label_text, default='planned')}"
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "rewrite_root": str(self.rewrite_root),
            "research_root": str(self.research_root),
            "feature_frames_root": str(self.feature_frames_root),
            "label_frames_root": str(self.label_frames_root),
            "training_sets_root": str(self.training_sets_root),
            "training_runs_root": str(self.training_runs_root),
            "model_bundles_root": str(self.model_bundles_root),
            "active_bundles_root": str(self.active_bundles_root),
            "backtests_root": str(self.backtests_root),
            "suite_specs_root": str(self.suite_specs_root),
            "experiment_runs_root": str(self.experiment_runs_root),
            "evaluations_root": str(self.evaluations_root),
            "research_var_root": str(self.var_root),
            "research_cache_root": str(self.cache_root),
            "research_locks_root": str(self.locks_root),
            "research_logs_root": str(self.logs_root),
            "research_tmp_root": str(self.tmp_root),
        }


@dataclass(frozen=True)
class MarketResearchLayout:
    storage: ResearchLayout
    asset: AssetSpec
    cycle: str

    @property
    def cycle_root(self) -> str:
        return f"cycle={self.cycle}"

    @property
    def asset_root(self) -> str:
        return f"asset={self.asset.slug}"

    @property
    def feature_frames_root(self) -> Path:
        return self.storage.feature_frames_root / self.cycle_root / self.asset_root

    @property
    def label_frames_root(self) -> Path:
        return self.storage.label_frames_root / self.cycle_root / self.asset_root

    @property
    def training_sets_root(self) -> Path:
        return self.storage.training_sets_root / self.cycle_root / self.asset_root

    @property
    def training_runs_root(self) -> Path:
        return self.storage.training_runs_root / self.cycle_root / self.asset_root

    @property
    def model_bundles_root(self) -> Path:
        return self.storage.model_bundles_root / self.cycle_root / self.asset_root

    @property
    def active_bundles_root(self) -> Path:
        return self.storage.active_bundles_root / self.cycle_root / self.asset_root

    @property
    def backtests_root(self) -> Path:
        return self.storage.backtests_root / self.cycle_root / self.asset_root

    @property
    def evaluations_root(self) -> Path:
        return self.storage.evaluations_root

    def feature_frame_dir(self, feature_set: str, *, source_surface: str = "backtest") -> Path:
        return (
            self.feature_frames_root
            / f"feature_set={slug_token(feature_set)}"
            / f"source_surface={normalize_source_surface(source_surface)}"
        )

    def feature_frame_path(self, feature_set: str, *, source_surface: str = "backtest") -> Path:
        return self.feature_frame_dir(feature_set, source_surface=source_surface) / "data.parquet"

    def feature_frame_manifest_path(self, feature_set: str, *, source_surface: str = "backtest") -> Path:
        return self.feature_frame_dir(feature_set, source_surface=source_surface) / "manifest.json"

    def label_frame_dir(self, label_set: str) -> Path:
        return self.label_frames_root / f"label_set={slug_token(label_set)}"

    def label_frame_path(self, label_set: str) -> Path:
        return self.label_frame_dir(label_set) / "data.parquet"

    def label_frame_manifest_path(self, label_set: str) -> Path:
        return self.label_frame_dir(label_set) / "manifest.json"

    def training_set_dir(
        self,
        *,
        feature_set: str,
        label_set: str,
        target: str,
        window: str,
        offset: int,
    ) -> Path:
        return (
            self.training_sets_root
            / f"feature_set={slug_token(feature_set)}"
            / f"label_set={slug_token(label_set)}"
            / f"target={normalize_target(target)}"
            / f"window={slug_token(window)}"
            / f"offset={int(offset)}"
        )

    def training_set_path(
        self,
        *,
        feature_set: str,
        label_set: str,
        target: str,
        window: str,
        offset: int,
    ) -> Path:
        return self.training_set_dir(
            feature_set=feature_set,
            label_set=label_set,
            target=target,
            window=window,
            offset=offset,
        ) / "data.parquet"

    def training_set_manifest_path(
        self,
        *,
        feature_set: str,
        label_set: str,
        target: str,
        window: str,
        offset: int,
    ) -> Path:
        return self.training_set_dir(
            feature_set=feature_set,
            label_set=label_set,
            target=target,
            window=window,
            offset=offset,
        ) / "manifest.json"

    def training_run_dir(self, *, model_family: str, target: str, run_label_text: str) -> Path:
        return (
            self.training_runs_root
            / f"model_family={slug_token(model_family)}"
            / f"target={normalize_target(target)}"
            / f"run={slug_token(run_label_text, default='planned')}"
        )

    def training_run_manifest_path(self, *, model_family: str, target: str, run_label_text: str) -> Path:
        return self.training_run_dir(
            model_family=model_family,
            target=target,
            run_label_text=run_label_text,
        ) / "manifest.json"

    def bundle_dir(self, *, profile: str, target: str, bundle_label_text: str) -> Path:
        return (
            self.model_bundles_root
            / f"profile={slug_token(profile)}"
            / f"target={normalize_target(target)}"
            / f"bundle={slug_token(bundle_label_text, default='planned')}"
        )

    def bundle_manifest_path(self, *, profile: str, target: str, bundle_label_text: str) -> Path:
        return self.bundle_dir(
            profile=profile,
            target=target,
            bundle_label_text=bundle_label_text,
        ) / "manifest.json"

    def active_bundle_dir(self, *, profile: str, target: str) -> Path:
        return (
            self.active_bundles_root
            / f"profile={slug_token(profile)}"
            / f"target={normalize_target(target)}"
        )

    def active_bundle_selection_path(self, *, profile: str, target: str) -> Path:
        return self.active_bundle_dir(profile=profile, target=target) / "selection.json"

    def backtest_run_dir(self, *, profile: str, spec_name: str, run_label_text: str) -> Path:
        return (
            self.backtests_root
            / f"profile={slug_token(profile)}"
            / f"spec={slug_token(spec_name)}"
            / f"run={slug_token(run_label_text, default='planned')}"
        )

    def backtest_manifest_path(self, *, profile: str, spec_name: str, run_label_text: str) -> Path:
        return self.backtest_run_dir(
            profile=profile,
            spec_name=spec_name,
            run_label_text=run_label_text,
        ) / "manifest.json"

    def to_dict(self) -> dict[str, str]:
        return {
            **self.storage.to_dict(),
            "market": self.asset.slug,
            "asset_name": self.asset.asset_name,
            "binance_symbol": self.asset.binance_symbol,
            "cycle": self.cycle,
            "market_feature_frames_root": str(self.feature_frames_root),
            "market_label_frames_root": str(self.label_frames_root),
            "market_training_sets_root": str(self.training_sets_root),
            "market_training_runs_root": str(self.training_runs_root),
            "market_model_bundles_root": str(self.model_bundles_root),
            "market_active_bundles_root": str(self.active_bundles_root),
            "market_backtests_root": str(self.backtests_root),
        }
