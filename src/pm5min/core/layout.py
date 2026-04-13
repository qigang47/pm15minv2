from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pmshared.assets import resolve_asset


def workspace_root() -> Path:
    return _resolve_layout_roots()["workspace_root"]


def rewrite_root() -> Path:
    return _resolve_layout_roots()["rewrite_root"]


def _resolve_layout_roots() -> dict[str, Path]:
    file_path = Path(__file__).resolve()
    parents = list(file_path.parents)
    for parent in parents:
        if (parent / "v2" / "src" / "pm5min").exists():
            return {
                "workspace_root": parent,
                "rewrite_root": parent / "v2",
            }
    for parent in parents:
        if (parent / "src" / "pm5min").exists():
            return {
                "workspace_root": parent,
                "rewrite_root": parent,
            }
    fallback_workspace = file_path.parents[4]
    return {
        "workspace_root": fallback_workspace,
        "rewrite_root": fallback_workspace / "v2",
    }


@dataclass(frozen=True)
class WorkspaceLayout:
    root: Path

    @classmethod
    def discover(cls) -> "WorkspaceLayout":
        return cls(root=workspace_root())

    @property
    def data_root(self) -> Path:
        return self.root / "data"

    @property
    def markets_root(self) -> Path:
        return self.data_root / "markets"

    @property
    def raw_dir(self) -> Path:
        return self.data_root / "raw"

    @property
    def training_raw_dir(self) -> Path:
        return self.data_root / "training_raw"

    def to_dict(self) -> dict[str, str]:
        return {
            "workspace_root": str(self.root),
            "legacy_data_root": str(self.data_root),
            "legacy_markets_root": str(self.markets_root),
            "legacy_raw_dir": str(self.raw_dir),
            "legacy_training_raw_dir": str(self.training_raw_dir),
        }


@dataclass(frozen=True)
class RewriteLayout:
    root: Path

    @classmethod
    def discover(cls) -> "RewriteLayout":
        return cls(root=rewrite_root())

    @property
    def source_root(self) -> Path:
        return self.root / "src"

    @property
    def package_root(self) -> Path:
        return self.source_root / "pm5min"

    @property
    def tests_root(self) -> Path:
        return self.root / "tests"

    @property
    def data_root(self) -> Path:
        return self.root / "data"

    @property
    def live_data_root(self) -> Path:
        return self.data_root / "live"

    @property
    def backtest_data_root(self) -> Path:
        return self.data_root / "backtest"

    @property
    def research_root(self) -> Path:
        return self.root / "research"

    @property
    def var_root(self) -> Path:
        return self.root / "var"

    @property
    def live_var_root(self) -> Path:
        return self.var_root / "live"

    @property
    def research_var_root(self) -> Path:
        return self.var_root / "research"

    def to_dict(self) -> dict[str, str]:
        return {
            "rewrite_root": str(self.root),
            "source_root": str(self.source_root),
            "package_root": str(self.package_root),
            "tests_root": str(self.tests_root),
            "rewrite_data_root": str(self.data_root),
            "rewrite_live_data_root": str(self.live_data_root),
            "rewrite_backtest_data_root": str(self.backtest_data_root),
            "rewrite_research_root": str(self.research_root),
            "rewrite_var_root": str(self.var_root),
            "rewrite_live_var_root": str(self.live_var_root),
            "rewrite_research_var_root": str(self.research_var_root),
        }


@dataclass(frozen=True)
class LegacyMarketReferenceLayout:
    market: str
    workspace: WorkspaceLayout

    @property
    def market_root(self) -> Path:
        return self.workspace.markets_root / self.market

    @property
    def shared_market_root(self) -> Path:
        return self.workspace.markets_root / "_shared"

    @property
    def polymarket_root(self) -> Path:
        return self.market_root / "data" / "polymarket"

    @property
    def orderbook_root(self) -> Path:
        return self.polymarket_root / "raw" / "orderbooks_full"

    @property
    def artifacts_root(self) -> Path:
        return self.market_root / "artifacts_runs"

    @property
    def live_log_dir(self) -> Path:
        return self.market_root / "logs" / "live_trading"

    @property
    def oracle_root(self) -> Path:
        return self.shared_market_root / "oracle"

    def to_dict(self) -> dict[str, str]:
        return {
            "legacy_reference_market_root": str(self.market_root),
            "legacy_reference_shared_market_root": str(self.shared_market_root),
            "legacy_reference_polymarket_root": str(self.polymarket_root),
            "legacy_reference_orderbook_root": str(self.orderbook_root),
            "legacy_reference_artifacts_root": str(self.artifacts_root),
            "legacy_reference_live_log_dir": str(self.live_log_dir),
            "legacy_reference_oracle_root": str(self.oracle_root),
        }


@dataclass(frozen=True)
class MarketLayout:
    market: str
    workspace: WorkspaceLayout
    rewrite: RewriteLayout
    legacy: LegacyMarketReferenceLayout

    @classmethod
    def for_market(cls, market: str | None) -> "MarketLayout":
        asset = resolve_asset(market)
        workspace = WorkspaceLayout.discover()
        rewrite = RewriteLayout.discover()
        return cls(
            market=asset.slug,
            workspace=workspace,
            rewrite=rewrite,
            legacy=LegacyMarketReferenceLayout(market=asset.slug, workspace=workspace),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "market": self.market,
            **self.workspace.to_dict(),
            **self.rewrite.to_dict(),
            **self.legacy.to_dict(),
        }
