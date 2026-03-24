from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pm15min.core.assets import AssetSpec, resolve_asset
from pm15min.core.layout import rewrite_root
from .helpers import (
    cycle_seconds,
    normalize_cycle,
    normalize_surface,
    utc_snapshot_label,
)
from .paths import (
    cycle_asset_file,
    cycle_asset_root,
    snapshot_file,
    snapshot_history_file,
    year_month_file,
)


@dataclass(frozen=True)
class DataLayout:
    rewrite_root: Path

    @classmethod
    def discover(cls, root: Path | None = None) -> "DataLayout":
        return cls(rewrite_root=Path(root) if root is not None else rewrite_root())

    @property
    def data_root(self) -> Path:
        return self.rewrite_root / "data"

    @property
    def sources_root(self) -> Path:
        return self.data_root / "sources"

    @property
    def tables_root(self) -> Path:
        return self.data_root / "tables"

    @property
    def exports_root(self) -> Path:
        return self.data_root / "exports"

    @property
    def var_root(self) -> Path:
        return self.rewrite_root / "var"

    @property
    def cache_root(self) -> Path:
        return self.var_root / "cache"

    @property
    def state_root(self) -> Path:
        return self.var_root / "state"

    @property
    def logs_root(self) -> Path:
        return self.var_root / "logs"

    def ensure_base_dirs(self) -> None:
        for path in (
            self.sources_root,
            self.tables_root,
            self.exports_root,
            self.cache_root,
            self.state_root,
            self.logs_root,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def for_market(self, market: str | AssetSpec, cycle: str | int, surface: str = "backtest") -> "MarketDataLayout":
        asset = market if isinstance(market, AssetSpec) else resolve_asset(str(market))
        return MarketDataLayout(
            storage=self,
            asset=asset,
            cycle=normalize_cycle(cycle),
            surface=normalize_surface(surface),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "rewrite_root": str(self.rewrite_root),
            "data_root": str(self.data_root),
            "sources_root": str(self.sources_root),
            "tables_root": str(self.tables_root),
            "exports_root": str(self.exports_root),
            "var_root": str(self.var_root),
            "cache_root": str(self.cache_root),
            "state_root": str(self.state_root),
            "logs_root": str(self.logs_root),
        }


@dataclass(frozen=True)
class MarketDataLayout:
    storage: DataLayout
    asset: AssetSpec
    cycle: str
    surface: str

    @property
    def cycle_seconds(self) -> int:
        return cycle_seconds(self.cycle)

    @property
    def surface_data_root(self) -> Path:
        return self.storage.data_root / self.surface

    @property
    def surface_var_root(self) -> Path:
        return self.storage.var_root / self.surface

    @property
    def sources_root(self) -> Path:
        return self.surface_data_root / "sources"

    @property
    def tables_root(self) -> Path:
        return self.surface_data_root / "tables"

    @property
    def exports_root(self) -> Path:
        return self.surface_data_root / "exports"

    @property
    def cache_root(self) -> Path:
        return self.surface_var_root / "cache"

    @property
    def state_root(self) -> Path:
        return self.surface_var_root / "state"

    @property
    def logs_root(self) -> Path:
        return self.surface_var_root / "logs"

    def _cycle_asset_root(self, base: Path, *prefix: str) -> Path:
        return cycle_asset_root(base, self.cycle, self.asset.slug, *prefix)

    def _cycle_asset_file(self, base: Path, *prefix: str, filename: str = "data.parquet") -> Path:
        return cycle_asset_file(base, self.cycle, self.asset.slug, *prefix, filename=filename)

    @property
    def market_catalog_snapshot_root(self) -> Path:
        return self._cycle_asset_root(self.sources_root, "polymarket", "market_catalogs")

    def market_catalog_snapshot_path(self, snapshot_ts: str) -> Path:
        return snapshot_file(self.market_catalog_snapshot_root, snapshot_ts, filename="data.parquet")

    @property
    def market_catalog_table_path(self) -> Path:
        return self._cycle_asset_file(self.tables_root, "markets")

    @property
    def streams_source_root(self) -> Path:
        return (
            self.sources_root
            / "chainlink"
            / "streams"
            / f"asset={self.asset.slug}"
        )

    @property
    def datafeeds_source_root(self) -> Path:
        return (
            self.sources_root
            / "chainlink"
            / "datafeeds"
            / f"asset={self.asset.slug}"
        )

    @property
    def binance_klines_root(self) -> Path:
        return self.sources_root / "binance" / "klines_1m"

    def binance_klines_path(self, symbol: str | None = None) -> Path:
        resolved_symbol = str(symbol or self.asset.binance_symbol).strip().upper()
        return self.binance_klines_root / f"symbol={resolved_symbol}" / "data.parquet"

    def streams_partition_path(self, year: int, month: int) -> Path:
        return year_month_file(self.streams_source_root, year, month)

    def datafeeds_partition_path(self, year: int, month: int) -> Path:
        return year_month_file(self.datafeeds_source_root, year, month)

    @property
    def settlement_truth_source_path(self) -> Path:
        return self._cycle_asset_file(self.sources_root, "polymarket", "settlement_truth")

    @property
    def direct_oracle_source_path(self) -> Path:
        return self._cycle_asset_file(self.sources_root, "polymarket", "oracle_prices")

    @property
    def orderbook_source_root(self) -> Path:
        return self._cycle_asset_root(self.sources_root, "polymarket", "orderbooks")

    def orderbook_day_root(self, date_str: str) -> Path:
        return self.orderbook_source_root / f"date={date_str}"

    def orderbook_depth_path(self, date_str: str) -> Path:
        return self.orderbook_day_root(date_str) / "depth.ndjson.zst"

    def orderbook_index_path(self, date_str: str) -> Path:
        return self._cycle_asset_root(self.tables_root, "orderbook_index") / f"date={date_str}" / "data.parquet"

    @property
    def oracle_prices_table_path(self) -> Path:
        return self._cycle_asset_file(self.tables_root, "oracle_prices")

    @property
    def truth_table_path(self) -> Path:
        return self._cycle_asset_file(self.tables_root, "truth")

    @property
    def oracle_prices_export_path(self) -> Path:
        return self._cycle_asset_file(self.exports_root, "oracle_prices", filename="oracle_prices.csv")

    @property
    def truth_export_path(self) -> Path:
        return self._cycle_asset_file(self.exports_root, "truth", filename="truth.csv")

    @property
    def orderbook_state_path(self) -> Path:
        return self._cycle_asset_file(self.state_root, "orderbooks", filename="state.json")

    @property
    def orderbook_recent_path(self) -> Path:
        return self._cycle_asset_file(self.state_root, "orderbooks", filename="recent.parquet")

    @property
    def recorder_log_dir(self) -> Path:
        return self._cycle_asset_root(self.logs_root, "data", "recorders")

    @property
    def recorder_log_path(self) -> Path:
        return self.recorder_log_dir / "recorder.jsonl"

    @property
    def foundation_state_path(self) -> Path:
        return self._cycle_asset_file(self.state_root, "foundation", filename="state.json")

    @property
    def foundation_log_dir(self) -> Path:
        return self._cycle_asset_root(self.logs_root, "data", "foundation")

    @property
    def foundation_log_path(self) -> Path:
        return self.foundation_log_dir / "refresh.jsonl"

    @property
    def summary_state_dir(self) -> Path:
        return self._cycle_asset_root(self.state_root, "summary")

    @property
    def latest_summary_path(self) -> Path:
        return self.summary_state_dir / "latest.json"

    @property
    def latest_summary_manifest_path(self) -> Path:
        return self.summary_state_dir / "latest.manifest.json"

    def summary_snapshot_path(self, snapshot_ts: str) -> Path:
        return snapshot_history_file(self.summary_state_dir, snapshot_ts, filename="summary.json")

    def summary_manifest_snapshot_path(self, snapshot_ts: str) -> Path:
        return snapshot_history_file(self.summary_state_dir, snapshot_ts, filename="manifest.json")

    def to_dict(self) -> dict[str, str]:
        return {
            **self.storage.to_dict(),
            "surface": self.surface,
            "surface_data_root": str(self.surface_data_root),
            "surface_var_root": str(self.surface_var_root),
            "surface_sources_root": str(self.sources_root),
            "surface_tables_root": str(self.tables_root),
            "surface_exports_root": str(self.exports_root),
            "surface_cache_root": str(self.cache_root),
            "surface_state_root": str(self.state_root),
            "surface_logs_root": str(self.logs_root),
            "market": self.asset.slug,
            "asset_name": self.asset.asset_name,
            "binance_symbol": self.asset.binance_symbol,
            "cycle": self.cycle,
            "market_catalog_snapshot_root": str(self.market_catalog_snapshot_root),
            "market_catalog_table_path": str(self.market_catalog_table_path),
            "binance_klines_root": str(self.binance_klines_root),
            "binance_klines_path": str(self.binance_klines_path()),
            "streams_source_root": str(self.streams_source_root),
            "datafeeds_source_root": str(self.datafeeds_source_root),
            "settlement_truth_source_path": str(self.settlement_truth_source_path),
            "direct_oracle_source_path": str(self.direct_oracle_source_path),
            "orderbook_source_root": str(self.orderbook_source_root),
            "oracle_prices_table_path": str(self.oracle_prices_table_path),
            "truth_table_path": str(self.truth_table_path),
            "oracle_prices_export_path": str(self.oracle_prices_export_path),
            "truth_export_path": str(self.truth_export_path),
            "orderbook_state_path": str(self.orderbook_state_path),
            "recorder_log_dir": str(self.recorder_log_dir),
            "recorder_log_path": str(self.recorder_log_path),
            "foundation_state_path": str(self.foundation_state_path),
            "foundation_log_dir": str(self.foundation_log_dir),
            "foundation_log_path": str(self.foundation_log_path),
            "summary_state_dir": str(self.summary_state_dir),
            "latest_summary_path": str(self.latest_summary_path),
            "latest_summary_manifest_path": str(self.latest_summary_manifest_path),
        }
