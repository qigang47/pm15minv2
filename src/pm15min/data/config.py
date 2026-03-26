from __future__ import annotations

from dataclasses import dataclass

from pm15min.core.assets import AssetSpec, resolve_asset
from .layout import DataLayout, MarketDataLayout, normalize_cycle, normalize_surface


@dataclass(frozen=True)
class DataConfig:
    asset: AssetSpec
    cycle: str
    surface: str
    layout: MarketDataLayout
    poll_interval_sec: float = 0.35
    orderbook_timeout_sec: float = 1.2
    recent_window_minutes: int = 15
    market_depth: int = 1
    market_start_offset: int = 0
    gamma_limit: int = 500
    max_pages: int = 200
    sleep_sec: float = 0.03

    @classmethod
    def build(
        cls,
        *,
        market: str,
        cycle: str | int = "15m",
        surface: str = "backtest",
        poll_interval_sec: float = 0.35,
        orderbook_timeout_sec: float = 1.2,
        recent_window_minutes: int = 15,
        market_depth: int = 1,
        market_start_offset: int = 0,
        gamma_limit: int = 500,
        max_pages: int = 200,
        sleep_sec: float = 0.03,
        root=None,
    ) -> "DataConfig":
        asset = resolve_asset(market)
        storage = DataLayout.discover(root=root)
        storage.ensure_base_dirs()
        cycle_slug = normalize_cycle(cycle)
        surface_slug = normalize_surface(surface)
        return cls(
            asset=asset,
            cycle=cycle_slug,
            surface=surface_slug,
            layout=storage.for_market(asset, cycle_slug, surface=surface_slug),
            poll_interval_sec=max(0.05, float(poll_interval_sec)),
            orderbook_timeout_sec=max(0.1, float(orderbook_timeout_sec)),
            recent_window_minutes=max(1, int(recent_window_minutes)),
            market_depth=max(1, int(market_depth)),
            market_start_offset=max(0, int(market_start_offset)),
            gamma_limit=max(1, int(gamma_limit)),
            max_pages=max(1, int(max_pages)),
            sleep_sec=max(0.0, float(sleep_sec)),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "market": self.asset.slug,
            "asset_name": self.asset.asset_name,
            "binance_symbol": self.asset.binance_symbol,
            "cycle": self.cycle,
            "surface": self.surface,
            "poll_interval_sec": self.poll_interval_sec,
            "orderbook_timeout_sec": self.orderbook_timeout_sec,
            "recent_window_minutes": self.recent_window_minutes,
            "market_depth": self.market_depth,
            "market_start_offset": self.market_start_offset,
            "gamma_limit": self.gamma_limit,
            "max_pages": self.max_pages,
            "sleep_sec": self.sleep_sec,
            "layout": self.layout.to_dict(),
        }
