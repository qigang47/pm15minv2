from __future__ import annotations

from dataclasses import dataclass

from pmshared.assets import AssetSpec, resolve_asset

from .layout import MarketLayout


@dataclass(frozen=True)
class BaseConfig:
    asset: AssetSpec
    layout: MarketLayout

    def to_dict(self) -> dict[str, object]:
        return {
            "market": self.asset.slug,
            "asset_name": self.asset.asset_name,
            "binance_symbol": self.asset.binance_symbol,
            "layout": self.layout.to_dict(),
        }


@dataclass(frozen=True)
class LiveConfig(BaseConfig):
    profile: str = "default"
    cycle_minutes: int = 15
    loop: bool = False
    refresh_interval_minutes: int = 30
    decision_poll_interval_sec: float = 1.0

    @classmethod
    def build(
        cls,
        *,
        market: str,
        profile: str,
        cycle_minutes: int = 15,
        loop: bool = False,
        refresh_interval_minutes: int = 30,
        decision_poll_interval_sec: float = 1.0,
    ) -> "LiveConfig":
        asset = resolve_asset(market)
        return cls(
            asset=asset,
            layout=MarketLayout.for_market(asset.slug),
            profile=profile,
            cycle_minutes=cycle_minutes,
            loop=loop,
            refresh_interval_minutes=refresh_interval_minutes,
            decision_poll_interval_sec=decision_poll_interval_sec,
        )

    def to_dict(self) -> dict[str, object]:
        payload = super().to_dict()
        payload.update(
            {
                "profile": self.profile,
                "cycle_minutes": self.cycle_minutes,
                "loop": self.loop,
                "refresh_interval_minutes": self.refresh_interval_minutes,
                "decision_poll_interval_sec": self.decision_poll_interval_sec,
            }
        )
        return payload
