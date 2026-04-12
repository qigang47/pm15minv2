from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AssetSpec:
    slug: str
    asset_name: str
    binance_symbol: str


ASSET_SPECS: dict[str, AssetSpec] = {
    "btc": AssetSpec(slug="btc", asset_name="bitcoin", binance_symbol="BTCUSDT"),
    "eth": AssetSpec(slug="eth", asset_name="ethereum", binance_symbol="ETHUSDT"),
    "sol": AssetSpec(slug="sol", asset_name="solana", binance_symbol="SOLUSDT"),
    "xrp": AssetSpec(slug="xrp", asset_name="xrp", binance_symbol="XRPUSDT"),
}


def supported_markets() -> tuple[str, ...]:
    return tuple(ASSET_SPECS.keys())


def resolve_asset(slug: str | None) -> AssetSpec:
    normalized = (slug or "btc").strip().lower()
    try:
        return ASSET_SPECS[normalized]
    except KeyError as exc:
        supported = ", ".join(supported_markets())
        raise ValueError(f"Unsupported market {normalized!r}. Supported: {supported}") from exc
