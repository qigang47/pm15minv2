from __future__ import annotations

import os

from dotenv import load_dotenv

from .contracts import DataApiConfig, RedeemRelayConfig, TradingAuthConfig


DEFAULT_DATA_API_BASE = "https://data-api.polymarket.com"
DEFAULT_CLOB_HOST = "https://clob.polymarket.com"
DEFAULT_CHAIN_ID = 137
DEFAULT_RELAYER_URL = "https://relayer-v2.polymarket.com/"
DEFAULT_RPC_FALLBACKS = (
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
    "https://1rpc.io/matic",
)


def load_trading_auth_config_from_env() -> TradingAuthConfig:
    load_dotenv()
    return TradingAuthConfig(
        host=_clean(os.getenv("POLYMARKET_HOST")) or DEFAULT_CLOB_HOST,
        chain_id=_int_or_default(os.getenv("POLYMARKET_CHAIN_ID"), default=DEFAULT_CHAIN_ID),
        private_key=_clean(os.getenv("POLYMARKET_PRIVATE_KEY")) or "",
        signature_type=_int_or_default(os.getenv("POLYMARKET_SIGNATURE_TYPE"), default=2),
        funder_address=_clean(os.getenv("POLYMARKET_FUNDER")),
    )


def load_data_api_config_from_env() -> DataApiConfig:
    load_dotenv()
    return DataApiConfig(
        user_address=_clean(os.getenv("POLYMARKET_USER_ADDRESS")),
        base_url=_clean(os.getenv("POLYMARKET_DATA_API_BASE")) or DEFAULT_DATA_API_BASE,
    )


def load_redeem_relay_config_from_env() -> RedeemRelayConfig:
    load_dotenv()
    rpc_urls = _resolve_rpc_candidates_from_env()
    return RedeemRelayConfig(
        rpc_urls=tuple(rpc_urls),
        relayer_url=_clean(os.getenv("POLYMARKET_RELAYER_URL")) or DEFAULT_RELAYER_URL,
        builder_api_key=_clean(os.getenv("BUILDER_API_KEY")) or _clean(os.getenv("POLYMARKET_API_KEY")) or "",
        builder_secret=_clean(os.getenv("BUILDER_SECRET")) or _clean(os.getenv("POLYMARKET_API_SECRET")) or "",
        builder_passphrase=_clean(os.getenv("BUILDER_PASS_PHRASE")) or _clean(os.getenv("POLYMARKET_API_PASSPHRASE")) or "",
    )


def _clean(value: object) -> str | None:
    if value is None:
        return None
    out = str(value).strip()
    return out or None


def _int_or_default(value: object, *, default: int) -> int:
    try:
        if value is None:
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _split_rpc_list(raw: object) -> list[str]:
    if raw is None:
        return []
    text = str(raw).strip()
    if not text:
        return []
    parts = [part.strip() for token in text.replace(";", ",").split(",") for part in token.split()]
    return [part for part in parts if part]


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _resolve_rpc_candidates_from_env() -> list[str]:
    out: list[str] = []
    for key in ("RPC_URL", "POLYGON_RPC", "POLYGON_RPC_URL", "WEB3_PROVIDER_URI"):
        out.extend(_split_rpc_list(os.getenv(key)))
    for key in ("RPC_URL_BACKUPS", "POLYGON_RPC_BACKUPS", "RPC_FALLBACKS", "POLYGON_RPC_FALLBACKS"):
        out.extend(_split_rpc_list(os.getenv(key)))
    out.extend(DEFAULT_RPC_FALLBACKS)
    return _dedupe_keep_order(out)
