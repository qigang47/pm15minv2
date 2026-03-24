from __future__ import annotations

from pm15min.live.trading.auth import (
    DEFAULT_CHAIN_ID,
    DEFAULT_CLOB_HOST,
    DEFAULT_DATA_API_BASE,
    DEFAULT_RELAYER_URL,
    load_data_api_config_from_env,
    load_redeem_relay_config_from_env,
    load_trading_auth_config_from_env,
)


def test_load_trading_auth_config_from_env(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", " test-private-key ")
    monkeypatch.setenv("POLYMARKET_SIGNATURE_TYPE", "3")
    monkeypatch.setenv("POLYMARKET_FUNDER", " 0xfunder ")

    cfg = load_trading_auth_config_from_env()

    assert cfg.host == DEFAULT_CLOB_HOST
    assert cfg.chain_id == DEFAULT_CHAIN_ID
    assert cfg.private_key == "test-private-key"
    assert cfg.signature_type == 3
    assert cfg.funder_address == "0xfunder"
    assert cfg.is_configured is True


def test_load_data_api_config_from_env_defaults(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_USER_ADDRESS", "")
    monkeypatch.setenv("POLYMARKET_DATA_API_BASE", "")

    cfg = load_data_api_config_from_env()

    assert cfg.user_address is None
    assert cfg.base_url == DEFAULT_DATA_API_BASE
    assert cfg.is_configured is False


def test_load_redeem_relay_config_from_env(monkeypatch) -> None:
    monkeypatch.setenv("RPC_URL", "https://rpc-1")
    monkeypatch.setenv("RPC_URL_BACKUPS", "https://rpc-2,https://rpc-1")
    monkeypatch.setenv("POLYGON_RPC", "")
    monkeypatch.setenv("POLYGON_RPC_URL", "")
    monkeypatch.setenv("WEB3_PROVIDER_URI", "")
    monkeypatch.setenv("POLYGON_RPC_BACKUPS", "")
    monkeypatch.setenv("RPC_FALLBACKS", "")
    monkeypatch.setenv("POLYGON_RPC_FALLBACKS", "")
    monkeypatch.setenv("POLYMARKET_RELAYER_URL", "https://relayer")
    monkeypatch.setenv("BUILDER_API_KEY", "builder-key")
    monkeypatch.setenv("BUILDER_SECRET", "builder-secret")
    monkeypatch.setenv("BUILDER_PASS_PHRASE", "builder-pass")

    cfg = load_redeem_relay_config_from_env()

    assert cfg.rpc_urls[:2] == ("https://rpc-1", "https://rpc-2")
    assert cfg.relayer_url == "https://relayer"
    assert cfg.builder_api_key == "builder-key"
    assert cfg.builder_secret == "builder-secret"
    assert cfg.builder_passphrase == "builder-pass"
    assert cfg.is_configured is True
    assert DEFAULT_RELAYER_URL != ""
