from __future__ import annotations

from pm15min.data.sources.polygon_rpc import DEFAULT_RPC_URLS, PolygonRpcClient


def test_polygon_rpc_client_prefers_env_urls(monkeypatch) -> None:
    monkeypatch.setenv("POLYGON_RPC_URL", "https://polygon-rpc.com")
    monkeypatch.setenv("POLYGON_RPC_BACKUPS", "https://polygon-backup.example, https://polygon.drpc.org")

    client = PolygonRpcClient()

    assert client.urls == [
        "https://polygon-rpc.com",
        "https://polygon-backup.example",
        "https://polygon.drpc.org",
        *[url for url in DEFAULT_RPC_URLS if url != "https://polygon.drpc.org"],
    ]
    assert client.primary_url == "https://polygon-rpc.com"
