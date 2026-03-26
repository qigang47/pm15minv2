from __future__ import annotations

from pm15min.data.pipelines.orderbook_fleet import parse_orderbook_fleet_markets, run_orderbook_recorder_fleet


def test_parse_orderbook_fleet_markets_defaults_and_dedupes() -> None:
    assert parse_orderbook_fleet_markets(None) == ["btc", "eth", "sol", "xrp"]
    assert parse_orderbook_fleet_markets("sol,xrp,sol") == ["sol", "xrp"]


def test_run_orderbook_recorder_fleet_runs_all_markets() -> None:
    calls: list[tuple[str, int]] = []

    def _fake_run_orderbook_recorder(cfg, *, iterations=1, loop=False, sleep_sec=None):
        calls.append((cfg.asset.slug, cfg.market_start_offset))
        return {
            "status": "ok",
            "market": cfg.asset.slug,
            "iterations": iterations,
            "loop": loop,
        }

    payload = run_orderbook_recorder_fleet(
        markets="btc,eth,sol,xrp",
        market_start_offset=7,
        iterations=1,
        loop=False,
        run_orderbook_recorder_fn=_fake_run_orderbook_recorder,
    )

    assert payload["status"] == "ok"
    assert payload["markets"] == ["btc", "eth", "sol", "xrp"]
    assert {market for market, _ in calls} == {"btc", "eth", "sol", "xrp"}
    assert {offset for _, offset in calls} == {7}
    assert payload["results"]["sol"]["market"] == "sol"
    assert payload["market_start_offset"] == 7
