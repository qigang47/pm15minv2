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
    assert payload["scheduler_mode"] == "parallel_per_market"
    assert payload["completed_rounds"] == 1


def test_run_orderbook_recorder_fleet_runs_markets_independently() -> None:
    calls: list[tuple[str, int, bool, float | None]] = []

    def _fake_run_orderbook_recorder(cfg, *, iterations=1, loop=False, sleep_sec=None):
        calls.append((cfg.asset.slug, iterations, loop, sleep_sec))
        return {
            "status": "ok",
            "market": cfg.asset.slug,
            "iterations": iterations,
            "loop": loop,
            "sleep_sec": sleep_sec,
        }

    payload = run_orderbook_recorder_fleet(
        markets="sol,xrp",
        iterations=2,
        loop=True,
        sleep_sec=0.0,
        run_orderbook_recorder_fn=_fake_run_orderbook_recorder,
    )

    assert len(calls) == 2
    assert {market for market, *_ in calls} == {"sol", "xrp"}
    assert {iterations for _, iterations, _, _ in calls} == {2}
    assert {loop for _, _, loop, _ in calls} == {True}
    assert {sleep_sec for _, _, _, sleep_sec in calls} == {0.0}
    assert payload["status"] == "ok"
    assert payload["completed_rounds"] == 2
    assert payload["scheduler_mode"] == "parallel_per_market"
