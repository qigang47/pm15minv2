from __future__ import annotations

import pytest

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


def test_run_orderbook_recorder_fleet_can_launch_process_per_market(monkeypatch) -> None:
    commands: list[list[str]] = []
    pids = iter([1001, 1002])

    class _FakeProcess:
        def __init__(self, pid: int) -> None:
            self.pid = pid

    def _fake_popen(cmd, **kwargs):
        del kwargs
        commands.append(list(cmd))
        return _FakeProcess(next(pids))

    monkeypatch.setattr("pm15min.data.pipelines.orderbook_fleet.subprocess.Popen", _fake_popen)

    payload = run_orderbook_recorder_fleet(
        markets="sol,xrp",
        cycle="15m",
        loop=True,
        iterations=0,
        sleep_sec=0.35,
        scheduler_mode="process_per_market",
    )

    assert payload["status"] == "ok"
    assert payload["scheduler_mode"] == "process_per_market"
    assert payload["pids"] == {"sol": 1001, "xrp": 1002}
    assert payload["results"] == {}
    assert len(commands) == 2
    assert any("market='sol'" in " ".join(command) for command in commands)
    assert any("market='xrp'" in " ".join(command) for command in commands)


def test_run_orderbook_recorder_fleet_process_mode_requires_default_recorder() -> None:
    with pytest.raises(ValueError, match="default run_orderbook_recorder"):
        run_orderbook_recorder_fleet(
            markets="sol",
            loop=True,
            iterations=0,
            scheduler_mode="process_per_market",
            run_orderbook_recorder_fn=lambda cfg, **kwargs: {"status": "ok"},
        )


def test_run_orderbook_recorder_fleet_process_mode_requires_infinite_loop() -> None:
    with pytest.raises(ValueError, match="loop=True and iterations=0"):
        run_orderbook_recorder_fleet(
            markets="sol",
            loop=False,
            iterations=1,
            scheduler_mode="process_per_market",
        )
