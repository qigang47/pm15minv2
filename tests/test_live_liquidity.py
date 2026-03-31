from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm15min.core.config import LiveConfig
from pm15min.live.liquidity import build_liquidity_state_snapshot


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def _patch_liquidity_snapshot_labels(monkeypatch, labels: list[str]) -> None:
    sequence = iter(labels)
    monkeypatch.setattr("pm15min.live.liquidity.utc_snapshot_label", lambda: next(sequence))


def _sample_liquidity_frame(*, start: str, periods: int, quote_volume: float, trades: float) -> pd.DataFrame:
    ts = pd.date_range(start, periods=periods, freq="min", tz="UTC")
    return pd.DataFrame(
        {
            "open_time": ts,
            "close_time": ts + pd.Timedelta(minutes=1) - pd.Timedelta(seconds=1),
            "quote_asset_volume": [quote_volume] * periods,
            "number_of_trades": [trades] * periods,
        }
    )


def test_build_liquidity_state_snapshot_persists_ok_state(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_liquidity_snapshot_labels(monkeypatch, ["2026-03-20T00-10-00Z"])
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    healthy = _sample_liquidity_frame(
        start="2026-03-19T20:00:00Z",
        periods=220,
        quote_volume=500_000.0,
        trades=400.0,
    )

    monkeypatch.setattr("pm15min.live.liquidity._fetch_klines", lambda *args, **kwargs: healthy)
    monkeypatch.setattr("pm15min.live.liquidity._fetch_book_ticker", lambda *args, **kwargs: (100.0, 100.05))
    monkeypatch.setattr("pm15min.live.liquidity._fetch_open_interest", lambda *args, **kwargs: 500_000.0)

    payload = build_liquidity_state_snapshot(
        cfg,
        persist=True,
        force_refresh=True,
        now=pd.Timestamp("2026-03-20T00:10:00Z"),
    )

    assert payload["status"] == "ok"
    assert payload["ok"] is True
    assert payload["blocked"] is False
    assert payload["reason_codes"] == ["ok"]
    assert payload["metrics"]["soft_fail_count"] == 0.0
    assert "latest_liquidity_path" in payload


def test_build_liquidity_state_snapshot_blocks_after_repeated_failures(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_liquidity_snapshot_labels(
        monkeypatch,
        ["2026-03-20T00-10-00Z", "2026-03-20T00-11-00Z"],
    )
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    degraded = _sample_liquidity_frame(
        start="2026-03-19T20:00:00Z",
        periods=220,
        quote_volume=100_000.0,
        trades=50.0,
    )

    monkeypatch.setattr("pm15min.live.liquidity._fetch_klines", lambda *args, **kwargs: degraded)
    monkeypatch.setattr("pm15min.live.liquidity._fetch_book_ticker", lambda *args, **kwargs: (100.0, 100.20))
    monkeypatch.setattr("pm15min.live.liquidity._fetch_open_interest", lambda *args, **kwargs: 100_000.0)

    first = build_liquidity_state_snapshot(
        cfg,
        persist=True,
        force_refresh=True,
        now=pd.Timestamp("2026-03-20T00:10:00Z"),
    )
    second = build_liquidity_state_snapshot(
        cfg,
        persist=True,
        force_refresh=True,
        now=pd.Timestamp("2026-03-20T00:11:00Z"),
    )

    assert first["ok"] is True
    assert first["blocked"] is False
    assert first["reason_codes"][0] == "filtered_pending"
    assert second["ok"] is False
    assert second["blocked"] is False
    assert "spot_quote_window" in second["reason_codes"]
    assert second["temporal_state"]["raw_fail_streak"] == 2
