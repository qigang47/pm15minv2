from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm15min.core.config import LiveConfig
from pm15min.live.regime import build_regime_state_snapshot


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def _patch_regime_snapshot_labels(monkeypatch, labels: list[str]) -> None:
    sequence = iter(labels)
    monkeypatch.setattr("pm15min.live.regime.utc_snapshot_label", lambda: next(sequence))


def _sample_feature_frame(*, ret_15m: float, ret_30m: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "offset": 7,
                "ret_15m": ret_15m,
                "ret_30m": ret_30m,
            }
        ]
    )


def test_build_regime_state_snapshot_classifies_pressure(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_regime_snapshot_labels(monkeypatch, ["2026-03-20T00-08-00Z"])
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)

    payload = build_regime_state_snapshot(
        cfg,
        features=_sample_feature_frame(ret_15m=0.0020, ret_30m=0.0030),
        liquidity_payload={
            "snapshot_ts": "2026-03-20T00-07-59Z",
            "status": "ok",
            "reason": "ok",
            "blocked": False,
            "reason_codes": ["ok"],
            "metrics": {
                "spot_quote_ratio": 1.0,
                "perp_quote_ratio": 1.0,
                "spot_trades_ratio": 1.0,
                "perp_trades_ratio": 1.0,
                "soft_fail_count": 0,
                "hard_fail_count": 0,
            },
        },
        persist=True,
    )

    assert payload["status"] == "ok"
    assert payload["state"] == "NORMAL"
    assert payload["pressure"] == "up"
    assert payload["reason_codes"] == ["liquidity_ok"]
    assert payload["guard_hints"]["min_dir_prob_boost"] == 0.0
    assert "latest_regime_path" in payload


def test_build_regime_state_snapshot_enters_defense_after_confirmations(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_regime_snapshot_labels(
        monkeypatch,
        ["2026-03-20T00-08-00Z", "2026-03-20T00-09-00Z"],
    )
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    features = _sample_feature_frame(ret_15m=-0.0020, ret_30m=-0.0030)
    liquidity_payload = {
        "snapshot_ts": "2026-03-20T00-07-59Z",
        "status": "ok",
        "reason": "spot_quote_window",
        "blocked": True,
        "reason_codes": ["spot_quote_window"],
        "metrics": {
            "spot_quote_ratio": 0.3,
            "perp_quote_ratio": 0.3,
            "spot_trades_ratio": 0.3,
            "perp_trades_ratio": 0.3,
            "soft_fail_count": 3,
            "hard_fail_count": 0,
        },
    }

    first = build_regime_state_snapshot(
        cfg,
        features=features,
        liquidity_payload=liquidity_payload,
        persist=True,
    )
    second = build_regime_state_snapshot(
        cfg,
        features=features,
        liquidity_payload=liquidity_payload,
        persist=True,
    )

    assert first["state"] == "NORMAL"
    assert first["target_state"] == "DEFENSE"
    assert first["pending_target"] == "DEFENSE"
    assert first["pending_count"] == 1
    assert second["state"] == "DEFENSE"
    assert second["pressure"] == "down"
    assert second["guard_hints"]["defense_force_with_pressure"] is True
