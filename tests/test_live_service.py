from __future__ import annotations

import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pandas as pd

from pm15min.core.config import LiveConfig
from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.live.service import (
    check_live_latest,
    prewarm_live_signal_cache,
    prewarm_live_signal_preview,
    check_live_trading_gateway,
    score_live_latest,
    show_live_latest_runner,
    show_live_ready,
)
from pm15min.live.signal.service import (
    decide_live_latest as decide_live_latest_impl,
    prewarm_live_signal_cache as prewarm_live_signal_cache_impl,
    prewarm_live_signal_inputs as prewarm_live_signal_inputs_impl,
    prewarm_live_signal_preview as prewarm_live_signal_preview_impl,
)
from pm15min.live.signal import scoring_bundle as scoring_bundle_module
from pm15min.live.signal import utils as signal_utils_module
from pm15min.live.service import facade_helpers as service_facade_helpers_module
from pm15min.live.signal.scoring_bundle import resolve_bundle_resolution
from pm15min.live.signal.utils import build_live_feature_frame


class FakeGateway:
    adapter_name = "fake"

    def list_open_orders(self):
        return [{"order_id": "order-1"}]

    def list_positions(self):
        return [{"condition_id": "cond-1"}]


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def _prepare_nan_feature_score_case(tmp_path: Path, monkeypatch) -> LiveConfig:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    decision_ts = pd.Timestamp.now(tz="UTC").floor("min")
    cycle_start_ts = decision_ts.floor("15min")
    cycle_end_ts = cycle_start_ts + pd.Timedelta(minutes=15)
    bundle_dir = root / "bundles" / "bundle=nan_guard"
    (bundle_dir / "offsets" / "offset=7").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "pm15min.live.service.get_active_bundle_selection",
        lambda *args, **kwargs: {
            "selection": {"bundle_label": "nan_guard"},
            "selection_path": str(root / "selection.json"),
        },
    )
    monkeypatch.setattr("pm15min.live.service.resolve_model_bundle_dir", lambda *args, **kwargs: bundle_dir)
    monkeypatch.setattr(
        "pm15min.live.service.read_model_bundle_manifest",
        lambda *args, **kwargs: SimpleNamespace(spec={"feature_set": "v6_user_core", "bundle_label": "nan_guard"}),
    )
    monkeypatch.setattr(
        "pm15min.live.service.read_bundle_config",
        lambda *args, **kwargs: {
            "feature_columns": ["ret_30m", "delta_rsi_5"],
            "signal_target": "direction",
            "allowed_blacklist_columns": [],
        },
    )
    monkeypatch.setattr(
        "pm15min.live.service._build_live_feature_frame",
        lambda *args, **kwargs: pd.DataFrame(
            [
                {
                    "decision_ts": decision_ts.isoformat(),
                    "cycle_start_ts": cycle_start_ts.isoformat(),
                    "cycle_end_ts": cycle_end_ts.isoformat(),
                    "offset": 7,
                    "ret_30m": 0.01,
                    "delta_rsi_5": float("nan"),
                }
            ]
        ),
    )
    monkeypatch.setattr(
        "pm15min.live.service.score_bundle_offset",
        lambda *args, **kwargs: pd.DataFrame(
            [
                {
                    "decision_ts": decision_ts,
                    "cycle_start_ts": cycle_start_ts,
                    "cycle_end_ts": cycle_end_ts,
                    "offset": 7,
                    "p_lgb": 0.83,
                    "p_lr": 0.77,
                    "p_signal": 0.80,
                    "w_lgb": 0.6,
                    "w_lr": 0.4,
                    "p_up": 0.80,
                    "p_down": 0.20,
                    "probability_mode": "raw_blend",
                    "score_valid": True,
                    "score_reason": "",
                }
            ]
        ),
    )
    return cfg


def test_score_live_latest_marks_nan_features_invalid(tmp_path: Path, monkeypatch) -> None:
    cfg = _prepare_nan_feature_score_case(tmp_path, monkeypatch)

    payload = score_live_latest(cfg, persist=False)

    assert payload["market"] == "sol"
    assert len(payload["offset_signals"]) == 1
    row = payload["offset_signals"][0]
    assert row["score_valid"] is False
    assert row["score_reason"] == "nan_features"
    assert row["p_lgb"] == 0.83
    assert row["p_lr"] == 0.77
    assert row["w_lgb"] == 0.6
    assert row["w_lr"] == 0.4
    assert row["probability_mode"] == "raw_blend"
    assert row["feature_snapshot"]["ret_30m"] == 0.01
    assert row["feature_snapshot"]["delta_rsi_5"] is None
    assert row["coverage"]["nan_feature_count"] == 1
    assert row["coverage"]["nan_feature_columns"] == ["delta_rsi_5"]


def test_check_live_latest_fails_when_nan_features_present(tmp_path: Path, monkeypatch) -> None:
    cfg = _prepare_nan_feature_score_case(tmp_path, monkeypatch)

    payload = check_live_latest(cfg)

    assert payload["ok"] is False
    offset_check = next(item for item in payload["checks"] if item["name"] == "offset_signals_valid")
    assert offset_check["ok"] is False
    assert offset_check["detail"][0]["nan_feature_count"] == 1


def test_prewarm_live_signal_cache_populates_session_cache(tmp_path: Path, monkeypatch) -> None:
    cfg = _prepare_nan_feature_score_case(tmp_path, monkeypatch)
    session_state: dict[str, object] = {}

    first = prewarm_live_signal_cache(cfg, persist=False, session_state=session_state)
    second = prewarm_live_signal_cache(cfg, persist=False, session_state=session_state)

    assert first["status"] == "ok"
    assert first["cache_hit"] is False
    assert first["offsets"] == [7]
    assert second["cache_hit"] is True


def test_prewarm_live_signal_cache_writes_marker_only_on_refresh(tmp_path: Path, monkeypatch) -> None:
    cfg = _prepare_nan_feature_score_case(tmp_path, monkeypatch)
    session_state: dict[str, object] = {}
    marker_path = (
        cfg.layout.rewrite.root
        / "var"
        / "live"
        / "logs"
        / "markers"
        / "signal_refresh_cycle=15m_asset=sol_profile=deep_otm_target=direction.jsonl"
    )

    first = prewarm_live_signal_cache(cfg, persist=False, session_state=session_state)
    second = prewarm_live_signal_cache(cfg, persist=False, session_state=session_state)

    assert first["cache_hit"] is False
    assert second["cache_hit"] is True
    lines = marker_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["source"] == "prewarm"
    assert payload["cache_hit"] is False
    assert payload["market"] == "sol"
    assert payload["feature_rows"] == 1
    assert payload["offset_rows"][0]["offset"] == 7


def test_prewarm_live_signal_cache_supports_custom_marker_source(tmp_path: Path, monkeypatch) -> None:
    cfg = _prepare_nan_feature_score_case(tmp_path, monkeypatch)
    session_state: dict[str, object] = {}
    marker_path = (
        cfg.layout.rewrite.root
        / "var"
        / "live"
        / "logs"
        / "markers"
        / "signal_refresh_cycle=15m_asset=sol_profile=deep_otm_target=direction.jsonl"
    )

    payload = prewarm_live_signal_cache(
        cfg,
        persist=False,
        session_state=session_state,
        marker_source="prewarm_prepare",
    )

    assert payload["status"] == "ok"
    assert payload["marker_source"] == "prewarm_prepare"
    row = json.loads(marker_path.read_text(encoding="utf-8").strip().splitlines()[-1])
    assert row["source"] == "prewarm_prepare"


def test_prewarm_finalize_bypasses_unexpired_signal_cache(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    session_state = {
        "live_signal_cache": {
            "sol|deep_otm|15|direction|": {
                "valid_until_ts": "2099-01-01T00:00:00+00:00",
                "payload": {
                    "snapshot_ts": "2000-01-01T00:00:00Z",
                    "latest_feature_decision_ts": "2000-01-01T00:00:00+00:00",
                    "offset_signals": [
                        {
                            "offset": 7,
                            "status": "offset_not_yet_open",
                            "window_start_ts": "2099-01-01T00:07:00+00:00",
                            "window_end_ts": "2099-01-01T00:08:00+00:00",
                            "cycle_end_ts": "2099-01-01T00:15:00+00:00",
                        }
                    ],
                },
            }
        }
    }
    marker_path = (
        cfg.layout.rewrite.root
        / "var"
        / "live"
        / "logs"
        / "markers"
        / "signal_refresh_cycle=15m_asset=sol_profile=deep_otm_target=direction.jsonl"
    )
    calls = {"score": 0}

    def _score(*args, **kwargs):
        calls["score"] += 1
        return {
            "snapshot_ts": "2026-03-28T06:08:00Z",
            "latest_feature_decision_ts": "2026-03-28T06:07:00+00:00",
            "builder_feature_set": "bs_q_replace_direction",
            "feature_rows": 12,
            "offset_signals": [
                {
                    "offset": 7,
                    "status": None,
                    "decision_ts": "2026-03-28T06:07:00+00:00",
                    "window_start_ts": "2099-03-28T06:07:00+00:00",
                    "window_end_ts": "2099-03-28T06:08:00+00:00",
                    "cycle_end_ts": "2099-03-28T06:15:00+00:00",
                }
            ],
        }

    payload = prewarm_live_signal_cache_impl(
        cfg,
        persist=False,
        session_state=session_state,
        marker_source="prewarm_finalize",
        score_live_latest_fn=_score,
    )

    assert calls["score"] == 1
    assert payload["cache_hit"] is False
    assert payload["latest_feature_decision_ts"] == "2026-03-28T06:07:00+00:00"
    cached = session_state["live_signal_cache"]["sol|deep_otm|15|direction|"]["payload"]
    assert cached["latest_feature_decision_ts"] == "2026-03-28T06:07:00+00:00"
    marker = json.loads(marker_path.read_text(encoding="utf-8").strip().splitlines()[-1])
    assert marker["source"] == "prewarm_finalize"
    assert marker["cache_hit"] is False


def test_prewarm_live_signal_inputs_writes_prepare_marker(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    marker_path = (
        cfg.layout.rewrite.root
        / "var"
        / "live"
        / "logs"
        / "markers"
        / "signal_refresh_cycle=15m_asset=sol_profile=deep_otm_target=direction.jsonl"
    )
    bundle_dir = root / "bundles" / "bundle=prepare_guard"
    (bundle_dir / "offsets" / "offset=7").mkdir(parents=True, exist_ok=True)
    (bundle_dir / "offsets" / "offset=9").mkdir(parents=True, exist_ok=True)

    payload = prewarm_live_signal_inputs_impl(
        cfg,
        target="direction",
        feature_set="bs_q_replace_direction",
        resolve_live_profile_spec_fn=lambda profile: SimpleNamespace(default_feature_set="bs_q_replace_direction"),
        get_active_bundle_selection_fn=lambda *args, **kwargs: {
            "selection": {"bundle_label": "prepare_guard"},
            "selection_path": str(root / "selection.json"),
        },
        resolve_model_bundle_dir_fn=lambda *args, **kwargs: bundle_dir,
        read_model_bundle_manifest_fn=lambda *args, **kwargs: SimpleNamespace(
            spec={"feature_set": "bs_q_replace_direction", "bundle_label": "prepare_guard"}
        ),
        supports_feature_set_fn=lambda *args, **kwargs: True,
        build_live_feature_frame_fn=lambda *args, **kwargs: pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-28T06:07:00+00:00",
                    "offset": 7,
                },
                {
                    "decision_ts": "2026-03-28T06:09:00+00:00",
                    "offset": 9,
                },
            ]
        ),
        iso_or_none_fn=lambda value: None
        if value is None
        else pd.to_datetime(value, utc=True, errors="coerce").isoformat(),
    )

    assert payload["status"] == "ok"
    assert payload["marker_source"] == "prewarm_prepare"
    assert payload["offsets"] == [7, 9]
    lines = marker_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    marker = json.loads(lines[0])
    assert marker["source"] == "prewarm_prepare"
    assert marker["cache_key"].endswith("|prepare")
    assert marker["feature_rows"] == 2
    assert marker["offset_rows"] == [
        {
            "offset": 7,
            "status": "prepared",
            "decision_ts": None,
            "window_start_ts": None,
            "window_end_ts": None,
            "cycle_end_ts": None,
        },
        {
            "offset": 9,
            "status": "prepared",
            "decision_ts": None,
            "window_start_ts": None,
            "window_end_ts": None,
            "cycle_end_ts": None,
        },
    ]


def test_prewarm_live_signal_preview_uses_preview_open_bar_and_writes_preview_marker(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    marker_path = (
        cfg.layout.rewrite.root
        / "var"
        / "live"
        / "logs"
        / "markers"
        / "signal_refresh_cycle=15m_asset=sol_profile=deep_otm_target=direction.jsonl"
    )
    calls: dict[str, object] = {}

    def _score(cfg_arg, *, target, feature_set, persist, allow_preview_open_bar):
        calls["cfg"] = cfg_arg
        calls["target"] = target
        calls["feature_set"] = feature_set
        calls["persist"] = persist
        calls["allow_preview_open_bar"] = allow_preview_open_bar
        return {
            "snapshot_ts": "2026-03-28T06:07:59Z",
            "latest_feature_decision_ts": "2026-03-28T06:07:00+00:00",
            "builder_feature_set": "bs_q_replace_direction",
            "feature_rows": 12,
            "offset_signals": [
                {
                    "offset": 7,
                    "status": "ok",
                    "decision_ts": "2026-03-28T06:07:00+00:00",
                    "window_start_ts": "2026-03-28T06:07:00+00:00",
                    "window_end_ts": "2026-03-28T06:08:00+00:00",
                    "cycle_end_ts": "2026-03-28T06:15:00+00:00",
                }
            ],
        }

    payload = prewarm_live_signal_preview_impl(
        cfg,
        target="direction",
        feature_set="bs_q_replace_direction",
        score_live_latest_fn=_score,
    )

    assert payload["status"] == "ok"
    assert payload["preview_only"] is True
    assert payload["feature_rows"] == 12
    assert payload["latest_feature_decision_ts"] == "2026-03-28T06:07:00+00:00"
    assert payload["offsets"] == [7]
    assert calls["cfg"] == cfg
    assert calls["target"] == "direction"
    assert calls["feature_set"] == "bs_q_replace_direction"
    assert calls["persist"] is False
    assert calls["allow_preview_open_bar"] is True

    lines = marker_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    marker = json.loads(lines[0])
    assert marker["source"] == "prewarm_preview"
    assert marker["cache_key"].endswith("|preview")
    assert marker["feature_rows"] == 12
    assert marker["offset_rows"][0]["offset"] == 7


def test_service_prewarm_live_signal_preview_passes_through_public_wrapper(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)

    monkeypatch.setattr(
        "pm15min.live.service._prewarm_live_signal_preview_impl",
        lambda cfg_arg, *, target, feature_set, score_live_latest_fn: {
            "status": "ok",
            "cfg_market": cfg_arg.asset.slug,
            "target": target,
            "feature_set": feature_set,
            "score_callable": callable(score_live_latest_fn),
        },
    )

    payload = prewarm_live_signal_preview(
        cfg,
        target="direction",
        feature_set="bs_q_replace_direction",
    )

    assert payload == {
        "status": "ok",
        "cfg_market": "sol",
        "target": "direction",
        "feature_set": "bs_q_replace_direction",
        "score_callable": True,
    }


def test_service_facade_build_live_feature_frame_supports_preview_flag(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    calls: dict[str, object] = {}

    def _build(cfg_arg, *, feature_set, retain_offsets, allow_preview_open_bar):
        calls["cfg"] = cfg_arg
        calls["feature_set"] = feature_set
        calls["retain_offsets"] = retain_offsets
        calls["allow_preview_open_bar"] = allow_preview_open_bar
        return pd.DataFrame([{"offset": 7}])

    monkeypatch.setattr(
        "pm15min.live.service.facade_helpers._build_live_feature_frame_impl",
        _build,
    )

    payload = service_facade_helpers_module.build_live_feature_frame(
        cfg,
        feature_set="bs_q_replace_direction",
        retain_offsets=(7, 8, 9),
        allow_preview_open_bar=True,
    )

    assert list(payload["offset"]) == [7]
    assert calls["cfg"] == cfg
    assert calls["feature_set"] == "bs_q_replace_direction"
    assert calls["retain_offsets"] == (7, 8, 9)
    assert calls["allow_preview_open_bar"] is True


def test_score_live_latest_ignores_expired_offset_rows(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    current_decision_ts = pd.Timestamp.now(tz="UTC").floor("min")
    expired_decision_ts = current_decision_ts - pd.Timedelta(minutes=20)
    current_cycle_start_ts = current_decision_ts.floor("15min")
    expired_cycle_start_ts = expired_decision_ts.floor("15min")
    bundle_dir = root / "bundles" / "bundle=fresh_guard"
    (bundle_dir / "offsets" / "offset=7").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "pm15min.live.service.get_active_bundle_selection",
        lambda *args, **kwargs: {
            "selection": {"bundle_label": "fresh_guard"},
            "selection_path": str(root / "selection.json"),
        },
    )
    monkeypatch.setattr("pm15min.live.service.resolve_model_bundle_dir", lambda *args, **kwargs: bundle_dir)
    monkeypatch.setattr(
        "pm15min.live.service.read_model_bundle_manifest",
        lambda *args, **kwargs: SimpleNamespace(spec={"feature_set": "v6_user_core", "bundle_label": "fresh_guard"}),
    )
    monkeypatch.setattr(
        "pm15min.live.service.read_bundle_config",
        lambda *args, **kwargs: {
            "feature_columns": ["ret_30m"],
            "signal_target": "direction",
            "allowed_blacklist_columns": [],
        },
    )
    monkeypatch.setattr(
        "pm15min.live.service._build_live_feature_frame",
        lambda *args, **kwargs: pd.DataFrame(
            [
                {
                    "decision_ts": expired_decision_ts.isoformat(),
                    "cycle_start_ts": expired_cycle_start_ts.isoformat(),
                    "cycle_end_ts": (expired_cycle_start_ts + pd.Timedelta(minutes=15)).isoformat(),
                    "offset": 7,
                    "ret_30m": 0.01,
                },
                {
                    "decision_ts": current_decision_ts.isoformat(),
                    "cycle_start_ts": current_cycle_start_ts.isoformat(),
                    "cycle_end_ts": (current_cycle_start_ts + pd.Timedelta(minutes=15)).isoformat(),
                    "offset": 7,
                    "ret_30m": 0.02,
                },
            ]
        ),
    )
    monkeypatch.setattr(
        "pm15min.live.service.score_bundle_offset",
        lambda *args, **kwargs: pd.DataFrame(
            [
                {
                    "decision_ts": expired_decision_ts,
                    "cycle_start_ts": expired_cycle_start_ts,
                    "cycle_end_ts": expired_cycle_start_ts + pd.Timedelta(minutes=15),
                    "offset": 7,
                    "p_lgb": 0.70,
                    "p_lr": 0.65,
                    "p_signal": 0.68,
                    "w_lgb": 0.5,
                    "w_lr": 0.5,
                    "p_up": 0.68,
                    "p_down": 0.32,
                    "probability_mode": "raw_blend",
                    "score_valid": True,
                    "score_reason": "",
                },
                {
                    "decision_ts": current_decision_ts,
                    "cycle_start_ts": current_cycle_start_ts,
                    "cycle_end_ts": current_cycle_start_ts + pd.Timedelta(minutes=15),
                    "offset": 7,
                    "p_lgb": 0.83,
                    "p_lr": 0.77,
                    "p_signal": 0.80,
                    "w_lgb": 0.6,
                    "w_lr": 0.4,
                    "p_up": 0.80,
                    "p_down": 0.20,
                    "probability_mode": "raw_blend",
                    "score_valid": True,
                    "score_reason": "",
                },
            ]
        ),
    )

    payload = score_live_latest(cfg, persist=False)

    assert len(payload["offset_signals"]) == 1
    row = payload["offset_signals"][0]
    assert row.get("status") in (None, "")
    assert row["decision_ts"] == current_decision_ts.isoformat()
    assert row["window_start_ts"] == current_decision_ts.isoformat()
    assert row["offset"] == 7
    assert row["feature_snapshot"]["ret_30m"] == 0.02


def test_build_live_feature_frame_refreshes_trade_inputs_when_missing(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    calls: list[tuple[str, str]] = []

    def _touch(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("ok", encoding="utf-8")

    def _sync_market_catalog(data_cfg, **kwargs):
        calls.append(("market_catalog", data_cfg.asset.slug))
        _touch(data_cfg.layout.market_catalog_table_path)
        return {"status": "ok"}

    def _sync_binance(data_cfg, *, symbol=None, **kwargs):
        calls.append(("binance", str(symbol or data_cfg.asset.binance_symbol)))
        _touch(data_cfg.layout.binance_klines_path(symbol=symbol))
        return {"status": "ok"}

    monkeypatch.setattr("pm15min.live.signal.utils.sync_market_catalog", _sync_market_catalog)
    monkeypatch.setattr("pm15min.live.signal.utils.sync_binance_klines_1m", _sync_binance)
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_binance_klines_1m",
        lambda *args, **kwargs: pd.DataFrame([{"open_time": pd.Timestamp("2026-03-20T00:00:00Z"), "close_time": pd.Timestamp("2026-03-20T00:00:59Z"), "close": 100.0}]),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_oracle_prices_table",
        lambda *args, **kwargs: pd.DataFrame([{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.build_live_runtime_oracle_prices",
        lambda **kwargs: pd.DataFrame([{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.build_feature_frame_df",
        lambda *args, **kwargs: pd.DataFrame([{"decision_ts": pd.Timestamp("2026-03-20T00:01:00Z"), "cycle_start_ts": pd.Timestamp("2026-03-20T00:00:00Z"), "cycle_end_ts": pd.Timestamp("2026-03-20T00:15:00Z"), "offset": 1, "ret_30m": 0.0}]),
    )

    out = build_live_feature_frame(cfg, feature_set="v6_user_core")

    assert not out.empty
    assert ("market_catalog", "sol") in calls
    assert ("binance", "SOLUSDT") in calls
    assert ("binance", "BTCUSDT") in calls


def test_build_live_feature_frame_skips_trade_input_refresh_when_files_are_fresh(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="live", root=root)
    for path in (
        data_cfg.layout.market_catalog_table_path,
        data_cfg.layout.binance_klines_path(),
        btc_cfg.layout.binance_klines_path(),
        data_cfg.layout.oracle_prices_table_path,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("ok", encoding="utf-8")

    monkeypatch.setattr(
        "pm15min.live.signal.utils.sync_market_catalog",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("market catalog should not refresh")),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.sync_binance_klines_1m",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("binance should not refresh")),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_binance_klines_1m",
        lambda *args, **kwargs: pd.DataFrame([{"open_time": pd.Timestamp("2026-03-20T00:00:00Z"), "close_time": pd.Timestamp("2026-03-20T00:00:59Z"), "close": 100.0}]),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_oracle_prices_table",
        lambda *args, **kwargs: pd.DataFrame([{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.build_live_runtime_oracle_prices",
        lambda **kwargs: pd.DataFrame([{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.build_feature_frame_df",
        lambda *args, **kwargs: pd.DataFrame([{"decision_ts": pd.Timestamp("2026-03-20T00:01:00Z"), "cycle_start_ts": pd.Timestamp("2026-03-20T00:00:00Z"), "cycle_end_ts": pd.Timestamp("2026-03-20T00:15:00Z"), "offset": 1, "ret_30m": 0.0}]),
    )

    out = build_live_feature_frame(cfg, feature_set="v6_user_core")

    assert not out.empty


def test_build_live_feature_frame_reuses_cached_frame_within_ttl(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC", "60")
    signal_utils_module._LIVE_FEATURE_FRAME_CACHE.clear()
    calls = {"refresh": 0, "builder": 0}

    monkeypatch.setattr(
        "pm15min.live.signal.utils.ensure_live_trade_inputs_fresh",
        lambda *args, **kwargs: calls.__setitem__("refresh", calls["refresh"] + 1) or {"status": "fresh"},
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_binance_klines_1m",
        lambda *args, **kwargs: pd.DataFrame(
            [{"open_time": pd.Timestamp("2026-03-20T00:00:00Z"), "close_time": pd.Timestamp("2026-03-20T00:00:59Z"), "close": 100.0}]
        ),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_oracle_prices_table",
        lambda *args, **kwargs: pd.DataFrame(
            [{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]
        ),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.build_live_runtime_oracle_prices",
        lambda **kwargs: pd.DataFrame(
            [{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]
        ),
    )

    def _build_feature_frame(*args, **kwargs):
        calls["builder"] += 1
        return pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-20T00:01:00Z"),
                    "cycle_start_ts": pd.Timestamp("2026-03-20T00:00:00Z"),
                    "cycle_end_ts": pd.Timestamp("2026-03-20T00:15:00Z"),
                    "offset": 1,
                    "ret_30m": 0.0,
                }
            ]
        )

    monkeypatch.setattr("pm15min.live.signal.utils.build_feature_frame_df", _build_feature_frame)

    first = build_live_feature_frame(cfg, feature_set="v6_user_core")
    second = build_live_feature_frame(cfg, feature_set="v6_user_core")

    assert not first.empty
    assert not second.empty
    assert calls["refresh"] == 2
    assert calls["builder"] == 1
    assert first is not second


def test_build_live_feature_frame_trims_to_recent_cycles_and_active_offsets(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC", "0")
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_TAIL_CYCLES", "2")

    monkeypatch.setattr("pm15min.live.signal.utils.ensure_live_trade_inputs_fresh", lambda *args, **kwargs: {"status": "fresh"})
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_binance_klines_1m",
        lambda *args, **kwargs: pd.DataFrame(
            [{"open_time": pd.Timestamp("2026-03-20T00:00:00Z"), "close_time": pd.Timestamp("2026-03-20T00:00:59Z"), "close": 100.0}]
        ),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_oracle_prices_table",
        lambda *args, **kwargs: pd.DataFrame(
            [{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]
        ),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.build_live_runtime_oracle_prices",
        lambda **kwargs: pd.DataFrame(
            [{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]
        ),
    )

    def _build_feature_frame(*args, **kwargs):
        rows = []
        start = pd.Timestamp("2026-03-20T00:00:00Z")
        for minute in range(45):
            decision_ts = start + pd.Timedelta(minutes=minute)
            cycle_start = decision_ts.floor("15min")
            rows.append(
                {
                    "decision_ts": decision_ts,
                    "cycle_start_ts": cycle_start,
                    "cycle_end_ts": cycle_start + pd.Timedelta(minutes=15),
                    "offset": minute % 15,
                    "ret_30m": float(minute),
                }
            )
        return pd.DataFrame(rows)

    monkeypatch.setattr("pm15min.live.signal.utils.build_feature_frame_df", _build_feature_frame)

    out = build_live_feature_frame(cfg, feature_set="v6_user_core", retain_offsets=(7, 8, 9))

    assert len(out) == 18
    assert sorted(out.loc[out["cycle_start_ts"] == pd.Timestamp("2026-03-20T00:30:00Z"), "offset"].tolist()) == list(range(15))
    assert sorted(out.loc[out["cycle_start_ts"] == pd.Timestamp("2026-03-20T00:15:00Z"), "offset"].tolist()) == [7, 8, 9]


def test_build_live_feature_frame_limits_builder_input_to_bounded_kline_tail(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC", "0")
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_BUILD_TAIL_BARS", "300")
    signal_utils_module._LIVE_KLINES_TAIL_CACHE.clear()

    monkeypatch.setattr("pm15min.live.signal.utils.ensure_live_trade_inputs_fresh", lambda *args, **kwargs: {"status": "fresh"})
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_oracle_prices_table",
        lambda *args, **kwargs: pd.DataFrame(
            [{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]
        ),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.build_live_runtime_oracle_prices",
        lambda **kwargs: pd.DataFrame(
            [{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]
        ),
    )

    base_rows = [
        {
            "open_time": pd.Timestamp("2026-03-20T00:00:00Z") + pd.Timedelta(minutes=minute),
            "close_time": pd.Timestamp("2026-03-20T00:00:59Z") + pd.Timedelta(minutes=minute),
            "open": 100.0 + minute,
            "high": 101.0 + minute,
            "low": 99.0 + minute,
            "close": 100.5 + minute,
            "volume": 10.0 + minute,
            "quote_asset_volume": 20.0 + minute,
            "taker_buy_quote_volume": 8.0 + minute,
            "number_of_trades": 5 + minute,
        }
        for minute in range(1000)
    ]
    captured: dict[str, tuple[int, int]] = {}

    def _load_binance(data_cfg, symbol=None):
        return pd.DataFrame(base_rows)

    def _build_feature_frame(raw_klines, *, btc_klines=None, **kwargs):
        captured["sizes"] = (len(raw_klines), 0 if btc_klines is None else len(btc_klines))
        return pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-20T16:40:00Z"),
                    "cycle_start_ts": pd.Timestamp("2026-03-20T16:30:00Z"),
                    "cycle_end_ts": pd.Timestamp("2026-03-20T16:45:00Z"),
                    "offset": 10,
                    "ret_30m": 0.0,
                }
            ]
        )

    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="live", root=root)
    data_cfg.layout.binance_klines_path().parent.mkdir(parents=True, exist_ok=True)
    data_cfg.layout.binance_klines_path().write_text("x", encoding="utf-8")
    btc_cfg.layout.binance_klines_path(symbol="BTCUSDT").parent.mkdir(parents=True, exist_ok=True)
    btc_cfg.layout.binance_klines_path(symbol="BTCUSDT").write_text("x", encoding="utf-8")

    monkeypatch.setattr("pm15min.live.signal.utils.load_binance_klines_1m", _load_binance)
    monkeypatch.setattr("pm15min.live.signal.utils.build_feature_frame_df", _build_feature_frame)

    out = build_live_feature_frame(cfg, feature_set="v6_user_core")

    assert not out.empty
    assert captured["sizes"] == (300, 300)


def test_build_live_feature_frame_reuses_live_feature_state_for_identical_source_signature(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC", "0")
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_BUILD_TAIL_BARS", "300")
    signal_utils_module._LIVE_KLINES_TAIL_CACHE.clear()
    signal_utils_module._LIVE_FEATURE_STATE_CACHE.clear()

    monkeypatch.setattr("pm15min.live.signal.utils.ensure_live_trade_inputs_fresh", lambda *args, **kwargs: {"status": "fresh"})
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_oracle_prices_table",
        lambda *args, **kwargs: pd.DataFrame(
            [{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]
        ),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.build_live_runtime_oracle_prices",
        lambda **kwargs: pd.DataFrame(
            [{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]
        ),
    )

    rows = pd.DataFrame(
        [
            {
                "open_time": pd.Timestamp("2026-03-20T00:00:00Z") + pd.Timedelta(minutes=minute),
                "close_time": pd.Timestamp("2026-03-20T00:00:59Z") + pd.Timedelta(minutes=minute),
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume": 10.0 + minute,
                "quote_asset_volume": 20.0 + minute,
                "taker_buy_quote_volume": 8.0 + minute,
                "number_of_trades": 5 + minute,
            }
            for minute in range(500)
        ]
    )
    builder_calls = {"count": 0}

    def _load_binance(data_cfg, symbol=None):
        return rows.copy()

    def _build_feature_frame(*args, **kwargs):
        builder_calls["count"] += 1
        return pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-20T08:20:00Z"),
                    "cycle_start_ts": pd.Timestamp("2026-03-20T08:15:00Z"),
                    "cycle_end_ts": pd.Timestamp("2026-03-20T08:30:00Z"),
                    "offset": 5,
                    "ret_30m": 0.0,
                }
            ]
        )

    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="live", root=root)
    data_cfg.layout.binance_klines_path().parent.mkdir(parents=True, exist_ok=True)
    data_cfg.layout.binance_klines_path().write_text("x", encoding="utf-8")
    btc_cfg.layout.binance_klines_path(symbol="BTCUSDT").parent.mkdir(parents=True, exist_ok=True)
    btc_cfg.layout.binance_klines_path(symbol="BTCUSDT").write_text("x", encoding="utf-8")

    monkeypatch.setattr("pm15min.live.signal.utils.load_binance_klines_1m", _load_binance)
    monkeypatch.setattr("pm15min.live.signal.utils.build_feature_frame_df", _build_feature_frame)

    first = build_live_feature_frame(cfg, feature_set="v6_user_core")
    second = build_live_feature_frame(cfg, feature_set="v6_user_core")

    assert not first.empty
    assert not second.empty
    assert builder_calls["count"] == 1


def test_build_live_feature_frame_reuses_cached_kline_tail_when_source_unchanged(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC", "0")
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_BUILD_TAIL_BARS", "300")
    signal_utils_module._LIVE_KLINES_TAIL_CACHE.clear()

    monkeypatch.setattr("pm15min.live.signal.utils.ensure_live_trade_inputs_fresh", lambda *args, **kwargs: {"status": "fresh"})
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_oracle_prices_table",
        lambda *args, **kwargs: pd.DataFrame(
            [{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]
        ),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.build_live_runtime_oracle_prices",
        lambda **kwargs: pd.DataFrame(
            [{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]
        ),
    )

    rows = pd.DataFrame(
        [
            {
                "open_time": pd.Timestamp("2026-03-20T00:00:00Z") + pd.Timedelta(minutes=minute),
                "close_time": pd.Timestamp("2026-03-20T00:00:59Z") + pd.Timedelta(minutes=minute),
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume": 10.0 + minute,
                "quote_asset_volume": 20.0 + minute,
                "taker_buy_quote_volume": 8.0 + minute,
                "number_of_trades": 5 + minute,
            }
            for minute in range(500)
        ]
    )
    calls = {"primary": 0, "btc": 0}

    def _load_binance(data_cfg, symbol=None):
        if symbol == "BTCUSDT":
            calls["btc"] += 1
        else:
            calls["primary"] += 1
        return rows.copy()

    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="live", root=root)
    data_cfg.layout.binance_klines_path().parent.mkdir(parents=True, exist_ok=True)
    data_cfg.layout.binance_klines_path().write_text("x", encoding="utf-8")
    btc_cfg.layout.binance_klines_path(symbol="BTCUSDT").parent.mkdir(parents=True, exist_ok=True)
    btc_cfg.layout.binance_klines_path(symbol="BTCUSDT").write_text("x", encoding="utf-8")

    monkeypatch.setattr("pm15min.live.signal.utils.load_binance_klines_1m", _load_binance)
    monkeypatch.setattr(
        "pm15min.live.signal.utils.build_feature_frame_df",
        lambda *args, **kwargs: pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-20T08:20:00Z"),
                    "cycle_start_ts": pd.Timestamp("2026-03-20T08:15:00Z"),
                    "cycle_end_ts": pd.Timestamp("2026-03-20T08:30:00Z"),
                    "offset": 5,
                    "ret_30m": 0.0,
                }
            ]
        ),
    )

    first = build_live_feature_frame(cfg, feature_set="v6_user_core")
    second = build_live_feature_frame(cfg, feature_set="v6_user_core")

    assert not first.empty
    assert not second.empty
    assert calls == {"primary": 1, "btc": 1}


def test_build_live_feature_frame_uses_incremental_tail_after_source_advances(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC", "0")
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_BUILD_TAIL_BARS", "384")
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_INCREMENTAL_BUILD_TAIL_BARS", "320")
    signal_utils_module._LIVE_KLINES_TAIL_CACHE.clear()
    signal_utils_module._LIVE_FEATURE_STATE_CACHE.clear()

    monkeypatch.setattr("pm15min.live.signal.utils.ensure_live_trade_inputs_fresh", lambda *args, **kwargs: {"status": "fresh"})
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_oracle_prices_table",
        lambda *args, **kwargs: pd.DataFrame(
            [{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]
        ),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.build_live_runtime_oracle_prices",
        lambda **kwargs: pd.DataFrame(
            [{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]
        ),
    )

    base_rows = pd.DataFrame(
        [
            {
                "open_time": pd.Timestamp("2026-03-20T00:00:00Z") + pd.Timedelta(minutes=minute),
                "close_time": pd.Timestamp("2026-03-20T00:00:59Z") + pd.Timedelta(minutes=minute),
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume": 10.0 + minute,
                "quote_asset_volume": 20.0 + minute,
                "taker_buy_quote_volume": 8.0 + minute,
                "number_of_trades": 5 + minute,
            }
            for minute in range(500)
        ]
    )
    appended_rows = pd.concat(
        [
            base_rows,
            pd.DataFrame(
                [
                    {
                        "open_time": pd.Timestamp("2026-03-20T08:20:00Z"),
                        "close_time": pd.Timestamp("2026-03-20T08:20:59Z"),
                        "open": 600.0,
                        "high": 601.0,
                        "low": 599.0,
                        "close": 600.5,
                        "volume": 510.0,
                        "quote_asset_volume": 520.0,
                        "taker_buy_quote_volume": 508.0,
                        "number_of_trades": 505,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    current_rows = {"value": base_rows}
    captured_sizes: list[tuple[int, int]] = []

    def _load_binance(data_cfg, symbol=None):
        return current_rows["value"].copy()

    def _build_feature_frame(raw_klines, *, btc_klines=None, **kwargs):
        captured_sizes.append((len(raw_klines), 0 if btc_klines is None else len(btc_klines)))
        return pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-20T08:20:00Z"),
                    "cycle_start_ts": pd.Timestamp("2026-03-20T08:15:00Z"),
                    "cycle_end_ts": pd.Timestamp("2026-03-20T08:30:00Z"),
                    "offset": 5,
                    "ret_30m": 0.0,
                }
            ]
        )

    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="live", root=root)
    data_cfg.layout.binance_klines_path().parent.mkdir(parents=True, exist_ok=True)
    data_cfg.layout.binance_klines_path().write_text("x", encoding="utf-8")
    btc_cfg.layout.binance_klines_path(symbol="BTCUSDT").parent.mkdir(parents=True, exist_ok=True)
    btc_cfg.layout.binance_klines_path(symbol="BTCUSDT").write_text("x", encoding="utf-8")

    monkeypatch.setattr("pm15min.live.signal.utils.load_binance_klines_1m", _load_binance)
    monkeypatch.setattr("pm15min.live.signal.utils.build_feature_frame_df", _build_feature_frame)

    first = build_live_feature_frame(cfg, feature_set="v6_user_core")
    current_rows["value"] = appended_rows
    data_cfg.layout.binance_klines_path().write_text("xx", encoding="utf-8")
    btc_cfg.layout.binance_klines_path(symbol="BTCUSDT").write_text("xx", encoding="utf-8")
    second = build_live_feature_frame(cfg, feature_set="v6_user_core")

    assert not first.empty
    assert not second.empty
    assert captured_sizes[0] == (384, 384)
    assert captured_sizes[1] == (320, 320)


def test_load_live_binance_klines_tail_appends_from_latest_marker_without_reload(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    signal_utils_module._LIVE_KLINES_TAIL_CACHE.clear()
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    target_path = data_cfg.layout.binance_klines_path()

    def _rows(start_minute: int, count: int) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "open_time": pd.Timestamp("2026-03-20T00:00:00Z") + pd.Timedelta(minutes=start_minute + minute),
                    "close_time": pd.Timestamp("2026-03-20T00:00:59Z") + pd.Timedelta(minutes=start_minute + minute),
                    "open": 100.0 + start_minute + minute,
                    "high": 101.0 + start_minute + minute,
                    "low": 99.0 + start_minute + minute,
                    "close": 100.5 + start_minute + minute,
                    "volume": 10.0 + start_minute + minute,
                    "quote_asset_volume": 20.0 + start_minute + minute,
                    "taker_buy_quote_volume": 8.0 + start_minute + minute,
                    "number_of_trades": 5 + start_minute + minute,
                }
                for minute in range(count)
            ]
        )

    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text("v1", encoding="utf-8")
    base_rows = _rows(0, 384)
    signal_utils_module._LIVE_KLINES_TAIL_CACHE[(str(target_path), "SOLUSDT")] = (
        signal_utils_module._path_signature(target_path),
        base_rows.copy(),
    )

    appended_rows = _rows(384, 12)
    marker_path = signal_utils_module._live_binance_latest_tail_path(data_cfg=data_cfg, symbol=None)
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(
        json.dumps(
            {
                "tail_rows": [
                    {
                        "open_time": row["open_time"].isoformat(),
                        "close_time": row["close_time"].isoformat(),
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": row["volume"],
                        "quote_asset_volume": row["quote_asset_volume"],
                        "taker_buy_quote_volume": row["taker_buy_quote_volume"],
                        "number_of_trades": row["number_of_trades"],
                    }
                    for row in appended_rows.to_dict("records")
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    target_path.write_text("v2", encoding="utf-8")

    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_binance_klines_1m",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("full parquet load should not be used")),
    )

    out = signal_utils_module._load_live_binance_klines_tail(
        data_cfg=data_cfg,
        tail_bars=384,
    )

    assert len(out) == 384
    assert out["open_time"].max() == appended_rows["open_time"].max()
    assert out["open_time"].min() == base_rows.iloc[12]["open_time"]


def test_load_live_binance_klines_tail_cached_signature_still_merges_preview_tail(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    signal_utils_module._LIVE_KLINES_TAIL_CACHE.clear()
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    target_path = data_cfg.layout.binance_klines_path()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text("v1", encoding="utf-8")

    base_rows = pd.DataFrame(
        [
            {
                "open_time": pd.Timestamp("2026-03-20T00:00:00Z") + pd.Timedelta(minutes=minute),
                "close_time": pd.Timestamp("2026-03-20T00:00:59Z") + pd.Timedelta(minutes=minute),
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume": 10.0 + minute,
                "quote_asset_volume": 20.0 + minute,
                "taker_buy_quote_volume": 8.0 + minute,
                "number_of_trades": 5 + minute,
            }
            for minute in range(384)
        ]
    )
    signal_utils_module._LIVE_KLINES_TAIL_CACHE[(str(target_path), "SOLUSDT")] = (
        signal_utils_module._path_signature(target_path),
        base_rows.copy(),
    )
    preview_rows = pd.DataFrame(
        [
            {
                "open_time": pd.Timestamp("2026-03-20T06:24:00Z"),
                "close_time": pd.Timestamp("2026-03-20T06:24:59Z"),
                "open": 600.0,
                "high": 601.0,
                "low": 599.0,
                "close": 600.5,
                "volume": 610.0,
                "quote_asset_volume": 620.0,
                "taker_buy_quote_volume": 608.0,
                "number_of_trades": 605,
            }
        ]
    )

    monkeypatch.setattr(
        "pm15min.live.signal.utils._load_live_binance_preview_tail",
        lambda **kwargs: preview_rows.copy(),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_binance_klines_1m",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("full parquet load should not be used")),
    )

    out = signal_utils_module._load_live_binance_klines_tail(
        data_cfg=data_cfg,
        tail_bars=384,
        allow_preview_open_bar=True,
        now_utc=pd.Timestamp("2026-03-20T06:24:30Z"),
    )

    assert out["open_time"].max() == pd.Timestamp("2026-03-20T06:24:00Z")
    assert len(out) == 384


def test_load_live_binance_klines_tail_cached_signature_still_merges_latest_closed_marker(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    signal_utils_module._LIVE_KLINES_TAIL_CACHE.clear()
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    target_path = data_cfg.layout.binance_klines_path()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text("v1", encoding="utf-8")

    base_rows = pd.DataFrame(
        [
            {
                "open_time": pd.Timestamp("2026-03-20T00:00:00Z") + pd.Timedelta(minutes=minute),
                "close_time": pd.Timestamp("2026-03-20T00:00:59Z") + pd.Timedelta(minutes=minute),
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume": 10.0 + minute,
                "quote_asset_volume": 20.0 + minute,
                "taker_buy_quote_volume": 8.0 + minute,
                "number_of_trades": 5 + minute,
            }
            for minute in range(384)
        ]
    )
    signal_utils_module._LIVE_KLINES_TAIL_CACHE[(str(target_path), "SOLUSDT")] = (
        signal_utils_module._path_signature(target_path),
        base_rows.copy(),
    )
    appended_rows = pd.DataFrame(
        [
            {
                "open_time": pd.Timestamp("2026-03-20T06:24:00Z"),
                "close_time": pd.Timestamp("2026-03-20T06:24:59Z"),
                "open": 600.0,
                "high": 601.0,
                "low": 599.0,
                "close": 600.5,
                "volume": 610.0,
                "quote_asset_volume": 620.0,
                "taker_buy_quote_volume": 608.0,
                "number_of_trades": 605,
            }
        ]
    )
    marker_path = signal_utils_module._live_binance_latest_tail_path(data_cfg=data_cfg, symbol=None)
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(
        json.dumps(
            {
                "tail_rows": [
                    {
                        "open_time": row["open_time"].isoformat(),
                        "close_time": row["close_time"].isoformat(),
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": row["volume"],
                        "quote_asset_volume": row["quote_asset_volume"],
                        "taker_buy_quote_volume": row["taker_buy_quote_volume"],
                        "number_of_trades": row["number_of_trades"],
                    }
                    for row in appended_rows.to_dict("records")
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_binance_klines_1m",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("full parquet load should not be used")),
    )

    out = signal_utils_module._load_live_binance_klines_tail(
        data_cfg=data_cfg,
        tail_bars=384,
        allow_preview_open_bar=False,
    )

    assert out["open_time"].max() == pd.Timestamp("2026-03-20T06:24:00Z")
    assert len(out) == 384


def test_build_live_feature_frame_stateful_marker_append_matches_full_rebuild_for_active_offsets(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm_baseline", cycle_minutes=15)
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC", "0")
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_BUILD_TAIL_BARS", "384")
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_TAIL_CYCLES", "2")
    signal_utils_module._LIVE_KLINES_TAIL_CACHE.clear()
    signal_utils_module._LIVE_FEATURE_STATE_CACHE.clear()

    def _rows(start_minute: int, count: int, *, base_price: float) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "open_time": pd.Timestamp("2026-03-20T00:00:00Z") + pd.Timedelta(minutes=start_minute + minute),
                    "close_time": pd.Timestamp("2026-03-20T00:00:59Z") + pd.Timedelta(minutes=start_minute + minute),
                    "open": base_price + start_minute + minute * 0.5,
                    "high": base_price + start_minute + minute * 0.5 + 0.8,
                    "low": base_price + start_minute + minute * 0.5 - 0.7,
                    "close": base_price + start_minute + minute * 0.5 + 0.2,
                    "volume": 100.0 + start_minute + minute,
                    "quote_asset_volume": 200.0 + start_minute + minute * 2.0,
                    "taker_buy_quote_volume": 90.0 + start_minute + minute,
                    "number_of_trades": 50 + start_minute + minute,
                }
                for minute in range(count)
            ]
        )

    base_rows = _rows(0, 384, base_price=100.0)
    appended_rows = _rows(384, 12, base_price=100.0)
    full_rows = pd.concat([base_rows, appended_rows], ignore_index=True)
    btc_base_rows = _rows(0, 384, base_price=200.0)
    btc_appended_rows = _rows(384, 12, base_price=200.0)
    btc_full_rows = pd.concat([btc_base_rows, btc_appended_rows], ignore_index=True)
    oracle_prices = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": int((pd.Timestamp("2026-03-20T00:00:00Z") + pd.Timedelta(minutes=15 * idx)).timestamp()),
                "cycle_end_ts": int((pd.Timestamp("2026-03-20T00:15:00Z") + pd.Timedelta(minutes=15 * idx)).timestamp()),
                "price_to_beat": 150.0 + idx,
                "final_price": None,
            }
            for idx in range(40)
        ]
    )

    monkeypatch.setattr("pm15min.live.signal.utils.ensure_live_trade_inputs_fresh", lambda *args, **kwargs: {"status": "fresh"})
    monkeypatch.setattr("pm15min.live.signal.utils.load_oracle_prices_table", lambda *args, **kwargs: oracle_prices.copy())
    monkeypatch.setattr("pm15min.live.signal.utils.build_live_runtime_oracle_prices", lambda **kwargs: oracle_prices.copy())

    load_calls = {"sol": 0, "btc": 0}

    def _load_binance(data_cfg, symbol=None):
        if symbol == "BTCUSDT":
            load_calls["btc"] += 1
            return btc_base_rows.copy()
        load_calls["sol"] += 1
        return base_rows.copy()

    monkeypatch.setattr("pm15min.live.signal.utils.load_binance_klines_1m", _load_binance)

    first = build_live_feature_frame(cfg, feature_set="bs_q_replace_direction", retain_offsets=(7, 8, 9))
    assert not first.empty
    assert load_calls == {"sol": 1, "btc": 1}

    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="live", root=root)
    data_cfg.layout.binance_klines_path().parent.mkdir(parents=True, exist_ok=True)
    data_cfg.layout.binance_klines_path().write_text("v1", encoding="utf-8")
    btc_cfg.layout.binance_klines_path(symbol="BTCUSDT").parent.mkdir(parents=True, exist_ok=True)
    btc_cfg.layout.binance_klines_path(symbol="BTCUSDT").write_text("v1", encoding="utf-8")
    signal_utils_module._LIVE_KLINES_TAIL_CACHE[(str(data_cfg.layout.binance_klines_path()), "SOLUSDT")] = (
        signal_utils_module._path_signature(data_cfg.layout.binance_klines_path()),
        base_rows.copy(),
    )
    signal_utils_module._LIVE_KLINES_TAIL_CACHE[(str(btc_cfg.layout.binance_klines_path(symbol="BTCUSDT")), "BTCUSDT")] = (
        signal_utils_module._path_signature(btc_cfg.layout.binance_klines_path(symbol="BTCUSDT")),
        btc_base_rows.copy(),
    )

    for marker_cfg, rows, symbol in (
        (data_cfg, appended_rows, "SOLUSDT"),
        (btc_cfg, btc_appended_rows, "BTCUSDT"),
    ):
        marker_path = signal_utils_module._live_binance_latest_tail_path(data_cfg=marker_cfg, symbol=symbol)
        marker_path.parent.mkdir(parents=True, exist_ok=True)
        marker_path.write_text(
            json.dumps(
                {
                    "tail_rows": [
                        {
                            "open_time": row["open_time"].isoformat(),
                            "close_time": row["close_time"].isoformat(),
                            "open": row["open"],
                            "high": row["high"],
                            "low": row["low"],
                            "close": row["close"],
                            "volume": row["volume"],
                            "quote_asset_volume": row["quote_asset_volume"],
                            "taker_buy_quote_volume": row["taker_buy_quote_volume"],
                            "number_of_trades": row["number_of_trades"],
                        }
                        for row in rows.to_dict("records")
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
    data_cfg.layout.binance_klines_path().write_text("v2", encoding="utf-8")
    btc_cfg.layout.binance_klines_path(symbol="BTCUSDT").write_text("v2", encoding="utf-8")

    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_binance_klines_1m",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("stateful marker append should avoid full parquet load")),
    )

    actual = build_live_feature_frame(cfg, feature_set="bs_q_replace_direction", retain_offsets=(7, 8, 9))

    full_tail_bars = signal_utils_module._live_feature_build_tail_bars(cycle_minutes=15, tail_cycles=2)
    expected = signal_utils_module.build_feature_frame_df(
        full_rows.tail(full_tail_bars).reset_index(drop=True),
        feature_set="bs_q_replace_direction",
        oracle_prices=oracle_prices.copy(),
        btc_klines=btc_full_rows.tail(full_tail_bars).reset_index(drop=True),
        cycle="15m",
    )
    expected = signal_utils_module._trim_live_feature_frame(
        expected,
        cycle_minutes=15,
        retain_offsets=(7, 8, 9),
        tail_cycles=2,
    )
    expected_latest_cycle = pd.to_datetime(expected["cycle_start_ts"], utc=True, errors="coerce").max()
    expected_latest = expected.loc[pd.to_datetime(expected["cycle_start_ts"], utc=True, errors="coerce").eq(expected_latest_cycle)].reset_index(drop=True)
    actual_latest = actual.loc[pd.to_datetime(actual["cycle_start_ts"], utc=True, errors="coerce").eq(expected_latest_cycle)].reset_index(drop=True)

    pd.testing.assert_frame_equal(actual_latest, expected_latest, check_exact=False, atol=1e-9, rtol=1e-9)


def test_build_live_feature_frame_invalidates_state_cache_when_mid_tail_value_changes(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setenv("PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC", "0")
    signal_utils_module._LIVE_FEATURE_STATE_CACHE.clear()

    monkeypatch.setattr("pm15min.live.signal.utils.ensure_live_trade_inputs_fresh", lambda *args, **kwargs: {"status": "fresh"})
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_oracle_prices_table",
        lambda *args, **kwargs: pd.DataFrame([{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]),
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.build_live_runtime_oracle_prices",
        lambda **kwargs: pd.DataFrame([{"asset": "sol", "cycle_start_ts": 0, "cycle_end_ts": 900, "price_to_beat": 100.0, "final_price": None}]),
    )

    base_rows = pd.DataFrame(
        [
            {
                "open_time": pd.Timestamp("2026-03-20T00:00:00Z") + pd.Timedelta(minutes=minute),
                "close_time": pd.Timestamp("2026-03-20T00:00:59Z") + pd.Timedelta(minutes=minute),
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume": 10.0 + minute,
                "quote_asset_volume": 20.0 + minute,
                "taker_buy_quote_volume": 8.0 + minute,
                "number_of_trades": 5 + minute,
            }
            for minute in range(16)
        ]
    )
    corrected_rows = base_rows.copy()
    corrected_rows.loc[5, "close"] = 999.0
    current_rows = {"value": base_rows}
    builder_calls = {"count": 0}

    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_binance_klines_1m",
        lambda *args, **kwargs: current_rows["value"].copy(),
    )

    def _build_feature_frame(*args, **kwargs):
        builder_calls["count"] += 1
        return pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-20T00:15:00Z"),
                    "cycle_start_ts": pd.Timestamp("2026-03-20T00:00:00Z"),
                    "cycle_end_ts": pd.Timestamp("2026-03-20T00:15:00Z"),
                    "offset": 1,
                    "ret_30m": float(builder_calls["count"]),
                }
            ]
        )

    monkeypatch.setattr("pm15min.live.signal.utils.build_feature_frame_df", _build_feature_frame)

    first = build_live_feature_frame(cfg, feature_set="v6_user_core")
    current_rows["value"] = corrected_rows
    second = build_live_feature_frame(cfg, feature_set="v6_user_core")

    assert builder_calls["count"] == 2
    assert float(first.iloc[0]["ret_30m"]) == 1.0
    assert float(second.iloc[0]["ret_30m"]) == 2.0


def test_ensure_live_closed_bar_inputs_ready_waits_for_latest_tail_markers(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    monkeypatch.setenv("PM15MIN_LIVE_ENFORCE_EXPECTED_CLOSED_BAR", "1")
    monkeypatch.setenv("PM15MIN_LIVE_EXPECTED_CLOSED_BAR_WAIT_SEC", "0.5")
    monkeypatch.setenv("PM15MIN_LIVE_EXPECTED_CLOSED_BAR_POLL_SEC", "0.01")

    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    btc_cfg = DataConfig.build(market="btc", cycle="15m", surface="live", root=root)

    def _write_marker(path: Path, latest_open_time: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "latest_open_time": latest_open_time,
                    "tail_rows": [
                        {
                            "open_time": latest_open_time,
                            "close_time": (pd.Timestamp(latest_open_time) + pd.Timedelta(seconds=59)).isoformat(),
                            "close": 1.0,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

    sol_marker = signal_utils_module._live_binance_latest_tail_path(data_cfg=data_cfg, symbol="SOLUSDT")
    btc_marker = signal_utils_module._live_binance_latest_tail_path(data_cfg=btc_cfg, symbol="BTCUSDT")
    _write_marker(sol_marker, "2026-03-20T00:05:00+00:00")
    _write_marker(btc_marker, "2026-03-20T00:05:00+00:00")

    def _late_update() -> None:
        time.sleep(0.05)
        _write_marker(sol_marker, "2026-03-20T00:06:00+00:00")
        _write_marker(btc_marker, "2026-03-20T00:06:00+00:00")

    worker = threading.Thread(target=_late_update, daemon=True)
    worker.start()

    signal_utils_module._ensure_live_closed_bar_inputs_ready(
        data_cfg=data_cfg,
        btc_cfg=btc_cfg,
        now_utc=pd.Timestamp("2026-03-20T00:07:00.100000+00:00"),
        cycle_minutes=15,
        retain_offsets=(7, 8, 9),
    )


def test_ensure_live_closed_bar_inputs_ready_raises_when_latest_tail_marker_stale(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    monkeypatch.setenv("PM15MIN_LIVE_ENFORCE_EXPECTED_CLOSED_BAR", "1")
    monkeypatch.setenv("PM15MIN_LIVE_EXPECTED_CLOSED_BAR_WAIT_SEC", "0.05")
    monkeypatch.setenv("PM15MIN_LIVE_EXPECTED_CLOSED_BAR_POLL_SEC", "0.01")

    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    marker_path = signal_utils_module._live_binance_latest_tail_path(data_cfg=data_cfg, symbol="SOLUSDT")
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(
        json.dumps(
            {
                "latest_open_time": "2026-03-20T00:05:00+00:00",
                "tail_rows": [
                    {
                        "open_time": "2026-03-20T00:05:00+00:00",
                        "close_time": "2026-03-20T00:05:59+00:00",
                        "close": 1.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    try:
        signal_utils_module._ensure_live_closed_bar_inputs_ready(
            data_cfg=data_cfg,
            btc_cfg=None,
            now_utc=pd.Timestamp("2026-03-20T00:07:00.100000+00:00"),
            cycle_minutes=15,
            retain_offsets=(7, 8, 9),
        )
    except signal_utils_module.LiveClosedBarNotReadyError as exc:
        assert "expected_open_time=2026-03-20T00:06:00+00:00" in str(exc)
        assert "SOLUSDT=2026-03-20T00:05:00+00:00" in str(exc)
    else:
        raise AssertionError("expected LiveClosedBarNotReadyError")


def test_ensure_live_closed_bar_inputs_ready_skips_non_boundary_minutes(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    monkeypatch.setenv("PM15MIN_LIVE_ENFORCE_EXPECTED_CLOSED_BAR", "1")

    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    marker_path = signal_utils_module._live_binance_latest_tail_path(data_cfg=data_cfg, symbol="SOLUSDT")
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_path.write_text(
        json.dumps(
            {
                "latest_open_time": "2026-03-20T00:01:00+00:00",
                "tail_rows": [],
            }
        ),
        encoding="utf-8",
    )

    signal_utils_module._ensure_live_closed_bar_inputs_ready(
        data_cfg=data_cfg,
        btc_cfg=None,
        now_utc=pd.Timestamp("2026-03-20T00:03:00.100000+00:00"),
        cycle_minutes=15,
        retain_offsets=(7, 8, 9),
    )


def test_load_live_account_context_reuses_cached_snapshot_when_files_unchanged(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setenv("PM15MIN_LIVE_ACCOUNT_CONTEXT_CACHE_SEC", "60")
    signal_utils_module._LIVE_ACCOUNT_CONTEXT_CACHE.clear()
    calls = {"open_orders": 0, "positions": 0}

    def _open_orders(**kwargs):
        calls["open_orders"] += 1
        return {"status": "ok", "orders": []}

    def _positions(**kwargs):
        calls["positions"] += 1
        return {"status": "ok", "positions": [], "cash_balance_usd": 123.0}

    layout = signal_utils_module.LiveStateLayout.discover(root=root)
    layout.latest_open_orders_path(market="sol").parent.mkdir(parents=True, exist_ok=True)
    layout.latest_open_orders_path(market="sol").write_text("{}", encoding="utf-8")
    layout.latest_positions_path(market="sol").parent.mkdir(parents=True, exist_ok=True)
    layout.latest_positions_path(market="sol").write_text("{}", encoding="utf-8")

    monkeypatch.setattr("pm15min.live.signal.utils.load_latest_open_orders_snapshot", _open_orders)
    monkeypatch.setattr("pm15min.live.signal.utils.load_latest_positions_snapshot", _positions)

    first = signal_utils_module.load_live_account_context(cfg, utc_snapshot_label_fn=lambda: "2026-03-27T00-00-00Z")
    second = signal_utils_module.load_live_account_context(cfg, utc_snapshot_label_fn=lambda: "2026-03-27T00-00-01Z")

    assert first["summary"]["cash_balance_usd"] == 123.0
    assert second["summary"]["cash_balance_usd"] == 123.0
    assert first["open_orders"].get("orders") is None
    assert first["positions"].get("positions") is None
    assert calls["open_orders"] == 1
    assert calls["positions"] == 1


def test_resolve_bundle_resolution_reuses_cached_result_within_ttl(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    bundle_dir = root / "bundles" / "bundle=cached"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("PM15MIN_LIVE_BUNDLE_CACHE_SEC", "60")
    scoring_bundle_module._BUNDLE_RESOLUTION_CACHE.clear()
    calls = {"selection": 0, "manifest": 0}

    def _get_active_bundle_selection(*args, **kwargs):
        calls["selection"] += 1
        return {
            "selection": {"bundle_label": "cached"},
            "selection_path": str(root / "selection.json"),
        }

    def _read_model_bundle_manifest(*args, **kwargs):
        calls["manifest"] += 1
        return SimpleNamespace(spec={"feature_set": "v6_user_core", "bundle_label": "cached"})

    first = resolve_bundle_resolution(
        cfg,
        target="direction",
        feature_set="v6_user_core",
        resolve_live_profile_spec_fn=lambda profile: SimpleNamespace(default_feature_set="v6_user_core"),
        get_active_bundle_selection_fn=_get_active_bundle_selection,
        resolve_model_bundle_dir_fn=lambda *args, **kwargs: bundle_dir,
        read_model_bundle_manifest_fn=_read_model_bundle_manifest,
        supports_feature_set_fn=lambda feature_set: True,
    )
    second = resolve_bundle_resolution(
        cfg,
        target="direction",
        feature_set="v6_user_core",
        resolve_live_profile_spec_fn=lambda profile: SimpleNamespace(default_feature_set="v6_user_core"),
        get_active_bundle_selection_fn=_get_active_bundle_selection,
        resolve_model_bundle_dir_fn=lambda *args, **kwargs: bundle_dir,
        read_model_bundle_manifest_fn=_read_model_bundle_manifest,
        supports_feature_set_fn=lambda feature_set: True,
    )

    assert first.bundle_dir == bundle_dir
    assert second.bundle_dir == bundle_dir
    assert calls["selection"] == 1
    assert calls["manifest"] == 1


def test_decide_live_latest_reuses_cached_signal_until_next_transition(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setenv("PM15MIN_LIVE_WINDOW_SIGNAL_CACHE", "1")
    session_state: dict[str, object] = {}
    calls = {"score": 0}

    signal_payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-27T14-00-00Z",
        "offset_signals": [
            {
                "offset": 7,
                "decision_ts": "2026-03-27T14:07:00+00:00",
                "window_start_ts": "2099-03-27T14:07:00+00:00",
                "window_end_ts": "2099-03-27T14:08:00+00:00",
                "cycle_end_ts": "2099-03-27T14:15:00+00:00",
                "recommended_side": "UP",
                "confidence": 0.8,
                "edge": 0.3,
                "score_valid": True,
                "coverage": {"effective_missing_feature_count": 0, "not_allowed_blacklist_count": 0},
            }
        ],
    }

    def _score(*args, **kwargs):
        calls["score"] += 1
        return dict(signal_payload)

    def _quote(**kwargs):
        return {"snapshot_ts": "2026-03-27T14-00-01Z", "quote_rows": []}

    def _decision(signal, quote, account_state, **kwargs):
        return {
            "snapshot_ts": "2026-03-27T14-00-02Z",
            "decision": {"status": "reject", "selected_offset": None},
            "signal_snapshot_ts": signal.get("snapshot_ts"),
        }

    first = decide_live_latest_impl(
        cfg,
        persist=False,
        session_state=session_state,
        score_live_latest_fn=_score,
        build_quote_snapshot_fn=_quote,
        load_live_account_context_fn=lambda *_args, **_kwargs: {},
        build_decision_snapshot_fn=_decision,
        persist_decision_snapshot_fn=lambda **kwargs: {},
    )
    second = decide_live_latest_impl(
        cfg,
        persist=False,
        session_state=session_state,
        score_live_latest_fn=_score,
        build_quote_snapshot_fn=_quote,
        load_live_account_context_fn=lambda *_args, **_kwargs: {},
        build_decision_snapshot_fn=_decision,
        persist_decision_snapshot_fn=lambda **kwargs: {},
    )

    assert first["decision"]["status"] == "reject"
    assert second["decision"]["status"] == "reject"
    assert calls["score"] == 1


def test_decide_live_latest_refreshes_when_cached_signal_is_expired(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setenv("PM15MIN_LIVE_WINDOW_SIGNAL_CACHE", "1")
    session_state = {
        "live_signal_cache": {
            "sol|deep_otm|15|direction|": {
                "valid_until_ts": "2000-01-01T00:00:00+00:00",
                "payload": {"market": "sol"},
            }
        }
    }
    calls = {"score": 0}

    def _score(*args, **kwargs):
        calls["score"] += 1
        return {
            "market": "sol",
            "profile": "deep_otm",
            "cycle": "15m",
            "target": "direction",
            "snapshot_ts": "2026-03-27T14-00-00Z",
            "offset_signals": [
                {
                    "offset": 7,
                    "decision_ts": "2026-03-27T14:07:00+00:00",
                    "window_start_ts": "2099-03-27T14:07:00+00:00",
                    "window_end_ts": "2099-03-27T14:08:00+00:00",
                    "cycle_end_ts": "2099-03-27T14:15:00+00:00",
                    "recommended_side": "UP",
                    "confidence": 0.8,
                    "edge": 0.3,
                    "score_valid": True,
                    "coverage": {"effective_missing_feature_count": 0, "not_allowed_blacklist_count": 0},
                }
            ],
        }

    decide_live_latest_impl(
        cfg,
        persist=False,
        session_state=session_state,
        score_live_latest_fn=_score,
        build_quote_snapshot_fn=lambda **kwargs: {"snapshot_ts": "2026-03-27T14-00-01Z", "quote_rows": []},
        load_live_account_context_fn=lambda *_args, **_kwargs: {},
        build_decision_snapshot_fn=lambda signal, quote, account_state, **kwargs: {
            "snapshot_ts": "2026-03-27T14-00-02Z",
            "decision": {"status": "reject", "selected_offset": None},
        },
        persist_decision_snapshot_fn=lambda **kwargs: {},
    )

    assert calls["score"] == 1


def test_check_live_trading_gateway_reports_injected_gateway(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    availability = {
        "live_trading.infra.polymarket_client": True,
        "py_clob_client.client": True,
        "py_builder_relayer_client.client": True,
        "py_builder_signing_sdk.config": True,
        "web3": True,
    }
    monkeypatch.setattr("pm15min.live.service._module_available", lambda name: availability.get(name, False))
    monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", "pk")
    monkeypatch.setenv("POLYMARKET_USER_ADDRESS", "0xuser")
    monkeypatch.setenv("RPC_URL", "https://rpc-1")
    monkeypatch.setenv("POLYGON_RPC", "")
    monkeypatch.setenv("POLYGON_RPC_URL", "")
    monkeypatch.setenv("WEB3_PROVIDER_URI", "")
    monkeypatch.setenv("RPC_URL_BACKUPS", "")
    monkeypatch.setenv("POLYGON_RPC_BACKUPS", "")
    monkeypatch.setenv("RPC_FALLBACKS", "")
    monkeypatch.setenv("POLYGON_RPC_FALLBACKS", "")
    monkeypatch.setenv("BUILDER_API_KEY", "builder-key")
    monkeypatch.setenv("BUILDER_SECRET", "builder-secret")
    monkeypatch.setenv("BUILDER_PASS_PHRASE", "builder-pass")

    payload = check_live_trading_gateway(
        cfg,
        gateway=FakeGateway(),
        adapter="direct",
        probe_open_orders=True,
        probe_positions=True,
    )

    assert payload["ok"] is True
    assert payload["adapter_override"] == "direct"
    assert payload["trading_gateway"]["adapter"] == "fake"
    assert payload["capabilities"]["list_open_orders"]["ready"] is True
    assert payload["capabilities"]["list_positions"]["ready"] is True
    assert payload["capabilities"]["redeem_positions"]["ready"] is True
    assert payload["recommended_smoke_runs"][0]["command"].endswith("--adapter direct")
    assert payload["probes"]["open_orders"]["status"] == "ok"
    assert payload["probes"]["open_orders"]["row_count"] == 1
    assert payload["probes"]["positions"]["status"] == "ok"
    assert payload["probes"]["positions"]["row_count"] == 1


def test_check_live_trading_gateway_reports_capability_blockers(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)

    availability = {
        "live_trading.infra.polymarket_client": True,
        "py_clob_client.client": True,
        "py_builder_relayer_client.client": True,
        "py_builder_signing_sdk.config": True,
        "web3": True,
    }
    monkeypatch.setattr("pm15min.live.service._module_available", lambda name: availability.get(name, False))
    monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", "")
    monkeypatch.setenv("POLYMARKET_USER_ADDRESS", "")
    monkeypatch.setenv("RPC_URL", "")
    monkeypatch.setenv("POLYGON_RPC", "")
    monkeypatch.setenv("POLYGON_RPC_URL", "")
    monkeypatch.setenv("WEB3_PROVIDER_URI", "")
    monkeypatch.setenv("RPC_URL_BACKUPS", "")
    monkeypatch.setenv("POLYGON_RPC_BACKUPS", "")
    monkeypatch.setenv("RPC_FALLBACKS", "")
    monkeypatch.setenv("POLYGON_RPC_FALLBACKS", "")
    monkeypatch.setenv("BUILDER_API_KEY", "")
    monkeypatch.setenv("BUILDER_SECRET", "")
    monkeypatch.setenv("BUILDER_PASS_PHRASE", "")

    payload = check_live_trading_gateway(cfg, adapter="direct")

    assert payload["ok"] is False
    assert payload["capabilities"]["list_open_orders"]["blocked_by"] == ["missing_auth_config"]
    assert payload["capabilities"]["list_positions"]["blocked_by"] == ["missing_data_api_config"]
    assert payload["capabilities"]["redeem_positions"]["blocked_by"] == [
        "missing_auth_config",
        "missing_redeem_relay_config",
    ]


def test_check_live_trading_gateway_builds_env_gateway(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)

    class BuiltGateway:
        adapter_name = "direct"

        def list_open_orders(self):
            return []

        def list_positions(self):
            return []

    availability = {
        "live_trading.infra.polymarket_client": True,
        "py_clob_client.client": True,
        "py_builder_relayer_client.client": True,
        "py_builder_signing_sdk.config": True,
        "web3": True,
    }
    monkeypatch.setattr("pm15min.live.service._module_available", lambda name: availability.get(name, False))
    monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", "pk")
    monkeypatch.setenv("POLYMARKET_USER_ADDRESS", "0xuser")
    monkeypatch.setenv("RPC_URL", "https://rpc-1")
    monkeypatch.setenv("BUILDER_API_KEY", "builder-key")
    monkeypatch.setenv("BUILDER_SECRET", "builder-secret")
    monkeypatch.setenv("BUILDER_PASS_PHRASE", "builder-pass")
    monkeypatch.setattr(
        "pm15min.live.service.build_live_trading_gateway_from_env",
        lambda adapter_override=None: BuiltGateway(),
    )

    payload = check_live_trading_gateway(cfg, adapter="direct")

    gateway_check = next(item for item in payload["checks"] if item["name"] == "gateway_buildable")
    assert gateway_check["ok"] is True
    assert payload["trading_gateway"]["adapter"] == "direct"


def test_show_live_latest_runner_reads_latest_summary(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "risk_summary": {"decision": {"status": "accept"}, "execution": {"status": "plan"}},
                    "risk_alerts": [{"severity": "warning", "code": "regime_defense", "detail": {}}],
                    "decision": {"status": "accept"},
                    "execution": {"status": "plan"},
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["status"] == "ok"
    assert payload["canonical_live_scope"]["ok"] is True
    assert payload["risk_alert_summary"]["counts"]["warning"] == 1
    assert payload["risk_alert_summary"]["highest_severity"] == "warning"
    assert payload["operator_summary"]["runner_status"] == "ok"
    assert payload["operator_summary"]["can_run_side_effects"] is True
    assert payload["operator_summary"]["blocking_issue_count"] == 0
    assert payload["operator_summary"]["warning_issue_count"] == 0
    assert payload["operator_summary"]["orderbook_hot_cache_status"] == "missing"
    assert payload["next_actions"] == ["run live check-trading-gateway and then runner-once --dry-run-side-effects before enabling side effects"]
    assert payload["decision"]["status"] == "accept"
    assert "last_iteration" not in payload


def test_show_live_latest_runner_surfaces_hot_cache_staleness(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "captured_ts_ms": int(pd.Timestamp("2026-03-20T00:00:00Z").timestamp() * 1000),
                    "market_id": "market-1",
                    "token_id": "token-up",
                    "side": "up",
                    "best_ask": 0.4,
                    "best_bid": 0.39,
                    "ask_size_1": 10.0,
                    "bid_size_1": 11.0,
                    "spread": 0.01,
                }
            ]
        ),
        data_cfg.layout.orderbook_recent_path,
    )
    data_cfg.layout.orderbook_state_path.parent.mkdir(parents=True, exist_ok=True)
    data_cfg.layout.orderbook_state_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "provider": "DirectOrderbookProvider",
                "last_summary": {"recent_window_minutes": 15},
            }
        ),
        encoding="utf-8",
    )
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "risk_summary": {"decision": {"status": "accept"}, "execution": {"status": "plan"}},
                    "risk_alerts": [],
                    "decision": {"status": "accept"},
                    "execution": {"status": "plan"},
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    hot_cache = payload["operator_summary"]["orderbook_hot_cache_summary"]
    assert payload["operator_summary"]["orderbook_hot_cache_status"] == "stale"
    assert hot_cache["reason"] == "recent_cache_stale"
    assert hot_cache["provider"] == "DirectOrderbookProvider"
    assert hot_cache["recent_window_minutes"] == 15


def test_show_live_latest_runner_surfaces_truth_runtime_staleness(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "risk_summary": {"decision": {"status": "accept"}, "execution": {"status": "plan"}},
                    "risk_alerts": [],
                    "decision": {"status": "accept"},
                    "execution": {"status": "plan"},
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "pm15min.live.readiness.build_truth_runtime_summary",
        lambda cfg: {
            "truth_runtime_recent_refresh_status": "fresh",
            "truth_runtime_truth_table_status": "ok",
            "truth_runtime_truth_table_freshness_state": "stale",
            "truth_runtime_truth_table_freshness_max": "2026-03-19T23:40:00+00:00",
            "truth_runtime_oracle_prices_table_status": "ok",
            "truth_runtime_oracle_prices_table_freshness_state": "fresh",
            "truth_runtime_oracle_prices_table_freshness_max": "2026-03-20T00:00:00+00:00",
        },
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["operator_summary"]["truth_runtime_status"] == "stale"
    assert payload["operator_summary"]["truth_runtime_reason"] == "truth_table_stale"
    assert payload["operator_summary"]["truth_runtime_truth_status"] == "stale"
    assert payload["operator_summary"]["truth_runtime_oracle_status"] == "fresh"
    assert payload["operator_summary"]["truth_runtime_window_refresh_status"] == "fresh"
    assert payload["next_actions"] == [
        "run live check-trading-gateway and then runner-once --dry-run-side-effects before enabling side effects",
    ]


def test_show_live_latest_runner_uses_runner_health_blocker(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok_with_errors",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "runner_health": {
                        "overall_status": "error",
                        "pre_side_effect_status": "ok",
                        "post_side_effect_status": "error",
                        "primary_blocker": "order_action_error",
                        "blocker_stage": "order",
                        "blocking_issue_count": 1,
                        "warning_issue_count": 2,
                        "checks": [],
                    },
                    "risk_summary": {
                        "decision": {"status": "accept"},
                        "execution": {"status": "plan"},
                        "side_effects": {"order_status": "error", "order_reason": "submit failed"},
                    },
                    "risk_alerts": [{"severity": "critical", "code": "order_action_error", "detail": "submit failed"}],
                    "decision": {"status": "accept"},
                    "execution": {"status": "plan"},
                    "order_action": {"status": "error", "reason": "submit failed"},
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["status"] == "ok_with_errors"
    assert payload["runner_health"]["primary_blocker"] == "order_action_error"
    assert payload["operator_summary"]["can_run_side_effects"] is False
    assert payload["operator_summary"]["primary_blocker"] == "order_action_error"
    assert payload["operator_summary"]["blocker_stage"] == "order"
    assert payload["operator_summary"]["blocking_issue_count"] == 1
    assert payload["operator_summary"]["warning_issue_count"] == 2
    assert payload["next_actions"] == [
        "inspect latest runner order_action payload and trading gateway state before retrying side effects",
        "use operator_summary.order_action_reason together with the latest execution snapshot to narrow whether submit failed in request construction, gateway auth, or order placement",
    ]


def test_show_live_latest_runner_recommends_account_state_followups(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok_with_errors",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "runner_health": {
                        "overall_status": "error",
                        "pre_side_effect_status": "ok",
                        "post_side_effect_status": "error",
                        "primary_blocker": "account_state_sync_error",
                        "blocker_stage": "account",
                        "blocking_issue_count": 1,
                        "warning_issue_count": 0,
                        "checks": [],
                    },
                    "risk_summary": {
                        "decision": {"status": "accept"},
                        "execution": {"status": "plan"},
                        "side_effects": {
                            "order_status": "ok",
                            "account_state_status": "error",
                            "account_open_orders_status": "error",
                            "account_positions_status": "ok",
                        },
                    },
                    "risk_alerts": [{"severity": "critical", "code": "account_state_sync_error", "detail": {"open_orders_status": "error", "positions_status": "ok"}}],
                    "decision": {"status": "accept"},
                    "execution": {"status": "plan"},
                    "order_action": {"status": "ok", "reason": "order_submitted"},
                    "account_state": {"snapshot_ts": "2026-03-20T00-00-02Z", "open_orders_status": "error", "positions_status": "ok"},
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["operator_summary"]["account_state_status"] == "error"
    assert payload["operator_summary"]["account_open_orders_status"] == "error"
    assert payload["operator_summary"]["account_positions_status"] == "ok"
    assert payload["next_actions"] == [
        "inspect latest runner account_state payload and rerun live sync-account-state",
        "rerun live check-trading-gateway --probe-open-orders to isolate the account open-orders read path",
        "treat this as a post-submit account refresh/read-path issue before changing decision or execution logic",
    ]


def test_show_live_latest_runner_surfaces_secondary_account_and_cancel_followups_under_order_error(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok_with_errors",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "runner_health": {
                        "overall_status": "error",
                        "pre_side_effect_status": "ok",
                        "post_side_effect_status": "error",
                        "primary_blocker": "order_action_error",
                        "blocker_stage": "order",
                        "blocking_issue_count": 1,
                        "warning_issue_count": 2,
                        "checks": [],
                    },
                    "risk_summary": {
                        "decision": {"status": "accept"},
                        "execution": {"status": "plan"},
                        "side_effects": {
                            "order_status": "error",
                            "order_reason": "submit failed",
                            "account_state_status": "error",
                            "account_open_orders_status": "error",
                            "account_positions_status": "ok",
                            "cancel_status": "ok_with_errors",
                            "cancel_reason": "some cancels failed",
                        },
                    },
                    "risk_alerts": [
                        {"severity": "critical", "code": "order_action_error", "detail": "submit failed"},
                        {"severity": "critical", "code": "account_state_sync_error", "detail": {"open_orders_status": "error", "positions_status": "ok"}},
                        {"severity": "warning", "code": "cancel_action_error", "detail": "some cancels failed"},
                    ],
                    "decision": {"status": "accept"},
                    "execution": {"status": "plan"},
                    "order_action": {"status": "error", "reason": "submit failed"},
                    "account_state": {"snapshot_ts": "2026-03-20T00-00-02Z", "open_orders_status": "error", "positions_status": "ok"},
                    "cancel_action": {"status": "ok_with_errors", "reason": "some cancels failed", "summary": {"cancelled_orders": 1}},
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["operator_summary"]["primary_blocker"] == "order_action_error"
    assert payload["operator_summary"]["account_state_status"] == "error"
    assert payload["operator_summary"]["cancel_action_status"] == "ok_with_errors"
    assert payload["next_actions"] == [
        "inspect latest runner order_action payload and trading gateway state before retrying side effects",
        "use operator_summary.order_action_reason together with the latest execution snapshot to narrow whether submit failed in request construction, gateway auth, or order placement",
        "latest account refresh also failed after the submit path; rerun live sync-account-state after stabilizing order submit",
        "rerun live check-trading-gateway --probe-open-orders to isolate whether open-orders refresh failed independently of order submit",
        "after stabilizing order submit, inspect operator_summary.cancel_action_reason and reconcile latest open_orders before retrying cancel side effects",
    ]


def test_show_live_latest_runner_uses_side_effect_warning_followups_when_no_blocker(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok_with_errors",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "runner_health": {
                        "overall_status": "warning",
                        "pre_side_effect_status": "ok",
                        "post_side_effect_status": "warning",
                        "primary_blocker": None,
                        "blocker_stage": None,
                        "blocking_issue_count": 0,
                        "warning_issue_count": 2,
                        "checks": [],
                    },
                    "risk_summary": {
                        "decision": {"status": "accept"},
                        "execution": {"status": "plan"},
                        "side_effects": {
                            "order_status": "ok",
                            "account_state_status": "ok",
                            "account_open_orders_status": "ok",
                            "account_positions_status": "ok",
                            "cancel_status": "ok_with_errors",
                            "cancel_reason": "some cancels failed",
                            "redeem_status": "error",
                            "redeem_reason": "redeem relay unavailable",
                        },
                    },
                    "risk_alerts": [
                        {"severity": "warning", "code": "cancel_action_error", "detail": "some cancels failed"},
                        {"severity": "warning", "code": "redeem_action_error", "detail": "redeem relay unavailable"},
                    ],
                    "decision": {"status": "accept"},
                    "execution": {"status": "plan"},
                    "cancel_action": {"status": "ok_with_errors", "reason": "some cancels failed", "summary": {"cancelled_orders": 1}},
                    "redeem_action": {"status": "error", "reason": "redeem relay unavailable", "summary": {"redeemed_conditions": 0}},
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["operator_summary"]["primary_blocker"] is None
    assert payload["operator_summary"]["cancel_action_status"] == "ok_with_errors"
    assert payload["operator_summary"]["redeem_action_status"] == "error"
    assert payload["next_actions"] == [
        "inspect latest cancel_action payload and latest open_orders snapshot before retrying",
        "use operator_summary.cancel_action_reason together with latest open_orders snapshot to determine whether the failure came from candidate selection or gateway cancel submit",
        "treat cancel ok_with_errors as follow-up reconciliation work; confirm which order_ids remain open before retrying cancel side effects",
        "inspect latest redeem_action payload and latest positions snapshot before retrying",
        "use operator_summary.redeem_action_reason together with latest positions snapshot and redeemable conditions to determine whether the failure came from candidate selection or redeem relay submit",
    ]


def test_show_live_latest_runner_recommends_positions_probe_for_redeem_and_positions_read_path_error(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok_with_errors",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "runner_health": {
                        "overall_status": "warning",
                        "pre_side_effect_status": "ok",
                        "post_side_effect_status": "warning",
                        "primary_blocker": "redeem_action_error",
                        "blocker_stage": "redeem",
                        "blocking_issue_count": 1,
                        "warning_issue_count": 0,
                        "checks": [],
                    },
                    "risk_summary": {
                        "decision": {"status": "accept"},
                        "execution": {"status": "plan"},
                        "side_effects": {
                            "order_status": "ok",
                            "account_state_status": "error",
                            "account_open_orders_status": "ok",
                            "account_positions_status": "error",
                            "redeem_status": "error",
                            "redeem_reason": "redeem relay unavailable",
                        },
                    },
                    "risk_alerts": [{"severity": "warning", "code": "redeem_action_error", "detail": "redeem relay unavailable"}],
                    "decision": {"status": "accept"},
                    "execution": {"status": "plan"},
                    "redeem_action": {
                        "status": "error",
                        "reason": "redeem relay unavailable",
                        "summary": {"redeemed_conditions": 0},
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["operator_summary"]["primary_blocker"] == "redeem_action_error"
    assert payload["operator_summary"]["account_positions_status"] == "error"
    assert payload["next_actions"] == [
        "inspect latest redeem_action payload and latest positions snapshot before retrying",
        "use operator_summary.redeem_action_reason together with latest positions snapshot and redeemable conditions to determine whether the failure came from candidate selection or redeem relay submit",
        "rerun live sync-account-state and live check-trading-gateway --probe-positions before retrying redeem side effects",
    ]


def test_show_live_latest_runner_surfaces_foundation_warning_reason(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok_with_errors",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 1,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "foundation_summary": {
                        "status": "ok_with_errors",
                        "reason": "oracle:oracle_direct_rate_limited:429 too many requests",
                        "issue_codes": ["oracle_direct_rate_limited"],
                        "degraded_tasks": [
                            {
                                "task": "oracle",
                                "issue_code": "oracle_direct_rate_limited",
                                "error_type": "RuntimeError",
                                "error": "429 too many requests",
                                "fail_open": True,
                            }
                        ],
                    },
                    "runner_health": {
                        "overall_status": "warning",
                        "pre_side_effect_status": "warning",
                        "post_side_effect_status": "dry_run",
                        "primary_blocker": None,
                        "blocker_stage": None,
                        "blocking_issue_count": 0,
                        "warning_issue_count": 1,
                        "checks": [],
                    },
                    "risk_summary": {
                        "foundation": {
                            "status": "ok_with_errors",
                            "reason": "oracle:oracle_direct_rate_limited:429 too many requests",
                            "issue_codes": ["oracle_direct_rate_limited"],
                            "degraded_tasks": [
                                {
                                    "task": "oracle",
                                    "issue_code": "oracle_direct_rate_limited",
                                    "error_type": "RuntimeError",
                                    "error": "429 too many requests",
                                    "fail_open": True,
                                }
                            ],
                        },
                        "decision": {"status": "accept", "top_reject_reasons": []},
                        "execution": {"status": "plan", "reason": None},
                    },
                    "risk_alerts": [{"severity": "warning", "code": "foundation_ok_with_errors", "detail": "oracle:oracle_direct_rate_limited:429 too many requests"}],
                    "decision": {"status": "accept"},
                    "execution": {"status": "plan"},
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "pm15min.live.readiness.build_truth_runtime_summary",
        lambda cfg: {
            "truth_runtime_foundation_status": "ok_with_errors",
            "truth_runtime_foundation_reason": "oracle:oracle_direct_rate_limited:429 too many requests",
            "truth_runtime_foundation_issue_codes": ["oracle_direct_rate_limited"],
            "truth_runtime_foundation_run_started_at": "2026-03-20T00:00:00+00:00",
            "truth_runtime_foundation_last_completed_at": "2026-03-20T00:00:05+00:00",
            "truth_runtime_foundation_finished_at": "2026-03-20T00:00:05+00:00",
            "truth_runtime_foundation_completed_iterations": 1,
            "truth_runtime_recent_refresh_status": "fail_open",
            "truth_runtime_direct_oracle_fail_open": True,
            "truth_runtime_truth_table_status": "ok",
            "truth_runtime_truth_table_freshness_state": "fresh",
            "truth_runtime_oracle_prices_table_status": "ok",
            "truth_runtime_oracle_prices_table_freshness_state": "fresh",
        },
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["operator_summary"]["primary_blocker"] == "foundation_ok_with_errors"
    assert payload["operator_summary"]["foundation_reason"] == "oracle:oracle_direct_rate_limited:429 too many requests"
    assert payload["operator_summary"]["foundation_issue_codes"] == ["oracle_direct_rate_limited"]
    assert payload["operator_summary"]["foundation_degraded_tasks"] == [
        {
            "task": "oracle",
            "issue_code": "oracle_direct_rate_limited",
            "error_type": "RuntimeError",
            "error": "429 too many requests",
            "fail_open": True,
        }
    ]
    assert payload["operator_summary"]["truth_runtime_status"] == "fail_open"
    assert payload["operator_summary"]["truth_runtime_oracle_status"] == "fail_open"
    assert payload["operator_summary"]["truth_runtime_window_refresh_status"] == "fail_open"
    assert payload["operator_summary"]["truth_runtime_window_refresh_reason"] == "oracle:oracle_direct_rate_limited:429 too many requests"
    assert payload["operator_summary"]["foundation_run_started_at"] == "2026-03-20T00:00:00+00:00"
    assert payload["operator_summary"]["foundation_last_completed_at"] == "2026-03-20T00:00:05+00:00"
    assert payload["operator_summary"]["foundation_finished_at"] == "2026-03-20T00:00:05+00:00"
    assert payload["operator_summary"]["foundation_completed_iterations"] == 1
    assert payload["operator_summary"]["foundation_recent_refresh_status"] == "fail_open"
    assert payload["operator_summary"]["foundation_recent_refresh_reason"] == "oracle:oracle_direct_rate_limited:429 too many requests"
    assert payload["next_actions"] == [
        "foundation is degraded by direct oracle rate limiting; inspect operator_summary.foundation_reason before retrying",
        "treat the latest oracle_prices_table as fail-open fallback and retry after the rate limit window if you need green readiness",
        "use operator_summary.foundation_reason to identify the degraded foundation task without opening raw logs",
    ]


def test_show_live_latest_runner_combines_foundation_and_decision_followups(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok_with_errors",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 1,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "foundation_summary": {
                        "status": "ok_with_errors",
                        "reason": "oracle:oracle_direct_rate_limited:429 too many requests",
                        "issue_codes": ["oracle_direct_rate_limited"],
                        "degraded_tasks": [
                            {
                                "task": "oracle",
                                "issue_code": "oracle_direct_rate_limited",
                                "error_type": "RuntimeError",
                                "error": "429 too many requests",
                                "fail_open": True,
                            }
                        ],
                    },
                    "risk_summary": {
                        "foundation": {
                            "status": "ok_with_errors",
                            "reason": "oracle:oracle_direct_rate_limited:429 too many requests",
                            "issue_codes": ["oracle_direct_rate_limited"],
                            "degraded_tasks": [
                                {
                                    "task": "oracle",
                                    "issue_code": "oracle_direct_rate_limited",
                                    "error_type": "RuntimeError",
                                    "error": "429 too many requests",
                                    "fail_open": True,
                                }
                            ],
                        },
                        "decision": {
                            "status": "reject",
                            "top_reject_reasons": ["quote_missing_inputs", "quote_up_quote_missing"],
                        },
                        "execution": {"status": "no_action", "reason": "decision_reject"},
                    },
                    "risk_alerts": [
                        {"severity": "warning", "code": "foundation_ok_with_errors", "detail": "oracle:oracle_direct_rate_limited:429 too many requests"},
                        {"severity": "warning", "code": "decision_reject", "detail": ["quote_missing_inputs", "quote_up_quote_missing"]},
                    ],
                    "decision": {"status": "reject"},
                    "execution": {"status": "no_action", "reason": "decision_reject"},
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["operator_summary"]["primary_blocker"] == "foundation_ok_with_errors"
    assert payload["operator_summary"]["secondary_blockers"] == ["decision_not_accept", "execution_not_plan"]
    assert payload["operator_summary"]["decision_reject_category"] == "quote_inputs_missing"
    assert payload["next_actions"] == [
        "foundation is degraded by direct oracle rate limiting; inspect operator_summary.foundation_reason before retrying",
        "treat the latest oracle_prices_table as fail-open fallback and retry after the rate limit window if you need green readiness",
        "use operator_summary.foundation_reason to identify the degraded foundation task without opening raw logs",
        "inspect latest quote snapshot and orderbook_index coverage for the rejected market before retrying",
        "inspect operator_summary.orderbook_hot_cache_summary to see whether recent orderbook cache is missing, empty, or stale",
        "rerun data run live-foundation or data record orderbooks if quote inputs are still missing",
    ]


def test_show_live_latest_runner_recommends_quote_actions_for_decision_reject(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "runner_health": {
                        "overall_status": "warning",
                        "pre_side_effect_status": "blocked",
                        "post_side_effect_status": "dry_run",
                        "primary_blocker": "decision_not_accept",
                        "blocker_stage": "decision",
                        "blocking_issue_count": 2,
                        "warning_issue_count": 1,
                        "checks": [],
                    },
                    "risk_summary": {
                        "decision": {
                            "status": "reject",
                            "top_reject_reasons": [
                                "confidence_below_threshold",
                                "quote_missing_inputs",
                                "quote_up_quote_missing",
                            ],
                        },
                        "execution": {"status": "no_action", "reason": "decision_reject"},
                    },
                    "risk_alerts": [{"severity": "warning", "code": "decision_reject", "detail": ["quote_missing_inputs"]}],
                    "decision": {"status": "reject"},
                    "execution": {"status": "no_action", "reason": "decision_reject"},
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["operator_summary"]["decision_reject_category"] == "quote_inputs_missing"
    assert payload["operator_summary"]["decision_top_reject_reasons"] == [
        "confidence_below_threshold",
        "quote_missing_inputs",
        "quote_up_quote_missing",
    ]
    assert payload["next_actions"] == [
        "inspect latest quote snapshot and orderbook_index coverage for the rejected market before retrying",
        "inspect operator_summary.orderbook_hot_cache_summary to see whether recent orderbook cache is missing, empty, or stale",
        "rerun data run live-foundation or data record orderbooks if quote inputs are still missing",
    ]


def test_show_live_latest_runner_surfaces_threshold_reject_diagnostics(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "runner_health": {
                        "overall_status": "warning",
                        "pre_side_effect_status": "blocked",
                        "post_side_effect_status": "dry_run",
                        "primary_blocker": "decision_not_accept",
                        "blocker_stage": "decision",
                        "blocking_issue_count": 2,
                        "warning_issue_count": 1,
                        "checks": [],
                    },
                    "decision_payload": {
                        "accepted_offsets": [],
                        "rejected_offsets": [
                            {
                                "offset": 7,
                                "decision_ts": "2026-03-20T00:08:00+00:00",
                                "recommended_side": "UP",
                                "confidence": 0.72,
                                "guard_reasons": [
                                    "entry_price_max",
                                    "net_edge_below_quote_threshold",
                                    "roi_net_below_threshold",
                                ],
                                "quote_metrics": {
                                    "entry_price": 0.95,
                                    "entry_price_max": 0.30,
                                    "entry_price_min": 0.01,
                                    "p_side": 0.72,
                                    "edge_vs_quote": -0.23,
                                    "min_net_edge_required": 0.012,
                                    "roi_net_vs_quote": -0.24,
                                    "roi_threshold_required": 0.0,
                                    "quote_market_id": "1650536",
                                },
                                "quote_row": {"market_id": "1650536", "condition_id": "cond-7"},
                            },
                            {
                                "offset": 9,
                                "decision_ts": "2026-03-20T00:10:00+00:00",
                                "recommended_side": "UP",
                                "confidence": 0.785,
                                "guard_reasons": [
                                    "entry_price_max",
                                    "net_edge_below_quote_threshold",
                                    "roi_net_below_threshold",
                                ],
                                "quote_metrics": {
                                    "entry_price": 0.97,
                                    "entry_price_max": 0.30,
                                    "entry_price_min": 0.01,
                                    "p_side": 0.785,
                                    "edge_vs_quote": -0.185,
                                    "min_net_edge_required": 0.018,
                                    "roi_net_vs_quote": -0.191,
                                    "roi_threshold_required": 0.0,
                                    "quote_market_id": "1650536",
                                },
                                "quote_row": {"market_id": "1650536", "condition_id": "cond-9"},
                            },
                        ],
                    },
                    "risk_summary": {
                        "decision": {
                            "status": "reject",
                            "top_reject_reasons": [
                                "entry_price_max",
                                "net_edge_below_quote_threshold",
                                "roi_net_below_threshold",
                            ],
                        },
                        "execution": {"status": "no_action", "reason": "decision_reject"},
                    },
                    "risk_alerts": [
                        {
                            "severity": "warning",
                            "code": "decision_reject",
                            "detail": [
                                "entry_price_max",
                                "net_edge_below_quote_threshold",
                                "roi_net_below_threshold",
                            ],
                        }
                    ],
                    "decision": {"status": "reject"},
                    "execution": {"status": "no_action", "reason": "decision_reject"},
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    diagnostics = payload["operator_summary"]["decision_reject_diagnostics"]
    assert payload["operator_summary"]["decision_reject_category"] == "entry_or_quote_threshold"
    assert payload["operator_summary"]["decision_reject_interpretation"] == "market_priced_through_signal"
    assert diagnostics["rejected_offset_count"] == 2
    assert diagnostics["shared_guard_reasons"] == [
        "entry_price_max",
        "net_edge_below_quote_threshold",
        "roi_net_below_threshold",
    ]
    assert diagnostics["best_rejected_offset"] == {
        "offset": 9,
        "decision_ts": "2026-03-20T00:10:00+00:00",
        "side": "UP",
        "confidence": 0.785,
        "market_id": "1650536",
        "condition_id": "cond-9",
        "entry_price": 0.97,
        "entry_price_min": 0.01,
        "entry_price_max": 0.3,
        "p_side": 0.785,
        "edge_vs_quote": -0.185,
        "min_net_edge_required": 0.018,
        "roi_net_vs_quote": -0.191,
        "roi_threshold_required": 0.0,
        "quote_market_id": "1650536",
        "guard_reasons": [
            "entry_price_max",
            "net_edge_below_quote_threshold",
            "roi_net_below_threshold",
        ],
    }
    assert payload["next_actions"] == [
        "latest quotes already price the selected side above live entry cap and above model fair value; keep side effects disabled for this cycle",
        "inspect operator_summary.decision_reject_diagnostics.best_rejected_offset before changing live profile thresholds",
    ]


def test_show_live_latest_runner_includes_capital_usage_summary(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "runner_health": {
                        "overall_status": "warning",
                        "pre_side_effect_status": "blocked",
                        "post_side_effect_status": "dry_run",
                        "primary_blocker": "decision_not_accept",
                        "blocker_stage": "decision",
                        "blocking_issue_count": 2,
                        "warning_issue_count": 1,
                        "checks": [],
                    },
                    "regime_state": {
                        "state": "DEFENSE",
                        "pressure": "up",
                    },
                    "account_state_payload": {
                        "snapshot_ts": "2026-03-20T00:00:01Z",
                        "open_orders": {
                            "status": "ok",
                            "orders": [
                                {"market_id": "market-1", "price": 0.20, "size": 5.0},
                                {"market_id": "other-market", "price": 0.10, "size": 10.0},
                            ],
                        },
                        "positions": {
                            "status": "ok",
                            "positions": [
                                {
                                    "condition_id": "cond-1",
                                    "market_id": None,
                                    "size": 3.5,
                                    "redeemable": True,
                                    "current_value": 1.2,
                                    "cash_pnl": -0.5,
                                },
                                {
                                    "condition_id": "cond-2",
                                    "market_id": None,
                                    "size": 1.0,
                                    "redeemable": False,
                                    "current_value": 0.4,
                                    "cash_pnl": -0.1,
                                },
                            ],
                            "redeem_plan": {},
                        },
                    },
                    "decision_payload": {
                        "decision": {"status": "reject"},
                        "accepted_offsets": [],
                        "rejected_offsets": [
                            {
                                "offset": 7,
                                "decision_ts": "2026-03-20T00:08:00+00:00",
                                "recommended_side": "UP",
                                "confidence": 0.72,
                                "guard_reasons": ["regime_trade_count_cap"],
                                "quote_metrics": {
                                    "entry_price": 0.20,
                                    "entry_price_max": 0.30,
                                    "entry_price_min": 0.01,
                                    "p_side": 0.72,
                                    "edge_vs_quote": 0.52,
                                    "min_net_edge_required": 0.012,
                                    "roi_net_vs_quote": 2.6,
                                    "roi_threshold_required": 0.0,
                                    "quote_market_id": "market-1",
                                },
                                "quote_row": {"market_id": "market-1", "condition_id": "cond-1"},
                            }
                        ],
                    },
                    "risk_summary": {
                        "decision": {
                            "status": "reject",
                            "top_reject_reasons": ["regime_trade_count_cap"],
                        },
                        "execution": {"status": "no_action", "reason": "decision_reject"},
                    },
                    "risk_alerts": [{"severity": "warning", "code": "decision_reject", "detail": ["regime_trade_count_cap"]}],
                    "decision": {"status": "reject"},
                    "execution": {
                        "status": "no_action",
                        "reason": "decision_reject",
                        "stake_base_usd": 1.0,
                        "stake_multiplier": 1.0,
                        "stake_regime_state": "DEFENSE",
                        "requested_notional_usd": 1.0,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    capital = payload["operator_summary"]["capital_usage_summary"]
    assert capital["portfolio"]["visible_open_order_notional_usd"] == 2.0
    assert capital["portfolio"]["visible_position_mark_usd"] == 1.6
    assert capital["portfolio"]["visible_position_cash_pnl_usd"] == -0.6
    assert capital["portfolio"]["visible_capital_usage_usd"] == 3.6
    assert capital["account_overview"] == {
        "snapshot_ts": "2026-03-20T00:00:01Z",
        "total_open_orders": 2,
        "open_orders_market_count": 2,
        "open_orders_token_count": 0,
        "total_positions": 2,
        "positions_market_count": 0,
        "positions_condition_count": 2,
        "redeemable_positions": 1,
        "redeemable_conditions": 1,
        "positions_without_market_id": 2,
        "visible_open_order_notional_usd": 2.0,
        "visible_position_mark_usd": 1.6,
        "visible_position_cash_pnl_usd": -0.6,
        "visible_capital_usage_usd": 3.6,
        "has_open_orders": True,
        "has_positions": True,
        "has_redeemable_positions": True,
        "cash_balance_available": False,
        "full_account_equity_view": False,
        "coverage": {
            "source": "runner_account_state_payload",
            "open_orders_status": "ok",
            "positions_status": "ok",
            "uses_open_order_notional": True,
            "uses_position_current_value": True,
            "uses_account_cash_balance": False,
            "uses_total_account_equity": False,
        },
        "composition": {
            "open_orders_share_of_visible_capital_usage": 0.5555555555555556,
            "position_mark_share_of_visible_capital_usage": 0.4444444444444445,
        },
        "focus_market": {
            "source": "best_rejected_offset",
            "offset": 7,
            "market_id": "market-1",
            "condition_id": "cond-1",
            "active_trade_count": 2,
            "visible_usage_usd": 2.2,
            "share_of_visible_capital_usage": 0.6111111111111112,
        },
        "notes": [
            "visible totals only include open-order notional plus current position current_value from the latest account snapshot",
            "cash balance / total account equity is not available from the current live gateway contracts",
        ],
    }
    assert capital["focus_market"] == {
        "source": "best_rejected_offset",
        "offset": 7,
        "market_id": "market-1",
        "condition_id": "cond-1",
        "account_state_available": True,
        "position_match_basis": "condition_id",
        "open_orders_count": 1,
        "open_orders_notional_usd": 1.0,
        "positions_count": 1,
        "positions_size_sum": 3.5,
        "positions_current_value_usd": 1.2,
        "positions_cash_pnl_usd": -0.5,
        "redeemable_positions": 1,
        "active_trade_count": 2,
    }
    assert capital["regime_context"] == {
        "state": "DEFENSE",
        "pressure": "up",
        "regime_apply_stake_scale": False,
        "defense_max_trades_per_market": 1,
        "current_market_trade_slots_remaining": 0,
    }
    assert capital["execution_budget"] == {
        "stake_base_usd": 1.0,
        "stake_multiplier": 1.0,
        "stake_regime_state": "DEFENSE",
        "requested_notional_usd": 1.0,
        "max_notional_usd": 2000.0,
        "requested_vs_max_notional_ratio": 0.0005,
    }
    assert capital["interpretation"] == "defense_trade_cap_reached"
    assert payload["next_actions"] == [
        "inspect operator_summary.capital_usage_summary.focus_market and regime_context before retrying",
        "reduce existing open orders / positions for the focus market or wait for market rollover before retrying",
    ]


def test_show_live_latest_runner_account_overview_falls_back_to_latest_account_snapshots(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_runner_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_open_orders_path = root / "var" / "live" / "state" / "open_orders" / "asset=sol" / "latest.json"
    latest_positions_path = root / "var" / "live" / "state" / "positions" / "asset=sol" / "latest.json"
    latest_runner_path.parent.mkdir(parents=True, exist_ok=True)
    latest_open_orders_path.parent.mkdir(parents=True, exist_ok=True)
    latest_positions_path.parent.mkdir(parents=True, exist_ok=True)
    latest_runner_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "risk_summary": {
                        "decision": {"status": "accept"},
                        "execution": {"status": "plan"},
                    },
                    "risk_alerts": [],
                    "decision": {"status": "accept"},
                    "execution": {"status": "plan"},
                },
            }
        ),
        encoding="utf-8",
    )
    latest_open_orders_path.write_text(
        json.dumps(
            {
                "snapshot_ts": "2026-03-20T00:00:02Z",
                "status": "ok",
                "orders": [
                    {"market_id": "market-1", "token_id": "token-1", "price": 0.25, "size": 4.0},
                    {"market_id": "market-2", "token_id": "token-2", "price": 0.50, "size": 2.0},
                ],
            }
        ),
        encoding="utf-8",
    )
    latest_positions_path.write_text(
        json.dumps(
            {
                "snapshot_ts": "2026-03-20T00:00:03Z",
                "status": "ok",
                "positions": [
                    {
                        "condition_id": "cond-1",
                        "market_id": "market-1",
                        "size": 3.0,
                        "redeemable": True,
                        "current_value": 1.5,
                        "cash_pnl": 0.2,
                    },
                    {
                        "condition_id": "cond-2",
                        "market_id": "market-2",
                        "size": 1.0,
                        "redeemable": False,
                        "current_value": 0.5,
                        "cash_pnl": -0.1,
                    },
                ],
                "redeem_plan": {
                    "cond-1": {
                        "condition_id": "cond-1",
                        "positions_count": 1,
                        "size_sum": 3.0,
                        "current_value_sum": 1.5,
                        "cash_pnl_sum": 0.2,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    overview = payload["operator_summary"]["capital_usage_summary"]["account_overview"]
    assert overview == {
        "snapshot_ts": None,
        "total_open_orders": 2,
        "open_orders_market_count": 2,
        "open_orders_token_count": 2,
        "total_positions": 2,
        "positions_market_count": 2,
        "positions_condition_count": 2,
        "redeemable_positions": 1,
        "redeemable_conditions": 1,
        "positions_without_market_id": 0,
        "visible_open_order_notional_usd": 2.0,
        "visible_position_mark_usd": 2.0,
        "visible_position_cash_pnl_usd": 0.1,
        "visible_capital_usage_usd": 4.0,
        "has_open_orders": True,
        "has_positions": True,
        "has_redeemable_positions": True,
        "cash_balance_available": False,
        "full_account_equity_view": False,
        "coverage": {
            "source": "latest_state_fallback",
            "open_orders_status": "ok",
            "positions_status": "ok",
            "uses_open_order_notional": True,
            "uses_position_current_value": True,
            "uses_account_cash_balance": False,
            "uses_total_account_equity": False,
        },
        "composition": {
            "open_orders_share_of_visible_capital_usage": 0.5,
            "position_mark_share_of_visible_capital_usage": 0.5,
        },
        "focus_market": None,
        "notes": [
            "visible totals only include open-order notional plus current position current_value from the latest account snapshot",
            "cash balance / total account equity is not available from the current live gateway contracts",
        ],
    }


def test_show_live_latest_runner_recommends_depth_actions_for_execution_block(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "runner_health": {
                        "overall_status": "warning",
                        "pre_side_effect_status": "blocked",
                        "post_side_effect_status": "dry_run",
                        "primary_blocker": "execution_not_plan",
                        "blocker_stage": "execution",
                        "blocking_issue_count": 1,
                        "warning_issue_count": 0,
                        "checks": [],
                    },
                    "risk_summary": {
                        "decision": {"status": "accept", "top_reject_reasons": []},
                        "execution": {"status": "blocked", "reason": "depth_fill_ratio_below_threshold"},
                    },
                    "risk_alerts": [{"severity": "warning", "code": "execution_blocked", "detail": "depth_fill_ratio_below_threshold"}],
                    "decision": {"status": "accept"},
                    "execution": {
                        "status": "blocked",
                        "reason": "depth_fill_ratio_below_threshold",
                        "execution_reasons": ["depth_fill_ratio_below_threshold"],
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["operator_summary"]["execution_block_category"] == "orderbook_depth"
    assert payload["next_actions"] == [
        "inspect latest execution depth_plan and orderbook fill_ratio before retrying",
        "rerun data record orderbooks or live-foundation if depth snapshots are stale or missing",
    ]


def test_show_live_latest_runner_recommends_repriced_actions_for_execution_block(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 0,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "runner_health": {
                        "overall_status": "warning",
                        "pre_side_effect_status": "blocked",
                        "post_side_effect_status": "dry_run",
                        "primary_blocker": "execution_not_plan",
                        "blocker_stage": "execution",
                        "blocking_issue_count": 1,
                        "warning_issue_count": 0,
                        "checks": [],
                    },
                    "risk_summary": {
                        "decision": {"status": "accept", "top_reject_reasons": []},
                        "execution": {"status": "blocked", "reason": "repriced_entry_price_max"},
                    },
                    "risk_alerts": [{"severity": "warning", "code": "execution_blocked", "detail": "repriced_entry_price_max"}],
                    "decision": {"status": "accept"},
                    "execution": {
                        "status": "blocked",
                        "reason": "repriced_entry_price_max",
                        "execution_reasons": ["repriced_entry_price_max", "repriced_roi_below_threshold"],
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["operator_summary"]["execution_block_category"] == "repriced_quote_threshold"
    assert payload["next_actions"] == [
        "inspect latest execution repriced_metrics and compare repriced entry/edge/roi vs live profile thresholds",
    ]


def test_show_live_latest_runner_combines_execution_and_foundation_followups(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    latest_path = (
        root
        / "var"
        / "live"
        / "state"
        / "runner"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "target=direction"
        / "latest.json"
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "status": "ok_with_errors",
                "run_started_at": "2026-03-20T00:00:00Z",
                "completed_iterations": 1,
                "errors": 1,
                "runner_log_path": str(root / "var" / "live" / "logs" / "runner.jsonl"),
                "last_iteration": {
                    "snapshot_ts": "2026-03-20T00-00-01Z",
                    "foundation_summary": {
                        "status": "ok_with_errors",
                        "reason": "oracle:oracle_direct_rate_limited:429 too many requests",
                        "issue_codes": ["oracle_direct_rate_limited"],
                        "degraded_tasks": [
                            {
                                "task": "oracle",
                                "issue_code": "oracle_direct_rate_limited",
                                "error_type": "RuntimeError",
                                "error": "429 too many requests",
                                "fail_open": True,
                            }
                        ],
                    },
                    "runner_health": {
                        "overall_status": "warning",
                        "pre_side_effect_status": "blocked",
                        "post_side_effect_status": "dry_run",
                        "primary_blocker": "execution_not_plan",
                        "blocker_stage": "execution",
                        "blocking_issue_count": 1,
                        "warning_issue_count": 1,
                        "checks": [],
                    },
                    "risk_summary": {
                        "foundation": {
                            "status": "ok_with_errors",
                            "reason": "oracle:oracle_direct_rate_limited:429 too many requests",
                            "issue_codes": ["oracle_direct_rate_limited"],
                            "degraded_tasks": [
                                {
                                    "task": "oracle",
                                    "issue_code": "oracle_direct_rate_limited",
                                    "error_type": "RuntimeError",
                                    "error": "429 too many requests",
                                    "fail_open": True,
                                }
                            ],
                        },
                        "decision": {"status": "accept", "top_reject_reasons": []},
                        "execution": {"status": "blocked", "reason": "depth_fill_ratio_below_threshold"},
                    },
                    "risk_alerts": [
                        {"severity": "warning", "code": "foundation_ok_with_errors", "detail": "oracle:oracle_direct_rate_limited:429 too many requests"},
                        {"severity": "warning", "code": "execution_blocked", "detail": "depth_fill_ratio_below_threshold"},
                    ],
                    "decision": {"status": "accept"},
                    "execution": {
                        "status": "blocked",
                        "reason": "depth_fill_ratio_below_threshold",
                        "execution_reasons": ["depth_fill_ratio_below_threshold"],
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["operator_summary"]["primary_blocker"] == "execution_not_plan"
    assert payload["operator_summary"]["secondary_blockers"] == ["foundation_ok_with_errors"]
    assert payload["operator_summary"]["execution_block_category"] == "orderbook_depth"
    assert payload["next_actions"] == [
        "inspect latest execution depth_plan and orderbook fill_ratio before retrying",
        "rerun data record orderbooks or live-foundation if depth snapshots are stale or missing",
        "foundation is degraded by direct oracle rate limiting; inspect operator_summary.foundation_reason before retrying",
        "treat the latest oracle_prices_table as fail-open fallback and retry after the rate limit window if you need green readiness",
        "use operator_summary.foundation_reason to identify the degraded foundation task without opening raw logs",
    ]


def test_show_live_latest_runner_missing_still_returns_operator_summary(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["status"] == "missing"
    assert payload["reason"] == "latest_runner_missing"
    assert payload["operator_summary"]["canonical_live_scope_ok"] is True
    assert payload["operator_summary"]["runner_status"] is None
    assert payload["next_actions"] == ["run live check-trading-gateway and then runner-once --dry-run-side-effects before enabling side effects"]


def test_show_live_latest_runner_missing_still_surfaces_foundation_refresh_meta(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setattr(
        "pm15min.live.readiness.build_truth_runtime_summary",
        lambda cfg: {
            "truth_runtime_foundation_status": "ok_with_errors",
            "truth_runtime_foundation_reason": "oracle:oracle_direct_rate_limited:429 too many requests",
            "truth_runtime_foundation_issue_codes": ["oracle_direct_rate_limited"],
            "truth_runtime_foundation_run_started_at": "2026-03-20T00:00:00+00:00",
            "truth_runtime_foundation_last_completed_at": "2026-03-20T00:00:05+00:00",
            "truth_runtime_foundation_finished_at": "2026-03-20T00:00:05+00:00",
            "truth_runtime_foundation_completed_iterations": 1,
            "truth_runtime_recent_refresh_status": "fail_open",
            "truth_runtime_recent_refresh_interpretation": "recent_refresh_degraded_but_existing_oracle_table_is_still_serving_reads",
            "truth_runtime_direct_oracle_fail_open": True,
            "truth_runtime_truth_table_status": "ok",
            "truth_runtime_truth_table_freshness_state": "fresh",
            "truth_runtime_truth_table_freshness_max": "2026-03-20T00:00:00+00:00",
            "truth_runtime_oracle_prices_table_status": "ok",
            "truth_runtime_oracle_prices_table_freshness_state": "fresh",
            "truth_runtime_oracle_prices_table_freshness_max": "2026-03-20T00:00:00+00:00",
        },
    )

    payload = show_live_latest_runner(cfg, target="direction", risk_only=True)

    assert payload["status"] == "missing"
    assert "/foundation/" in payload["latest_state_paths"]["foundation_state"]
    assert payload["latest_state_paths"]["foundation_state"].endswith("/state.json")
    assert payload["operator_summary"]["foundation_status"] == "ok_with_errors"
    assert payload["operator_summary"]["foundation_reason"] == "oracle:oracle_direct_rate_limited:429 too many requests"
    assert payload["operator_summary"]["foundation_run_started_at"] == "2026-03-20T00:00:00+00:00"
    assert payload["operator_summary"]["foundation_last_completed_at"] == "2026-03-20T00:00:05+00:00"
    assert payload["operator_summary"]["foundation_recent_refresh_status"] == "fail_open"
    assert payload["operator_summary"]["truth_runtime_status"] == "fail_open"
    assert payload["operator_summary"]["truth_runtime_window_refresh_status"] == "fail_open"


def test_show_live_ready_reports_not_ready_when_gateway_fails(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setattr(
        "pm15min.live.service.check_live_trading_gateway",
        lambda cfg, adapter=None, probe_open_orders=False, probe_positions=False: {
            "ok": False,
            "checks": [{"name": "gateway_buildable", "ok": False}],
            "probes": {
                "open_orders": {"status": "not_run", "ok": False},
                "positions": {"status": "not_run", "ok": False},
            },
            "recommended_smoke_runs": [{"step": "gateway_check"}],
        },
    )
    monkeypatch.setattr(
        "pm15min.live.service.show_live_latest_runner",
        lambda cfg, target="direction", risk_only=True: {
            "canonical_live_scope": {"ok": True},
            "status": "missing",
            "operator_summary": {"can_run_side_effects": False, "primary_blocker": "latest_runner_missing"},
            "next_actions": ["run live check-trading-gateway and then runner-once --dry-run-side-effects before enabling side effects"],
        },
    )

    payload = show_live_ready(cfg, target="direction", adapter="direct")

    assert payload["status"] == "not_ready"
    assert payload["ready_for_side_effects"] is False
    assert payload["primary_blocker"] == "gateway:gateway_buildable"
    assert payload["operator_smoke_summary"]["status"] == "blocked"
    assert payload["operator_smoke_summary"]["reason"] == "gateway_checks_failed"
    assert "resolve failed gateway checks before enabling side effects" in payload["next_actions"]


def test_show_live_ready_reports_ready_when_gateway_and_runner_are_ready(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setattr(
        "pm15min.live.service.check_live_trading_gateway",
        lambda cfg, adapter=None, probe_open_orders=False, probe_positions=False: {
            "ok": True,
            "checks": [{"name": "gateway_buildable", "ok": True}],
            "probes": {
                "open_orders": {"status": "ok", "ok": True},
                "positions": {"status": "ok", "ok": True},
            },
            "recommended_smoke_runs": [],
        },
    )
    monkeypatch.setattr(
        "pm15min.live.service.show_live_latest_runner",
        lambda cfg, target="direction", risk_only=True: {
            "canonical_live_scope": {"ok": True},
            "status": "ok",
            "operator_summary": {"can_run_side_effects": True, "primary_blocker": None},
            "next_actions": ["run live check-trading-gateway and then runner-once --dry-run-side-effects before enabling side effects"],
        },
    )

    payload = show_live_ready(cfg, target="direction", adapter="direct")

    assert payload["status"] == "ready"
    assert payload["ready_for_side_effects"] is True
    assert payload["primary_blocker"] is None
    assert payload["operator_smoke_summary"]["status"] == "operational"
    assert payload["operator_smoke_summary"]["reason"] == "path_operational"


def test_show_live_ready_marks_smoke_operational_when_only_strategy_rejects(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setattr(
        "pm15min.live.service.check_live_trading_gateway",
        lambda cfg, adapter=None, probe_open_orders=False, probe_positions=False: {
            "ok": True,
            "checks": [{"name": "gateway_buildable", "ok": True}],
            "probes": {
                "open_orders": {"status": "ok", "ok": True},
                "positions": {"status": "ok", "ok": True},
            },
            "recommended_smoke_runs": [],
        },
    )
    monkeypatch.setattr(
        "pm15min.live.service.show_live_latest_runner",
        lambda cfg, target="direction", risk_only=True: {
            "canonical_live_scope": {"ok": True},
            "status": "ok",
            "last_iteration_snapshot_ts": "2026-03-20T00-00-01Z",
            "operator_summary": {
                "canonical_live_scope_ok": True,
                "runner_status": "ok",
                "can_run_side_effects": False,
                "primary_blocker": "decision_not_accept",
                "decision_reject_category": "entry_or_quote_threshold",
                "decision_reject_interpretation": "market_priced_through_signal",
                "execution_reason": "decision_reject",
                "risk_alert_summary": {"has_critical": False},
            },
            "next_actions": [
                "latest quotes already price the selected side above live entry cap and above model fair value; keep side effects disabled for this cycle"
            ],
        },
    )

    payload = show_live_ready(cfg, target="direction", adapter="direct")

    assert payload["status"] == "not_ready"
    assert payload["ready_for_side_effects"] is False
    assert payload["primary_blocker"] == "decision_not_accept"
    assert payload["operator_smoke_summary"] == {
        "status": "operational",
        "reason": "strategy_reject_only",
        "can_validate_real_side_effect_path": True,
        "gateway_check_failures": [],
        "gateway_probe_failures": [],
        "truth_runtime_status": None,
        "truth_runtime_reason": None,
        "truth_runtime_window_refresh_status": None,
        "truth_runtime_oracle_status": None,
        "orderbook_hot_cache_status": None,
        "orderbook_hot_cache_reason": None,
        "runner_smoke_status": "strategy_only_blocked",
        "runner_primary_blocker": "decision_not_accept",
        "runner_decision_reject_category": "entry_or_quote_threshold",
        "runner_decision_reject_interpretation": "market_priced_through_signal",
        "runner_snapshot_ts": "2026-03-20T00-00-01Z",
    }


def test_show_live_ready_surfaces_foundation_warning_reason(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setattr(
        "pm15min.live.service.check_live_trading_gateway",
        lambda cfg, adapter=None, probe_open_orders=False, probe_positions=False: {
            "ok": True,
            "checks": [{"name": "gateway_buildable", "ok": True}],
            "probes": {
                "open_orders": {"status": "ok", "ok": True},
                "positions": {"status": "ok", "ok": True},
            },
            "recommended_smoke_runs": [],
        },
    )
    monkeypatch.setattr(
        "pm15min.live.service.show_live_latest_runner",
        lambda cfg, target="direction", risk_only=True: {
            "canonical_live_scope": {"ok": True},
            "status": "ok_with_errors",
            "last_iteration_snapshot_ts": "2026-03-20T00-00-01Z",
            "operator_summary": {
                "canonical_live_scope_ok": True,
                "runner_status": "ok_with_errors",
                "can_run_side_effects": False,
                "primary_blocker": "foundation_ok_with_errors",
                "foundation_status": "ok_with_errors",
                "foundation_reason": "oracle:oracle_direct_rate_limited:429 too many requests",
                "foundation_issue_codes": ["oracle_direct_rate_limited"],
                "truth_runtime_status": "fail_open",
                "truth_runtime_reason": "oracle:oracle_direct_rate_limited:429 too many requests",
                "truth_runtime_window_refresh_status": "fail_open",
                "truth_runtime_oracle_status": "fail_open",
                "risk_alert_summary": {"has_critical": False},
            },
            "next_actions": [
                "foundation is degraded by direct oracle rate limiting; inspect operator_summary.foundation_reason before retrying",
                "treat the latest oracle_prices_table as fail-open fallback and retry after the rate limit window if you need green readiness",
                "use operator_summary.foundation_reason to identify the degraded foundation task without opening raw logs",
            ],
        },
    )

    payload = show_live_ready(cfg, target="direction", adapter="direct")

    assert payload["status"] == "not_ready"
    assert payload["primary_blocker"] == "foundation_ok_with_errors"
    assert payload["operator_smoke_summary"] == {
        "status": "operational",
        "reason": "foundation_warning_only",
        "can_validate_real_side_effect_path": True,
        "gateway_check_failures": [],
        "gateway_probe_failures": [],
        "truth_runtime_status": "fail_open",
        "truth_runtime_reason": "oracle:oracle_direct_rate_limited:429 too many requests",
        "truth_runtime_window_refresh_status": "fail_open",
        "truth_runtime_oracle_status": "fail_open",
        "orderbook_hot_cache_status": None,
        "orderbook_hot_cache_reason": None,
        "runner_smoke_status": "foundation_warning_only",
        "runner_primary_blocker": "foundation_ok_with_errors",
        "runner_decision_reject_category": None,
        "runner_decision_reject_interpretation": None,
        "runner_snapshot_ts": "2026-03-20T00-00-01Z",
    }
    assert payload["next_actions"] == [
        "foundation is degraded by direct oracle rate limiting; inspect operator_summary.foundation_reason before retrying",
        "treat the latest oracle_prices_table as fail-open fallback and retry after the rate limit window if you need green readiness",
        "use operator_summary.foundation_reason to identify the degraded foundation task without opening raw logs",
        "wait for the direct oracle rate-limit window to clear, then rerun data run live-foundation or runner-once --dry-run-side-effects",
        "treat oracle_prices_table as temporary fail-open fallback until direct oracle recovers",
    ]


def test_show_live_ready_uses_failed_probe_as_primary_blocker(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setattr(
        "pm15min.live.service.check_live_trading_gateway",
        lambda cfg, adapter=None, probe_open_orders=False, probe_positions=False: {
            "ok": False,
            "checks": [{"name": "gateway_buildable", "ok": True}],
            "probes": {
                "open_orders": {"status": "ok", "ok": True},
                "positions": {"status": "error", "ok": False},
            },
            "recommended_smoke_runs": [{"step": "probe_positions"}],
        },
    )
    monkeypatch.setattr(
        "pm15min.live.service.show_live_latest_runner",
        lambda cfg, target="direction", risk_only=True: {
            "canonical_live_scope": {"ok": True},
            "status": "ok",
            "last_iteration_snapshot_ts": "2026-03-20T00-00-01Z",
            "operator_summary": {
                "canonical_live_scope_ok": True,
                "runner_status": "ok",
                "can_run_side_effects": True,
                "primary_blocker": None,
                "risk_alert_summary": {"has_critical": False},
            },
            "next_actions": [],
        },
    )

    payload = show_live_ready(cfg, target="direction", adapter="direct")

    assert payload["status"] == "not_ready"
    assert payload["ready_for_side_effects"] is False
    assert payload["gateway_failed_probes"] == ["positions"]
    assert payload["primary_blocker"] == "gateway_probe:positions"
    assert payload["operator_smoke_summary"]["status"] == "blocked"
    assert payload["operator_smoke_summary"]["reason"] == "gateway_probes_failed"
    assert "inspect failed gateway probes before enabling side effects" in payload["next_actions"]


def test_show_live_ready_appends_foundation_rate_limit_context_even_when_strategy_blocked(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setattr(
        "pm15min.live.service.check_live_trading_gateway",
        lambda cfg, adapter=None, probe_open_orders=False, probe_positions=False: {
            "ok": True,
            "checks": [{"name": "gateway_buildable", "ok": True}],
            "probes": {
                "open_orders": {"status": "ok", "ok": True},
                "positions": {"status": "ok", "ok": True},
            },
            "recommended_smoke_runs": [],
        },
    )
    monkeypatch.setattr(
        "pm15min.live.service.show_live_latest_runner",
        lambda cfg, target="direction", risk_only=True: {
            "canonical_live_scope": {"ok": True},
            "status": "ok",
            "last_iteration_snapshot_ts": "2026-03-20T00-00-01Z",
            "operator_summary": {
                "canonical_live_scope_ok": True,
                "runner_status": "ok",
                "can_run_side_effects": False,
                "primary_blocker": "decision_not_accept",
                "decision_reject_category": "confidence_threshold",
                "foundation_status": "ok_with_errors",
                "foundation_reason": "oracle:oracle_direct_rate_limited:429 too many requests",
                "foundation_issue_codes": ["oracle_direct_rate_limited"],
                "risk_alert_summary": {"has_critical": False},
            },
            "next_actions": [
                "inspect latest decision confidence vs threshold and active bundle output before retrying"
            ],
        },
    )

    payload = show_live_ready(cfg, target="direction", adapter="direct")

    assert payload["next_actions"] == [
        "inspect latest decision confidence vs threshold and active bundle output before retrying",
        "wait for the direct oracle rate-limit window to clear, then rerun data run live-foundation or runner-once --dry-run-side-effects",
        "treat oracle_prices_table as temporary fail-open fallback until direct oracle recovers",
    ]
