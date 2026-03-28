from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm15min.live.signal.scoring_offsets import (
    OffsetScoreContext,
    _resolve_latest_live_row,
    build_offset_signal,
    score_offset_signals,
)


def _feature_coverage_stub(**kwargs) -> dict[str, object]:
    nan_feature_columns = list(kwargs.get("nan_feature_columns") or [])
    return {
        "required_feature_count": len(kwargs.get("required_columns") or []),
        "present_feature_count": len(kwargs.get("required_columns") or []),
        "missing_feature_count": 0,
        "effective_missing_feature_count": 0,
        "coverage_ratio": 1.0,
        "present_columns": list(kwargs.get("required_columns") or []),
        "missing_columns": [],
        "blacklisted_columns": [],
        "not_allowed_blacklist_columns": [],
        "not_allowed_blacklist_count": 0,
        "effective_missing_columns": [],
        "nan_feature_columns": nan_feature_columns,
        "nan_feature_count": len(nan_feature_columns),
    }


def _latest_nan_feature_columns_stub(**kwargs) -> list[str]:
    return []


def _iso_or_none(value) -> str | None:
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if ts is None or pd.isna(ts):
        return None
    return ts.isoformat()


def test_resolve_latest_live_row_does_not_fallback_to_previous_cycle_before_offset_opens() -> None:
    now_utc = pd.Timestamp("2026-03-27T09:21:16Z")
    current_cycle_start = pd.Timestamp("2026-03-27T09:15:00Z")
    current_cycle_end = pd.Timestamp("2026-03-27T09:30:00Z")
    previous_cycle_start = pd.Timestamp("2026-03-27T09:00:00Z")
    previous_cycle_end = pd.Timestamp("2026-03-27T09:15:00Z")

    ctx = OffsetScoreContext(
        offset=7,
        bundle_cfg={"signal_target": "direction"},
        feature_columns=["ret_30m"],
        effective_blacklist=[],
        not_allowed_blacklist=[],
        features=pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-27T09:20:00Z"),
                    "cycle_start_ts": current_cycle_start,
                    "cycle_end_ts": current_cycle_end,
                    "offset": 5,
                    "ret_30m": 0.01,
                }
            ]
        ),
        scored=pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-27T09:07:00Z"),
                    "cycle_start_ts": previous_cycle_start,
                    "cycle_end_ts": previous_cycle_end,
                    "offset": 7,
                    "p_lgb": 0.81,
                    "p_lr": 0.77,
                    "p_signal": 0.79,
                    "w_lgb": 0.5,
                    "w_lr": 0.5,
                    "p_up": 0.79,
                    "p_down": 0.21,
                    "probability_mode": "raw_blend",
                    "score_valid": True,
                    "score_reason": "",
                }
            ]
        ),
    )

    row, coverage, inactive_reason = _resolve_latest_live_row(
        ctx=ctx,
        now_utc=now_utc,
        feature_coverage_fn=_feature_coverage_stub,
        latest_nan_feature_columns_fn=_latest_nan_feature_columns_stub,
        iso_or_none_fn=_iso_or_none,
    )

    assert row is None
    assert inactive_reason == "offset_not_yet_open"
    assert coverage["effective_missing_feature_count"] == 0


def test_resolve_latest_live_row_infers_current_cycle_when_feature_frame_lags() -> None:
    now_utc = pd.Timestamp("2026-03-27T15:31:16Z")
    previous_cycle_start = pd.Timestamp("2026-03-27T15:15:00Z")
    previous_cycle_end = pd.Timestamp("2026-03-27T15:30:00Z")

    ctx = OffsetScoreContext(
        offset=7,
        bundle_cfg={"signal_target": "direction"},
        feature_columns=["ret_30m"],
        effective_blacklist=[],
        not_allowed_blacklist=[],
        features=pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-27T15:29:00Z"),
                    "cycle_start_ts": previous_cycle_start,
                    "cycle_end_ts": previous_cycle_end,
                    "offset": 14,
                    "ret_30m": 0.01,
                }
            ]
        ),
        scored=pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-27T15:22:00Z"),
                    "cycle_start_ts": previous_cycle_start,
                    "cycle_end_ts": previous_cycle_end,
                    "offset": 7,
                    "p_lgb": 0.81,
                    "p_lr": 0.77,
                    "p_signal": 0.79,
                    "w_lgb": 0.5,
                    "w_lr": 0.5,
                    "p_up": 0.79,
                    "p_down": 0.21,
                    "probability_mode": "raw_blend",
                    "score_valid": True,
                    "score_reason": "",
                }
            ]
        ),
    )

    row, coverage, inactive_reason = _resolve_latest_live_row(
        ctx=ctx,
        now_utc=now_utc,
        feature_coverage_fn=_feature_coverage_stub,
        latest_nan_feature_columns_fn=_latest_nan_feature_columns_stub,
        iso_or_none_fn=_iso_or_none,
    )

    assert row is None
    assert inactive_reason == "offset_not_yet_open"
    assert coverage["effective_missing_feature_count"] == 0


def test_build_offset_signal_keeps_window_bounds_for_not_yet_open_offsets() -> None:
    now_utc = pd.Timestamp("2026-03-27T15:31:16Z")
    previous_cycle_start = pd.Timestamp("2026-03-27T15:15:00Z")
    previous_cycle_end = pd.Timestamp("2026-03-27T15:30:00Z")

    ctx = OffsetScoreContext(
        offset=7,
        bundle_cfg={"signal_target": "direction"},
        feature_columns=["ret_30m"],
        effective_blacklist=[],
        not_allowed_blacklist=[],
        features=pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-27T15:29:00Z"),
                    "cycle_start_ts": previous_cycle_start,
                    "cycle_end_ts": previous_cycle_end,
                    "offset": 14,
                    "ret_30m": 0.01,
                }
            ]
        ),
        scored=pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-27T15:22:00Z"),
                    "cycle_start_ts": previous_cycle_start,
                    "cycle_end_ts": previous_cycle_end,
                    "offset": 7,
                    "p_lgb": 0.81,
                    "p_lr": 0.77,
                    "p_signal": 0.79,
                    "w_lgb": 0.5,
                    "w_lr": 0.5,
                    "p_up": 0.79,
                    "p_down": 0.21,
                    "probability_mode": "raw_blend",
                    "score_valid": True,
                    "score_reason": "",
                }
            ]
        ),
    )

    signal = build_offset_signal(
        selected_target="direction",
        ctx=ctx,
        feature_coverage_fn=_feature_coverage_stub,
        latest_nan_feature_columns_fn=_latest_nan_feature_columns_stub,
        extract_feature_snapshot_fn=lambda *args, **kwargs: {},
        iso_or_none_fn=_iso_or_none,
        now_utc=now_utc,
    )

    assert signal["status"] == "offset_not_yet_open"
    assert signal["window_start_ts"] == "2026-03-27T15:37:00+00:00"
    assert signal["window_end_ts"] == "2026-03-27T15:38:00+00:00"
    assert signal["cycle_start_ts"] == "2026-03-27T15:30:00+00:00"
    assert signal["cycle_end_ts"] == "2026-03-27T15:45:00+00:00"


def test_build_offset_signal_keeps_window_bounds_for_missing_score_rows() -> None:
    now_utc = pd.Timestamp("2026-03-27T15:37:16Z")
    current_cycle_start = pd.Timestamp("2026-03-27T15:30:00Z")
    current_cycle_end = pd.Timestamp("2026-03-27T15:45:00Z")

    ctx = OffsetScoreContext(
        offset=7,
        bundle_cfg={"signal_target": "direction"},
        feature_columns=["ret_30m"],
        effective_blacklist=[],
        not_allowed_blacklist=[],
        features=pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-27T15:36:00Z"),
                    "cycle_start_ts": current_cycle_start,
                    "cycle_end_ts": current_cycle_end,
                    "offset": 6,
                    "ret_30m": 0.01,
                }
            ]
        ),
        scored=pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-27T15:22:00Z"),
                    "cycle_start_ts": pd.Timestamp("2026-03-27T15:15:00Z"),
                    "cycle_end_ts": pd.Timestamp("2026-03-27T15:30:00Z"),
                    "offset": 7,
                    "p_lgb": 0.81,
                    "p_lr": 0.77,
                    "p_signal": 0.79,
                    "w_lgb": 0.5,
                    "w_lr": 0.5,
                    "p_up": 0.79,
                    "p_down": 0.21,
                    "probability_mode": "raw_blend",
                    "score_valid": True,
                    "score_reason": "",
                }
            ]
        ),
    )

    signal = build_offset_signal(
        selected_target="direction",
        ctx=ctx,
        feature_coverage_fn=_feature_coverage_stub,
        latest_nan_feature_columns_fn=_latest_nan_feature_columns_stub,
        extract_feature_snapshot_fn=lambda *args, **kwargs: {},
        iso_or_none_fn=_iso_or_none,
        now_utc=now_utc,
    )

    assert signal["status"] == "missing_score_row"
    assert signal["window_start_ts"] == "2026-03-27T15:37:00+00:00"
    assert signal["window_end_ts"] == "2026-03-27T15:38:00+00:00"
    assert signal["cycle_start_ts"] == "2026-03-27T15:30:00+00:00"
    assert signal["cycle_end_ts"] == "2026-03-27T15:45:00+00:00"


def test_resolve_latest_live_row_prefers_current_cycle_row_once_offset_opens() -> None:
    now_utc = pd.Timestamp("2026-03-27T09:22:30Z")
    current_cycle_start = pd.Timestamp("2026-03-27T09:15:00Z")
    current_cycle_end = pd.Timestamp("2026-03-27T09:30:00Z")
    previous_cycle_start = pd.Timestamp("2026-03-27T09:00:00Z")
    previous_cycle_end = pd.Timestamp("2026-03-27T09:15:00Z")
    current_decision_ts = pd.Timestamp("2026-03-27T09:22:00Z")
    latest_closed_feature_ts = pd.Timestamp("2026-03-27T09:21:00Z")

    ctx = OffsetScoreContext(
        offset=7,
        bundle_cfg={"signal_target": "direction"},
        feature_columns=["ret_30m"],
        effective_blacklist=[],
        not_allowed_blacklist=[],
        features=pd.DataFrame(
            [
                {
                    "decision_ts": latest_closed_feature_ts,
                    "cycle_start_ts": current_cycle_start,
                    "cycle_end_ts": current_cycle_end,
                    "offset": 7,
                    "ret_30m": 0.02,
                }
            ]
        ),
        scored=pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-27T09:07:00Z"),
                    "cycle_start_ts": previous_cycle_start,
                    "cycle_end_ts": previous_cycle_end,
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
                    "cycle_start_ts": current_cycle_start,
                    "cycle_end_ts": current_cycle_end,
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

    row, coverage, inactive_reason = _resolve_latest_live_row(
        ctx=ctx,
        now_utc=now_utc,
        feature_coverage_fn=_feature_coverage_stub,
        latest_nan_feature_columns_fn=_latest_nan_feature_columns_stub,
        iso_or_none_fn=_iso_or_none,
    )

    assert inactive_reason is None
    assert row is not None
    assert pd.Timestamp(row["decision_ts"]) == current_decision_ts
    assert coverage["effective_missing_feature_count"] == 0


def test_score_offset_signals_reuses_blacklisted_feature_frame_for_matching_offsets(tmp_path: Path) -> None:
    now_utc = pd.Timestamp("2026-03-27T09:23:30Z")
    current_cycle_start = pd.Timestamp("2026-03-27T09:15:00Z")
    current_cycle_end = pd.Timestamp("2026-03-27T09:30:00Z")
    base_features = pd.DataFrame(
        [
            {
                "decision_ts": pd.Timestamp("2026-03-27T09:22:00Z"),
                "cycle_start_ts": current_cycle_start,
                "cycle_end_ts": current_cycle_end,
                "offset": 7,
                "ret_30m": 0.11,
                "move_z": 0.21,
            },
            {
                "decision_ts": pd.Timestamp("2026-03-27T09:23:00Z"),
                "cycle_start_ts": current_cycle_start,
                "cycle_end_ts": current_cycle_end,
                "offset": 8,
                "ret_30m": 0.12,
                "move_z": 0.22,
            },
        ]
    )
    apply_calls: list[tuple[str, ...]] = []
    bundle_dir = tmp_path / "bundle_test_shared_blacklist"
    (bundle_dir / "offsets" / "offset=7").mkdir(parents=True, exist_ok=True)
    (bundle_dir / "offsets" / "offset=8").mkdir(parents=True, exist_ok=True)

    class _ProfileSpec:
        def blacklist_for(self, _asset_slug: str) -> list[str]:
            return ["move_z"]

    def _read_bundle_config(_bundle_dir, *, offset: int) -> dict[str, object]:
        return {
            "signal_target": "direction",
            "feature_columns": ["ret_30m", "move_z"],
            "allowed_blacklist_columns": ["move_z"],
            "offset": offset,
        }

    def _resolve_live_blacklist_fn(*, profile_blacklist, bundle_allowed_blacklist):
        effective = sorted(set(profile_blacklist) & set(bundle_allowed_blacklist))
        not_allowed = sorted(set(profile_blacklist) - set(bundle_allowed_blacklist))
        return effective, not_allowed

    def _apply_live_blacklist_fn(features: pd.DataFrame, *, blacklist_columns: list[str]) -> None:
        apply_calls.append(tuple(sorted(blacklist_columns)))
        for column in blacklist_columns:
            if column in features.columns:
                features.loc[:, column] = 0.0

    def _score_bundle_offset_fn(_bundle_dir, features: pd.DataFrame, *, offset: int) -> pd.DataFrame:
        rows = features[pd.to_numeric(features.get("offset"), errors="coerce") == int(offset)].copy()
        if rows.empty:
            return pd.DataFrame()
        return pd.DataFrame(
            [
                {
                    "decision_ts": rows.iloc[-1]["decision_ts"],
                    "cycle_start_ts": rows.iloc[-1]["cycle_start_ts"],
                    "cycle_end_ts": rows.iloc[-1]["cycle_end_ts"],
                    "offset": int(offset),
                    "p_lgb": 0.6,
                    "p_lr": 0.6,
                    "p_signal": 0.6,
                    "w_lgb": 0.5,
                    "w_lr": 0.5,
                    "p_up": 0.6,
                    "p_down": 0.4,
                    "probability_mode": "raw_blend",
                    "score_valid": True,
                    "score_reason": "",
                }
            ]
        )

    signals, timings = score_offset_signals(
        type("Cfg", (), {"asset": type("Asset", (), {"slug": "sol"})()})(),
        selected_target="direction",
        profile_spec=_ProfileSpec(),
        bundle_dir=bundle_dir,
        base_features=base_features,
        read_bundle_config_fn=_read_bundle_config,
        resolve_live_blacklist_fn=_resolve_live_blacklist_fn,
        apply_live_blacklist_fn=_apply_live_blacklist_fn,
        score_bundle_offset_fn=_score_bundle_offset_fn,
        feature_coverage_fn=_feature_coverage_stub,
        latest_nan_feature_columns_fn=_latest_nan_feature_columns_stub,
        extract_feature_snapshot_fn=lambda *args, **kwargs: {},
        iso_or_none_fn=_iso_or_none,
        now_utc=now_utc,
    )

    assert len(signals) == 2
    assert {signal["offset"] for signal in signals} == {7, 8}
    assert apply_calls == [("move_z",)]
    assert sorted((timings.get("offset_scoring_offsets_ms") or {}).keys()) == ["7", "8"]
