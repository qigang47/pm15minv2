from __future__ import annotations

import pandas as pd

from pm15min.live.signal.scoring_offsets import OffsetScoreContext, _resolve_latest_live_row, build_offset_signal


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


def test_resolve_latest_live_row_prefers_current_cycle_row_once_offset_opens() -> None:
    now_utc = pd.Timestamp("2026-03-27T09:22:30Z")
    current_cycle_start = pd.Timestamp("2026-03-27T09:15:00Z")
    current_cycle_end = pd.Timestamp("2026-03-27T09:30:00Z")
    previous_cycle_start = pd.Timestamp("2026-03-27T09:00:00Z")
    previous_cycle_end = pd.Timestamp("2026-03-27T09:15:00Z")
    current_decision_ts = pd.Timestamp("2026-03-27T09:22:00Z")

    ctx = OffsetScoreContext(
        offset=7,
        bundle_cfg={"signal_target": "direction"},
        feature_columns=["ret_30m"],
        effective_blacklist=[],
        not_allowed_blacklist=[],
        features=pd.DataFrame(
            [
                {
                    "decision_ts": pd.Timestamp("2026-03-27T09:22:00Z"),
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
