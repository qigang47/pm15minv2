from __future__ import annotations

import pandas as pd

from pm15min.research.features.builders import build_feature_frame


def _raw_klines(rows: int = 120) -> pd.DataFrame:
    start = pd.Timestamp("2026-03-20T00:00:00Z")
    payload: list[dict[str, object]] = []
    for idx in range(rows):
        open_time = start + pd.Timedelta(minutes=idx)
        close = 100.0 + float(idx) * 0.1
        payload.append(
            {
                "open_time": open_time,
                "open": close - 0.05,
                "high": close + 0.10,
                "low": close - 0.10,
                "close": close,
                "volume": 1000.0 + idx,
                "quote_asset_volume": 2000.0 + idx * 5.0,
                "taker_buy_quote_volume": 900.0 + idx * 3.0,
                "number_of_trades": 100 + idx,
            }
        )
    return pd.DataFrame(payload)


def _oracle_prices(cycles: int = 8) -> pd.DataFrame:
    start = pd.Timestamp("2026-03-20T00:00:00Z")
    payload: list[dict[str, object]] = []
    for idx in range(cycles):
        cycle_start = start + pd.Timedelta(minutes=idx * 15)
        payload.append(
            {
                "cycle_start_ts": int(cycle_start.timestamp()),
                "cycle_end_ts": int((cycle_start + pd.Timedelta(minutes=15)).timestamp()),
                "price_to_beat": 100.0 + idx * 0.5,
                "final_price": 100.5 + idx * 0.5,
            }
        )
    return pd.DataFrame(payload)


def _oscillating_klines() -> pd.DataFrame:
    start = pd.Timestamp("2026-03-20T00:00:00Z")
    closes = [100.0, 100.4, 99.6, 100.5, 99.5, 100.6, 99.4, 100.7, 99.3, 100.8, 99.2, 100.9]
    payload: list[dict[str, object]] = []
    for idx, close in enumerate(closes):
        open_time = start + pd.Timedelta(minutes=idx)
        payload.append(
            {
                "open_time": open_time,
                "open": close,
                "high": close + 0.05,
                "low": close - 0.05,
                "close": close,
                "volume": 1000.0 + idx,
                "quote_asset_volume": 2000.0 + idx * 10.0,
                "taker_buy_quote_volume": 900.0 + idx * 5.0,
                "number_of_trades": 100 + idx,
            }
        )
    return pd.DataFrame(payload)


def _late_up_cross_klines(rows: int = 60) -> pd.DataFrame:
    start = pd.Timestamp("2026-03-20T00:00:00Z")
    payload: list[dict[str, object]] = []
    for idx in range(rows):
        open_time = start + pd.Timedelta(minutes=idx)
        close = 99.0 + (0.02 * idx)
        if 30 <= idx <= 39:
            close = 99.20 - (0.02 * (idx - 30))
        elif 40 <= idx <= 44:
            close = 100.05 + (0.05 * (idx - 40))
        payload.append(
            {
                "open_time": open_time,
                "open": close - 0.03,
                "high": close + 0.08,
                "low": close - 0.08,
                "close": close,
                "volume": 1000.0 + idx,
                "quote_asset_volume": 2000.0 + idx * 5.0,
                "taker_buy_quote_volume": 900.0 + idx * 3.0,
                "number_of_trades": 100 + idx,
            }
        )
    return pd.DataFrame(payload)


def test_build_feature_frame_preserves_legacy_auxiliary_columns_for_selected_groups() -> None:
    raw_klines = _raw_klines()
    oracle_prices = _oracle_prices()
    btc_klines = _raw_klines()

    deep_otm_features = build_feature_frame(
        raw_klines,
        feature_set="deep_otm_v1",
        oracle_prices=oracle_prices,
        btc_klines=btc_klines,
        cycle="15m",
    )
    alpha_live_features = build_feature_frame(
        raw_klines,
        feature_set="alpha_search_direction_live",
        oracle_prices=oracle_prices,
        btc_klines=btc_klines,
        cycle="15m",
    )
    user_core_features = build_feature_frame(
        raw_klines,
        feature_set="v6_user_core",
        oracle_prices=oracle_prices,
        btc_klines=btc_klines,
        cycle="15m",
    )

    assert "first_half_ret" in deep_otm_features.columns
    assert "dow_sin" in alpha_live_features.columns
    assert "dow_cos" in alpha_live_features.columns
    assert "ret_60m" in user_core_features.columns
    assert "q_bs_up_strike" in user_core_features.columns


def test_build_feature_frame_emits_new_low_cost_feature_columns() -> None:
    features = build_feature_frame(
        _raw_klines(),
        feature_set="deep_otm_v1",
        oracle_prices=_oracle_prices(),
        btc_klines=_raw_klines(),
        cycle="15m",
        requested_columns={
            "rv_30_change",
            "taker_buy_ratio_change",
            "cycle_range_vs_rv",
            "strike_abs_z",
            "strike_flip_count_cycle",
        },
    )

    for column in (
        "rv_30_change",
        "taker_buy_ratio_change",
        "cycle_range_vs_rv",
        "strike_abs_z",
        "strike_flip_count_cycle",
    ):
        assert column in features.columns
        assert features[column].notna().any()


def test_build_feature_frame_uses_5m_first_half_anchor() -> None:
    features = build_feature_frame(
        _raw_klines(rows=15),
        feature_set="deep_otm_v1",
        oracle_prices=pd.DataFrame(
            [
                {
                    "cycle_start_ts": int(pd.Timestamp("2026-03-20T00:00:00Z").timestamp()),
                    "cycle_end_ts": int(pd.Timestamp("2026-03-20T00:05:00Z").timestamp()),
                    "price_to_beat": 100.0,
                    "final_price": 101.0,
                }
            ]
        ),
        cycle="5m",
        requested_columns={"ret_from_cycle_open", "first_half_ret", "second_half_ret_proxy"},
    )

    anchor_row = features.loc[features["offset"].eq(2)].iloc[0]
    late_row = features.loc[features["offset"].eq(4)].iloc[0]

    assert anchor_row["first_half_ret"] == anchor_row["ret_from_cycle_open"]
    assert late_row["second_half_ret_proxy"] > 0.0


def test_build_feature_frame_counts_strike_side_flips_within_cycle() -> None:
    features = build_feature_frame(
        _oscillating_klines(),
        feature_set="deep_otm_v1",
        oracle_prices=_oracle_prices(cycles=1),
        cycle="15m",
        requested_columns={"strike_flip_count_cycle"},
    )

    assert int(features.iloc[-1]["strike_flip_count_cycle"]) == 10


def test_build_feature_frame_emits_time_pressure_and_hold_features_without_future_leakage() -> None:
    oracle_prices = pd.DataFrame(
        [
            {
                "cycle_start_ts": int((pd.Timestamp("2026-03-20T00:00:00Z") + pd.Timedelta(minutes=idx * 15)).timestamp()),
                "cycle_end_ts": int((pd.Timestamp("2026-03-20T00:15:00Z") + pd.Timedelta(minutes=idx * 15)).timestamp()),
                "price_to_beat": 100.0,
                "final_price": 100.5,
            }
            for idx in range(4)
        ]
    )
    features = build_feature_frame(
        _late_up_cross_klines(),
        feature_set="deep_otm_v1",
        oracle_prices=oracle_prices,
        cycle="15m",
        requested_columns={
            "minutes_left_to_settle",
            "up_move_remaining_per_minute",
            "up_move_remaining_z_per_minute",
            "first_up_cross_offset",
            "minutes_since_first_up_cross",
            "up_hold_minutes",
        },
    )

    cycle_rows = features.loc[features["cycle_start_ts"].eq(pd.Timestamp("2026-03-20T00:30:00Z"))].reset_index(drop=True)
    before_cross = cycle_rows.loc[cycle_rows["offset"].eq(10)].iloc[0]
    cross_row = cycle_rows.loc[cycle_rows["offset"].eq(11)].iloc[0]
    late_row = cycle_rows.loc[cycle_rows["offset"].eq(14)].iloc[0]

    assert float(before_cross["minutes_left_to_settle"]) == 5.0
    assert pd.isna(before_cross["first_up_cross_offset"])
    assert pd.isna(before_cross["minutes_since_first_up_cross"])
    assert float(before_cross["up_move_remaining_per_minute"]) > 0.0
    assert float(before_cross["up_move_remaining_z_per_minute"]) > 0.0

    assert float(cross_row["minutes_left_to_settle"]) == 4.0
    assert float(cross_row["first_up_cross_offset"]) == 11.0
    assert float(cross_row["minutes_since_first_up_cross"]) == 0.0
    assert float(cross_row["up_hold_minutes"]) == 1.0
    assert float(cross_row["up_move_remaining_per_minute"]) == 0.0

    assert float(late_row["minutes_left_to_settle"]) == 1.0
    assert float(late_row["first_up_cross_offset"]) == 11.0
    assert float(late_row["minutes_since_first_up_cross"]) == 3.0
    assert float(late_row["up_hold_minutes"]) == 4.0
    assert float(late_row["up_move_remaining_z_per_minute"]) == 0.0
