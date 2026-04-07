from __future__ import annotations

from pm15min.research.features.registry import feature_group
from pm15min.research.automation.focus_feature_search import (
    build_market_focus_feature_sets,
    rank_focus_features,
    rank_focus_features_by_offset,
)


def test_rank_focus_features_aggregates_multiple_explainability_views() -> None:
    ranked = rank_focus_features(
        [
            {
                "explainability": {
                    "top_lgb_importance": [
                        {"feature": "q_bs_up_strike", "rank": 1},
                        {"feature": "first_half_ret", "rank": 2},
                    ],
                    "top_logreg_coefficients": [
                        {"feature": "q_bs_up_strike", "rank": 1},
                        {"feature": "bb_pos_20", "rank": 2},
                    ],
                    "top_positive_factors": [
                        {"feature": "q_bs_up_strike", "rank": 1},
                        {"feature": "cycle_range_pos", "rank": 2},
                    ],
                    "top_negative_factors": [
                        {"feature": "volume_z_3", "rank": 1},
                    ],
                }
            },
            {
                "explainability": {
                    "top_lgb_importance": [
                        {"feature": "q_bs_up_strike", "rank": 1},
                        {"feature": "ret_from_cycle_open", "rank": 2},
                    ],
                    "top_logreg_coefficients": [
                        {"feature": "first_half_ret", "rank": 1},
                    ],
                    "top_positive_factors": [
                        {"feature": "bb_pos_20", "rank": 1},
                    ],
                    "top_negative_factors": [
                        {"feature": "volume_z", "rank": 1},
                    ],
                }
            },
        ]
    )

    assert ranked[:4] == [
        "q_bs_up_strike",
        "first_half_ret",
        "bb_pos_20",
        "ret_from_cycle_open",
    ]
    assert "volume_z_3" in ranked[:6]
    assert "cycle_range_pos" in ranked[:7]


def test_build_market_focus_feature_sets_uses_global_core_then_market_tilt() -> None:
    feature_sets = build_market_focus_feature_sets(
        market="btc",
        global_ranked_features=[
            "q_bs_up_strike",
            "first_half_ret",
            "bb_pos_20",
            "ret_from_cycle_open",
            "cycle_range_pos",
            "volume_z_3",
            "volume_z",
            "adx_14",
        ],
        market_ranked_features=[
            "q_bs_up_strike",
            "first_half_ret",
            "bb_pos_20",
            "ret_from_cycle_open",
            "cycle_range_pos",
            "volume_z_3",
            "volume_z",
            "adx_14",
            "gk_vol_30",
            "rv_30",
            "ret_from_strike",
            "macd_hist",
            "delta_rsi",
            "ma_15_slope",
            "rr_30",
            "rs_vol_30",
            "ma_gap_15",
            "ret_5m_lag1",
            "ret_15m_lag1",
            "rsi_divergence",
            "trade_intensity",
            "vwap_gap_20",
            "donch_pos_20",
            "bias_60",
            "vol_ratio_5_60",
            "z_ret_30m",
            "z_ret_60m",
            "atr_14",
            "macd_z",
            "regime_trend",
            "taker_buy_ratio_z",
            "obv_z",
        ],
        widths=(12, 18, 24, 32),
        fill_candidates=[
            "ret_3m",
            "ret_5m",
            "ret_30m",
            "ret_60m",
            "rr_30",
            "vwap_gap_20",
            "vwap_gap_60",
            "bias_60",
        ],
    )

    assert list(feature_sets) == [
        "focus_btc_12_v1",
        "focus_btc_18_v1",
        "focus_btc_24_v1",
        "focus_btc_32_v1",
    ]
    assert "q_bs_up_strike" in feature_sets["focus_btc_12_v1"]["columns"]
    assert "ret_from_cycle_open" in feature_sets["focus_btc_12_v1"]["columns"]
    assert "volume_z_3" in feature_sets["focus_btc_12_v1"]["columns"]
    assert "rv_30" in feature_sets["focus_btc_18_v1"]["columns"]
    assert len(feature_sets["focus_btc_12_v1"]["columns"]) == 12
    assert len(feature_sets["focus_btc_18_v1"]["columns"]) == 18
    assert len(feature_sets["focus_btc_24_v1"]["columns"]) == 24
    assert len(feature_sets["focus_btc_32_v1"]["columns"]) == 32


def test_build_market_focus_feature_sets_can_fill_wider_widths_from_baseline_candidates() -> None:
    feature_sets = build_market_focus_feature_sets(
        market="btc",
        global_ranked_features=[
            "q_bs_up_strike",
            "first_half_ret",
            "bb_pos_20",
            "ret_from_cycle_open",
        ],
        market_ranked_features=[
            "q_bs_up_strike",
            "first_half_ret",
            "bb_pos_20",
            "ret_from_cycle_open",
            "cycle_range_pos",
            "volume_z_3",
        ],
        widths=(8,),
        fill_candidates=[
            "ret_3m",
            "ret_5m",
            "ret_30m",
            "ret_60m",
            "rr_30",
            "vwap_gap_20",
        ],
        version="v2",
    )

    assert list(feature_sets) == ["focus_btc_8_v2"]
    assert len(feature_sets["focus_btc_8_v2"]["columns"]) == 8
    assert "ret_3m" in feature_sets["focus_btc_8_v2"]["columns"]
    assert any(
        feature in feature_sets["focus_btc_8_v2"]["columns"]
        for feature in ("ret_5m", "ret_30m", "ret_60m", "rr_30", "vwap_gap_20")
    )


def test_rank_focus_features_by_offset_keeps_offset_specific_rankings() -> None:
    ranked = rank_focus_features_by_offset(
        [
            {
                "offset": 7,
                "explainability": {
                    "top_lgb_importance": [
                        {"feature": "q_bs_up_strike", "rank": 1},
                        {"feature": "first_half_ret", "rank": 2},
                    ]
                },
            },
            {
                "offset": 8,
                "explainability": {
                    "top_lgb_importance": [
                        {"feature": "volume_z_3", "rank": 1},
                        {"feature": "ret_from_cycle_open", "rank": 2},
                    ]
                },
            },
        ]
    )

    assert ranked[7][:2] == ["q_bs_up_strike", "first_half_ret"]
    assert ranked[8][:2] == ["volume_z_3", "ret_from_cycle_open"]


def test_build_market_focus_feature_sets_balances_feature_families_within_range() -> None:
    feature_sets = build_market_focus_feature_sets(
        market="btc",
        global_ranked_features=[
            "q_bs_up_strike",
            "ret_from_strike",
            "basis_bp",
            "has_cl_strike",
            "strike_abs_z",
            "q_bs_up_strike_centered",
            "first_half_ret",
            "ret_from_cycle_open",
            "volume_z_3",
            "volume_z",
            "rv_30",
            "gk_vol_30",
            "hour_sin",
            "btc_ret_5m",
        ],
        market_ranked_features=[
            "q_bs_up_strike",
            "ret_from_strike",
            "basis_bp",
            "has_cl_strike",
            "strike_abs_z",
            "q_bs_up_strike_centered",
            "first_half_ret",
                "ret_from_cycle_open",
                "cycle_range_pos",
                "pullback_from_cycle_high",
                "rebound_from_cycle_low",
                "second_half_ret_proxy",
                "volume_z_3",
                "volume_z",
                "trade_intensity",
                "obv_z",
                "taker_buy_ratio_z",
            "donch_pos_20",
            "rv_30",
            "gk_vol_30",
            "ma_gap_15",
            "atr_14",
            "hour_sin",
            "hour_cos",
            "btc_ret_5m",
            "rel_strength_15m",
        ],
        market_offset_ranked_features={
            7: ["q_bs_up_strike", "basis_bp", "first_half_ret", "volume_z_3", "rv_30", "hour_sin"],
            8: ["ret_from_strike", "has_cl_strike", "ret_from_cycle_open", "volume_z", "gk_vol_30", "btc_ret_5m"],
            9: ["strike_abs_z", "cycle_range_pos", "pullback_from_cycle_high", "trade_intensity", "ma_gap_15", "hour_cos"],
        },
        global_offset_ranked_features={
            7: ["q_bs_up_strike", "basis_bp", "first_half_ret", "volume_z_3", "rv_30", "hour_sin"],
            8: ["ret_from_strike", "has_cl_strike", "ret_from_cycle_open", "volume_z", "gk_vol_30", "btc_ret_5m"],
            9: ["strike_abs_z", "cycle_range_pos", "pullback_from_cycle_high", "trade_intensity", "ma_gap_15", "hour_cos"],
        },
        widths=(30,),
        fill_candidates=[
            "bb_pos_20",
            "ret_3m",
            "ret_5m",
            "ret_15m",
            "ret_30m",
            "ret_60m",
            "rsi_14",
            "delta_rsi",
            "obv_z",
            "macd_z",
            "dow_sin",
            "dow_cos",
        ],
        version="v4",
    )

    columns = feature_sets["focus_btc_30_v4"]["columns"]
    groups = [feature_group(name) for name in columns]

    assert len(columns) == 30
    assert groups.count("strike") >= 6
    assert groups.count("cycle") >= 6
    assert groups.count("volume") >= 6
    assert groups.count("price") >= 8
    assert groups.count("calendar") + groups.count("cross_asset") <= 4
