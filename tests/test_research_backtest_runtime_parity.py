from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.backtests.retry_contract import (
    attach_pre_submit_orderbook_retry_contract,
    build_backtest_retry_contract,
)
from pm15min.research.backtests.regime_parity import resolve_backtest_profile_spec
from pm15min.research.labels.runtime import build_label_runtime_summary, build_truth_runtime_summary


def test_build_backtest_retry_contract_reflects_live_profile_retry_fields() -> None:
    profile_spec = resolve_backtest_profile_spec(profile="deep_otm")

    summary = build_backtest_retry_contract(profile_spec).to_dict()

    assert summary["pre_submit_depth_retry_interval_sec"] == profile_spec.orderbook_fast_retry_interval_seconds
    assert summary["pre_submit_depth_retry_max"] == profile_spec.orderbook_fast_retry_max
    assert summary["pre_submit_depth_retry_state_key"] == "orderbook_retry_count"
    assert summary["post_submit_order_retry_max"] == profile_spec.max_order_retries
    assert summary["post_submit_order_retry_state_keys"] == [
        "attempts",
        "last_attempt",
        "last_error",
        "fast_retry",
        "retry_interval_seconds",
    ]
    assert summary["post_submit_fak_immediate_retry_max"] == profile_spec.fak_immediate_retry_max
    assert summary["post_submit_fak_response_driven"] is True
    assert "depth_fill_unavailable" in summary["pre_submit_depth_retry_reasons"]
    assert "no orders found to match" in summary["post_submit_fak_retry_message_hints"]


def test_attach_pre_submit_orderbook_retry_contract_arms_only_orderbook_limit_reject() -> None:
    profile_spec = resolve_backtest_profile_spec(profile="deep_otm")
    rows = pd.DataFrame(
        [
            {"decision_engine_reason": "orderbook_limit_reject"},
            {"decision_engine_reason": "orderbook_missing"},
            {"decision_engine_reason": "direction_prob"},
        ]
    )

    out = attach_pre_submit_orderbook_retry_contract(rows, spec=profile_spec)

    assert out["pre_submit_orderbook_retry_armed"].tolist() == [True, False, False]
    assert out["pre_submit_orderbook_retry_reason"].tolist() == ["orderbook_limit_reject", "", ""]
    assert float(out.loc[0, "pre_submit_orderbook_retry_interval_sec"]) == profile_spec.orderbook_fast_retry_interval_seconds
    assert int(out.loc[0, "pre_submit_orderbook_retry_max"]) == profile_spec.orderbook_fast_retry_max
    assert out.loc[0, "pre_submit_orderbook_retry_state_key"] == "orderbook_retry_count"


def test_build_label_runtime_summary_tracks_truth_and_oracle_metadata(tmp_path: Path) -> None:
    truth_table = pd.DataFrame(
        [
            {"truth_source": "settlement_truth"},
            {"truth_source": "chainlink_streams"},
        ]
    )
    oracle_table = pd.DataFrame(
        [
            {
                "source_price_to_beat": "direct_api",
                "source_final_price": "streams_rpc",
                "has_both": True,
            },
            {
                "source_price_to_beat": "direct_api",
                "source_final_price": "datafeeds_rpc",
                "has_both": False,
            },
        ]
    )
    truth_path = tmp_path / "truth.parquet"
    oracle_path = tmp_path / "oracle.parquet"
    write_parquet_atomic(truth_table, truth_path)
    write_parquet_atomic(oracle_table, oracle_path)

    summary = build_label_runtime_summary(
        truth_table=truth_table,
        oracle_prices_table=oracle_table,
        truth_path=truth_path,
        oracle_path=oracle_path,
    )

    assert summary["status"] == "ok"
    assert summary["truth_table_rows"] == 2
    assert summary["truth_source_counts"] == {"settlement_truth": 1, "streams": 1}
    assert summary["oracle_table_rows"] == 2
    assert summary["oracle_has_both_rows"] == 1
    assert summary["oracle_source_counts"] == {"datafeeds": 1, "streams": 1}
    assert summary["truth_table_updated_at"]
    assert summary["oracle_table_updated_at"]


def test_build_truth_runtime_summary_reads_foundation_and_dataset_statuses(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(
        pd.DataFrame([{"asset": "sol", "cycle_start_ts": 1, "cycle_end_ts": 2, "source": "direct_api"}]),
        cfg.layout.direct_oracle_source_path,
    )
    write_parquet_atomic(
        pd.DataFrame([{"market_id": "m-1", "cycle_start_ts": 1, "cycle_end_ts": 2, "winner_side": "UP"}]),
        cfg.layout.settlement_truth_source_path,
    )
    write_parquet_atomic(
        pd.DataFrame([{"asset": "sol", "cycle_start_ts": 1, "cycle_end_ts": 2, "price_to_beat": 1.0, "final_price": 2.0}]),
        cfg.layout.oracle_prices_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame([{"asset": "sol", "cycle_start_ts": 1, "cycle_end_ts": 2, "winner_side": "UP"}]),
        cfg.layout.truth_table_path,
    )

    streams_path = cfg.layout.streams_source_root / "year=2026" / "month=03" / "data.parquet"
    streams_path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(
        pd.DataFrame([{"tx_hash": "0x1", "perform_idx": 1, "value_idx": 1, "observation_ts": 1, "extra_ts": 1}]),
        streams_path,
    )
    datafeeds_path = cfg.layout.datafeeds_source_root / "year=2026" / "month=03" / "data.parquet"
    datafeeds_path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(
        pd.DataFrame([{"tx_hash": "0x2", "log_index": 1, "updated_at": 1}]),
        datafeeds_path,
    )
    cfg.layout.foundation_state_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.layout.foundation_state_path.write_text(
        json.dumps(
            {
                "status": "degraded",
                "reason": "oracle_direct_rate_limited",
                "issue_codes": ["oracle_direct_rate_limited"],
                "run_started_at": "2026-03-23T10:00:00Z",
                "last_completed_at": "2026-03-23T10:05:00Z",
                "finished_at": "2026-03-23T10:05:01Z",
                "completed_iterations": 3,
            }
        ),
        encoding="utf-8",
    )

    summary = build_truth_runtime_summary(cfg)

    assert summary["truth_runtime_foundation_status"] == "degraded"
    assert summary["truth_runtime_foundation_reason"] == "oracle_direct_rate_limited"
    assert summary["truth_runtime_foundation_issue_codes"] == ["oracle_direct_rate_limited"]
    assert summary["truth_runtime_foundation_run_started_at"] == "2026-03-23T10:00:00+00:00"
    assert summary["truth_runtime_foundation_last_completed_at"] == "2026-03-23T10:05:00+00:00"
    assert summary["truth_runtime_foundation_finished_at"] == "2026-03-23T10:05:01+00:00"
    assert summary["truth_runtime_foundation_completed_iterations"] == 3
    assert summary["truth_runtime_direct_oracle_fail_open"] is True
    assert summary["truth_runtime_recent_refresh_status"] == "fail_open"
    assert (
        summary["truth_runtime_recent_refresh_interpretation"]
        == "recent_refresh_degraded_but_existing_oracle_table_is_still_serving_reads"
    )
    assert summary["truth_runtime_truth_table_status"] == "ok"
    assert summary["truth_runtime_truth_table_freshness_max"] == "1970-01-01T00:00:01+00:00"
    assert summary["truth_runtime_truth_table_freshness_state"] == "fresh"
    assert summary["truth_runtime_truth_table_recent_refresh_status"] == "fresh"
    assert summary["truth_runtime_oracle_prices_table_status"] == "ok"
    assert summary["truth_runtime_oracle_prices_table_freshness_max"] == "1970-01-01T00:00:01+00:00"
    assert summary["truth_runtime_oracle_prices_table_freshness_state"] == "fresh"
    assert summary["truth_runtime_oracle_prices_table_recent_refresh_status"] == "fresh"
    assert summary["truth_runtime_direct_oracle_source_status"] == "ok"
    assert summary["truth_runtime_direct_oracle_source_freshness_max"] == "1970-01-01T00:00:02+00:00"
    assert summary["truth_runtime_direct_oracle_source_freshness_state"] == "fresh"
    assert summary["truth_runtime_direct_oracle_source_recent_refresh_status"] == "fresh"
    assert summary["truth_runtime_settlement_truth_source_status"] == "ok"
    assert summary["truth_runtime_settlement_truth_source_freshness_max"] == "1970-01-01T00:00:02+00:00"
    assert summary["truth_runtime_settlement_truth_source_freshness_state"] == "fresh"
    assert summary["truth_runtime_settlement_truth_source_recent_refresh_status"] == "fresh"
    assert summary["truth_runtime_streams_source_status"] == "ok"
    assert summary["truth_runtime_streams_source_freshness_max"] == "1970-01-01T00:00:01+00:00"
    assert summary["truth_runtime_streams_source_freshness_state"] == "fresh"
    assert summary["truth_runtime_streams_source_recent_refresh_status"] == "fresh"
    assert summary["truth_runtime_datafeeds_source_status"] == "ok"
    assert summary["truth_runtime_datafeeds_source_freshness_max"] == "1970-01-01T00:00:01+00:00"
    assert summary["truth_runtime_datafeeds_source_freshness_state"] == "fresh"
    assert summary["truth_runtime_datafeeds_source_recent_refresh_status"] == "fresh"


def test_build_truth_runtime_summary_marks_stale_dataset_refresh_from_live_audits(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    now = pd.Timestamp.now(tz="UTC").floor("s")
    fresh_ts = int((now - pd.Timedelta(minutes=15)).timestamp())
    stale_ts = int((now - pd.Timedelta(days=2)).timestamp())

    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "cycle_start_ts": stale_ts - 900,
                    "cycle_end_ts": stale_ts,
                    "fetched_at": stale_ts,
                    "source": "direct_api",
                }
            ]
        ),
        cfg.layout.direct_oracle_source_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "m-1",
                    "cycle_start_ts": fresh_ts - 900,
                    "cycle_end_ts": fresh_ts,
                    "winner_side": "UP",
                }
            ]
        ),
        cfg.layout.settlement_truth_source_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "cycle_start_ts": fresh_ts - 900,
                    "cycle_end_ts": fresh_ts,
                    "price_to_beat": 1.0,
                    "final_price": 2.0,
                }
            ]
        ),
        cfg.layout.oracle_prices_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "cycle_start_ts": fresh_ts - 900,
                    "cycle_end_ts": fresh_ts,
                    "winner_side": "UP",
                }
            ]
        ),
        cfg.layout.truth_table_path,
    )

    streams_path = cfg.layout.streams_source_root / "year=2026" / "month=03" / "data.parquet"
    streams_path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "tx_hash": "0x1",
                    "perform_idx": 1,
                    "value_idx": 1,
                    "observation_ts": fresh_ts,
                    "extra_ts": fresh_ts,
                }
            ]
        ),
        streams_path,
    )
    datafeeds_path = cfg.layout.datafeeds_source_root / "year=2026" / "month=03" / "data.parquet"
    datafeeds_path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(
        pd.DataFrame([{"tx_hash": "0x2", "log_index": 1, "updated_at": fresh_ts}]),
        datafeeds_path,
    )
    cfg.layout.foundation_state_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.layout.foundation_state_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "run_started_at": "2026-03-23T10:00:00Z",
                "last_completed_at": "2026-03-23T10:15:00Z",
                "finished_at": "2026-03-23T10:15:00Z",
                "completed_iterations": 1,
            }
        ),
        encoding="utf-8",
    )

    summary = build_truth_runtime_summary(cfg)

    assert summary["truth_runtime_foundation_status"] == "ok"
    assert summary["truth_runtime_direct_oracle_fail_open"] is False
    assert summary["truth_runtime_direct_oracle_source_freshness_max"] == pd.Timestamp(
        stale_ts, unit="s", tz="UTC"
    ).isoformat()
    assert summary["truth_runtime_direct_oracle_source_freshness_age_seconds"] is not None
    assert summary["truth_runtime_direct_oracle_source_freshness_age_seconds"] > 12 * 3600
    assert summary["truth_runtime_direct_oracle_source_freshness_state"] == "stale"
    assert summary["truth_runtime_direct_oracle_source_recent_refresh_status"] == "stale"
    assert summary["truth_runtime_truth_table_freshness_state"] == "fresh"
    assert summary["truth_runtime_oracle_prices_table_freshness_state"] == "fresh"
    assert summary["truth_runtime_streams_source_freshness_state"] == "fresh"
    assert summary["truth_runtime_recent_refresh_status"] == "degraded"
    assert (
        summary["truth_runtime_recent_refresh_interpretation"]
        == "recent_refresh_completed_with_dataset_gaps:direct_oracle_source:stale"
    )
