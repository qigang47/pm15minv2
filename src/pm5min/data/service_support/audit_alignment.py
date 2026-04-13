from __future__ import annotations

from typing import Any

from ..config import DataConfig
from .shared import latest_partition_date, latest_semantic_timestamp


def build_alignment_checks(
    *,
    cfg: DataConfig,
    datasets: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    checks = [
        evaluate_time_lag_check(
            name="oracle_prices_table_vs_direct_oracle_source",
            left_dataset="oracle_prices_table",
            right_dataset="direct_oracle_source",
            left=datasets.get("oracle_prices_table") or {},
            right=datasets.get("direct_oracle_source") or {},
            max_lag_seconds=cfg.layout.cycle_seconds,
            severity="error" if cfg.surface == "live" else "warning",
        ),
        evaluate_time_lag_check(
            name="truth_table_vs_settlement_truth_source",
            left_dataset="truth_table",
            right_dataset="settlement_truth_source",
            left=datasets.get("truth_table") or {},
            right=datasets.get("settlement_truth_source") or {},
            max_lag_seconds=cfg.layout.cycle_seconds,
            severity="warning" if cfg.surface == "live" else "error",
        ),
    ]
    if cfg.surface == "live":
        checks.append(
            evaluate_date_lag_check(
                name="orderbook_index_table_vs_orderbook_depth_source",
                left_dataset="orderbook_index_table",
                right_dataset="orderbook_depth_source",
                left=datasets.get("orderbook_index_table") or {},
                right=datasets.get("orderbook_depth_source") or {},
                max_lag_days=0,
                severity="warning",
            )
        )
    return checks


def evaluate_time_lag_check(
    *,
    name: str,
    left_dataset: str,
    right_dataset: str,
    left: dict[str, Any],
    right: dict[str, Any],
    max_lag_seconds: int,
    severity: str,
) -> dict[str, Any]:
    if not bool(left.get("exists")) or not bool(right.get("exists")):
        return {
            "name": name,
            "kind": "time_lag",
            "status": "skipped",
            "severity": severity,
            "left_dataset": left_dataset,
            "right_dataset": right_dataset,
        }
    left_ts = latest_semantic_timestamp(left)
    right_ts = latest_semantic_timestamp(right)
    if left_ts is None or right_ts is None:
        return {
            "name": name,
            "kind": "time_lag",
            "status": "skipped",
            "severity": severity,
            "left_dataset": left_dataset,
            "right_dataset": right_dataset,
        }
    actual_lag_seconds = max(0.0, (right_ts - left_ts).total_seconds())
    status = severity if actual_lag_seconds > float(max_lag_seconds) else "ok"
    return {
        "name": name,
        "kind": "time_lag",
        "status": status,
        "severity": severity,
        "left_dataset": left_dataset,
        "right_dataset": right_dataset,
        "left_latest_timestamp": left_ts.isoformat(),
        "right_latest_timestamp": right_ts.isoformat(),
        "max_lag_seconds": int(max_lag_seconds),
        "actual_lag_seconds": float(actual_lag_seconds),
    }


def evaluate_date_lag_check(
    *,
    name: str,
    left_dataset: str,
    right_dataset: str,
    left: dict[str, Any],
    right: dict[str, Any],
    max_lag_days: int,
    severity: str,
) -> dict[str, Any]:
    if not bool(left.get("exists")) or not bool(right.get("exists")):
        return {
            "name": name,
            "kind": "date_lag",
            "status": "skipped",
            "severity": severity,
            "left_dataset": left_dataset,
            "right_dataset": right_dataset,
        }
    left_date = latest_semantic_timestamp(left)
    right_date = latest_partition_date(right)
    if left_date is None or right_date is None:
        return {
            "name": name,
            "kind": "date_lag",
            "status": "skipped",
            "severity": severity,
            "left_dataset": left_dataset,
            "right_dataset": right_dataset,
        }
    actual_lag_days = max(0, (right_date.normalize() - left_date.normalize()).days)
    status = severity if actual_lag_days > int(max_lag_days) else "ok"
    return {
        "name": name,
        "kind": "date_lag",
        "status": status,
        "severity": severity,
        "left_dataset": left_dataset,
        "right_dataset": right_dataset,
        "left_latest_date": left_date.strftime("%Y-%m-%d"),
        "right_latest_date": right_date.strftime("%Y-%m-%d"),
        "max_lag_days": int(max_lag_days),
        "actual_lag_days": int(actual_lag_days),
    }
