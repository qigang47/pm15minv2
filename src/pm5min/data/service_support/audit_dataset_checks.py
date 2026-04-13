from __future__ import annotations

from typing import Any

import pandas as pd

from ..config import DataConfig
from .audit_rules import dataset_audit_rules
from .shared import latest_freshness_timestamp, latest_partition_date


def run_dataset_audits(
    *,
    cfg: DataConfig,
    datasets: dict[str, dict[str, Any]],
    now: pd.Timestamp,
) -> dict[str, dict[str, Any]]:
    return {
        name: evaluate_dataset_audit(name=name, item=datasets.get(name) or {}, rule=rule, now=now)
        for name, rule in dataset_audit_rules(cfg).items()
    }


def evaluate_dataset_audit(
    *,
    name: str,
    item: dict[str, Any],
    rule: dict[str, Any],
    now: pd.Timestamp,
) -> dict[str, Any]:
    severity = str(rule.get("severity") or "warning")
    checks: list[dict[str, Any]] = []
    failed_checks: list[str] = []
    if not bool(item.get("exists")):
        return {
            "dataset": name,
            "severity": severity,
            "status": "missing",
            "failed_checks": [],
            "checks": [],
        }
    min_row_count = rule.get("min_row_count")
    if min_row_count is not None:
        actual = int(item.get("row_count") or 0)
        check_status = severity if actual < int(min_row_count) else "ok"
        checks.append(
            {
                "code": "min_row_count",
                "status": check_status,
                "threshold": int(min_row_count),
                "actual": actual,
            }
        )
        if check_status != "ok":
            failed_checks.append("min_row_count")
    min_partition_count = rule.get("min_partition_count")
    if min_partition_count is not None:
        actual = int(item.get("partition_count") or item.get("file_count") or 0)
        check_status = severity if actual < int(min_partition_count) else "ok"
        checks.append(
            {
                "code": "min_partition_count",
                "status": check_status,
                "threshold": int(min_partition_count),
                "actual": actual,
            }
        )
        if check_status != "ok":
            failed_checks.append("min_partition_count")
    max_age_seconds = rule.get("max_age_seconds")
    if max_age_seconds is not None:
        latest_ts = latest_freshness_timestamp(item)
        actual = None if latest_ts is None else max(0.0, (now - latest_ts).total_seconds())
        check_status = severity if latest_ts is None or actual is None or actual > float(max_age_seconds) else "ok"
        checks.append(
            {
                "code": "max_age_seconds",
                "status": check_status,
                "threshold": float(max_age_seconds),
                "actual": None if actual is None else float(actual),
                "latest_timestamp": None if latest_ts is None else latest_ts.isoformat(),
            }
        )
        if check_status != "ok":
            failed_checks.append("max_age_seconds")
    max_partition_age_days = rule.get("max_partition_age_days")
    if max_partition_age_days is not None:
        latest_date = latest_partition_date(item)
        actual = None if latest_date is None else max(0, (now.normalize() - latest_date.normalize()).days)
        check_status = severity if latest_date is None or actual is None or actual > int(max_partition_age_days) else "ok"
        checks.append(
            {
                "code": "max_partition_age_days",
                "status": check_status,
                "threshold": int(max_partition_age_days),
                "actual": actual,
                "latest_date": None if latest_date is None else latest_date.strftime("%Y-%m-%d"),
            }
        )
        if check_status != "ok":
            failed_checks.append("max_partition_age_days")
    status = "ok"
    if any(str(check.get("status")) == "error" for check in checks):
        status = "error"
    elif any(str(check.get("status")) == "warning" for check in checks):
        status = "warning"
    return {
        "dataset": name,
        "severity": severity,
        "status": status,
        "failed_checks": failed_checks,
        "checks": checks,
    }
