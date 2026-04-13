from __future__ import annotations

from typing import Any

import pandas as pd

from ..config import DataConfig
from .audit_alignment import (
    build_alignment_checks,
    evaluate_date_lag_check,
    evaluate_time_lag_check,
)
from .audit_dataset_checks import evaluate_dataset_audit, run_dataset_audits
from .audit_rules import critical_dataset_names, dataset_audit_rules


def build_data_audit(
    *,
    cfg: DataConfig,
    datasets: dict[str, dict[str, Any]],
    now: pd.Timestamp,
) -> dict[str, object]:
    critical_expected = critical_dataset_names(surface=cfg.surface)
    critical_missing = [
        name
        for name in critical_expected
        if not bool((datasets.get(name) or {}).get("exists"))
    ]
    duplicate_issues = [
        name
        for name, item in datasets.items()
        if int(item.get("duplicate_count") or 0) > 0
    ]
    null_key_issues = [
        name
        for name, item in datasets.items()
        if int(item.get("null_key_count") or 0) > 0
    ]
    warning_missing = [
        name
        for name, item in datasets.items()
        if name not in set(critical_expected) and not bool(item.get("exists"))
    ]
    dataset_audits = run_dataset_audits(cfg=cfg, datasets=datasets, now=now)
    alignment_checks = build_alignment_checks(cfg=cfg, datasets=datasets)
    stale_issues = sorted(
        name
        for name, item in dataset_audits.items()
        if "max_age_seconds" in set(item.get("failed_checks") or ())
        or "max_partition_age_days" in set(item.get("failed_checks") or ())
    )
    low_row_count_issues = sorted(
        name
        for name, item in dataset_audits.items()
        if "min_row_count" in set(item.get("failed_checks") or ())
        or "min_partition_count" in set(item.get("failed_checks") or ())
    )
    dataset_error_issues = sorted(
        name for name, item in dataset_audits.items() if str(item.get("status")) == "error"
    )
    dataset_warning_issues = sorted(
        name for name, item in dataset_audits.items() if str(item.get("status")) == "warning"
    )
    alignment_issue_checks = sorted(
        str(item.get("name"))
        for item in alignment_checks
        if str(item.get("status")) in {"error", "warning"}
    )
    has_alignment_error = any(str(item.get("status")) == "error" for item in alignment_checks)
    has_alignment_warning = any(str(item.get("status")) == "warning" for item in alignment_checks)
    if critical_missing or duplicate_issues or null_key_issues or has_alignment_error or dataset_error_issues:
        status = "error"
    elif warning_missing or has_alignment_warning or dataset_warning_issues:
        status = "warning"
    else:
        status = "ok"
    return {
        "status": status,
        "audit_now": now.isoformat(),
        "critical_expected_datasets": critical_expected,
        "critical_missing_datasets": critical_missing,
        "warning_missing_datasets": warning_missing,
        "duplicate_issue_datasets": duplicate_issues,
        "null_key_issue_datasets": null_key_issues,
        "stale_issue_datasets": stale_issues,
        "low_row_count_issue_datasets": low_row_count_issues,
        "dataset_error_datasets": dataset_error_issues,
        "dataset_warning_datasets": dataset_warning_issues,
        "dataset_audits": dataset_audits,
        "alignment_checks": alignment_checks,
        "alignment_issue_checks": alignment_issue_checks,
    }


__all__ = [
    "build_data_audit",
    "build_alignment_checks",
    "critical_dataset_names",
    "dataset_audit_rules",
    "evaluate_dataset_audit",
    "evaluate_date_lag_check",
    "evaluate_time_lag_check",
    "run_dataset_audits",
]
