from __future__ import annotations

from typing import Any

from ..config import DataConfig
from .shared import (
    dataset_issue_codes,
    dataset_layer,
    dataset_storage_path,
    optional_int,
    ratio,
)


def build_data_summary_manifest(
    *,
    cfg: DataConfig,
    payload: dict[str, Any],
) -> dict[str, Any]:
    datasets = dict(payload.get("datasets") or {})
    audit = dict(payload.get("audit") or {})
    issues = list(payload.get("issues") or [])
    completeness = dict(payload.get("completeness") or {})
    critical_expected = set(audit.get("critical_expected_datasets") or [])
    dataset_inventory = [
        {
            "name": name,
            "layer": dataset_layer(name),
            "kind": item.get("kind"),
            "exists": bool(item.get("exists")),
            "dataset_status": item.get("status"),
            "audit_status": str(((audit.get("dataset_audits") or {}).get(name) or {}).get("status") or ""),
            "critical_expected": name in critical_expected,
            "row_count": optional_int(item.get("row_count")),
            "partition_count": optional_int(item.get("partition_count") or item.get("file_count")),
            "duplicate_count": optional_int(item.get("duplicate_count")),
            "null_key_count": optional_int(item.get("null_key_count")),
            "time_range": item.get("time_range"),
            "freshness_range": item.get("freshness_range"),
            "date_range": item.get("date_range"),
            "storage_path": dataset_storage_path(item),
            "issue_codes": dataset_issue_codes(dataset_name=name, issues=issues),
        }
        for name, item in datasets.items()
    ]
    return {
        "schema_version": 1,
        "object_type": "data_summary_manifest",
        "generated_at": payload.get("generated_at"),
        "generated_at_iso": payload.get("generated_at_iso"),
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "surface": cfg.surface,
        "audit_status": audit.get("status"),
        "paths": {
            "surface_data_root": payload.get("surface_data_root"),
            "summary_state_dir": str(cfg.layout.summary_state_dir),
            "latest_summary_path": str(cfg.layout.latest_summary_path),
            "latest_manifest_path": str(cfg.layout.latest_summary_manifest_path),
            "summary_snapshot_path": str(cfg.layout.summary_snapshot_path(str(payload.get("generated_at")))),
            "manifest_snapshot_path": str(cfg.layout.summary_manifest_snapshot_path(str(payload.get("generated_at")))),
        },
        "expected_datasets": {
            "critical": list(audit.get("critical_expected_datasets") or []),
            "non_critical": [
                name
                for name in datasets.keys()
                if name not in critical_expected
            ],
        },
        "summary": dict(payload.get("summary") or {}),
        "completeness": completeness,
        "issues": issues,
        "dataset_inventory": dataset_inventory,
    }


def build_data_completeness_report(
    *,
    datasets: dict[str, dict[str, Any]],
    audit: dict[str, Any],
    issues: list[dict[str, Any]],
) -> dict[str, Any]:
    dataset_count = int(len(datasets))
    existing_dataset_count = int(sum(1 for item in datasets.values() if bool(item.get("exists"))))
    dataset_audits = dict(audit.get("dataset_audits") or {})
    healthy_dataset_count = int(sum(1 for item in dataset_audits.values() if str(item.get("status")) == "ok"))
    warning_dataset_count = int(sum(1 for item in dataset_audits.values() if str(item.get("status")) == "warning"))
    error_dataset_count = int(sum(1 for item in dataset_audits.values() if str(item.get("status")) == "error"))
    missing_dataset_count = int(sum(1 for item in dataset_audits.values() if str(item.get("status")) == "missing"))
    affected_datasets = sorted(
        {
            str(issue.get("target"))
            for issue in issues
            if str(issue.get("target") or "") in datasets
        }
    )
    blocking_datasets = sorted(
        {
            str(issue.get("target"))
            for issue in issues
            if str(issue.get("severity")) == "error" and str(issue.get("target") or "") in datasets
        }
    )
    return {
        "status": audit.get("status"),
        "dataset_count": dataset_count,
        "existing_dataset_count": existing_dataset_count,
        "healthy_dataset_count": healthy_dataset_count,
        "warning_dataset_count": warning_dataset_count,
        "error_dataset_count": error_dataset_count,
        "missing_dataset_count": missing_dataset_count,
        "critical_missing_dataset_count": int(len(audit.get("critical_missing_datasets") or [])),
        "issue_count": int(len(issues)),
        "blocking_issue_count": int(sum(1 for issue in issues if str(issue.get("severity")) == "error")),
        "warning_issue_count": int(sum(1 for issue in issues if str(issue.get("severity")) == "warning")),
        "completeness_ratio": ratio(existing_dataset_count, dataset_count),
        "healthy_ratio": ratio(healthy_dataset_count, dataset_count),
        "affected_datasets": affected_datasets,
        "blocking_datasets": blocking_datasets,
    }


def build_data_issue_inventory(
    *,
    datasets: dict[str, dict[str, Any]],
    audit: dict[str, Any],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for name in audit.get("critical_missing_datasets") or []:
        issues.append(
            build_dataset_issue(
                code="critical_missing_dataset",
                severity="error",
                dataset_name=str(name),
                item=datasets.get(str(name)) or {},
                detail={"expected": "critical"},
            )
        )
    for name in audit.get("warning_missing_datasets") or []:
        issues.append(
            build_dataset_issue(
                code="warning_missing_dataset",
                severity="warning",
                dataset_name=str(name),
                item=datasets.get(str(name)) or {},
                detail={"expected": "non_critical"},
            )
        )
    for name in audit.get("duplicate_issue_datasets") or []:
        item = datasets.get(str(name)) or {}
        issues.append(
            build_dataset_issue(
                code="duplicate_keys",
                severity="error",
                dataset_name=str(name),
                item=item,
                detail={"duplicate_count": optional_int(item.get("duplicate_count"))},
            )
        )
    for name in audit.get("null_key_issue_datasets") or []:
        item = datasets.get(str(name)) or {}
        issues.append(
            build_dataset_issue(
                code="null_keys",
                severity="error",
                dataset_name=str(name),
                item=item,
                detail={"null_key_count": optional_int(item.get("null_key_count"))},
            )
        )
    for name, dataset_audit in (audit.get("dataset_audits") or {}).items():
        item = datasets.get(str(name)) or {}
        for check in dataset_audit.get("checks") or []:
            severity = str(check.get("status") or "")
            if severity not in {"warning", "error"}:
                continue
            issues.append(
                build_dataset_issue(
                    code=str(check.get("code") or "dataset_check_failed"),
                    severity=severity,
                    dataset_name=str(name),
                    item=item,
                    detail=dict(check),
                )
            )
    for item in audit.get("alignment_checks") or []:
        severity = str(item.get("status") or "")
        if severity not in {"warning", "error"}:
            continue
        issues.append(
            {
                "code": "alignment_check_failed",
                "severity": severity,
                "target": str(item.get("name") or ""),
                "detail": dict(item),
            }
        )
    return issues


def build_dataset_issue(
    *,
    code: str,
    severity: str,
    dataset_name: str,
    item: dict[str, Any],
    detail: dict[str, Any],
) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "target": dataset_name,
        "detail": {
            **detail,
            "storage_path": dataset_storage_path(item),
        },
    }
