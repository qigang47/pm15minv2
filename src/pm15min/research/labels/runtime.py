from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.service.audit_dataset_checks import run_dataset_audits
from pm15min.data.service.datasets import build_data_datasets
from pm15min.data.service.shared import latest_freshness_timestamp, parse_iso_timestamp
from pm15min.research.labels.sources import summarize_label_sources


def build_truth_runtime_summary(cfg: DataConfig) -> dict[str, object]:
    datasets = build_data_datasets(cfg)
    now = pd.Timestamp.now(tz="UTC").floor("s")
    dataset_audits = run_dataset_audits(cfg=cfg, datasets=datasets, now=now)
    foundation = _load_foundation_state(cfg.layout.foundation_state_path)
    issue_codes = [str(code) for code in foundation.get("issue_codes", []) if str(code)]
    oracle_table = datasets.get("oracle_prices_table") or {}
    truth_table = datasets.get("truth_table") or {}
    direct_oracle_source = datasets.get("direct_oracle_source") or {}
    settlement_truth_source = datasets.get("settlement_truth_source") or {}
    streams_source = datasets.get("chainlink_streams_source") or {}
    datafeeds_source = datasets.get("chainlink_datafeeds_source") or {}
    foundation_status = str(foundation.get("status") or "")
    direct_oracle_fail_open = bool(
        "oracle_direct_rate_limited" in issue_codes and str(oracle_table.get("status") or "") == "ok"
    )
    truth_table_summary = _dataset_runtime_summary(
        prefix="truth_runtime_truth_table",
        payload=truth_table,
        audit=dataset_audits.get("truth_table") or {},
        now=now,
    )
    oracle_table_summary = _dataset_runtime_summary(
        prefix="truth_runtime_oracle_prices_table",
        payload=oracle_table,
        audit=dataset_audits.get("oracle_prices_table") or {},
        now=now,
    )
    direct_oracle_summary = _dataset_runtime_summary(
        prefix="truth_runtime_direct_oracle_source",
        payload=direct_oracle_source,
        audit=dataset_audits.get("direct_oracle_source") or {},
        now=now,
    )
    settlement_truth_summary = _dataset_runtime_summary(
        prefix="truth_runtime_settlement_truth_source",
        payload=settlement_truth_source,
        audit=dataset_audits.get("settlement_truth_source") or {},
        now=now,
    )
    streams_summary = _dataset_runtime_summary(
        prefix="truth_runtime_streams_source",
        payload=streams_source,
        audit=dataset_audits.get("chainlink_streams_source") or {},
        now=now,
    )
    datafeeds_summary = _dataset_runtime_summary(
        prefix="truth_runtime_datafeeds_source",
        payload=datafeeds_source,
        audit=dataset_audits.get("chainlink_datafeeds_source") or {},
        now=now,
    )
    dataset_refresh_statuses = {
        "truth_table": _summary_recent_refresh_status(
            truth_table_summary,
            prefix="truth_runtime_truth_table",
        ),
        "oracle_prices_table": _summary_recent_refresh_status(
            oracle_table_summary,
            prefix="truth_runtime_oracle_prices_table",
        ),
        "direct_oracle_source": _summary_recent_refresh_status(
            direct_oracle_summary,
            prefix="truth_runtime_direct_oracle_source",
        ),
        "settlement_truth_source": _summary_recent_refresh_status(
            settlement_truth_summary,
            prefix="truth_runtime_settlement_truth_source",
        ),
        "streams_source": _summary_recent_refresh_status(
            streams_summary,
            prefix="truth_runtime_streams_source",
        ),
        "datafeeds_source": _summary_recent_refresh_status(
            datafeeds_summary,
            prefix="truth_runtime_datafeeds_source",
        ),
    }
    return {
        "truth_runtime_foundation_status": foundation_status,
        "truth_runtime_foundation_reason": str(foundation.get("reason") or ""),
        "truth_runtime_foundation_issue_codes": issue_codes,
        "truth_runtime_foundation_run_started_at": _timestamp_text(foundation.get("run_started_at")),
        "truth_runtime_foundation_last_completed_at": _timestamp_text(foundation.get("last_completed_at")),
        "truth_runtime_foundation_finished_at": _timestamp_text(foundation.get("finished_at")),
        "truth_runtime_foundation_completed_iterations": int(foundation.get("completed_iterations") or 0),
        "truth_runtime_direct_oracle_fail_open": direct_oracle_fail_open,
        "truth_runtime_recent_refresh_status": _recent_refresh_status(
            foundation_status=foundation_status,
            direct_oracle_fail_open=direct_oracle_fail_open,
            dataset_refresh_statuses=dataset_refresh_statuses,
        ),
        "truth_runtime_recent_refresh_interpretation": _recent_refresh_interpretation(
            foundation_status=foundation_status,
            direct_oracle_fail_open=direct_oracle_fail_open,
            issue_codes=issue_codes,
            dataset_refresh_statuses=dataset_refresh_statuses,
        ),
        **truth_table_summary,
        **oracle_table_summary,
        **direct_oracle_summary,
        **settlement_truth_summary,
        **streams_summary,
        **datafeeds_summary,
    }


def build_label_runtime_summary(
    *,
    truth_table: pd.DataFrame,
    oracle_prices_table: pd.DataFrame,
    truth_path: Path,
    oracle_path: Path,
) -> dict[str, object]:
    truth_sources = (
        summarize_label_sources(truth_table.get("truth_source", pd.Series(dtype="string")))
        if not truth_table.empty
        else {"label_source_counts": {}}
    )
    oracle_source_series = (
        oracle_prices_table.get(
            "source_final_price",
            oracle_prices_table.get("source_price_to_beat", pd.Series(dtype="string")),
        )
        if not oracle_prices_table.empty
        else pd.Series(dtype="string")
    )
    oracle_sources = (
        summarize_label_sources(oracle_source_series)
        if len(oracle_source_series) > 0
        else {"label_source_counts": {}}
    )
    truth_rows = int(len(truth_table))
    oracle_rows = int(len(oracle_prices_table))
    oracle_has_both_rows = int(
        pd.to_numeric(oracle_prices_table.get("has_both", pd.Series(dtype="float64")), errors="coerce")
        .fillna(0.0)
        .astype(float)
        .gt(0.0)
        .sum()
    ) if oracle_rows else 0
    status = "ok" if truth_rows > 0 or oracle_has_both_rows > 0 else "missing"
    return {
        "status": status,
        "truth_table_rows": truth_rows,
        "truth_source_counts": dict(truth_sources.get("label_source_counts") or {}),
        "truth_table_updated_at": _path_updated_at(truth_path),
        "oracle_table_rows": oracle_rows,
        "oracle_has_both_rows": oracle_has_both_rows,
        "oracle_source_counts": dict(oracle_sources.get("label_source_counts") or {}),
        "oracle_table_updated_at": _path_updated_at(oracle_path),
    }


def _load_foundation_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _dataset_status_summary(*, prefix: str, payload: dict[str, object]) -> dict[str, object]:
    return {
        f"{prefix}_status": str(payload.get("status") or ""),
        f"{prefix}_rows": int(payload.get("row_count") or 0),
    }


def _dataset_runtime_summary(
    *,
    prefix: str,
    payload: dict[str, object],
    audit: dict[str, object],
    now: pd.Timestamp,
) -> dict[str, object]:
    latest_ts = latest_freshness_timestamp(payload)
    latest_ts_text = None if latest_ts is None else latest_ts.isoformat()
    age_seconds = None if latest_ts is None else max(0.0, float((now - latest_ts).total_seconds()))
    freshness_state = _dataset_freshness_state(payload=payload, audit=audit)
    return {
        **_dataset_status_summary(prefix=prefix, payload=payload),
        f"{prefix}_freshness_max": latest_ts_text,
        f"{prefix}_freshness_age_seconds": age_seconds,
        f"{prefix}_freshness_state": freshness_state,
        f"{prefix}_recent_refresh_status": _dataset_recent_refresh_status(freshness_state=freshness_state),
    }


def _dataset_freshness_state(*, payload: dict[str, object], audit: dict[str, object]) -> str:
    status = str(payload.get("status") or "")
    if status == "missing":
        return "missing"
    if status == "empty":
        return "empty"
    failed_checks = {str(code) for code in (audit.get("failed_checks") or []) if str(code)}
    if "max_age_seconds" in failed_checks or "max_partition_age_days" in failed_checks:
        return "stale"
    if status == "ok":
        return "fresh"
    return status or "unknown"


def _dataset_recent_refresh_status(*, freshness_state: str) -> str:
    if freshness_state in {"fresh", "stale", "missing", "empty"}:
        return freshness_state
    return "unknown"


def _recent_refresh_status(
    *,
    foundation_status: str,
    direct_oracle_fail_open: bool,
    dataset_refresh_statuses: dict[str, str],
) -> str:
    token = str(foundation_status or "").strip().lower()
    if token == "running":
        return "running"
    if direct_oracle_fail_open:
        return "fail_open"
    if token == "error":
        return "error"
    if token in {"degraded", "ok_with_errors", "warning"}:
        return "degraded"
    if _dataset_refresh_gaps(dataset_refresh_statuses):
        return "degraded"
    if token == "ok":
        return "fresh"
    return "unknown"


def _recent_refresh_interpretation(
    *,
    foundation_status: str,
    direct_oracle_fail_open: bool,
    issue_codes: list[str],
    dataset_refresh_statuses: dict[str, str],
) -> str:
    token = _recent_refresh_status(
        foundation_status=foundation_status,
        direct_oracle_fail_open=direct_oracle_fail_open,
        dataset_refresh_statuses=dataset_refresh_statuses,
    )
    if token == "fail_open":
        return "recent_refresh_degraded_but_existing_oracle_table_is_still_serving_reads"
    if token == "fresh":
        return "recent_refresh_completed_and_key_datasets_are_fresh"
    if token == "degraded":
        dataset_gaps = _dataset_refresh_gaps(dataset_refresh_statuses)
        if dataset_gaps and issue_codes:
            return (
                "recent_refresh_completed_with_degraded_tasks_and_dataset_gaps:"
                f"{','.join(issue_codes)}|{','.join(dataset_gaps)}"
            )
        if dataset_gaps:
            return f"recent_refresh_completed_with_dataset_gaps:{','.join(dataset_gaps)}"
        codes = ",".join(issue_codes)
        return (
            f"recent_refresh_completed_with_degraded_tasks:{codes}"
            if codes
            else "recent_refresh_completed_with_degraded_tasks"
        )
    if token == "running":
        return "foundation_refresh_is_currently_running"
    if token == "error":
        return "foundation_refresh_failed"
    return "foundation_refresh_state_unknown"


def _summary_recent_refresh_status(summary: dict[str, object], *, prefix: str) -> str:
    return str(summary.get(f"{prefix}_recent_refresh_status") or "unknown")


def _dataset_refresh_gaps(dataset_refresh_statuses: dict[str, str]) -> list[str]:
    return [
        f"{name}:{status}"
        for name, status in dataset_refresh_statuses.items()
        if status in {"stale", "missing", "empty"}
    ]


def _timestamp_text(value: object) -> str | None:
    ts = parse_iso_timestamp(value)
    return None if ts is None else ts.isoformat()


def _path_updated_at(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC").isoformat()
    except Exception:
        return ""
