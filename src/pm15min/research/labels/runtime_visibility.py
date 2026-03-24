from __future__ import annotations

from collections.abc import Mapping


def summarize_truth_runtime_visibility(runtime_summary: Mapping[str, object] | None) -> dict[str, object]:
    summary = {} if runtime_summary is None else dict(runtime_summary)
    direct_oracle_fail_open = bool(summary.get("truth_runtime_direct_oracle_fail_open"))
    truth_status = _normalize_dataset_state(
        summary.get("truth_runtime_truth_table_freshness_state"),
        fallback=summary.get("truth_runtime_truth_table_status"),
    )
    oracle_status = "fail_open" if direct_oracle_fail_open else _normalize_dataset_state(
        summary.get("truth_runtime_oracle_prices_table_freshness_state"),
        fallback=summary.get("truth_runtime_oracle_prices_table_status"),
    )
    window_refresh_status = _normalize_window_refresh_status(
        summary.get("truth_runtime_recent_refresh_status"),
        foundation_status=summary.get("truth_runtime_foundation_status"),
        direct_oracle_fail_open=direct_oracle_fail_open,
    )
    window_refresh_reason = _window_refresh_reason(summary, window_refresh_status=window_refresh_status)
    overall_status = _overall_truth_runtime_status(
        truth_status=truth_status,
        oracle_status=oracle_status,
        window_refresh_status=window_refresh_status,
    )
    return {
        "truth_runtime_status": overall_status,
        "truth_runtime_reason": _overall_truth_runtime_reason(
            truth_status=truth_status,
            oracle_status=oracle_status,
            window_refresh_status=window_refresh_status,
            window_refresh_reason=window_refresh_reason,
            overall_status=overall_status,
        ),
        "truth_runtime_truth_status": truth_status,
        "truth_runtime_oracle_status": oracle_status,
        "truth_runtime_window_refresh_status": window_refresh_status,
        "truth_runtime_window_refresh_reason": window_refresh_reason,
        "truth_runtime_run_started_at": summary.get("truth_runtime_foundation_run_started_at") or None,
        "truth_runtime_last_completed_at": (
            summary.get("truth_runtime_foundation_last_completed_at")
            or summary.get("truth_runtime_foundation_finished_at")
            or None
        ),
        "truth_runtime_finished_at": summary.get("truth_runtime_foundation_finished_at") or None,
        "truth_runtime_completed_iterations": summary.get("truth_runtime_foundation_completed_iterations"),
        "truth_runtime_truth_freshness_max": (
            summary.get("truth_runtime_truth_table_freshness_max")
            or summary.get("truth_table_updated_at")
            or None
        ),
        "truth_runtime_oracle_freshness_max": (
            summary.get("truth_runtime_oracle_prices_table_freshness_max")
            or summary.get("oracle_table_updated_at")
            or None
        ),
    }


def _normalize_dataset_state(value: object, *, fallback: object) -> str:
    token = _token(value) or _token(fallback)
    if token in {"fresh", "ok", "available"}:
        return "fresh"
    if token in {"missing", "empty"}:
        return "missing"
    if token in {"stale"}:
        return "stale"
    if token in {"fail_open"}:
        return "fail_open"
    if not token:
        return "unknown"
    return token


def _normalize_window_refresh_status(
    value: object,
    *,
    foundation_status: object,
    direct_oracle_fail_open: bool,
) -> str:
    token = _token(value)
    if token:
        return token
    foundation = _token(foundation_status)
    if direct_oracle_fail_open:
        return "fail_open"
    if foundation == "ok":
        return "fresh"
    if foundation == "ok_with_errors":
        return "degraded"
    if foundation in {"running", "error"}:
        return foundation
    return "unknown"


def _window_refresh_reason(summary: Mapping[str, object], *, window_refresh_status: str) -> str:
    foundation_reason = str(summary.get("truth_runtime_foundation_reason") or "").strip()
    interpretation = str(summary.get("truth_runtime_recent_refresh_interpretation") or "").strip()
    if foundation_reason:
        return foundation_reason
    if interpretation:
        return interpretation
    if window_refresh_status == "fresh":
        return "recent_refresh_completed_without_foundation_errors"
    if window_refresh_status == "fail_open":
        return "recent_refresh_is_serving_existing_oracle_table_in_fail_open_mode"
    if window_refresh_status == "degraded":
        return "recent_refresh_completed_with_degraded_tasks"
    if window_refresh_status == "running":
        return "foundation_refresh_is_currently_running"
    if window_refresh_status == "error":
        return "foundation_refresh_failed"
    return ""


def _overall_truth_runtime_status(
    *,
    truth_status: str,
    oracle_status: str,
    window_refresh_status: str,
) -> str:
    if oracle_status == "fail_open" or window_refresh_status == "fail_open":
        return "fail_open"
    if "missing" in {truth_status, oracle_status}:
        return "missing"
    if "stale" in {truth_status, oracle_status}:
        return "stale"
    if truth_status == "fresh" and oracle_status == "fresh":
        return "fresh"
    if truth_status == "unknown" and oracle_status == "unknown" and window_refresh_status == "unknown":
        return "unknown"
    return "unknown"


def _overall_truth_runtime_reason(
    *,
    truth_status: str,
    oracle_status: str,
    window_refresh_status: str,
    window_refresh_reason: str,
    overall_status: str,
) -> str:
    if overall_status == "fail_open":
        return window_refresh_reason or "oracle_fail_open"
    if overall_status in {"missing", "stale"}:
        issues: list[str] = []
        if truth_status == "missing":
            issues.append("truth_table_missing")
        elif truth_status == "stale":
            issues.append("truth_table_stale")
        if oracle_status == "missing":
            issues.append("oracle_prices_table_missing")
        elif oracle_status == "stale":
            issues.append("oracle_prices_table_stale")
        return ",".join(issues) if issues else window_refresh_reason
    if overall_status == "fresh":
        if window_refresh_status not in {"", "fresh", "unknown"} and window_refresh_reason:
            return window_refresh_reason
        return "truth_and_oracle_fresh"
    return window_refresh_reason


def _token(value: object) -> str:
    return str(value or "").strip().lower()
