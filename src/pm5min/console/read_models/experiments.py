from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from pm5min.research.layout import ResearchLayout
from pm5min.research.layout_helpers import slug_token
from pm5min.research.manifests import read_manifest


_LEADERBOARD_PREVIEW_COLUMNS = (
    "rank",
    "market",
    "group_name",
    "run_name",
    "feature_set",
    "variant_label",
    "profile",
    "target",
    "trades",
    "pnl_sum",
    "roi_pct",
    "bundle_dir",
    "backtest_run_dir",
)
_COMPARE_PREVIEW_COLUMNS = (
    "case_key",
    "market",
    "group_name",
    "matrix_parent_run_name",
    "matrix_stake_label",
    "run_name",
    "feature_set",
    "variant_label",
    "profile",
    "target",
    "stake_usd",
    "max_notional_usd",
    "status",
    "trades",
    "pnl_sum",
    "roi_pct",
    "bundle_dir",
)
_MATRIX_SUMMARY_PREVIEW_COLUMNS = (
    "market",
    "group_name",
    "matrix_parent_run_name",
    "target",
    "cases",
    "completed_cases",
    "failed_cases",
    "avg_roi_pct",
    "stake_usd_values",
    "max_notional_usd_values",
    "best_run_name",
    "best_matrix_stake_label",
    "best_variant_label",
    "best_roi_pct",
    "total_pnl_sum",
    "total_trades",
)
_VARIANT_COMPARE_PREVIEW_COLUMNS = (
    "market",
    "group_name",
    "run_name",
    "feature_set",
    "target",
    "variant_label",
    "status",
    "reference_variant_label",
    "best_completed_variant_label",
    "rank_in_run_by_roi",
    "roi_pct_delta_vs_reference",
    "pnl_sum_delta_vs_reference",
    "comparison_vs_reference",
)
_FAILED_CASE_PREVIEW_COLUMNS = (
    "case_key",
    "market",
    "group_name",
    "run_name",
    "feature_set",
    "variant_label",
    "target",
    "status",
    "failure_stage",
    "error_type",
    "error_message",
)
_PREVIEW_LIMIT = 5


def list_console_experiment_runs(
    *,
    suite_name: str | None = None,
    prefix: str | None = None,
    root: Path | None = None,
) -> list[dict[str, object]]:
    storage = ResearchLayout.discover(root=root)
    runs_root = storage.experiment_runs_root
    if not runs_root.exists():
        return []

    rows = [path for path in runs_root.glob("suite=*/run=*") if path.is_dir()]
    if suite_name:
        token = f"suite={slug_token(suite_name)}"
        rows = [path for path in rows if path.parent.name == token]
    if prefix:
        token = f"run={slug_token(prefix)}"
        rows = [path for path in rows if path.name.startswith(token)]
    rows = sorted(rows, key=lambda path: (path.stat().st_mtime_ns, path.name), reverse=True)
    return [_build_experiment_run_row(path) for path in rows]


def describe_console_experiment_run(
    *,
    suite_name: str,
    run_label: str,
    root: Path | None = None,
) -> dict[str, object]:
    storage = ResearchLayout.discover(root=root)
    run_dir = storage.experiment_run_dir(suite_name, run_label)
    return _build_experiment_run_detail(run_dir)


def describe_console_experiment_matrix(
    *,
    suite_name: str,
    run_label: str,
    root: Path | None = None,
) -> dict[str, object]:
    storage = ResearchLayout.discover(root=root)
    run_dir = storage.experiment_run_dir(suite_name, run_label)
    return _build_experiment_matrix_detail(run_dir)


def _build_experiment_run_row(path: Path) -> dict[str, object]:
    summary_payload = _read_json_if_exists(path / "summary.json")
    manifest_payload = _read_manifest_if_exists(path / "manifest.json")
    path_tokens = _path_tokens(path)
    leaderboard_preview = _parquet_preview(
        path / "leaderboard.parquet",
        columns=_LEADERBOARD_PREVIEW_COLUMNS,
        limit=1,
    )
    matrix_summary_preview = _parquet_preview(
        path / "matrix_summary.parquet",
        columns=_MATRIX_SUMMARY_PREVIEW_COLUMNS,
        limit=1,
    )
    variant_compare_preview = _parquet_preview(
        path / "variant_compare.parquet",
        columns=_VARIANT_COMPARE_PREVIEW_COLUMNS,
        limit=1,
        filters={"is_best_completed_variant": True},
        sort_by=("best_completed_roi_pct", "roi_pct_delta_vs_reference", "rank_in_run_by_roi"),
        ascending=(False, False, True),
    )
    suite_name = summary_payload.get("suite_name", path_tokens.get("suite")) if isinstance(summary_payload, dict) else path_tokens.get("suite")
    run_label = summary_payload.get("run_label", path_tokens.get("run")) if isinstance(summary_payload, dict) else path_tokens.get("run")
    row = {
        "object_type": "experiment_run",
        "suite_name": suite_name,
        "run_label": run_label,
        "updated_at": _updated_at(path),
        "cases": summary_payload.get("cases") if isinstance(summary_payload, dict) else None,
        "completed_cases": summary_payload.get("completed_cases") if isinstance(summary_payload, dict) else None,
        "failed_cases": summary_payload.get("failed_cases") if isinstance(summary_payload, dict) else None,
        "top_roi_pct": summary_payload.get("top_roi_pct") if isinstance(summary_payload, dict) else None,
        "name": path.name,
        "path": str(path),
        "artifacts": _artifact_map(path),
    }
    row.update(path_tokens)
    if isinstance(summary_payload, dict):
        row.update(
            {
                "cases": summary_payload.get("cases"),
                "groups": summary_payload.get("groups"),
                "matrices": summary_payload.get("matrices"),
                "runs": summary_payload.get("runs"),
                "completed_cases": summary_payload.get("completed_cases"),
                "failed_cases": summary_payload.get("failed_cases"),
                "resumed_cases": summary_payload.get("resumed_cases"),
                "training_reused_cases": summary_payload.get("training_reused_cases"),
                "bundle_reused_cases": summary_payload.get("bundle_reused_cases"),
                "secondary_training_reused_cases": summary_payload.get("secondary_training_reused_cases"),
                "secondary_bundle_reused_cases": summary_payload.get("secondary_bundle_reused_cases"),
                "markets": summary_payload.get("markets"),
                "markets_count": len(summary_payload.get("markets", [])) if isinstance(summary_payload.get("markets"), list) else None,
                "top_roi_pct": summary_payload.get("top_roi_pct"),
                "leaderboard_rows": summary_payload.get("leaderboard_rows"),
                "training_rows": summary_payload.get("training_rows"),
                "backtest_rows": summary_payload.get("backtest_rows"),
            }
        )
    if isinstance(manifest_payload, dict):
        row["object_id"] = manifest_payload.get("object_id")
        row["created_at"] = manifest_payload.get("created_at")
    row["comparison_overview"] = _experiment_comparison_overview(
        summary=summary_payload,
        leaderboard_preview=leaderboard_preview,
        matrix_summary_preview=matrix_summary_preview,
        variant_compare_preview=variant_compare_preview,
    )
    row["best_case"] = _preview_first_row(leaderboard_preview)
    row["best_matrix"] = _preview_first_row(matrix_summary_preview)
    row["best_variant"] = _preview_first_row(variant_compare_preview)
    row["summary"] = summary_payload
    row["manifest"] = manifest_payload
    row["action_context"] = {
        "suite_name": suite_name,
        "run_label": run_label,
        "run_dir": str(path),
    }
    return row


def _build_experiment_run_detail(path: Path) -> dict[str, object]:
    row = _build_experiment_run_row(path)
    report_path = path / "report.md"
    leaderboard_path = path / "leaderboard.parquet"
    compare_path = path / "compare.parquet"
    compare_facets = _parquet_facets(compare_path)
    leaderboard_preview = _parquet_preview(
        leaderboard_path,
        columns=_LEADERBOARD_PREVIEW_COLUMNS,
        limit=_PREVIEW_LIMIT,
    )
    compare_preview = _parquet_preview(
        compare_path,
        columns=_COMPARE_PREVIEW_COLUMNS,
        limit=_PREVIEW_LIMIT,
        sort_by=("roi_pct", "pnl_sum"),
        ascending=(False, False),
    )
    matrix_summary_preview = _parquet_preview(
        path / "matrix_summary.parquet",
        columns=_MATRIX_SUMMARY_PREVIEW_COLUMNS,
        limit=_PREVIEW_LIMIT,
        sort_by=("best_roi_pct", "total_pnl_sum"),
        ascending=(False, False),
    )
    variant_compare_preview = _parquet_preview(
        path / "variant_compare.parquet",
        columns=_VARIANT_COMPARE_PREVIEW_COLUMNS,
        limit=_PREVIEW_LIMIT,
        filters={"is_best_completed_variant": True},
        sort_by=("best_completed_roi_pct", "roi_pct_delta_vs_reference", "rank_in_run_by_roi"),
        ascending=(False, False, True),
    )
    failed_cases_preview = _parquet_preview(
        path / "failed_cases.parquet",
        columns=_FAILED_CASE_PREVIEW_COLUMNS,
        limit=_PREVIEW_LIMIT,
    )
    best_by_market_preview = _parquet_group_preview(
        leaderboard_path,
        columns=_LEADERBOARD_PREVIEW_COLUMNS,
        key_columns=("market",),
        limit=_PREVIEW_LIMIT,
        sort_by=("rank", "roi_pct", "pnl_sum"),
        ascending=(True, False, False),
    )
    best_by_group_preview = _parquet_group_preview(
        leaderboard_path,
        columns=_LEADERBOARD_PREVIEW_COLUMNS,
        key_columns=("group_name",),
        limit=_PREVIEW_LIMIT,
        sort_by=("rank", "roi_pct", "pnl_sum"),
        ascending=(True, False, False),
    )
    best_by_market_group_preview = _parquet_group_preview(
        leaderboard_path,
        columns=_LEADERBOARD_PREVIEW_COLUMNS,
        key_columns=("market", "group_name"),
        limit=_PREVIEW_LIMIT,
        sort_by=("rank", "roi_pct", "pnl_sum"),
        ascending=(True, False, False),
    )
    best_by_run_preview = _parquet_group_preview(
        leaderboard_path,
        columns=_LEADERBOARD_PREVIEW_COLUMNS,
        key_columns=("market", "group_name", "run_name"),
        limit=_PREVIEW_LIMIT,
        sort_by=("rank", "roi_pct", "pnl_sum"),
        ascending=(True, False, False),
    )
    payload = {
        "dataset": "console_experiment_run_detail",
        **row,
        "exists": path.exists(),
        "report_excerpt": _read_text_excerpt(report_path),
        "leaderboard_preview": leaderboard_preview,
        "compare_preview": compare_preview,
        "matrix_summary_preview": matrix_summary_preview,
        "variant_compare_preview": variant_compare_preview,
        "failed_cases_preview": failed_cases_preview,
        "best_by_market_preview": best_by_market_preview,
        "best_by_group_preview": best_by_group_preview,
        "best_by_market_group_preview": best_by_market_group_preview,
        "best_by_run_preview": best_by_run_preview,
        "compare_facets": compare_facets,
        "compare_surface_summary": _compare_surface_summary(
            summary=row.get("summary"),
            compare_facets=compare_facets,
            leaderboard_preview=leaderboard_preview,
            compare_preview=compare_preview,
            matrix_summary_preview=matrix_summary_preview,
            variant_compare_preview=variant_compare_preview,
            failed_cases_preview=failed_cases_preview,
        ),
        "leaderboard_surface_summary": _leaderboard_surface_summary(
            best_by_market_preview=best_by_market_preview,
            best_by_group_preview=best_by_group_preview,
            best_by_run_preview=best_by_run_preview,
        ),
        "best_combo_summary": _best_combo_summary(
            summary=row.get("summary"),
            best_case=row.get("best_case"),
            best_matrix=row.get("best_matrix"),
            best_variant=row.get("best_variant"),
        ),
        "variant_surface_summary": _variant_surface_summary(path / "variant_compare.parquet"),
        "failure_overview": _failure_overview(
            path / "failed_cases.parquet",
            summary=row.get("summary"),
        ),
    }
    return payload


def _build_experiment_matrix_detail(path: Path) -> dict[str, object]:
    row = _build_experiment_run_row(path)
    compare_path = path / "compare.parquet"
    matrix_path = path / "matrix_summary.parquet"
    matrix_summary_preview = _parquet_preview(
        matrix_path,
        columns=_MATRIX_SUMMARY_PREVIEW_COLUMNS,
        limit=_PREVIEW_LIMIT,
        sort_by=("best_roi_pct", "total_pnl_sum"),
        ascending=(False, False),
    )
    compare_preview = _parquet_preview(
        compare_path,
        columns=_COMPARE_PREVIEW_COLUMNS,
        limit=_PREVIEW_LIMIT,
        sort_by=("roi_pct", "pnl_sum"),
        ascending=(False, False),
    )
    matrix_rows = _parquet_rows(
        matrix_path,
        columns=_MATRIX_SUMMARY_PREVIEW_COLUMNS,
        sort_by=("best_roi_pct", "total_pnl_sum"),
        ascending=(False, False),
    )
    compare_rows = _parquet_rows(
        compare_path,
        columns=_COMPARE_PREVIEW_COLUMNS,
        sort_by=("roi_pct", "pnl_sum"),
        ascending=(False, False),
    )
    best_by_run_preview = _parquet_group_preview(
        path / "leaderboard.parquet",
        columns=_LEADERBOARD_PREVIEW_COLUMNS,
        key_columns=("market", "group_name", "run_name"),
        limit=_PREVIEW_LIMIT,
        sort_by=("rank", "roi_pct", "pnl_sum"),
        ascending=(True, False, False),
    )
    surface_summary = _matrix_surface_summary(
        matrix_path=matrix_path,
        compare_path=compare_path,
        summary=row.get("summary"),
        matrix_summary_preview=matrix_summary_preview,
        compare_preview=compare_preview,
    )
    highlights = _experiment_matrix_highlights(
        best_matrix=row.get("best_matrix"),
        best_case=row.get("best_case"),
        best_variant=row.get("best_variant"),
        best_by_run_preview=best_by_run_preview,
    )
    return {
        "dataset": "console_experiment_matrix_detail",
        **row,
        "exists": path.exists(),
        "summary": row.get("summary"),
        "matrix_summary_preview": matrix_summary_preview,
        "compare_preview": compare_preview,
        "surface_summary": surface_summary,
        "highlights": highlights,
        "chart_rows": matrix_rows,
        "rows_by_theme": {
            "matrix_rows": matrix_rows,
            "compare_rows": compare_rows,
            "leaders_by_run": list(best_by_run_preview.get("rows") or []),
        },
    }


def _artifact_map(path: Path) -> dict[str, dict[str, object]]:
    return {
        "summary": _artifact_payload(path / "summary.json"),
        "report": _artifact_payload(path / "report.md"),
        "manifest": _artifact_payload(path / "manifest.json"),
        "training_runs": _artifact_payload(path / "training_runs.parquet"),
        "backtest_runs": _artifact_payload(path / "backtest_runs.parquet"),
        "failed_cases": _artifact_payload(path / "failed_cases.parquet"),
        "leaderboard": _artifact_payload(path / "leaderboard.parquet"),
        "compare": _artifact_payload(path / "compare.parquet"),
        "matrix_summary": _artifact_payload(path / "matrix_summary.parquet"),
        "variant_compare": _artifact_payload(path / "variant_compare.parquet"),
        "log": _artifact_payload(path / "logs" / "suite.jsonl"),
    }


def _artifact_payload(path: Path) -> dict[str, object]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "size_bytes": int(path.stat().st_size) if path.exists() else 0,
        "updated_at": _updated_at(path) if path.exists() else None,
    }


def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {"value": payload}


def _read_manifest_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return read_manifest(path).to_dict()


def _read_text_excerpt(path: Path, *, max_lines: int = 24) -> str | None:
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8")
    return "\n".join(text.splitlines()[: max(1, int(max_lines))])


def _updated_at(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _path_tokens(path: Path) -> dict[str, str]:
    tokens: dict[str, str] = {}
    for part in path.parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        tokens[key] = value
    return tokens


def _parquet_preview(
    path: Path,
    *,
    columns: tuple[str, ...],
    limit: int,
    filters: dict[str, object] | None = None,
    sort_by: tuple[str, ...] = (),
    ascending: tuple[bool, ...] = (),
) -> dict[str, object]:
    payload = {
        "path": str(path),
        "exists": path.exists(),
        "row_count": 0,
        "columns": [],
        "rows": [],
    }
    if not path.exists():
        return payload
    frame, error = _read_parquet_frame(path)
    if frame is None:
        if error:
            payload["error"] = error
        return payload
    frame = _filter_frame(frame, filters=filters)
    frame = _sort_frame(frame, sort_by=sort_by, ascending=ascending)
    selected_columns = [column for column in columns if column in frame.columns]
    preview = frame.loc[:, selected_columns].head(max(int(limit), 0)).copy() if selected_columns else frame.head(max(int(limit), 0)).copy()
    payload["row_count"] = int(len(frame))
    payload["columns"] = selected_columns if selected_columns else [str(column) for column in preview.columns.tolist()]
    payload["rows"] = [_json_ready_record(record) for record in preview.to_dict(orient="records")]
    return payload


def _preview_first_row(preview: dict[str, object]) -> dict[str, object] | None:
    rows = preview.get("rows")
    if not isinstance(rows, list) or not rows:
        return None
    first = rows[0]
    return dict(first) if isinstance(first, dict) else None


def _experiment_comparison_overview(
    *,
    summary: dict[str, Any] | None,
    leaderboard_preview: dict[str, object],
    matrix_summary_preview: dict[str, object],
    variant_compare_preview: dict[str, object],
) -> dict[str, object]:
    payload = {
        "cases": summary.get("cases") if isinstance(summary, dict) else None,
        "completed_cases": summary.get("completed_cases") if isinstance(summary, dict) else None,
        "failed_cases": summary.get("failed_cases") if isinstance(summary, dict) else None,
        "resumed_cases": summary.get("resumed_cases") if isinstance(summary, dict) else None,
        "training_reused_cases": summary.get("training_reused_cases") if isinstance(summary, dict) else None,
        "bundle_reused_cases": summary.get("bundle_reused_cases") if isinstance(summary, dict) else None,
        "leaderboard_rows": summary.get("leaderboard_rows") if isinstance(summary, dict) else None,
        "top_roi_pct": summary.get("top_roi_pct") if isinstance(summary, dict) else None,
    }
    best_case = _preview_first_row(leaderboard_preview)
    if best_case is not None:
        payload["best_case_run_name"] = best_case.get("run_name")
        payload["best_case_variant_label"] = best_case.get("variant_label")
        payload["best_case_roi_pct"] = best_case.get("roi_pct")
        payload["best_case_bundle_dir"] = best_case.get("bundle_dir")
    best_matrix = _preview_first_row(matrix_summary_preview)
    if best_matrix is not None:
        payload["best_matrix_parent_run_name"] = best_matrix.get("matrix_parent_run_name")
        payload["best_matrix_stake_label"] = best_matrix.get("best_matrix_stake_label")
        payload["best_matrix_variant_label"] = best_matrix.get("best_variant_label")
        payload["best_matrix_roi_pct"] = best_matrix.get("best_roi_pct")
    best_variant = _preview_first_row(variant_compare_preview)
    if best_variant is not None:
        payload["best_variant_label"] = best_variant.get("variant_label")
        payload["best_variant_run_name"] = best_variant.get("run_name")
        payload["best_variant_comparison"] = best_variant.get("comparison_vs_reference")
    return {key: value for key, value in payload.items() if value is not None}


def _parquet_group_preview(
    path: Path,
    *,
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
    limit: int = 5,
    filters: dict[str, object] | None = None,
    sort_by: tuple[str, ...] = (),
    ascending: tuple[bool, ...] = (),
) -> dict[str, object]:
    payload = {
        "path": str(path),
        "exists": path.exists(),
        "key_columns": list(key_columns),
        "row_count": 0,
        "source_row_count": 0,
        "rows": [],
    }
    if not path.exists():
        return payload
    frame, error = _read_parquet_frame(path)
    if frame is None:
        if error:
            payload["error"] = error
        return payload
    frame = _filter_frame(frame, filters=filters)
    frame = _sort_frame(frame, sort_by=sort_by, ascending=ascending)
    payload["source_row_count"] = int(len(frame))
    if frame.empty:
        return payload
    selected_columns = [column for column in columns if column in frame.columns]
    seen: set[tuple[object, ...]] = set()
    rows: list[dict[str, object]] = []
    for record in frame.to_dict(orient="records"):
        key = tuple(record.get(column) for column in key_columns)
        if key in seen:
            continue
        seen.add(key)
        selected = {column: record.get(column) for column in selected_columns} if selected_columns else dict(record)
        rows.append(_json_ready_record(selected))
        if len(rows) >= max(int(limit), 0):
            break
    payload["row_count"] = len(rows)
    payload["rows"] = rows
    return payload


def _parquet_facets(path: Path) -> dict[str, object]:
    frame, _error = _read_parquet_frame(path)
    if frame is None:
        return {}
    rows = [_json_ready_record(record) for record in frame.to_dict(orient="records")]
    return {
        "markets": _sorted_unique_values(rows, "market"),
        "groups": _sorted_unique_values(rows, "group_name"),
        "run_names": _sorted_unique_values(rows, "run_name"),
        "variant_labels": _sorted_unique_values(rows, "variant_label"),
        "targets": _sorted_unique_values(rows, "target"),
        "stake_usd_values": _sorted_unique_numeric_values(rows, "stake_usd"),
        "max_notional_usd_values": _sorted_unique_numeric_values(rows, "max_notional_usd"),
        "bundle_dirs": _sorted_unique_values(rows, "bundle_dir"),
    }


def _parquet_rows(
    path: Path,
    *,
    columns: tuple[str, ...],
    filters: dict[str, object] | None = None,
    sort_by: tuple[str, ...] = (),
    ascending: tuple[bool, ...] = (),
) -> list[dict[str, object]]:
    frame, _error = _read_parquet_frame(path)
    if frame is None:
        return []
    frame = _filter_frame(frame, filters=filters)
    frame = _sort_frame(frame, sort_by=sort_by, ascending=ascending)
    selected_columns = [column for column in columns if column in frame.columns]
    selected = frame.loc[:, selected_columns].copy() if selected_columns else frame.copy()
    return [_json_ready_record(record) for record in selected.to_dict(orient="records")]


def _compare_surface_summary(
    *,
    summary: dict[str, Any] | None,
    compare_facets: dict[str, object],
    leaderboard_preview: dict[str, object] | None = None,
    compare_preview: dict[str, object] | None = None,
    matrix_summary_preview: dict[str, object] | None = None,
    variant_compare_preview: dict[str, object] | None = None,
    failed_cases_preview: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "cases": summary.get("cases") if isinstance(summary, dict) else None,
        "completed_cases": summary.get("completed_cases") if isinstance(summary, dict) else None,
        "failed_cases": summary.get("failed_cases") if isinstance(summary, dict) else None,
        "market_count": len(compare_facets.get("markets", [])) if isinstance(compare_facets.get("markets"), list) else 0,
        "group_count": len(compare_facets.get("groups", [])) if isinstance(compare_facets.get("groups"), list) else 0,
        "run_name_count": len(compare_facets.get("run_names", [])) if isinstance(compare_facets.get("run_names"), list) else 0,
        "variant_count": len(compare_facets.get("variant_labels", [])) if isinstance(compare_facets.get("variant_labels"), list) else 0,
        "target_count": len(compare_facets.get("targets", [])) if isinstance(compare_facets.get("targets"), list) else 0,
        "stake_point_count": len(compare_facets.get("stake_usd_values", [])) if isinstance(compare_facets.get("stake_usd_values"), list) else 0,
        "notional_point_count": len(compare_facets.get("max_notional_usd_values", [])) if isinstance(compare_facets.get("max_notional_usd_values"), list) else 0,
        "bundle_count": len(compare_facets.get("bundle_dirs", [])) if isinstance(compare_facets.get("bundle_dirs"), list) else 0,
        "leaderboard_row_count": int((leaderboard_preview or {}).get("row_count") or 0),
        "compare_row_count": int((compare_preview or {}).get("row_count") or 0),
        "matrix_row_count": int((matrix_summary_preview or {}).get("row_count") or 0),
        "variant_row_count": int((variant_compare_preview or {}).get("row_count") or 0),
        "failed_row_count": int((failed_cases_preview or {}).get("row_count") or 0),
    }


def _leaderboard_surface_summary(
    *,
    best_by_market_preview: dict[str, object],
    best_by_group_preview: dict[str, object],
    best_by_run_preview: dict[str, object],
) -> dict[str, object]:
    best_market = _preview_first_row(best_by_market_preview) or {}
    best_group = _preview_first_row(best_by_group_preview) or {}
    best_run = _preview_first_row(best_by_run_preview) or {}
    return {
        "market_leader_count": int(best_by_market_preview.get("row_count") or 0),
        "group_leader_count": int(best_by_group_preview.get("row_count") or 0),
        "run_leader_count": int(best_by_run_preview.get("row_count") or 0),
        "best_market": best_market.get("market"),
        "best_market_run_name": best_market.get("run_name"),
        "best_market_variant_label": best_market.get("variant_label"),
        "best_market_roi_pct": best_market.get("roi_pct"),
        "best_group_name": best_group.get("group_name"),
        "best_group_run_name": best_group.get("run_name"),
        "best_group_variant_label": best_group.get("variant_label"),
        "best_group_roi_pct": best_group.get("roi_pct"),
        "best_run_market": best_run.get("market"),
        "best_run_group_name": best_run.get("group_name"),
        "best_run_name": best_run.get("run_name"),
        "best_run_variant_label": best_run.get("variant_label"),
        "best_run_roi_pct": best_run.get("roi_pct"),
    }


def _best_combo_summary(
    *,
    summary: dict[str, Any] | None,
    best_case: dict[str, object] | None,
    best_matrix: dict[str, object] | None,
    best_variant: dict[str, object] | None,
) -> dict[str, object]:
    case = best_case or {}
    matrix = best_matrix or {}
    variant = best_variant or {}
    payload = {
        "suite_name": summary.get("suite_name") if isinstance(summary, dict) else None,
        "run_label": summary.get("run_label") if isinstance(summary, dict) else None,
        "best_case_market": case.get("market"),
        "best_case_group_name": case.get("group_name"),
        "best_case_run_name": case.get("run_name"),
        "best_case_variant_label": case.get("variant_label"),
        "best_case_bundle_dir": case.get("bundle_dir"),
        "best_case_roi_pct": case.get("roi_pct"),
        "best_case_pnl_sum": case.get("pnl_sum"),
        "best_matrix_market": matrix.get("market"),
        "best_matrix_group_name": matrix.get("group_name"),
        "best_matrix_run_name": matrix.get("matrix_parent_run_name"),
        "best_matrix_stake_label": matrix.get("best_matrix_stake_label"),
        "best_matrix_variant_label": matrix.get("best_variant_label"),
        "best_matrix_roi_pct": matrix.get("best_roi_pct"),
        "best_variant_market": variant.get("market"),
        "best_variant_group_name": variant.get("group_name"),
        "best_variant_run_name": variant.get("run_name"),
        "best_variant_label": variant.get("variant_label"),
        "reference_variant_label": variant.get("reference_variant_label"),
        "best_variant_roi_delta_vs_reference": variant.get("roi_pct_delta_vs_reference"),
        "best_variant_pnl_delta_vs_reference": variant.get("pnl_sum_delta_vs_reference"),
        "best_variant_comparison": variant.get("comparison_vs_reference"),
    }
    return {key: value for key, value in payload.items() if value is not None}


def _variant_surface_summary(path: Path) -> dict[str, object]:
    frame, error = _read_parquet_frame(path)
    payload: dict[str, object] = {
        "path": str(path),
        "exists": path.exists(),
        "row_count": 0,
        "status_counts": {},
        "comparison_counts": {},
    }
    if frame is None:
        if error:
            payload["error"] = error
        return payload
    payload["row_count"] = int(len(frame))
    payload["status_counts"] = _frame_value_counts(frame, "status")
    payload["comparison_counts"] = _frame_value_counts(frame, "comparison_vs_reference")
    payload["run_count"] = _frame_unique_count(frame, "run_name")
    payload["variant_count"] = _frame_unique_count(frame, "variant_label")
    payload["reference_variant_count"] = _frame_unique_count(frame, "reference_variant_label")
    best_row = _best_frame_row(
        frame,
        sort_by=("roi_pct_delta_vs_reference", "pnl_sum_delta_vs_reference", "rank_in_run_by_roi"),
        ascending=(False, False, True),
    )
    if best_row is not None:
        payload["best_variant_label"] = best_row.get("variant_label")
        payload["best_variant_run_name"] = best_row.get("run_name")
        payload["best_variant_roi_delta_vs_reference"] = best_row.get("roi_pct_delta_vs_reference")
        payload["best_variant_pnl_delta_vs_reference"] = best_row.get("pnl_sum_delta_vs_reference")
        payload["best_variant_comparison"] = best_row.get("comparison_vs_reference")
    return payload


def _matrix_surface_summary(
    *,
    matrix_path: Path,
    compare_path: Path,
    summary: dict[str, Any] | None,
    matrix_summary_preview: dict[str, object],
    compare_preview: dict[str, object],
) -> dict[str, object]:
    matrix_frame, matrix_error = _read_parquet_frame(matrix_path)
    compare_frame, compare_error = _read_parquet_frame(compare_path)
    payload: dict[str, object] = {
        "path": str(matrix_path),
        "exists": matrix_path.exists(),
        "row_count": 0,
        "matrix_parent_run_count": 0,
        "market_count": 0,
        "group_count": 0,
        "target_count": 0,
        "stake_label_count": 0,
        "compare_row_count": int((compare_preview or {}).get("row_count") or 0),
        "completed_cases": summary.get("completed_cases") if isinstance(summary, dict) else None,
        "failed_cases": summary.get("failed_cases") if isinstance(summary, dict) else None,
    }
    if matrix_frame is None:
        if matrix_error:
            payload["error"] = matrix_error
        return payload
    payload["row_count"] = int(len(matrix_frame))
    payload["matrix_parent_run_count"] = _frame_unique_count(matrix_frame, "matrix_parent_run_name")
    payload["market_count"] = _frame_unique_count(matrix_frame, "market")
    payload["group_count"] = _frame_unique_count(matrix_frame, "group_name")
    payload["target_count"] = _frame_unique_count(matrix_frame, "target")
    payload["stake_label_count"] = _frame_unique_count(compare_frame, "matrix_stake_label") if compare_frame is not None else 0
    best_row = _preview_first_row(matrix_summary_preview)
    if best_row is not None:
        payload["best_run_name"] = best_row.get("best_run_name")
        payload["best_matrix_stake_label"] = best_row.get("best_matrix_stake_label")
        payload["best_variant_label"] = best_row.get("best_variant_label")
        payload["best_roi_pct"] = best_row.get("best_roi_pct")
        payload["total_pnl_sum"] = best_row.get("total_pnl_sum")
        payload["total_trades"] = best_row.get("total_trades")
    if compare_frame is None and compare_error:
        payload["compare_error"] = compare_error
    return payload


def _experiment_matrix_highlights(
    *,
    best_matrix: dict[str, object] | None,
    best_case: dict[str, object] | None,
    best_variant: dict[str, object] | None,
    best_by_run_preview: dict[str, object],
) -> dict[str, object]:
    return {
        "best_matrix": dict(best_matrix) if isinstance(best_matrix, dict) else None,
        "best_case": dict(best_case) if isinstance(best_case, dict) else None,
        "best_variant": dict(best_variant) if isinstance(best_variant, dict) else None,
        "best_run": _preview_first_row(best_by_run_preview),
    }


def _failure_overview(path: Path, *, summary: dict[str, Any] | None) -> dict[str, object]:
    frame, error = _read_parquet_frame(path)
    payload: dict[str, object] = {
        "path": str(path),
        "exists": path.exists(),
        "failed_cases": summary.get("failed_cases") if isinstance(summary, dict) else None,
        "row_count": 0,
        "failure_stage_counts": {},
        "error_type_counts": {},
        "market_counts": {},
        "group_counts": {},
    }
    if frame is None:
        if error:
            payload["error"] = error
        return payload
    payload["row_count"] = int(len(frame))
    payload["failure_stage_counts"] = _frame_value_counts(frame, "failure_stage")
    payload["error_type_counts"] = _frame_value_counts(frame, "error_type")
    payload["market_counts"] = _frame_value_counts(frame, "market")
    payload["group_counts"] = _frame_value_counts(frame, "group_name")
    first_row = _best_frame_row(
        frame,
        sort_by=("market", "group_name", "case_key"),
        ascending=(True, True, True),
    )
    if first_row is not None:
        payload["first_failed_case"] = {
            "case_key": first_row.get("case_key"),
            "market": first_row.get("market"),
            "group_name": first_row.get("group_name"),
            "run_name": first_row.get("run_name"),
            "variant_label": first_row.get("variant_label"),
            "failure_stage": first_row.get("failure_stage"),
            "error_type": first_row.get("error_type"),
        }
    return payload


def _sorted_unique_values(rows: list[object], key: str) -> list[str]:
    values = {
        str(row.get(key)).strip()
        for row in rows
        if isinstance(row, dict) and row.get(key) not in (None, "")
    }
    return sorted(value for value in values if value)


def _sorted_unique_numeric_values(rows: list[object], key: str) -> list[float]:
    values: set[float] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = row.get(key)
        if value in (None, ""):
            continue
        try:
            values.add(float(value))
        except Exception:
            continue
    return sorted(values)


def _frame_unique_count(frame: pd.DataFrame | None, column: str) -> int:
    if frame is None or column not in frame.columns:
        return 0
    series = frame[column]
    return int(series.dropna().astype(str).str.strip().loc[lambda item: item.ne("")].nunique())


def _json_ready_record(record: dict[str, object]) -> dict[str, object]:
    return {str(key): _json_ready_value(value) for key, value in record.items()}


def _json_ready_value(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_json_ready_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready_value(item) for item in value]
    if isinstance(value, set):
        return [_json_ready_value(item) for item in sorted(value, key=str)]
    if isinstance(value, dict):
        return {str(key): _json_ready_value(item) for key, item in value.items()}
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
        try:
            converted = value.tolist()
        except Exception:
            converted = None
        if converted is not None and converted is not value:
            return _json_ready_value(converted)
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            return _json_ready_value(value.item())
        except Exception:
            pass
    return str(value)


def _read_parquet_frame(path: Path) -> tuple[pd.DataFrame | None, str | None]:
    if not path.exists():
        return None, None
    try:
        return pd.read_parquet(path), None
    except Exception as exc:
        return None, f"{exc.__class__.__name__}: {exc}"


def _filter_frame(frame: pd.DataFrame, *, filters: dict[str, object] | None = None) -> pd.DataFrame:
    if not filters:
        return frame
    filtered = frame
    for key, value in filters.items():
        if key not in filtered.columns:
            continue
        filtered = filtered.loc[filtered[key].eq(value)].copy()
    return filtered


def _sort_frame(
    frame: pd.DataFrame,
    *,
    sort_by: tuple[str, ...] = (),
    ascending: tuple[bool, ...] = (),
) -> pd.DataFrame:
    if not sort_by:
        return frame
    available = [column for column in sort_by if column in frame.columns]
    if not available:
        return frame
    resolved_ascending = list(ascending[: len(available)]) if ascending else [True] * len(available)
    if len(resolved_ascending) < len(available):
        resolved_ascending.extend([True] * (len(available) - len(resolved_ascending)))
    return frame.sort_values(
        available,
        ascending=resolved_ascending,
        kind="stable",
        na_position="last",
    )


def _frame_value_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if column not in frame.columns:
        return {}
    counts = (
        frame.loc[frame[column].notna(), column]
        .astype(str)
        .str.strip()
        .loc[lambda series: series.ne("")]
        .value_counts(sort=False)
        .to_dict()
    )
    return {str(key): int(value) for key, value in sorted(counts.items(), key=lambda item: item[0])}


def _frame_unique_count(frame: pd.DataFrame, column: str) -> int:
    if column not in frame.columns:
        return 0
    values = frame.loc[frame[column].notna(), column].astype(str).str.strip()
    return int(values.loc[values.ne("")].nunique())


def _best_frame_row(
    frame: pd.DataFrame,
    *,
    sort_by: tuple[str, ...],
    ascending: tuple[bool, ...],
) -> dict[str, object] | None:
    if frame.empty:
        return None
    ordered = _sort_frame(frame, sort_by=sort_by, ascending=ascending)
    if ordered.empty:
        return None
    return _json_ready_record(dict(ordered.iloc[0].to_dict()))
