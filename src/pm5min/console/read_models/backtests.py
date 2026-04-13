from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from pm5min.research.config import ResearchConfig
from pm5min.research.manifests import read_manifest


_DEFAULT_BACKTEST_TARGET = "direction"


def list_console_backtest_runs(
    *,
    market: str,
    cycle: str | int = "5m",
    profile: str | None = None,
    spec_name: str | None = None,
    prefix: str | None = None,
    root: Path | None = None,
) -> list[dict[str, object]]:
    cfg = ResearchConfig.build(
        market=market,
        cycle=cycle,
        profile=profile or "deep_otm_5m",
        target=_DEFAULT_BACKTEST_TARGET,
        root=root,
    )
    runs_root = cfg.layout.backtests_root
    if not runs_root.exists():
        return []

    rows = [path for path in runs_root.glob("profile=*/target=*/spec=*/run=*") if path.is_dir()]
    rows = [path for path in rows if path.parent.parent.parent.name == f"profile={cfg.profile}"]
    rows = [path for path in rows if path.parent.parent.name == f"target={cfg.target}"]
    if spec_name:
        rows = [path for path in rows if path.parent.name == f"spec={_slug(spec_name)}"]
    if prefix:
        token = f"run={_slug(prefix)}"
        rows = [path for path in rows if path.name.startswith(token)]

    rows = sorted(rows, key=lambda path: (path.stat().st_mtime_ns, path.name), reverse=True)
    return [_build_backtest_run_row(path) for path in rows]


def describe_console_backtest_run(
    *,
    market: str,
    profile: str,
    spec_name: str,
    run_label: str,
    cycle: str | int = "5m",
    root: Path | None = None,
) -> dict[str, object]:
    cfg = ResearchConfig.build(
        market=market,
        cycle=cycle,
        profile=profile,
        target=_DEFAULT_BACKTEST_TARGET,
        root=root,
    )
    run_dir = cfg.layout.backtest_run_dir(
        profile=profile,
        target=_DEFAULT_BACKTEST_TARGET,
        spec_name=spec_name,
        run_label=run_label,
    )
    return _build_backtest_run_detail(run_dir)


def describe_console_backtest_stake_sweep(
    *,
    market: str,
    profile: str,
    spec_name: str,
    run_label: str,
    cycle: str | int = "5m",
    root: Path | None = None,
) -> dict[str, object]:
    cfg = ResearchConfig.build(
        market=market,
        cycle=cycle,
        profile=profile,
        target=_DEFAULT_BACKTEST_TARGET,
        root=root,
    )
    run_dir = cfg.layout.backtest_run_dir(
        profile=profile,
        target=_DEFAULT_BACKTEST_TARGET,
        spec_name=spec_name,
        run_label=run_label,
    )
    return _build_backtest_stake_sweep_detail(run_dir)


def _build_backtest_run_row(path: Path) -> dict[str, object]:
    summary_payload = _read_json_if_exists(path / "summary.json")
    manifest_payload = _read_manifest_if_exists(path / "manifest.json")
    path_tokens = _path_tokens(path)
    market = summary_payload.get("market", path_tokens.get("asset")) if isinstance(summary_payload, dict) else path_tokens.get("asset")
    cycle = summary_payload.get("cycle", path_tokens.get("cycle")) if isinstance(summary_payload, dict) else path_tokens.get("cycle")
    profile = summary_payload.get("profile", path_tokens.get("profile")) if isinstance(summary_payload, dict) else path_tokens.get("profile")
    spec_name = summary_payload.get("spec_name", path_tokens.get("spec")) if isinstance(summary_payload, dict) else path_tokens.get("spec")
    target = summary_payload.get("target") if isinstance(summary_payload, dict) else None
    run_label = path_tokens.get("run")
    result_summary = _backtest_result_summary(summary_payload) if isinstance(summary_payload, dict) else {}
    comparison_axes = _backtest_comparison_axes(summary_payload) if isinstance(summary_payload, dict) else {}
    performance_summary = _backtest_performance_summary(summary_payload) if isinstance(summary_payload, dict) else {}
    row = {
        "object_type": "backtest_run",
        "market": market,
        "cycle": cycle,
        "profile": profile,
        "spec_name": spec_name,
        "run_label": run_label,
        "target": target,
        "updated_at": _updated_at(path),
        "variant_label": comparison_axes.get("variant_label"),
        "stake_usd": comparison_axes.get("stake_usd"),
        "max_notional_usd": comparison_axes.get("max_notional_usd"),
        "trades": result_summary.get("trades"),
        "pnl_sum": result_summary.get("pnl_sum"),
        "roi_pct": result_summary.get("roi_pct"),
        "win_rate_pct": result_summary.get("win_rate_pct"),
        "performance_summary": performance_summary,
        "name": path.name,
        "path": str(path),
        "artifacts": _artifact_map(path),
    }
    row.update(path_tokens)
    if isinstance(summary_payload, dict):
        row.update(
            {
                "variant_label": summary_payload.get("variant_label"),
                "stake_usd": summary_payload.get("stake_usd"),
                "max_notional_usd": summary_payload.get("max_notional_usd"),
                "secondary_bundle_label": summary_payload.get("secondary_bundle_label"),
                "fallback_reasons": summary_payload.get("fallback_reasons"),
                "parity": summary_payload.get("parity"),
                "trades": summary_payload.get("trades"),
                "rejects": summary_payload.get("rejects"),
                "wins": summary_payload.get("wins"),
                "losses": summary_payload.get("losses"),
                "pnl_sum": summary_payload.get("pnl_sum"),
                "stake_sum": summary_payload.get("stake_sum"),
                "roi_pct": summary_payload.get("roi_pct"),
                "win_rate_pct": result_summary.get("win_rate_pct"),
                "avg_roi_pct": summary_payload.get("avg_roi_pct"),
                "result_summary": result_summary,
                "performance_summary": performance_summary,
                "comparison_axes": comparison_axes,
                "overview_cards": _backtest_overview_cards(summary_payload),
            }
        )
    if isinstance(manifest_payload, dict):
        row["object_id"] = manifest_payload.get("object_id")
        row["created_at"] = manifest_payload.get("created_at")
    row["summary"] = summary_payload
    row["manifest"] = manifest_payload
    row["action_context"] = {
        "market": market,
        "cycle": cycle,
        "profile": profile,
        "spec_name": spec_name,
        "run_label": run_label,
        "target": target,
        "run_dir": str(path),
    }
    return row


def _build_backtest_run_detail(path: Path) -> dict[str, object]:
    row = _build_backtest_run_row(path)
    artifact_previews = _artifact_previews(path)
    payload = {
        "dataset": "console_backtest_run_detail",
        **row,
        "exists": path.exists(),
        "report_excerpt": _read_text_excerpt(path / "report.md"),
        "artifact_previews": artifact_previews,
        "equity_curve_preview": artifact_previews["equity_curve"],
        "stake_sweep_preview": artifact_previews["stake_sweep"],
        "offset_summary_preview": artifact_previews["offset_summary"],
        "factor_pnl_preview": artifact_previews["factor_pnl"],
        "sweep_highlights": _backtest_sweep_highlights(artifact_previews),
        "equity_summary": _equity_summary(artifact_previews.get("equity_curve")),
        "stake_surface_summary": _stake_surface_summary(artifact_previews.get("stake_sweep")),
        "offset_surface_summary": _offset_surface_summary(artifact_previews.get("offset_summary")),
        "factor_surface_summary": _factor_surface_summary(artifact_previews.get("factor_pnl")),
        "decision_summary": _backtest_decision_summary(
            performance_summary=row.get("performance_summary"),
            sweep_highlights=_backtest_sweep_highlights(artifact_previews),
            equity_summary=_equity_summary(artifact_previews.get("equity_curve")),
        ),
        "artifact_preview_order": [
            "equity_curve",
            "stake_sweep",
            "offset_summary",
            "factor_pnl",
        ],
    }
    return payload


def _build_backtest_stake_sweep_detail(path: Path) -> dict[str, object]:
    row = _build_backtest_run_row(path)
    stake_sweep_preview = _stake_sweep_preview(path / "stake_sweep.parquet")
    surface_summary = _stake_surface_summary(stake_sweep_preview)
    highlights = _backtest_stake_sweep_highlights(
        preview=stake_sweep_preview,
        performance_summary=row.get("performance_summary"),
    )
    chart_rows = _stake_sweep_chart_rows(path / "stake_sweep.parquet")
    return {
        "dataset": "console_backtest_stake_sweep_detail",
        **row,
        "exists": path.exists(),
        "summary": row.get("summary"),
        "stake_sweep_preview": stake_sweep_preview,
        "surface_summary": surface_summary,
        "highlights": highlights,
        "chart_rows": chart_rows,
        "rows_by_theme": {
            "sorted_by_stake": chart_rows,
            "top_by_roi": list(stake_sweep_preview.get("top_by_roi") or []),
            "top_by_pnl": list(stake_sweep_preview.get("top_by_pnl") or []),
        },
    }


def _artifact_map(path: Path) -> dict[str, dict[str, object]]:
    return {
        "summary": _artifact_payload(path / "summary.json"),
        "report": _artifact_payload(path / "report.md"),
        "manifest": _artifact_payload(path / "manifest.json"),
        "decisions": _artifact_payload(path / "decisions.parquet"),
        "trades": _artifact_payload(path / "trades.parquet"),
        "rejects": _artifact_payload(path / "rejects.parquet"),
        "markets": _artifact_payload(path / "markets.parquet"),
        "equity_curve": _artifact_payload(path / "equity_curve.parquet"),
        "stake_sweep": _artifact_payload(path / "stake_sweep.parquet"),
        "offset_summary": _artifact_payload(path / "offset_summary.parquet"),
        "factor_pnl": _artifact_payload(path / "factor_pnl.parquet"),
        "log": _artifact_payload(path / "logs" / "backtest.jsonl"),
    }


def _artifact_payload(path: Path) -> dict[str, object]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "size_bytes": int(path.stat().st_size) if path.exists() else 0,
        "updated_at": _updated_at(path) if path.exists() else None,
    }


def _artifact_previews(path: Path) -> dict[str, dict[str, object]]:
    return {
        "equity_curve": _equity_curve_preview(path / "equity_curve.parquet"),
        "stake_sweep": _stake_sweep_preview(path / "stake_sweep.parquet"),
        "offset_summary": _offset_summary_preview(path / "offset_summary.parquet"),
        "factor_pnl": _factor_pnl_preview(path / "factor_pnl.parquet"),
    }


def _equity_curve_preview(path: Path) -> dict[str, object]:
    frame = _read_parquet_frame(path)
    payload = _preview_base_payload(path, frame)
    if frame is None or frame.empty:
        return payload
    ordered = frame.sort_values(by=_first_present_column(frame, ("decision_ts", "trade_number")), kind="stable")
    latest = ordered.iloc[-1].to_dict()
    payload.update(
        {
            "preview_rows": _frame_rows(ordered.tail(8), limit=8),
            "latest_point": _json_safe_row(latest),
            "pnl_range": {
                "min": _safe_float(ordered.get("cumulative_pnl").min()) if "cumulative_pnl" in ordered else None,
                "max": _safe_float(ordered.get("cumulative_pnl").max()) if "cumulative_pnl" in ordered else None,
            },
            "equity_summary": _equity_summary_from_frame(ordered),
        }
    )
    return payload


def _stake_sweep_preview(path: Path) -> dict[str, object]:
    frame = _read_parquet_frame(path)
    payload = _preview_base_payload(path, frame)
    if frame is None or frame.empty:
        return payload
    ordered = _sort_frame(frame, by=("stake_usd", "roi_pct", "pnl_sum"), ascending=(True, False, False))
    by_roi = _sort_frame(frame, by=("roi_pct", "pnl_sum", "stake_usd"), ascending=(False, False, True))
    by_pnl = _sort_frame(frame, by=("pnl_sum", "roi_pct", "stake_usd"), ascending=(False, False, True))
    payload.update(
        {
            "preview_rows": _frame_rows(ordered, limit=12),
            "best_by_roi": _best_row(frame, sort_by=("roi_pct", "pnl_sum"), ascending=(False, False)),
            "best_by_pnl": _best_row(frame, sort_by=("pnl_sum", "roi_pct"), ascending=(False, False)),
            "top_by_roi": _frame_rows(by_roi, limit=5),
            "top_by_pnl": _frame_rows(by_pnl, limit=5),
            "surface_summary": _stake_surface_summary_from_frame(frame),
        }
    )
    return payload


def _stake_sweep_chart_rows(path: Path) -> list[dict[str, object]]:
    frame = _read_parquet_frame(path)
    if frame is None or frame.empty:
        return []
    ordered = _sort_frame(frame, by=("stake_usd", "roi_pct", "pnl_sum"), ascending=(True, False, False))
    selected = _select_present_columns(
        ordered,
        (
            "stake_usd",
            "max_notional_usd",
            "roi_pct",
            "avg_roi_pct",
            "pnl_sum",
            "stake_sum",
            "trades",
            "wins",
            "losses",
            "rejects",
            "variant_label",
            "feature_set",
        ),
    )
    return _frame_rows(selected, limit=len(selected))


def _offset_summary_preview(path: Path) -> dict[str, object]:
    frame = _read_parquet_frame(path)
    payload = _preview_base_payload(path, frame)
    if frame is None or frame.empty:
        return payload
    ordered = _sort_frame(frame, by=("offset", "pnl_sum"), ascending=(True, False))
    by_pnl = _sort_frame(frame, by=("pnl_sum", "avg_roi_pct", "offset"), ascending=(False, False, True))
    by_roi = _sort_frame(frame, by=("avg_roi_pct", "pnl_sum", "offset"), ascending=(False, False, True))
    payload.update(
        {
            "preview_rows": _frame_rows(ordered, limit=12),
            "best_by_pnl": _best_row(frame, sort_by=("pnl_sum", "avg_roi_pct"), ascending=(False, False)),
            "best_by_roi": _best_row(frame, sort_by=("avg_roi_pct", "pnl_sum"), ascending=(False, False)),
            "top_by_pnl": _frame_rows(by_pnl, limit=5),
            "top_by_roi": _frame_rows(by_roi, limit=5),
            "surface_summary": _offset_surface_summary_from_frame(frame),
        }
    )
    return payload


def _factor_pnl_preview(path: Path) -> dict[str, object]:
    frame = _read_parquet_frame(path)
    payload = _preview_base_payload(path, frame)
    if frame is None or frame.empty:
        return payload
    strongest = _sort_frame(frame, by=("abs_pnl_correlation", "pnl_sum"), ascending=(False, False))
    pnl_sum = _numeric_series(frame, "pnl_sum").fillna(0.0)
    pnl_correlation = _numeric_series(frame, "pnl_correlation")
    positive = _sort_frame(frame.loc[pnl_sum.gt(0.0)].copy(), by=("pnl_sum", "avg_roi_pct"), ascending=(False, False))
    negative = _sort_frame(frame.loc[pnl_sum.lt(0.0)].copy(), by=("pnl_sum",), ascending=(True,))
    positive_correlation = _sort_frame(
        frame.loc[pnl_correlation.gt(0.0).fillna(False)].copy(),
        by=("pnl_correlation", "pnl_sum"),
        ascending=(False, False),
    )
    negative_correlation = _sort_frame(
        frame.loc[pnl_correlation.lt(0.0).fillna(False)].copy(),
        by=("pnl_correlation", "pnl_sum"),
        ascending=(True, False),
    )
    payload.update(
        {
            "preview_rows": _frame_rows(strongest, limit=12),
            "top_positive": _frame_rows(positive, limit=5),
            "top_negative": _frame_rows(negative, limit=5),
            "strongest_correlations": _frame_rows(strongest, limit=5),
            "top_positive_correlation": _frame_rows(positive_correlation, limit=5),
            "top_negative_correlation": _frame_rows(negative_correlation, limit=5),
            "surface_summary": _factor_surface_summary_from_frame(frame),
        }
    )
    return payload


def _preview_base_payload(path: Path, frame: pd.DataFrame | None) -> dict[str, object]:
    artifact = _artifact_payload(path)
    return {
        **artifact,
        "row_count": 0 if frame is None else int(len(frame)),
        "columns": [] if frame is None else [str(column) for column in frame.columns],
        "preview_rows": [],
    }


def _backtest_result_summary(summary_payload: dict[str, object]) -> dict[str, object]:
    trades = _safe_int(summary_payload.get("trades"))
    wins = _safe_int(summary_payload.get("wins"))
    losses = _safe_int(summary_payload.get("losses"))
    rejects = _safe_int(summary_payload.get("rejects"))
    win_rate_pct = None
    if trades and wins is not None:
        win_rate_pct = round((float(wins) / float(trades)) * 100.0, 4)
    return {
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "rejects": rejects,
        "pnl_sum": _safe_float(summary_payload.get("pnl_sum")),
        "stake_sum": _safe_float(summary_payload.get("stake_sum")),
        "roi_pct": _safe_float(summary_payload.get("roi_pct")),
        "avg_roi_pct": _safe_float(summary_payload.get("avg_roi_pct")),
        "win_rate_pct": win_rate_pct,
    }


def _backtest_performance_summary(summary_payload: dict[str, object]) -> dict[str, object]:
    result_summary = _backtest_result_summary(summary_payload)
    trades = result_summary.get("trades")
    rejects = result_summary.get("rejects")
    considered = None
    reject_rate_pct = None
    if trades is not None or rejects is not None:
        considered = int((trades or 0) + (rejects or 0))
        if considered > 0 and rejects is not None:
            reject_rate_pct = round((float(rejects) / float(considered)) * 100.0, 4)
    return {
        **result_summary,
        "stake_usd": _safe_float(summary_payload.get("stake_usd")),
        "max_notional_usd": _safe_float(summary_payload.get("max_notional_usd")),
        "secondary_bundle_label": summary_payload.get("secondary_bundle_label"),
        "considered_signals": considered,
        "reject_rate_pct": reject_rate_pct,
    }


def _backtest_comparison_axes(summary_payload: dict[str, object]) -> dict[str, object]:
    parity = summary_payload.get("parity")
    parity_keys = []
    if isinstance(parity, dict):
        parity_keys = [str(key) for key in parity.keys()]
    fallback_reasons = summary_payload.get("fallback_reasons")
    return {
        "profile": summary_payload.get("profile"),
        "spec_name": summary_payload.get("spec_name"),
        "variant_label": summary_payload.get("variant_label"),
        "target": summary_payload.get("target"),
        "bundle_label": summary_payload.get("bundle_label"),
        "secondary_bundle_label": summary_payload.get("secondary_bundle_label"),
        "stake_usd": _safe_float(summary_payload.get("stake_usd")),
        "max_notional_usd": _safe_float(summary_payload.get("max_notional_usd")),
        "fallback_reasons": list(fallback_reasons) if isinstance(fallback_reasons, list) else fallback_reasons,
        "parity_keys": parity_keys,
    }


def _backtest_overview_cards(summary_payload: dict[str, object]) -> list[dict[str, object]]:
    result_summary = _backtest_result_summary(summary_payload)
    return [
        _card("pnl_sum", "PnL", result_summary.get("pnl_sum")),
        _card("roi_pct", "ROI %", result_summary.get("roi_pct")),
        _card("win_rate_pct", "Win Rate %", result_summary.get("win_rate_pct")),
        _card("trades", "Trades", result_summary.get("trades")),
        _card("rejects", "Rejects", result_summary.get("rejects")),
        _card("stake_usd", "Stake USD", _safe_float(summary_payload.get("stake_usd"))),
    ]


def _backtest_sweep_highlights(previews: dict[str, dict[str, object]]) -> dict[str, object]:
    stake_preview = previews.get("stake_sweep") if isinstance(previews, dict) else {}
    offset_preview = previews.get("offset_summary") if isinstance(previews, dict) else {}
    factor_preview = previews.get("factor_pnl") if isinstance(previews, dict) else {}
    equity_preview = previews.get("equity_curve") if isinstance(previews, dict) else {}
    best_stake_by_roi = stake_preview.get("best_by_roi") if isinstance(stake_preview, dict) else {}
    best_stake_by_pnl = stake_preview.get("best_by_pnl") if isinstance(stake_preview, dict) else {}
    best_offset_by_pnl = offset_preview.get("best_by_pnl") if isinstance(offset_preview, dict) else {}
    best_offset_by_roi = offset_preview.get("best_by_roi") if isinstance(offset_preview, dict) else {}
    latest_point = equity_preview.get("latest_point") if isinstance(equity_preview, dict) else {}
    top_positive = factor_preview.get("top_positive") if isinstance(factor_preview, dict) else []
    top_negative = factor_preview.get("top_negative") if isinstance(factor_preview, dict) else []
    top_positive_correlation = factor_preview.get("top_positive_correlation") if isinstance(factor_preview, dict) else []
    top_negative_correlation = factor_preview.get("top_negative_correlation") if isinstance(factor_preview, dict) else []
    return {
        "best_stake_by_roi": _first_row_value(best_stake_by_roi, "stake_usd"),
        "best_stake_roi_pct": _first_row_value(best_stake_by_roi, "roi_pct"),
        "best_stake_by_pnl": _first_row_value(best_stake_by_pnl, "stake_usd"),
        "best_stake_pnl_sum": _first_row_value(best_stake_by_pnl, "pnl_sum"),
        "best_offset_by_pnl": _first_row_value(best_offset_by_pnl, "offset"),
        "best_offset_pnl_sum": _first_row_value(best_offset_by_pnl, "pnl_sum"),
        "best_offset_by_roi": _first_row_value(best_offset_by_roi, "offset"),
        "best_offset_roi_pct": _first_row_value(best_offset_by_roi, "avg_roi_pct"),
        "top_positive_factor": _first_list_row_value(top_positive, "feature"),
        "top_negative_factor": _first_list_row_value(top_negative, "feature"),
        "top_positive_correlation_factor": _first_list_row_value(top_positive_correlation, "feature"),
        "top_negative_correlation_factor": _first_list_row_value(top_negative_correlation, "feature"),
        "latest_cumulative_pnl": _first_row_value(latest_point, "cumulative_pnl"),
    }


def _equity_summary(preview: object) -> dict[str, object]:
    if not isinstance(preview, dict):
        return {}
    summary = preview.get("equity_summary")
    return dict(summary) if isinstance(summary, dict) else {}


def _equity_summary_from_frame(frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty:
        return {}
    cumulative_pnl = _numeric_series(frame, "cumulative_pnl")
    if cumulative_pnl.empty or cumulative_pnl.notna().sum() == 0:
        return {}
    running_peak = cumulative_pnl.cummax()
    drawdown_abs = cumulative_pnl - running_peak
    peak_base = running_peak.replace(0.0, pd.NA)
    drawdown_pct = (drawdown_abs / peak_base) * 100.0
    latest = frame.iloc[-1].to_dict()
    return {
        "point_count": int(len(frame)),
        "latest_cumulative_pnl": _safe_float(latest.get("cumulative_pnl")),
        "latest_cumulative_roi_pct": _safe_float(latest.get("cumulative_roi_pct")),
        "latest_cumulative_trades": _safe_int(latest.get("cumulative_trades") or latest.get("trade_number")),
        "max_cumulative_pnl": _safe_float(cumulative_pnl.max()),
        "min_cumulative_pnl": _safe_float(cumulative_pnl.min()),
        "max_drawdown_abs": _safe_float(drawdown_abs.min()),
        "max_drawdown_pct": _safe_float(drawdown_pct.min()),
    }


def _stake_surface_summary(preview: object) -> dict[str, object]:
    if not isinstance(preview, dict):
        return {}
    summary = preview.get("surface_summary")
    return dict(summary) if isinstance(summary, dict) else {}


def _stake_surface_summary_from_frame(frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty:
        return {}
    stakes = _numeric_series(frame, "stake_usd")
    roi = _numeric_series(frame, "roi_pct")
    pnl = _numeric_series(frame, "pnl_sum")
    return {
        "row_count": int(len(frame)),
        "stake_min": _safe_float(stakes.min()),
        "stake_max": _safe_float(stakes.max()),
        "roi_min": _safe_float(roi.min()),
        "roi_max": _safe_float(roi.max()),
        "pnl_min": _safe_float(pnl.min()),
        "pnl_max": _safe_float(pnl.max()),
    }


def _offset_surface_summary(preview: object) -> dict[str, object]:
    if not isinstance(preview, dict):
        return {}
    summary = preview.get("surface_summary")
    return dict(summary) if isinstance(summary, dict) else {}


def _offset_surface_summary_from_frame(frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty:
        return {}
    roi = _numeric_series(frame, "avg_roi_pct")
    pnl = _numeric_series(frame, "pnl_sum")
    return {
        "row_count": int(len(frame)),
        "offset_min": _safe_int(_numeric_series(frame, "offset").min()),
        "offset_max": _safe_int(_numeric_series(frame, "offset").max()),
        "avg_roi_min": _safe_float(roi.min()),
        "avg_roi_max": _safe_float(roi.max()),
        "pnl_min": _safe_float(pnl.min()),
        "pnl_max": _safe_float(pnl.max()),
    }


def _factor_surface_summary(preview: object) -> dict[str, object]:
    if not isinstance(preview, dict):
        return {}
    summary = preview.get("surface_summary")
    return dict(summary) if isinstance(summary, dict) else {}


def _factor_surface_summary_from_frame(frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty:
        return {}
    pnl_sum = _numeric_series(frame, "pnl_sum").fillna(0.0)
    pnl_correlation = _numeric_series(frame, "pnl_correlation")
    return {
        "row_count": int(len(frame)),
        "positive_factor_count": int(pnl_sum.gt(0.0).sum()),
        "negative_factor_count": int(pnl_sum.lt(0.0).sum()),
        "positive_correlation_count": int(pnl_correlation.gt(0.0).sum()),
        "negative_correlation_count": int(pnl_correlation.lt(0.0).sum()),
        "best_factor_pnl_sum": _safe_float(pnl_sum.max()),
        "worst_factor_pnl_sum": _safe_float(pnl_sum.min()),
        "strongest_abs_correlation": _safe_float(_numeric_series(frame, "abs_pnl_correlation").max()),
    }


def _backtest_decision_summary(
    *,
    performance_summary: object,
    sweep_highlights: object,
    equity_summary: object,
) -> dict[str, object]:
    performance = dict(performance_summary) if isinstance(performance_summary, dict) else {}
    highlights = dict(sweep_highlights) if isinstance(sweep_highlights, dict) else {}
    equity = dict(equity_summary) if isinstance(equity_summary, dict) else {}
    return {
        "roi_pct": performance.get("roi_pct"),
        "win_rate_pct": performance.get("win_rate_pct"),
        "reject_rate_pct": performance.get("reject_rate_pct"),
        "best_stake_by_roi": highlights.get("best_stake_by_roi"),
        "best_offset_by_pnl": highlights.get("best_offset_by_pnl"),
        "top_positive_factor": highlights.get("top_positive_factor"),
        "top_negative_factor": highlights.get("top_negative_factor"),
        "max_drawdown_abs": equity.get("max_drawdown_abs"),
        "max_drawdown_pct": equity.get("max_drawdown_pct"),
        "latest_cumulative_pnl": equity.get("latest_cumulative_pnl") or highlights.get("latest_cumulative_pnl"),
    }


def _backtest_stake_sweep_highlights(
    *,
    preview: dict[str, object],
    performance_summary: object,
) -> dict[str, object]:
    best_by_roi = preview.get("best_by_roi") if isinstance(preview, dict) else None
    best_by_pnl = preview.get("best_by_pnl") if isinstance(preview, dict) else None
    performance = dict(performance_summary) if isinstance(performance_summary, dict) else {}
    return {
        "best_by_roi": dict(best_by_roi) if isinstance(best_by_roi, dict) else None,
        "best_by_pnl": dict(best_by_pnl) if isinstance(best_by_pnl, dict) else None,
        "roi_pct": performance.get("roi_pct"),
        "win_rate_pct": performance.get("win_rate_pct"),
        "reject_rate_pct": performance.get("reject_rate_pct"),
        "stake_usd": performance.get("stake_usd"),
        "max_notional_usd": performance.get("max_notional_usd"),
    }


def _card(card_id: str, label: str, value: object) -> dict[str, object]:
    return {
        "card_id": card_id,
        "label": label,
        "value": value,
    }


def _read_parquet_frame(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def _sort_frame(
    frame: pd.DataFrame,
    *,
    by: tuple[str, ...],
    ascending: tuple[bool, ...],
) -> pd.DataFrame:
    available = [column for column in by if column in frame.columns]
    if not available:
        return frame
    resolved_ascending = [ascending[idx] for idx, column in enumerate(by) if column in frame.columns]
    return frame.sort_values(by=available, ascending=resolved_ascending, kind="stable")


def _best_row(
    frame: pd.DataFrame,
    *,
    sort_by: tuple[str, ...],
    ascending: tuple[bool, ...],
) -> dict[str, object] | None:
    ordered = _sort_frame(frame, by=sort_by, ascending=ascending)
    if ordered.empty:
        return None
    return _json_safe_row(ordered.iloc[0].to_dict())


def _frame_rows(frame: pd.DataFrame, *, limit: int) -> list[dict[str, object]]:
    if frame.empty:
        return []
    return [_json_safe_row(row) for row in frame.head(max(int(limit), 0)).to_dict(orient="records")]


def _select_present_columns(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
) -> pd.DataFrame:
    selected_columns = [column for column in columns if column in frame.columns]
    if not selected_columns:
        return frame
    return frame.loc[:, selected_columns].copy()


def _first_row_value(row: object, key: str) -> object:
    if not isinstance(row, dict):
        return None
    return row.get(key)


def _first_list_row_value(rows: object, key: str) -> object:
    if not isinstance(rows, list) or not rows:
        return None
    first = rows[0]
    return _first_row_value(first, key)


def _json_safe_row(row: dict[str, object]) -> dict[str, object]:
    return {str(key): _json_safe_value(value) for key, value in row.items()}


def _json_safe_value(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return _json_safe_value(value.item())
        except Exception:
            pass
    return str(value)


def _first_present_column(frame: pd.DataFrame, candidates: tuple[str, ...]) -> str:
    for candidate in candidates:
        if candidate in frame.columns:
            return candidate
    return frame.columns[0]


def _safe_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


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


def _slug(value: str) -> str:
    return str(value or "").strip().lower().replace(" ", "_")
