from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from .leaderboard import build_leaderboard_cut


GROUP_SUMMARY_COLUMNS = [
    "market",
    "group_name",
    "cases",
    "completed_cases",
    "failed_cases",
    "pending_cases",
    "resumed_cases",
    "reused_cases",
    "avg_roi_pct",
    "best_run_name",
    "best_variant_label",
    "best_roi_pct",
    "total_pnl_sum",
    "total_trades",
]

MATRIX_SUMMARY_COLUMNS = [
    "market",
    "group_name",
    "matrix_parent_run_name",
    "target",
    "cases",
    "completed_cases",
    "failed_cases",
    "pending_cases",
    "resumed_cases",
    "reused_cases",
    "avg_roi_pct",
    "stake_usd_values",
    "max_notional_usd_values",
    "best_run_name",
    "best_matrix_stake_label",
    "best_variant_label",
    "best_roi_pct",
    "total_pnl_sum",
    "total_trades",
]

RUN_SUMMARY_COLUMNS = [
    "market",
    "group_name",
    "run_name",
    "target",
    "cases",
    "completed_cases",
    "failed_cases",
    "pending_cases",
    "resumed_cases",
    "reused_cases",
    "avg_roi_pct",
    "best_variant_label",
    "best_roi_pct",
    "total_pnl_sum",
    "total_trades",
]

FOCUS_CASE_COLUMNS = [
    "rank",
    "market",
    "group_name",
    "run_name",
    "target",
    "variant_label",
    "roi_pct",
    "pnl_sum",
    "trades",
    "resumed_from_existing",
]

MATRIX_FOCUS_COLUMNS = [
    "rank",
    "market",
    "group_name",
    "matrix_parent_run_name",
    "matrix_stake_label",
    "run_name",
    "target",
    "variant_label",
    "roi_pct",
    "pnl_sum",
    "trades",
    "stake_usd",
    "max_notional_usd",
    "resumed_from_existing",
]

RUNTIME_NOTE_COLUMNS = [
    "market",
    "group_name",
    "matrix_parent_run_name",
    "matrix_stake_label",
    "run_name",
    "variant_label",
    "stake_usd",
    "max_notional_usd",
    "status",
    "training_reused",
    "bundle_reused",
    "secondary_training_reused",
    "secondary_bundle_reused",
    "resumed_from_existing",
]

FACTOR_SIGNAL_CASE_COLUMNS = [
    "rank",
    "case_key",
    "market",
    "group_name",
    "run_name",
    "feature_set",
    "variant_label",
    "target",
    "trades",
    "pnl_sum",
    "roi_pct",
]

FACTOR_SIGNAL_COLUMNS = [
    "feature",
    "hits",
    "case_hits",
    "offsets",
    "avg_direction_score",
    "avg_case_roi_pct",
    "avg_case_pnl_sum",
]


def build_experiment_compare_frame(
    *,
    training_runs: pd.DataFrame,
    backtest_runs: pd.DataFrame,
    failed_cases: pd.DataFrame | None = None,
) -> pd.DataFrame:
    training = training_runs.copy() if not training_runs.empty else pd.DataFrame()
    backtests = backtest_runs.copy() if not backtest_runs.empty else pd.DataFrame()
    failures = failed_cases.copy() if failed_cases is not None and not failed_cases.empty else pd.DataFrame()

    if training.empty and backtests.empty and failures.empty:
        return pd.DataFrame(
            columns=[
                "case_key",
                "market",
                "group_name",
                "matrix_parent_run_name",
                "matrix_stake_label",
                "run_name",
                "feature_set",
                "variant_label",
                "variant_notes",
                "profile",
                "target",
                "tags_json",
                "stake_usd",
                "max_notional_usd",
                "training_run_dir",
                "bundle_dir",
                "backtest_run_dir",
                "summary_path",
                "training_reused",
                "bundle_reused",
                "secondary_training_reused",
                "secondary_bundle_reused",
                "resumed_from_existing",
                "status",
                "failure_stage",
                "error_type",
                "error_message",
                "trades",
                "rejects",
                "wins",
                "losses",
                "pnl_sum",
                "stake_sum",
                "roi_pct",
            ]
        )

    if not training.empty:
        training = training.rename(
            columns={
                "resumed_from_existing": "training_resumed_from_existing",
            }
        )
    if not backtests.empty:
        backtests = backtests.rename(
            columns={
                "resumed_from_existing": "backtest_resumed_from_existing",
            }
        )

    if training.empty and backtests.empty:
        frame = pd.DataFrame(columns=["case_key"])
    elif training.empty:
        frame = backtests.copy()
    elif backtests.empty:
        frame = training.copy()
    else:
        frame = training.merge(backtests, on="case_key", how="outer", suffixes=("_train", ""))

    if not failures.empty:
        failure_cols = [
            "case_key",
            "market",
            "group_name",
            "matrix_parent_run_name",
            "matrix_stake_label",
            "run_name",
            "variant_label",
            "variant_notes",
            "profile",
            "target",
            "tags_json",
            "stake_usd",
            "max_notional_usd",
            "training_run_dir",
            "bundle_dir",
            "secondary_training_run_dir",
            "secondary_bundle_dir",
            "status",
            "failure_stage",
            "error_type",
            "error_message",
        ]
        frame = frame.merge(
            failures.loc[:, [col for col in failure_cols if col in failures.columns]],
            on="case_key",
            how="outer",
            suffixes=("", "_failed"),
        )

    frame["market"] = _coalesce_many(frame, "market", "market_train", "market_failed")
    frame["group_name"] = _coalesce_many(frame, "group_name", "group_name_train", "group_name_failed")
    frame["matrix_parent_run_name"] = _coalesce_many(
        frame,
        "matrix_parent_run_name",
        "matrix_parent_run_name_train",
        "matrix_parent_run_name_failed",
    )
    frame["matrix_stake_label"] = _coalesce_many(
        frame,
        "matrix_stake_label",
        "matrix_stake_label_train",
        "matrix_stake_label_failed",
    )
    frame["run_name"] = _coalesce_many(frame, "run_name", "run_name_train", "run_name_failed")
    frame["feature_set"] = _coalesce_many(frame, "feature_set", "feature_set_train", "feature_set_failed")
    frame["variant_label"] = _coalesce_many(
        frame,
        "variant_label",
        "variant_label_train",
        "variant_label_failed",
    ).replace("", "default")
    frame["variant_notes"] = _coalesce_many(frame, "variant_notes", "variant_notes_train", "variant_notes_failed")
    frame["profile"] = _coalesce_many(frame, "profile", "profile_train", "profile_failed")
    frame["target"] = _coalesce_many(frame, "target", "target_train", "target_failed")
    frame["tags_json"] = _coalesce_many(frame, "tags_json", "tags_json_train", "tags_json_failed")
    frame["stake_usd"] = _coalesce_many(frame, "stake_usd", "stake_usd_train", "stake_usd_failed")
    frame["max_notional_usd"] = _coalesce_many(
        frame,
        "max_notional_usd",
        "max_notional_usd_train",
        "max_notional_usd_failed",
    )
    frame["training_run_dir"] = _coalesce_many(frame, "training_run_dir", "training_run_dir_failed")
    frame["bundle_dir"] = _coalesce_many(frame, "bundle_dir", "bundle_dir_train", "bundle_dir_failed")
    frame["backtest_run_dir"] = _coalesce_many(frame, "backtest_run_dir")
    frame["summary_path"] = _coalesce_many(frame, "summary_path")
    frame["training_reused"] = _bool_coalesce(frame, "training_reused")
    frame["bundle_reused"] = _bool_coalesce(frame, "bundle_reused")
    frame["secondary_training_reused"] = _bool_coalesce(frame, "secondary_training_reused")
    frame["secondary_bundle_reused"] = _bool_coalesce(frame, "secondary_bundle_reused")
    frame["resumed_from_existing"] = _bool_or(
        frame,
        "training_resumed_from_existing",
        "backtest_resumed_from_existing",
        "resumed_from_existing",
        "resumed_from_existing_failed",
    )
    frame["status"] = _coalesce_many(frame, "status", "status_failed").astype("string").fillna("")
    frame["failure_stage"] = _coalesce_many(frame, "failure_stage", "failure_stage_failed")
    frame["error_type"] = _coalesce_many(frame, "error_type", "error_type_failed")
    frame["error_message"] = _coalesce_many(frame, "error_message", "error_message_failed")
    failure_mask = _coalesce_many(frame, "failure_stage", "error_type", "error_message").astype("string").fillna("").ne("")
    frame.loc[frame["status"].eq("") & failure_mask, "status"] = "failed"
    frame.loc[frame["status"].eq("") & frame["summary_path"].astype("string").fillna("").ne(""), "status"] = "completed"
    frame.loc[frame["status"].eq(""), "status"] = "pending"

    order = [
        "case_key",
        "market",
        "group_name",
        "matrix_parent_run_name",
        "matrix_stake_label",
        "run_name",
        "feature_set",
        "variant_label",
        "variant_notes",
        "profile",
        "target",
        "tags_json",
        "stake_usd",
        "max_notional_usd",
        "training_run_dir",
        "bundle_dir",
        "backtest_run_dir",
        "summary_path",
        "training_reused",
        "bundle_reused",
        "secondary_training_reused",
        "secondary_bundle_reused",
        "resumed_from_existing",
        "status",
        "failure_stage",
        "error_type",
        "error_message",
        "trades",
        "rejects",
        "wins",
        "losses",
        "pnl_sum",
        "stake_sum",
        "roi_pct",
    ]
    for column in order:
        if column not in frame.columns:
            frame[column] = pd.NA
    return (
        frame.loc[:, order]
        .sort_values(["market", "group_name", "run_name", "feature_set", "variant_label"], kind="stable")
        .reset_index(drop=True)
    )


def build_experiment_summary(
    *,
    suite_name: str,
    run_label: str,
    training_runs: pd.DataFrame,
    backtest_runs: pd.DataFrame,
    leaderboard: pd.DataFrame,
    compare_frame: pd.DataFrame,
    failed_cases: pd.DataFrame | None = None,
) -> dict[str, object]:
    failed = failed_cases if failed_cases is not None else pd.DataFrame()
    group_summary = build_group_summary_frame(compare_frame)
    matrix_summary = build_matrix_summary_frame(compare_frame)
    run_summary = build_run_summary_frame(compare_frame)
    factor_signal_summary = build_factor_signal_summary(compare_frame)
    return {
        "suite_name": suite_name,
        "run_label": run_label,
        "cases": int(len(compare_frame)),
        "groups": int(len(group_summary)),
        "matrices": int(len(matrix_summary)),
        "runs": int(len(run_summary)),
        "completed_cases": int(compare_frame.get("status", pd.Series(dtype="string")).astype("string").eq("completed").sum()) if not compare_frame.empty else 0,
        "failed_cases": int(len(failed)),
        "resumed_cases": int(_bool_series(compare_frame, "resumed_from_existing").sum()) if not compare_frame.empty else 0,
        "training_reused_cases": int(_bool_series(compare_frame, "training_reused").sum()) if not compare_frame.empty else 0,
        "bundle_reused_cases": int(_bool_series(compare_frame, "bundle_reused").sum()) if not compare_frame.empty else 0,
        "secondary_training_reused_cases": int(_bool_series(compare_frame, "secondary_training_reused").sum()) if not compare_frame.empty else 0,
        "secondary_bundle_reused_cases": int(_bool_series(compare_frame, "secondary_bundle_reused").sum()) if not compare_frame.empty else 0,
        "markets": sorted({str(value) for value in compare_frame.get("market", pd.Series(dtype="string")).astype("string").fillna("").tolist() if str(value)}),
        "feature_sets": sorted({str(value) for value in compare_frame.get("feature_set", pd.Series(dtype="string")).astype("string").fillna("").tolist() if str(value)}),
        "leaderboard_rows": int(len(leaderboard)),
        "top_roi_pct": None if leaderboard.empty else float(pd.to_numeric(leaderboard["roi_pct"], errors="coerce").fillna(0.0).iloc[0]),
        "training_rows": int(len(training_runs)),
        "backtest_rows": int(len(backtest_runs)),
        "factor_signal_summary": factor_signal_summary,
    }


def render_experiment_report(
    summary: dict[str, object],
    *,
    leaderboard: pd.DataFrame,
    compare_frame: pd.DataFrame,
    failed_cases: pd.DataFrame | None = None,
) -> str:
    group_summary = build_group_summary_frame(compare_frame)
    matrix_summary = build_matrix_summary_frame(compare_frame)
    run_summary = build_run_summary_frame(compare_frame)
    top_cases = build_top_cases_frame(leaderboard)
    best_by_market = build_best_by_market_frame(leaderboard if leaderboard is not None and not leaderboard.empty else compare_frame)
    best_by_group = build_best_by_group_frame(leaderboard if leaderboard is not None and not leaderboard.empty else compare_frame)
    best_by_matrix = build_best_by_matrix_frame(leaderboard if leaderboard is not None and not leaderboard.empty else compare_frame)
    best_by_run = build_best_by_run_frame(leaderboard if leaderboard is not None and not leaderboard.empty else compare_frame)
    runtime_notes = build_runtime_notes_frame(compare_frame)
    factor_signal_summary = summary.get("factor_signal_summary") if isinstance(summary.get("factor_signal_summary"), dict) else {}
    lines = [
        "# Experiment Summary",
        "",
        f"- suite_name: `{summary.get('suite_name')}`",
        f"- run_label: `{summary.get('run_label')}`",
        f"- cases: `{summary.get('cases')}`",
        f"- groups: `{summary.get('groups', len(group_summary))}`",
        f"- matrices: `{summary.get('matrices', len(matrix_summary))}`",
        f"- runs: `{summary.get('runs', len(run_summary))}`",
        f"- completed_cases: `{summary.get('completed_cases')}`",
        f"- failed_cases: `{summary.get('failed_cases')}`",
        f"- resumed_cases: `{summary.get('resumed_cases')}`",
        f"- training_reused_cases: `{summary.get('training_reused_cases')}`",
        f"- bundle_reused_cases: `{summary.get('bundle_reused_cases')}`",
        f"- secondary_training_reused_cases: `{summary.get('secondary_training_reused_cases')}`",
        f"- secondary_bundle_reused_cases: `{summary.get('secondary_bundle_reused_cases')}`",
        f"- markets: `{summary.get('markets')}`",
        "",
        "## Top Cases",
        "",
    ]
    if not top_cases.empty:
        lines.append(_render_markdown_table(top_cases, FOCUS_CASE_COLUMNS))
    else:
        lines.append("No top cases available.")
    lines.extend(["", "## Factor Signals From Good Cases", ""])
    if int(factor_signal_summary.get("selected_case_count") or 0) > 0:
        lines.append(f"- selection_mode: `{factor_signal_summary.get('selection_mode')}`")
        lines.append(f"- selected_case_count: `{factor_signal_summary.get('selected_case_count')}`")
        lines.extend(["", "### Source Cases", ""])
        lines.append(
            _render_markdown_table(
                pd.DataFrame(list(factor_signal_summary.get("selected_cases") or [])),
                FACTOR_SIGNAL_CASE_COLUMNS,
            )
        )
        lines.extend(["", "### Positive Factors", ""])
        positive_factors = pd.DataFrame(list(factor_signal_summary.get("positive_factors") or []))
        if not positive_factors.empty:
            lines.append(_render_markdown_table(positive_factors, FACTOR_SIGNAL_COLUMNS))
        else:
            lines.append("No positive factors collected.")
        lines.extend(["", "### Negative Factors", ""])
        negative_factors = pd.DataFrame(list(factor_signal_summary.get("negative_factors") or []))
        if not negative_factors.empty:
            lines.append(_render_markdown_table(negative_factors, FACTOR_SIGNAL_COLUMNS))
        else:
            lines.append("No negative factors collected.")
    else:
        lines.append("No factor signal summary available.")
    lines.extend(["", "## Group Summary", ""])
    if not group_summary.empty:
        lines.append(
            _render_markdown_table(
                group_summary,
                [
                    "market",
                    "group_name",
                    "cases",
                    "completed_cases",
                    "failed_cases",
                    "avg_roi_pct",
                    "best_run_name",
                    "best_variant_label",
                    "best_roi_pct",
                    "total_pnl_sum",
                    "total_trades",
                ],
            )
        )
    else:
        lines.append("No group summaries available.")
    lines.extend(["", "## Matrix Summary", ""])
    if not matrix_summary.empty:
        lines.append(
            _render_markdown_table(
                matrix_summary,
                [
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
                    "best_matrix_stake_label",
                    "best_variant_label",
                    "best_roi_pct",
                    "total_pnl_sum",
                    "total_trades",
                ],
            )
        )
    else:
        lines.append("No matrix summaries available.")
    lines.extend(["", "## Run Summary", ""])
    if not run_summary.empty:
        lines.append(
            _render_markdown_table(
                run_summary,
                [
                    "market",
                    "group_name",
                    "run_name",
                    "target",
                    "cases",
                    "completed_cases",
                    "failed_cases",
                    "avg_roi_pct",
                    "best_variant_label",
                    "best_roi_pct",
                    "total_pnl_sum",
                    "total_trades",
                ],
            )
        )
    else:
        lines.append("No run summaries available.")
    lines.extend(["", "## Focus Cuts", "", "### Best Per Market", ""])
    if not best_by_market.empty:
        lines.append(_render_markdown_table(best_by_market, FOCUS_CASE_COLUMNS))
    else:
        lines.append("No market focus cuts available.")
    lines.extend(["", "### Best Per Group", ""])
    if not best_by_group.empty:
        lines.append(_render_markdown_table(best_by_group, FOCUS_CASE_COLUMNS))
    else:
        lines.append("No group focus cuts available.")
    lines.extend(["", "### Best Variant Per Matrix", ""])
    if not best_by_matrix.empty:
        lines.append(_render_markdown_table(best_by_matrix, MATRIX_FOCUS_COLUMNS))
    else:
        lines.append("No matrix focus cuts available.")
    lines.extend(["", "### Best Variant Per Run", ""])
    if not best_by_run.empty:
        lines.append(_render_markdown_table(best_by_run, FOCUS_CASE_COLUMNS))
    else:
        lines.append("No per-run comparisons available.")
    lines.extend(["", "## Runtime Notes", ""])
    if not runtime_notes.empty:
        lines.append(_render_markdown_table(runtime_notes, RUNTIME_NOTE_COLUMNS))
    else:
        lines.append("No notable runtime flags.")
    lines.extend(["", "## Failures", ""])
    if failed_cases is not None and not failed_cases.empty:
        fail_cols = [
            column
            for column in ("market", "group_name", "run_name", "variant_label", "failure_stage", "error_type", "error_message")
            if column in failed_cases.columns
        ]
        lines.append(_render_markdown_table(failed_cases.loc[:, fail_cols].fillna(""), fail_cols))
    else:
        lines.append("No failed cases.")
    lines.append("")
    return "\n".join(lines)


def build_group_summary_frame(compare_frame: pd.DataFrame) -> pd.DataFrame:
    return _build_summary_frame(
        compare_frame,
        group_keys=["market", "group_name"],
        columns=GROUP_SUMMARY_COLUMNS,
        best_frame=build_best_by_group_frame(compare_frame).rename(
            columns={
                "run_name": "best_run_name",
                "variant_label": "best_variant_label",
                "roi_pct": "best_roi_pct",
            }
        ),
    )


def build_matrix_summary_frame(compare_frame: pd.DataFrame) -> pd.DataFrame:
    if compare_frame is None or compare_frame.empty:
        return pd.DataFrame(columns=MATRIX_SUMMARY_COLUMNS)
    frame = _matrix_source_frame(compare_frame)
    if frame.empty:
        return pd.DataFrame(columns=MATRIX_SUMMARY_COLUMNS)
    frame = _normalize_key_columns(frame, ["market", "group_name", "matrix_parent_run_name", "target"])
    frame["status"] = frame.get("status", pd.Series("", index=frame.index, dtype="string")).astype("string").fillna("")
    frame["roi_pct"] = pd.to_numeric(frame.get("roi_pct", pd.Series(pd.NA, index=frame.index)), errors="coerce")
    frame["pnl_sum"] = pd.to_numeric(frame.get("pnl_sum", pd.Series(0.0, index=frame.index)), errors="coerce").fillna(0.0)
    frame["trades"] = pd.to_numeric(frame.get("trades", pd.Series(0, index=frame.index)), errors="coerce").fillna(0).astype(int)
    frame["stake_usd"] = pd.to_numeric(frame.get("stake_usd", pd.Series(pd.NA, index=frame.index)), errors="coerce")
    frame["max_notional_usd"] = pd.to_numeric(frame.get("max_notional_usd", pd.Series(pd.NA, index=frame.index)), errors="coerce")
    frame["_reused"] = _reused_case_mask(frame)

    best_by_matrix = build_best_by_matrix_frame(compare_frame).rename(
        columns={
            "run_name": "best_run_name",
            "matrix_stake_label": "best_matrix_stake_label",
            "variant_label": "best_variant_label",
            "roi_pct": "best_roi_pct",
        }
    )
    rows: list[dict[str, object]] = []
    group_keys = ["market", "group_name", "matrix_parent_run_name", "target"]
    for key_values, group in frame.groupby(group_keys, sort=False, dropna=False):
        values = key_values if isinstance(key_values, tuple) else (key_values,)
        row = {key: value for key, value in zip(group_keys, values)}
        completed = group.loc[group["status"].eq("completed")].copy()
        failed = group.loc[group["status"].eq("failed")]
        avg_roi = pd.to_numeric(completed["roi_pct"], errors="coerce").dropna()
        row.update(
            {
                "cases": int(len(group)),
                "completed_cases": int(len(completed)),
                "failed_cases": int(len(failed)),
                "pending_cases": int(len(group) - len(completed) - len(failed)),
                "resumed_cases": int(_bool_series(group, "resumed_from_existing").sum()),
                "reused_cases": int(group["_reused"].sum()),
                "avg_roi_pct": None if avg_roi.empty else float(avg_roi.mean()),
                "stake_usd_values": _sorted_numeric_values(group["stake_usd"]),
                "max_notional_usd_values": _sorted_numeric_values(group["max_notional_usd"]),
                "total_pnl_sum": float(completed["pnl_sum"].sum()) if not completed.empty else 0.0,
                "total_trades": int(completed["trades"].sum()) if not completed.empty else 0,
            }
        )
        rows.append(row)

    summary = pd.DataFrame(rows)
    if not best_by_matrix.empty:
        best_by_matrix = _normalize_key_columns(best_by_matrix, group_keys)
        summary = summary.merge(
            best_by_matrix.loc[
                :,
                [
                    *group_keys,
                    *[
                        column
                        for column in ("best_run_name", "best_matrix_stake_label", "best_variant_label", "best_roi_pct")
                        if column in best_by_matrix.columns
                    ],
                ],
            ],
            on=group_keys,
            how="left",
        )
    for column in MATRIX_SUMMARY_COLUMNS:
        if column not in summary.columns:
            summary[column] = pd.NA
    return (
        summary.loc[:, MATRIX_SUMMARY_COLUMNS]
        .sort_values(
            ["best_roi_pct", "total_pnl_sum", "market", "group_name", "matrix_parent_run_name", "target"],
            ascending=[False, False, True, True, True, True],
            kind="stable",
            na_position="last",
        )
        .reset_index(drop=True)
    )


def build_run_summary_frame(compare_frame: pd.DataFrame) -> pd.DataFrame:
    return _build_summary_frame(
        compare_frame,
        group_keys=["market", "group_name", "run_name", "target"],
        columns=RUN_SUMMARY_COLUMNS,
        best_frame=build_best_by_run_frame(compare_frame).rename(
            columns={
                "variant_label": "best_variant_label",
                "roi_pct": "best_roi_pct",
            }
        ),
    )


def build_top_cases_frame(source: pd.DataFrame, *, limit: int = 5) -> pd.DataFrame:
    return build_leaderboard_cut(_focus_source_frame(source), limit=limit)


def build_best_by_market_frame(source: pd.DataFrame) -> pd.DataFrame:
    return build_leaderboard_cut(_focus_source_frame(source), partition_by=("market",), limit=1)


def build_best_by_group_frame(source: pd.DataFrame) -> pd.DataFrame:
    return build_leaderboard_cut(_focus_source_frame(source), partition_by=("market", "group_name"), limit=1)


def build_best_by_matrix_frame(source: pd.DataFrame) -> pd.DataFrame:
    return build_leaderboard_cut(
        _matrix_source_frame(_focus_source_frame(source)),
        partition_by=("market", "group_name", "matrix_parent_run_name", "target"),
        limit=1,
    )


def build_best_by_run_frame(source: pd.DataFrame) -> pd.DataFrame:
    return build_leaderboard_cut(
        _focus_source_frame(source),
        partition_by=("market", "group_name", "run_name", "target"),
        limit=1,
    )


def build_runtime_notes_frame(compare_frame: pd.DataFrame) -> pd.DataFrame:
    if compare_frame is None or compare_frame.empty:
        return pd.DataFrame(columns=RUNTIME_NOTE_COLUMNS)
    frame = compare_frame.copy()
    interesting = (
        frame.get("status", pd.Series("", index=frame.index, dtype="string")).astype("string").fillna("").ne("completed")
        | _bool_series(frame, "training_reused").reindex(frame.index, fill_value=False)
        | _bool_series(frame, "bundle_reused").reindex(frame.index, fill_value=False)
        | _bool_series(frame, "secondary_training_reused").reindex(frame.index, fill_value=False)
        | _bool_series(frame, "secondary_bundle_reused").reindex(frame.index, fill_value=False)
        | _bool_series(frame, "resumed_from_existing").reindex(frame.index, fill_value=False)
    )
    notes = frame.loc[interesting].copy()
    if notes.empty:
        return pd.DataFrame(columns=RUNTIME_NOTE_COLUMNS)
    for column in RUNTIME_NOTE_COLUMNS:
        if column not in notes.columns:
            notes[column] = pd.NA
    return (
        notes.loc[:, RUNTIME_NOTE_COLUMNS]
        .sort_values(["market", "group_name", "run_name", "variant_label"], kind="stable")
        .reset_index(drop=True)
    )


def build_factor_signal_summary(compare_frame: pd.DataFrame, *, limit: int = 5) -> dict[str, object]:
    empty = {
        "selection_mode": "none",
        "selected_case_count": 0,
        "selected_cases": [],
        "positive_factors": [],
        "negative_factors": [],
    }
    if compare_frame is None or compare_frame.empty:
        return empty
    source = _focus_source_frame(compare_frame)
    if source.empty:
        return empty

    source = source.copy()
    source["trades"] = pd.to_numeric(source.get("trades", pd.Series(0, index=source.index)), errors="coerce").fillna(0).astype(int)
    source["pnl_sum"] = pd.to_numeric(source.get("pnl_sum", pd.Series(0.0, index=source.index)), errors="coerce").fillna(0.0)
    source["roi_pct"] = pd.to_numeric(source.get("roi_pct", pd.Series(0.0, index=source.index)), errors="coerce").fillna(0.0)

    profitable_traded = source.loc[source["trades"].gt(0) & source["pnl_sum"].gt(0.0)].copy()
    if not profitable_traded.empty:
        selection_mode = "profitable_traded"
        selected_source = profitable_traded
    else:
        traded = source.loc[source["trades"].gt(0)].copy()
        if not traded.empty:
            selection_mode = "traded"
            selected_source = traded
        else:
            selection_mode = "completed"
            selected_source = source

    selected_cases = build_top_cases_frame(selected_source, limit=limit)
    if selected_cases.empty:
        return {**empty, "selection_mode": selection_mode}

    selected_case_rows: list[dict[str, object]] = []
    positive = _aggregate_case_factor_rows(selected_cases, row_key="top_positive_factors")
    negative = _aggregate_case_factor_rows(selected_cases, row_key="top_negative_factors")
    for row in selected_cases.reset_index(drop=True).itertuples(index=False):
        selected_case_rows.append(
            {
                "rank": int(len(selected_case_rows) + 1),
                "case_key": _optional_text(getattr(row, "case_key", "")),
                "market": _optional_text(getattr(row, "market", "")),
                "group_name": _optional_text(getattr(row, "group_name", "")),
                "run_name": _optional_text(getattr(row, "run_name", "")),
                "feature_set": _optional_text(getattr(row, "feature_set", "")),
                "variant_label": _optional_text(getattr(row, "variant_label", "")),
                "target": _optional_text(getattr(row, "target", "")),
                "trades": _optional_int(getattr(row, "trades", 0)),
                "pnl_sum": _optional_float(getattr(row, "pnl_sum", 0.0)),
                "roi_pct": _optional_float(getattr(row, "roi_pct", 0.0)),
            }
        )

    return {
        "selection_mode": selection_mode,
        "selected_case_count": int(len(selected_case_rows)),
        "selected_cases": selected_case_rows,
        "positive_factors": positive,
        "negative_factors": negative,
    }


def _build_summary_frame(
    compare_frame: pd.DataFrame,
    *,
    group_keys: list[str],
    columns: list[str],
    best_frame: pd.DataFrame,
) -> pd.DataFrame:
    if compare_frame is None or compare_frame.empty:
        return pd.DataFrame(columns=columns)
    frame = compare_frame.copy()
    frame = _normalize_key_columns(frame, group_keys)
    frame["status"] = frame.get("status", pd.Series("", index=frame.index, dtype="string")).astype("string").fillna("")
    frame["roi_pct"] = pd.to_numeric(frame.get("roi_pct", pd.Series(pd.NA, index=frame.index)), errors="coerce")
    frame["pnl_sum"] = pd.to_numeric(frame.get("pnl_sum", pd.Series(0.0, index=frame.index)), errors="coerce").fillna(0.0)
    frame["trades"] = pd.to_numeric(frame.get("trades", pd.Series(0, index=frame.index)), errors="coerce").fillna(0).astype(int)
    frame["_reused"] = _reused_case_mask(frame)

    rows: list[dict[str, object]] = []
    for key_values, group in frame.groupby(group_keys, sort=False, dropna=False):
        values = key_values if isinstance(key_values, tuple) else (key_values,)
        row = {key: value for key, value in zip(group_keys, values)}
        completed = group.loc[group["status"].eq("completed")].copy()
        failed = group.loc[group["status"].eq("failed")]
        avg_roi = pd.to_numeric(completed["roi_pct"], errors="coerce").dropna()
        row.update(
            {
                "cases": int(len(group)),
                "completed_cases": int(len(completed)),
                "failed_cases": int(len(failed)),
                "pending_cases": int(len(group) - len(completed) - len(failed)),
                "resumed_cases": int(_bool_series(group, "resumed_from_existing").sum()),
                "reused_cases": int(group["_reused"].sum()),
                "avg_roi_pct": None if avg_roi.empty else float(avg_roi.mean()),
                "total_pnl_sum": float(completed["pnl_sum"].sum()) if not completed.empty else 0.0,
                "total_trades": int(completed["trades"].sum()) if not completed.empty else 0,
            }
        )
        rows.append(row)

    summary = pd.DataFrame(rows)
    merge_keys = [column for column in group_keys if column in best_frame.columns]
    best_columns = [column for column in columns if column.startswith("best_") and column in best_frame.columns]
    if merge_keys and best_columns:
        best_frame = _normalize_key_columns(best_frame, merge_keys)
        summary = summary.merge(best_frame.loc[:, [*merge_keys, *best_columns]], on=merge_keys, how="left")
    for column in columns:
        if column not in summary.columns:
            summary[column] = pd.NA
    return (
        summary.loc[:, columns]
        .sort_values(
            ["best_roi_pct", "total_pnl_sum", *group_keys],
            ascending=[False, False, *([True] * len(group_keys))],
            kind="stable",
            na_position="last",
        )
        .reset_index(drop=True)
    )


def _focus_source_frame(source: pd.DataFrame) -> pd.DataFrame:
    if source is None or source.empty:
        return pd.DataFrame()
    frame = source.copy()
    if "status" in frame.columns:
        frame = frame.loc[frame["status"].astype("string").fillna("").eq("completed")].copy()
    return frame


def _matrix_source_frame(source: pd.DataFrame) -> pd.DataFrame:
    if source is None or source.empty:
        return pd.DataFrame()
    frame = source.copy()
    values = frame.get("matrix_parent_run_name", pd.Series("", index=frame.index, dtype="string")).astype("string").fillna("")
    return frame.loc[values.ne("")].copy()


def _aggregate_case_factor_rows(source: pd.DataFrame, *, row_key: str, limit: int = 10) -> list[dict[str, object]]:
    stats_by_feature: dict[str, dict[str, object]] = {}
    for row in source.itertuples(index=False):
        training_run_dir = _optional_text(getattr(row, "training_run_dir", ""))
        if not training_run_dir:
            continue
        case_key = _optional_text(getattr(row, "case_key", ""))
        case_roi_pct = _optional_float(getattr(row, "roi_pct", 0.0))
        case_pnl_sum = _optional_float(getattr(row, "pnl_sum", 0.0))
        for offset_value, factor_rows in _read_case_factor_rows(Path(training_run_dir), row_key=row_key):
            for factor_row in factor_rows:
                feature = str((factor_row or {}).get("feature") or "").strip()
                if not feature:
                    continue
                score = float((factor_row or {}).get("direction_score", 0.0) or 0.0)
                stat = stats_by_feature.setdefault(
                    feature,
                    {
                        "feature": feature,
                        "hits": 0,
                        "case_keys": set(),
                        "offsets": set(),
                        "direction_score_sum": 0.0,
                        "case_roi_pct_sum": 0.0,
                        "case_pnl_sum": 0.0,
                    },
                )
                stat["hits"] = int(stat["hits"]) + 1
                stat["case_keys"].add(case_key)
                stat["offsets"].add(int(offset_value))
                stat["direction_score_sum"] = float(stat["direction_score_sum"]) + score
                stat["case_roi_pct_sum"] = float(stat["case_roi_pct_sum"]) + case_roi_pct
                stat["case_pnl_sum"] = float(stat["case_pnl_sum"]) + case_pnl_sum

    rows: list[dict[str, object]] = []
    for stat in stats_by_feature.values():
        hits = max(1, int(stat["hits"]))
        rows.append(
            {
                "feature": str(stat["feature"]),
                "hits": hits,
                "case_hits": int(len(stat["case_keys"])),
                "offsets": sorted(int(value) for value in stat["offsets"]),
                "avg_direction_score": float(stat["direction_score_sum"]) / hits,
                "avg_case_roi_pct": float(stat["case_roi_pct_sum"]) / hits,
                "avg_case_pnl_sum": float(stat["case_pnl_sum"]) / hits,
            }
        )
    rows.sort(
        key=lambda item: (
            -int(item["hits"]),
            -int(item["case_hits"]),
            -abs(float(item["avg_direction_score"])),
            -float(item["avg_case_roi_pct"]),
            str(item["feature"]),
        )
    )
    return rows[: max(0, int(limit))]


def _read_case_factor_rows(training_run_dir: Path, *, row_key: str) -> list[tuple[int, list[dict[str, object]]]]:
    rows: list[tuple[int, list[dict[str, object]]]] = []
    offsets_dir = training_run_dir / "offsets"
    if not offsets_dir.exists():
        return rows
    for offset_dir in sorted(offsets_dir.glob("offset=*")):
        try:
            offset_value = int(str(offset_dir.name).split("=", 1)[1])
        except (IndexError, ValueError):
            continue
        payload = _read_factor_summary_payload(offset_dir)
        explainability = payload.get("explainability") if isinstance(payload.get("explainability"), dict) else payload
        factor_rows = list(explainability.get(row_key) or []) if isinstance(explainability, dict) else []
        rows.append((offset_value, [dict(item) for item in factor_rows if isinstance(item, dict)]))
    return rows


def _read_factor_summary_payload(offset_dir: Path) -> dict[str, object]:
    for candidate in (offset_dir / "summary.json", offset_dir / "factor_direction_summary.json"):
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def _render_markdown_table(frame: pd.DataFrame, columns: list[str]) -> str:
    selected = [column for column in columns if column in frame.columns]
    table = frame.loc[:, selected].copy()
    rendered = table.astype("object").where(table.notna(), "")
    try:
        return rendered.to_markdown(index=False)
    except ImportError:
        return _render_markdown_table_fallback(rendered)


def _reused_case_mask(frame: pd.DataFrame) -> pd.Series:
    return (
        _bool_series(frame, "training_reused").reindex(frame.index, fill_value=False)
        | _bool_series(frame, "bundle_reused").reindex(frame.index, fill_value=False)
        | _bool_series(frame, "secondary_training_reused").reindex(frame.index, fill_value=False)
        | _bool_series(frame, "secondary_bundle_reused").reindex(frame.index, fill_value=False)
    )


def _normalize_key_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for column in columns:
        values = out[column] if column in out.columns else pd.Series("", index=out.index, dtype="string")
        out[column] = values.astype("string").fillna("").astype(str)
    return out


def _coalesce_many(frame: pd.DataFrame, *columns: str) -> pd.Series:
    values = pd.Series(pd.NA, index=frame.index, dtype="object")
    for column in columns:
        if not column:
            continue
        candidate = frame.get(column, pd.Series(pd.NA, index=frame.index, dtype="object"))
        values = values.where(values.notna() & values.astype("string").fillna("").ne(""), candidate)
    return values


def _sorted_numeric_values(values: pd.Series) -> list[float]:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    out: list[float] = []
    for value in numeric.tolist():
        item = float(value)
        if item not in out:
            out.append(item)
    return sorted(out)


def _bool_coalesce(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame.get(column, pd.Series(False, index=frame.index, dtype="boolean"))
    return values.astype("boolean").fillna(False).astype(bool)


def _render_markdown_table_fallback(frame: pd.DataFrame) -> str:
    columns = [str(column) for column in frame.columns.tolist()]
    if not columns:
        return ""
    header = "| " + " | ".join(_markdown_cell(column) for column in columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    rows = [
        "| " + " | ".join(_markdown_cell(value) for value in row) + " |"
        for row in frame.itertuples(index=False, name=None)
    ]
    return "\n".join([header, divider, *rows])


def _markdown_cell(value: object) -> str:
    text = "" if value is None else str(value)
    return text.replace("\n", "<br>").replace("|", "\\|")


def _bool_or(frame: pd.DataFrame, *columns: str) -> pd.Series:
    out = pd.Series(False, index=frame.index, dtype=bool)
    for column in columns:
        if column in frame.columns:
            out = out | _bool_coalesce(frame, column)
    return out


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if frame.empty or column not in frame.columns:
        return pd.Series(dtype=bool)
    return frame[column].astype("boolean").fillna(False).astype(bool)


def _optional_text(value: object) -> str:
    if _is_missing_scalar(value):
        return ""
    return str(value).strip()


def _optional_float(value: object, default: float = 0.0) -> float:
    if _is_missing_scalar(value):
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _optional_int(value: object, default: int = 0) -> int:
    if _is_missing_scalar(value):
        return int(default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _is_missing_scalar(value: object) -> bool:
    if value is None:
        return True
    if not pd.api.types.is_scalar(value):
        return False
    return bool(pd.isna(value))
