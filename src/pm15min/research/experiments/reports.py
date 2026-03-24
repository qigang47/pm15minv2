from __future__ import annotations

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
        lines.append(failed_cases.loc[:, fail_cols].fillna("").to_markdown(index=False))
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


def _render_markdown_table(frame: pd.DataFrame, columns: list[str]) -> str:
    selected = [column for column in columns if column in frame.columns]
    table = frame.loc[:, selected].copy()
    return table.astype("object").where(table.notna(), "").to_markdown(index=False)


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
