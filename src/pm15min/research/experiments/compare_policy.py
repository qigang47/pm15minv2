from __future__ import annotations

import math

import pandas as pd


DEFAULT_REFERENCE_VARIANT_LABELS = ("default", "baseline", "control")

VARIANT_COMPARE_COLUMNS = [
    "case_key",
    "market",
    "group_name",
    "run_name",
    "target",
    "variant_label",
    "variant_notes",
    "status",
    "variants_in_run",
    "completed_variants_in_run",
    "reference_variant_label",
    "reference_case_key",
    "reference_status",
    "reference_policy",
    "is_reference_variant",
    "reference_roi_pct",
    "reference_pnl_sum",
    "reference_trades",
    "best_completed_variant_label",
    "best_completed_case_key",
    "best_completed_roi_pct",
    "best_completed_pnl_sum",
    "best_completed_trades",
    "is_best_completed_variant",
    "rank_in_run_by_roi",
    "rank_in_run_by_pnl",
    "roi_pct_delta_vs_reference",
    "pnl_sum_delta_vs_reference",
    "trades_delta_vs_reference",
    "comparison_vs_reference",
]


def build_variant_compare_frame(
    compare_frame: pd.DataFrame,
    *,
    reference_variant_labels: tuple[str, ...] = (),
) -> pd.DataFrame:
    frame = _prepare_compare_frame(compare_frame)
    if frame.empty:
        return pd.DataFrame(columns=VARIANT_COMPARE_COLUMNS)

    normalized_reference_labels = _normalize_reference_variant_labels(reference_variant_labels)
    rows: list[dict[str, object]] = []
    for _group_key, group in frame.groupby(["market", "group_name", "run_name", "target"], sort=False, dropna=False):
        rows.extend(
            _build_run_rows(
                group=group,
                reference_variant_labels=normalized_reference_labels,
            )
        )

    out = pd.DataFrame(rows)
    for column in VARIANT_COMPARE_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    out["_rank_sort"] = pd.to_numeric(out["rank_in_run_by_roi"], errors="coerce").fillna(float("inf"))
    out = out.sort_values(
        ["market", "group_name", "run_name", "target", "_rank_sort", "variant_label", "case_key"],
        ascending=[True, True, True, True, True, True, True],
        kind="stable",
    ).drop(columns=["_rank_sort"])
    return out.loc[:, VARIANT_COMPARE_COLUMNS].reset_index(drop=True)


def _prepare_compare_frame(source: pd.DataFrame) -> pd.DataFrame:
    if source is None or source.empty:
        return pd.DataFrame(columns=VARIANT_COMPARE_COLUMNS)
    frame = source.copy()
    defaults: tuple[tuple[str, object], ...] = (
        ("case_key", ""),
        ("market", ""),
        ("group_name", ""),
        ("run_name", ""),
        ("target", ""),
        ("variant_label", "default"),
        ("variant_notes", ""),
        ("status", ""),
        ("roi_pct", pd.NA),
        ("pnl_sum", pd.NA),
        ("trades", pd.NA),
    )
    for column, default in defaults:
        if column not in frame.columns:
            frame[column] = default
    for column in ("case_key", "market", "group_name", "run_name", "target", "variant_label", "variant_notes", "status"):
        frame[column] = frame[column].astype("string").fillna("")
    frame.loc[frame["variant_label"].eq(""), "variant_label"] = "default"
    frame["roi_pct"] = pd.to_numeric(frame["roi_pct"], errors="coerce")
    frame["pnl_sum"] = pd.to_numeric(frame["pnl_sum"], errors="coerce")
    frame["trades"] = pd.to_numeric(frame["trades"], errors="coerce")
    frame["_variant_token"] = frame["variant_label"].map(_variant_token)
    return frame


def _build_run_rows(
    *,
    group: pd.DataFrame,
    reference_variant_labels: tuple[str, ...],
) -> list[dict[str, object]]:
    reference_row, reference_policy = _select_reference_row(
        group=group,
        reference_variant_labels=reference_variant_labels,
    )
    best_completed_row = _select_best_completed_row(group)
    rank_by_roi = _build_rank_map(group, primary="roi_pct", secondary="pnl_sum")
    rank_by_pnl = _build_rank_map(group, primary="pnl_sum", secondary="roi_pct")
    variants_in_run = int(len(group))
    completed_variants_in_run = int(group["status"].eq("completed").sum())

    reference_case_key = None if reference_row is None else str(reference_row["case_key"])
    reference_status = None if reference_row is None else str(reference_row["status"])
    reference_variant_label = None if reference_row is None else str(reference_row["variant_label"])
    reference_roi_pct = _float_or_none(reference_row, "roi_pct")
    reference_pnl_sum = _float_or_none(reference_row, "pnl_sum")
    reference_trades = _int_or_none(reference_row, "trades")

    best_completed_case_key = None if best_completed_row is None else str(best_completed_row["case_key"])
    best_completed_variant_label = None if best_completed_row is None else str(best_completed_row["variant_label"])
    best_completed_roi_pct = _float_or_none(best_completed_row, "roi_pct")
    best_completed_pnl_sum = _float_or_none(best_completed_row, "pnl_sum")
    best_completed_trades = _int_or_none(best_completed_row, "trades")

    rows: list[dict[str, object]] = []
    for row in group.to_dict(orient="records"):
        case_key = str(row.get("case_key") or "")
        status = str(row.get("status") or "")
        roi_pct = _numeric_or_none(row.get("roi_pct"))
        pnl_sum = _numeric_or_none(row.get("pnl_sum"))
        trades = _int_or_none(row, "trades")
        roi_pct_delta = None
        pnl_sum_delta = None
        trades_delta = None
        comparison = "no_reference"
        if reference_case_key == case_key:
            comparison = "reference"
        elif status != "completed":
            comparison = "candidate_not_completed"
        elif reference_row is None:
            comparison = "no_reference"
        elif reference_status != "completed":
            comparison = "reference_not_completed"
        elif roi_pct is None or pnl_sum is None or reference_roi_pct is None or reference_pnl_sum is None:
            comparison = "not_comparable"
        else:
            roi_pct_delta = roi_pct - reference_roi_pct
            pnl_sum_delta = pnl_sum - reference_pnl_sum
            if trades is not None and reference_trades is not None:
                trades_delta = trades - reference_trades
            comparison = _comparison_label(roi_pct_delta=roi_pct_delta, pnl_sum_delta=pnl_sum_delta)

        rows.append(
            {
                "case_key": case_key,
                "market": str(row.get("market") or ""),
                "group_name": str(row.get("group_name") or ""),
                "run_name": str(row.get("run_name") or ""),
                "target": str(row.get("target") or ""),
                "variant_label": str(row.get("variant_label") or "default"),
                "variant_notes": str(row.get("variant_notes") or ""),
                "status": status,
                "variants_in_run": variants_in_run,
                "completed_variants_in_run": completed_variants_in_run,
                "reference_variant_label": reference_variant_label,
                "reference_case_key": reference_case_key,
                "reference_status": reference_status,
                "reference_policy": reference_policy,
                "is_reference_variant": bool(reference_case_key == case_key),
                "reference_roi_pct": reference_roi_pct,
                "reference_pnl_sum": reference_pnl_sum,
                "reference_trades": reference_trades,
                "best_completed_variant_label": best_completed_variant_label,
                "best_completed_case_key": best_completed_case_key,
                "best_completed_roi_pct": best_completed_roi_pct,
                "best_completed_pnl_sum": best_completed_pnl_sum,
                "best_completed_trades": best_completed_trades,
                "is_best_completed_variant": bool(best_completed_case_key == case_key and status == "completed"),
                "rank_in_run_by_roi": rank_by_roi.get(case_key),
                "rank_in_run_by_pnl": rank_by_pnl.get(case_key),
                "roi_pct_delta_vs_reference": roi_pct_delta,
                "pnl_sum_delta_vs_reference": pnl_sum_delta,
                "trades_delta_vs_reference": trades_delta,
                "comparison_vs_reference": comparison,
            }
        )
    return rows


def _select_reference_row(
    *,
    group: pd.DataFrame,
    reference_variant_labels: tuple[str, ...],
) -> tuple[dict[str, object] | None, str]:
    for label in reference_variant_labels:
        matches = group.loc[group["_variant_token"].eq(label)].copy()
        if not matches.empty:
            return _pick_reference_candidate(matches), "preferred_label"
    if len(group) == 1:
        return _pick_reference_candidate(group), "only_variant"
    completed = group.loc[group["status"].eq("completed")].copy()
    if not completed.empty:
        return _pick_reference_candidate(completed), "first_completed_variant"
    return _pick_reference_candidate(group), "first_variant"


def _pick_reference_candidate(group: pd.DataFrame) -> dict[str, object] | None:
    if group.empty:
        return None
    ranked = group.copy()
    ranked["_status_rank"] = ranked["status"].map(_status_rank).fillna(3)
    ranked = ranked.sort_values(
        ["_status_rank", "variant_label", "case_key"],
        ascending=[True, True, True],
        kind="stable",
    ).drop(columns=["_status_rank"])
    return ranked.iloc[0].to_dict()


def _select_best_completed_row(group: pd.DataFrame) -> dict[str, object] | None:
    completed = group.loc[group["status"].eq("completed")].copy()
    if completed.empty:
        return None
    ranked = _sort_completed_frame(completed, primary="roi_pct", secondary="pnl_sum")
    return ranked.iloc[0].to_dict()


def _build_rank_map(group: pd.DataFrame, *, primary: str, secondary: str) -> dict[str, int]:
    completed = group.loc[group["status"].eq("completed")].copy()
    if completed.empty:
        return {}
    ranked = _sort_completed_frame(completed, primary=primary, secondary=secondary).reset_index(drop=True)
    return {
        str(row.case_key): int(idx + 1)
        for idx, row in enumerate(ranked.itertuples(index=False))
    }


def _sort_completed_frame(frame: pd.DataFrame, *, primary: str, secondary: str) -> pd.DataFrame:
    ranked = frame.copy()
    ranked["_primary"] = pd.to_numeric(ranked[primary], errors="coerce").fillna(float("-inf"))
    ranked["_secondary"] = pd.to_numeric(ranked[secondary], errors="coerce").fillna(float("-inf"))
    ranked["_trades"] = pd.to_numeric(ranked["trades"], errors="coerce").fillna(float("-inf"))
    return ranked.sort_values(
        ["_primary", "_secondary", "_trades", "variant_label", "case_key"],
        ascending=[False, False, False, True, True],
        kind="stable",
    ).drop(columns=["_primary", "_secondary", "_trades"])


def _normalize_reference_variant_labels(raw: tuple[str, ...]) -> tuple[str, ...]:
    out: list[str] = []
    for label in raw or ():
        token = _variant_token(label)
        if token and token not in out:
            out.append(token)
    return tuple(out) if out else DEFAULT_REFERENCE_VARIANT_LABELS


def _variant_token(value: object) -> str:
    token = str(value or "").strip().lower()
    return token or "default"


def _status_rank(value: object) -> int:
    token = str(value or "").strip().lower()
    if token == "completed":
        return 0
    if token == "failed":
        return 1
    return 2


def _float_or_none(row: dict[str, object] | None, key: str) -> float | None:
    if row is None:
        return None
    return _numeric_or_none(row.get(key))


def _int_or_none(row: dict[str, object] | None, key: str) -> int | None:
    if row is None:
        return None
    value = _numeric_or_none(row.get(key))
    if value is None:
        return None
    return int(value)


def _numeric_or_none(value: object) -> float | None:
    if value is None or value is pd.NA:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    return numeric


def _comparison_label(*, roi_pct_delta: float, pnl_sum_delta: float) -> str:
    if roi_pct_delta > 0.0 or (roi_pct_delta == 0.0 and pnl_sum_delta > 0.0):
        return "better_than_reference"
    if roi_pct_delta < 0.0 or (roi_pct_delta == 0.0 and pnl_sum_delta < 0.0):
        return "worse_than_reference"
    return "same_as_reference"
