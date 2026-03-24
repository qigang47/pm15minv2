from __future__ import annotations

import pandas as pd


LEADERBOARD_COLUMNS = [
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
]


def build_leaderboard(backtest_runs: pd.DataFrame) -> pd.DataFrame:
    return _rank_leaderboard_frame(backtest_runs)


def build_leaderboard_cut(
    leaderboard: pd.DataFrame,
    *,
    partition_by: tuple[str, ...] = (),
    limit: int = 5,
) -> pd.DataFrame:
    frame = _rank_leaderboard_frame(leaderboard)
    if frame.empty:
        return frame
    limit = max(int(limit), 0)
    if limit == 0:
        return frame.iloc[0:0].copy()
    keys = [column for column in partition_by if column in frame.columns]
    if not keys:
        return frame.head(limit).reset_index(drop=True)
    return (
        frame.groupby(keys, sort=False, dropna=False, group_keys=False)
        .head(limit)
        .reset_index(drop=True)
    )


def _rank_leaderboard_frame(source: pd.DataFrame) -> pd.DataFrame:
    frame = _prepare_leaderboard_frame(source)
    if frame.empty:
        return frame
    frame["_sort_roi_pct"] = pd.to_numeric(frame["roi_pct"], errors="coerce").fillna(float("-inf"))
    frame["_sort_pnl_sum"] = pd.to_numeric(frame["pnl_sum"], errors="coerce").fillna(float("-inf"))
    frame["_sort_trades"] = pd.to_numeric(frame["trades"], errors="coerce").fillna(0.0)
    frame = (
        frame.sort_values(
            [
                "_sort_roi_pct",
                "_sort_pnl_sum",
                "_sort_trades",
                "market",
                "group_name",
                "run_name",
                "feature_set",
                "variant_label",
            ],
            ascending=[False, False, False, True, True, True, True, True],
            kind="stable",
        )
        .drop(columns=["_sort_roi_pct", "_sort_pnl_sum", "_sort_trades"])
        .reset_index(drop=True)
    )
    if "rank" in frame.columns:
        frame = frame.drop(columns=["rank"])
    frame.insert(0, "rank", range(1, len(frame) + 1))
    return frame


def _prepare_leaderboard_frame(source: pd.DataFrame) -> pd.DataFrame:
    if source is None or source.empty:
        return pd.DataFrame(columns=LEADERBOARD_COLUMNS)
    frame = source.copy()
    for column, default in (
        ("market", ""),
        ("group_name", ""),
        ("run_name", ""),
        ("feature_set", ""),
        ("variant_label", "default"),
        ("profile", ""),
        ("target", ""),
        ("trades", 0),
        ("pnl_sum", 0.0),
        ("roi_pct", 0.0),
        ("bundle_dir", ""),
        ("backtest_run_dir", ""),
    ):
        if column not in frame.columns:
            frame[column] = default
    ordered = [column for column in LEADERBOARD_COLUMNS if column != "rank"]
    extras = [column for column in frame.columns if column not in {"rank", *ordered}]
    return frame.loc[:, [*ordered, *extras]]
