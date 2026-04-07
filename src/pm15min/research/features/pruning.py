from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

from pm15min.research.features.registry import feature_set_columns, feature_set_drop_columns


_LEGACY_SHARED_BLACKLIST: dict[str, tuple[str, ...]] = {
    "btc": ("ma_gap_15", "ema_gap_12", "vwap_gap_20"),
    "eth": ("ret_5m", "ema_gap_12", "ma_gap_15"),
    "sol": (),
    "xrp": (),
}

_SHARED_META_BLACKLIST = (
    "asset",
    "condition_id",
    "current_ret",
    "current_ret_col",
    "current_up",
    "cycle_end_ts",
    "cycle_start_ts",
    "decision_ts",
    "final_price",
    "full_truth",
    "label_set",
    "label_source",
    "label_alignment_gap_seconds",
    "label_alignment_mode",
    "label_alignment_status",
    "market_id",
    "offset",
    "price_to_beat",
    "resolved",
    "settlement_source",
    "target",
    "winner_side",
    "y",
    "y_direction",
)


@dataclass(frozen=True)
class FeatureDrop:
    column: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class FeaturePruningPlan:
    feature_set: str
    market: str
    requested_drop_columns: tuple[str, ...]
    shared_blacklist_columns: tuple[str, ...]
    feature_set_blacklist_columns: tuple[str, ...]
    dropped_columns: tuple[str, ...]
    missing_columns: tuple[str, ...]
    not_in_feature_set_columns: tuple[str, ...]
    kept_columns: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class FeaturePruningReport:
    feature_set: str
    requested_columns: tuple[str, ...]
    kept_columns: tuple[str, ...]
    dropped: tuple[FeatureDrop, ...]
    shared_blacklist: tuple[str, ...]
    feature_set_drop_columns: tuple[str, ...]
    extra_blacklist: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "feature_set": self.feature_set,
            "requested_columns": list(self.requested_columns),
            "kept_columns": list(self.kept_columns),
            "dropped": [item.to_dict() for item in self.dropped],
            "shared_blacklist": list(self.shared_blacklist),
            "feature_set_drop_columns": list(self.feature_set_drop_columns),
            "extra_blacklist": list(self.extra_blacklist),
        }


def shared_blacklist_columns() -> tuple[str, ...]:
    return _SHARED_META_BLACKLIST


def build_feature_pruning_plan(
    columns: list[str] | tuple[str, ...],
    *,
    feature_set: str,
    market: str,
    root: str | None = None,
    extra_drop_columns: list[str] | tuple[str, ...] = (),
    apply_shared_blacklist: bool = False,
) -> FeaturePruningPlan:
    ordered_columns = [str(column) for column in columns]
    allowed = set(feature_set_columns(feature_set, root=root))
    shared = _LEGACY_SHARED_BLACKLIST.get(str(market).strip().lower(), ()) if apply_shared_blacklist else ()
    feature_set_blacklist = feature_set_drop_columns(feature_set, root=root)

    requested = tuple(dict.fromkeys(str(column) for column in extra_drop_columns if str(column)))
    effective_drop = tuple(dict.fromkeys([*requested, *(_SHARED_META_BLACKLIST if apply_shared_blacklist else ()), *shared, *feature_set_blacklist]))
    existing = tuple(column for column in effective_drop if column in ordered_columns)
    missing = tuple(column for column in effective_drop if column not in ordered_columns)
    not_in_feature_set = tuple(column for column in ordered_columns if column not in allowed and column not in set(existing))
    dropped = tuple(dict.fromkeys([*existing, *not_in_feature_set]))
    kept = tuple(column for column in ordered_columns if column not in set(dropped))
    return FeaturePruningPlan(
        feature_set=str(feature_set).strip().lower(),
        market=str(market).strip().lower(),
        requested_drop_columns=requested,
        shared_blacklist_columns=tuple(dict.fromkeys([*(_SHARED_META_BLACKLIST if apply_shared_blacklist else ()), *shared])),
        feature_set_blacklist_columns=tuple(feature_set_blacklist),
        dropped_columns=dropped,
        missing_columns=missing,
        not_in_feature_set_columns=not_in_feature_set,
        kept_columns=kept,
    )


def resolve_feature_pruning(
    feature_set: str,
    *,
    available_columns: list[str] | tuple[str, ...],
    market: str = "",
    root: str | None = None,
    extra_blacklist: list[str] | tuple[str, ...] = (),
    apply_shared_blacklist: bool = True,
) -> FeaturePruningReport:
    plan = build_feature_pruning_plan(
        available_columns,
        feature_set=feature_set,
        market=market,
        root=root,
        extra_drop_columns=extra_blacklist,
        apply_shared_blacklist=apply_shared_blacklist,
    )
    extra = set(plan.requested_drop_columns)
    shared = set(plan.shared_blacklist_columns)
    feature_blacklist = set(plan.feature_set_blacklist_columns)
    dropped: list[FeatureDrop] = []
    for column in plan.requested_drop_columns:
        if column in plan.dropped_columns:
            dropped.append(FeatureDrop(column=column, reason="extra_blacklist"))
    for column in plan.dropped_columns:
        if column in extra:
            continue
        if column in feature_blacklist:
            dropped.append(FeatureDrop(column=column, reason="feature_set_drop_policy"))
            continue
        if column in shared:
            dropped.append(FeatureDrop(column=column, reason="shared_blacklist"))
            continue
        if column in plan.not_in_feature_set_columns:
            dropped.append(FeatureDrop(column=column, reason="not_in_feature_set"))
    return FeaturePruningReport(
        feature_set=plan.feature_set,
        requested_columns=tuple(str(column) for column in available_columns),
        kept_columns=plan.kept_columns,
        dropped=tuple(dropped),
        shared_blacklist=plan.shared_blacklist_columns,
        feature_set_drop_columns=plan.feature_set_blacklist_columns,
        extra_blacklist=plan.requested_drop_columns,
    )


def prune_feature_frame(
    frame: pd.DataFrame,
    *,
    feature_set: str,
    market: str = "",
    root: str | None = None,
    extra_blacklist: list[str] | tuple[str, ...] = (),
    apply_shared_blacklist: bool = True,
) -> tuple[pd.DataFrame, FeaturePruningReport]:
    report = resolve_feature_pruning(
        feature_set,
        available_columns=[str(column) for column in frame.columns],
        market=market,
        root=root,
        extra_blacklist=extra_blacklist,
        apply_shared_blacklist=apply_shared_blacklist,
    )
    return frame.loc[:, list(report.kept_columns)].copy(), report
