from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class LabelBuildPlan:
    requested_label_set: str
    base_label_set: str
    label_source: str | None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "requested_label_set": self.requested_label_set,
            "base_label_set": self.base_label_set,
            "label_source": self.label_source,
        }


_TRUTH_LABEL_SETS = {
    "truth": None,
    "settlement_truth": "settlement_truth",
}

_ORACLE_LABEL_SETS = {
    "oracle_prices": None,
    "streams": "streams",
    "datafeeds": "datafeeds",
    "chainlink_mixed": "chainlink_mixed",
}


def resolve_label_build_plan(label_set: str) -> LabelBuildPlan:
    key = str(label_set).strip().lower()
    if key in _TRUTH_LABEL_SETS:
        return LabelBuildPlan(requested_label_set=key, base_label_set="truth", label_source=_TRUTH_LABEL_SETS[key])
    if key in _ORACLE_LABEL_SETS:
        return LabelBuildPlan(
            requested_label_set=key,
            base_label_set="oracle_prices",
            label_source=_ORACLE_LABEL_SETS[key],
        )
    supported = sorted(set(_TRUTH_LABEL_SETS) | set(_ORACLE_LABEL_SETS))
    raise ValueError(f"Unsupported label_set {label_set!r}. Expected one of: {', '.join(supported)}")


def filter_truth_table_by_label_source(truth_table: pd.DataFrame, *, label_source: str | None) -> pd.DataFrame:
    if truth_table.empty or not label_source:
        return truth_table.copy()
    if "truth_source" not in truth_table.columns:
        return truth_table.iloc[0:0].copy()
    normalized = normalize_label_source_series(truth_table["truth_source"], default="")
    mask = normalized.eq(_normalized_label_source_key(label_source))
    return truth_table.loc[mask].copy()


def filter_oracle_prices_by_label_source(oracle_prices_table: pd.DataFrame, *, label_source: str | None) -> pd.DataFrame:
    if oracle_prices_table.empty or not label_source:
        return oracle_prices_table.copy()
    combined = oracle_prices_table.get(
        "source_final_price",
        oracle_prices_table.get("source_price_to_beat", pd.Series("", index=oracle_prices_table.index)),
    )
    normalized = normalize_label_source_series(combined, default="")
    mask = normalized.eq(_normalized_label_source_key(label_source))
    return oracle_prices_table.loc[mask].copy()


def normalize_label_source_series(series: pd.Series, *, default: str) -> pd.Series:
    if series is None or len(series) == 0:
        return pd.Series(default, index=getattr(series, "index", None), dtype="string")
    values = series.fillna("").astype(str).str.strip().str.lower()
    values = values.mask(values.eq(""), str(default).strip().lower())
    normalized = values.astype("string")
    normalized = normalized.mask(values.str.contains("stream", na=False), "streams")
    normalized = normalized.mask(values.str.contains("datafeed", na=False), "datafeeds")
    normalized = normalized.mask(values.str.contains("mixed", na=False), "chainlink_mixed")
    normalized = normalized.mask(values.str.contains("settlement_truth", na=False), "settlement_truth")
    normalized = normalized.mask(values.str.contains("oracle_prices|direct_api", na=False), "oracle_prices")
    return normalized.fillna(str(default).strip().lower()).astype("string")


def summarize_label_sources(series: pd.Series) -> dict[str, object]:
    normalized = normalize_label_source_series(series, default="unknown")
    non_empty = normalized[normalized.ne("")]
    counts = non_empty.value_counts(dropna=False).sort_index()
    return {
        "label_source_count": int(len(counts)),
        "label_sources": counts.index.astype(str).tolist(),
        "label_source_counts": {str(index): int(value) for index, value in counts.items()},
    }


def _label_source_tokens(label_source: str) -> tuple[str, ...]:
    key = str(label_source).strip().lower()
    if key == "streams":
        return ("stream",)
    if key == "datafeeds":
        return ("datafeed",)
    if key == "chainlink_mixed":
        return ("stream", "datafeed", "mixed", "chainlink")
    return (key,)


def _normalized_label_source_key(label_source: str) -> str:
    value = pd.Series([str(label_source or "")], dtype="string")
    return str(normalize_label_source_series(value, default="").iloc[0] or "")
