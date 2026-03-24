from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from pm15min.live.execution.retry_policy import (
    FAK_IMMEDIATE_RETRY_ERROR_HINTS,
    FAST_RETRY_ERROR_HINTS,
    ORDERBOOK_RETRYABLE_REASONS,
)
from pm15min.live.profiles.spec import LiveProfileSpec


PRE_SUBMIT_ORDERBOOK_RETRY_COLUMNS: tuple[str, ...] = (
    "pre_submit_orderbook_retry_armed",
    "pre_submit_orderbook_retry_reason",
    "pre_submit_orderbook_retry_interval_sec",
    "pre_submit_orderbook_retry_max",
    "pre_submit_orderbook_retry_state_key",
)


@dataclass(frozen=True)
class BacktestRetryContractSummary:
    pre_submit_depth_retry_interval_sec: float
    pre_submit_depth_retry_max: int
    pre_submit_depth_retry_reasons: tuple[str, ...]
    pre_submit_depth_retry_state_key: str
    post_submit_order_retry_interval_sec: float
    post_submit_fast_retry_interval_sec: float
    post_submit_order_retry_max: int
    post_submit_fast_retry_message_hints: tuple[str, ...]
    post_submit_order_retry_state_keys: tuple[str, ...]
    post_submit_fak_immediate_retry_max: int
    post_submit_fak_retry_message_hints: tuple[str, ...]
    post_submit_fak_response_driven: bool

    @property
    def max_fak_refresh_candidates(self) -> int:
        return max(1, int(self.post_submit_fak_immediate_retry_max) + 1)

    @property
    def max_pre_submit_orderbook_retry_candidates(self) -> int:
        return max(1, int(self.pre_submit_depth_retry_max))

    def to_dict(self) -> dict[str, object]:
        return {
            "pre_submit_depth_retry_interval_sec": float(self.pre_submit_depth_retry_interval_sec),
            "pre_submit_depth_retry_max": int(self.pre_submit_depth_retry_max),
            "pre_submit_depth_retry_reasons": list(self.pre_submit_depth_retry_reasons),
            "pre_submit_depth_retry_state_key": str(self.pre_submit_depth_retry_state_key),
            "post_submit_order_retry_interval_sec": float(self.post_submit_order_retry_interval_sec),
            "post_submit_fast_retry_interval_sec": float(self.post_submit_fast_retry_interval_sec),
            "post_submit_order_retry_max": int(self.post_submit_order_retry_max),
            "post_submit_fast_retry_message_hints": list(self.post_submit_fast_retry_message_hints),
            "post_submit_order_retry_state_keys": list(self.post_submit_order_retry_state_keys),
            "post_submit_fak_immediate_retry_max": int(self.post_submit_fak_immediate_retry_max),
            "post_submit_fak_retry_message_hints": list(self.post_submit_fak_retry_message_hints),
            "post_submit_fak_response_driven": bool(self.post_submit_fak_response_driven),
        }


def build_backtest_retry_contract(spec: LiveProfileSpec) -> BacktestRetryContractSummary:
    return BacktestRetryContractSummary(
        pre_submit_depth_retry_interval_sec=float(spec.orderbook_fast_retry_interval_seconds),
        pre_submit_depth_retry_max=int(spec.orderbook_fast_retry_max),
        pre_submit_depth_retry_reasons=tuple(sorted(str(reason) for reason in ORDERBOOK_RETRYABLE_REASONS)),
        pre_submit_depth_retry_state_key="orderbook_retry_count",
        post_submit_order_retry_interval_sec=float(spec.order_retry_interval_seconds),
        post_submit_fast_retry_interval_sec=float(spec.fast_retry_interval_seconds),
        post_submit_order_retry_max=int(spec.max_order_retries),
        post_submit_fast_retry_message_hints=tuple(str(hint) for hint in FAST_RETRY_ERROR_HINTS),
        post_submit_order_retry_state_keys=(
            "attempts",
            "last_attempt",
            "last_error",
            "fast_retry",
            "retry_interval_seconds",
        ),
        post_submit_fak_immediate_retry_max=int(spec.fak_immediate_retry_max),
        post_submit_fak_retry_message_hints=tuple(str(hint) for hint in FAK_IMMEDIATE_RETRY_ERROR_HINTS),
        post_submit_fak_response_driven=True,
    )


def attach_pre_submit_orderbook_retry_contract(
    rows: pd.DataFrame,
    *,
    spec: LiveProfileSpec,
    reject_reason_column: str = "decision_engine_reason",
) -> pd.DataFrame:
    out = rows.copy()
    contract = build_backtest_retry_contract(spec)
    reason = (
        out.get(reject_reason_column, pd.Series("", index=out.index, dtype="string"))
        .astype("string")
        .fillna("")
        .astype(str)
    )
    armed = reason.eq("orderbook_limit_reject")
    out["pre_submit_orderbook_retry_armed"] = armed
    out["pre_submit_orderbook_retry_reason"] = reason.where(armed, "")
    out["pre_submit_orderbook_retry_interval_sec"] = pd.Series(pd.NA, index=out.index, dtype="Float64")
    out["pre_submit_orderbook_retry_max"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    out["pre_submit_orderbook_retry_state_key"] = pd.Series("", index=out.index, dtype="string")
    if bool(armed.any()):
        out.loc[armed, "pre_submit_orderbook_retry_interval_sec"] = float(contract.pre_submit_depth_retry_interval_sec)
        out.loc[armed, "pre_submit_orderbook_retry_max"] = int(contract.pre_submit_depth_retry_max)
        out.loc[armed, "pre_submit_orderbook_retry_state_key"] = str(contract.pre_submit_depth_retry_state_key)
    return out


def limit_legacy_pre_submit_orderbook_retry_candidates(
    raw_depth_candidates: list[dict[str, object]],
    *,
    spec: LiveProfileSpec | None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    total_count = int(len(raw_depth_candidates))
    if spec is None:
        return list(raw_depth_candidates), {
            "candidate_total_count": total_count,
            "retry_budget": total_count,
            "budget_exhausted": False,
            "retry_budget_source": "",
        }
    contract = build_backtest_retry_contract(spec)
    capped = list(raw_depth_candidates[: contract.max_pre_submit_orderbook_retry_candidates])
    return capped, {
        "candidate_total_count": total_count,
        "retry_budget": int(contract.max_pre_submit_orderbook_retry_candidates),
        "budget_exhausted": bool(total_count > len(capped)),
        "retry_budget_source": "orderbook_fast_retry_max",
    }


def limit_legacy_fak_refresh_candidates(
    raw_depth_candidates: list[dict[str, object]],
    *,
    spec: LiveProfileSpec | None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    return limit_legacy_pre_submit_orderbook_retry_candidates(
        raw_depth_candidates,
        spec=spec,
    )
