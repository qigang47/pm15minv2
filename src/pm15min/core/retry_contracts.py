from __future__ import annotations


ORDERBOOK_RETRYABLE_REASONS = {
    "depth_snapshot_missing",
    "depth_fill_unavailable",
    "depth_fill_ratio_below_threshold",
}

FAK_IMMEDIATE_RETRY_ERROR_HINTS = (
    "no orders found to match",
)

FAST_RETRY_ERROR_HINTS = (
    "no orders found to match",
    "fill and kill",
    "fok orders are fully filled or killed",
    "couldn't be fully filled",
)
