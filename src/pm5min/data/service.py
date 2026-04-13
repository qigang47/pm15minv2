from __future__ import annotations

from .service_support.orderbook_coverage import build_orderbook_coverage_report as _build_orderbook_coverage_report
from .service_support.summary import (
    describe_data_runtime as _describe_data_runtime,
    show_data_summary as _show_data_summary,
)


def show_data_summary(cfg, *, persist: bool = False, now=None) -> dict[str, object]:
    return _show_data_summary(cfg, persist=persist, now=now)


def describe_data_runtime(cfg) -> dict[str, object]:
    return _describe_data_runtime(cfg)


def build_orderbook_coverage_report(
    cfg,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict[str, object]:
    return _build_orderbook_coverage_report(cfg, date_from=date_from, date_to=date_to)
