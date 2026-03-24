from __future__ import annotations

from pm15min.data.layout import utc_snapshot_label
from .controller import (
    PRESSURE_NEUTRAL,
    REGIME_CAUTION,
    REGIME_DEFENSE,
    REGIME_NORMAL,
)
from .persistence import (
    load_latest_regime_state_snapshot,
    persist_regime_state_snapshot,
    summarize_regime_state,
)
from .state import build_regime_state_snapshot as _build_regime_state_snapshot_impl


def build_regime_state_snapshot(
    cfg,
    *,
    features=None,
    liquidity_payload=None,
    persist: bool = True,
    now=None,
):
    return _build_regime_state_snapshot_impl(
        cfg,
        features=features,
        liquidity_payload=liquidity_payload,
        persist=persist,
        now=now,
        utc_snapshot_label_fn=utc_snapshot_label,
    )
