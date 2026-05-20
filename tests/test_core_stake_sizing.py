from __future__ import annotations

import pytest

from pm15min.core.stake_sizing import resolve_tiered_kelly_stake


def test_tiered_kelly_stake_keeps_weak_edge_at_base_size() -> None:
    stake, context = resolve_tiered_kelly_stake(
        probability=0.55,
        entry_price=0.49,
        base_stake=2.0,
        max_stake=10.0,
        enabled=True,
    )

    assert stake == pytest.approx(2.0)
    assert context["stake_source"] == "kelly_tier_base"
    assert context["kelly_fractional"] < 0.04


def test_tiered_kelly_stake_scales_size_from_kelly_fraction() -> None:
    base_stake, base_context = resolve_tiered_kelly_stake(
        probability=0.55,
        entry_price=0.49,
        base_stake=2.0,
        max_stake=10.0,
        enabled=True,
    )
    scaled_stake, scaled_context = resolve_tiered_kelly_stake(
        probability=0.625,
        entry_price=0.49,
        base_stake=2.0,
        max_stake=10.0,
        enabled=True,
    )
    strong_stake, strong_context = resolve_tiered_kelly_stake(
        probability=0.65,
        entry_price=0.49,
        base_stake=2.0,
        max_stake=10.0,
        enabled=True,
    )

    assert base_stake == pytest.approx(2.0)
    assert base_context["stake_source"] == "kelly_tier_base"
    assert scaled_stake == pytest.approx(6.0)
    assert scaled_context["stake_source"] == "kelly_tier_scaled"
    assert strong_stake == pytest.approx(10.0)
    assert strong_context["stake_source"] == "kelly_tier_strong"


def test_tiered_kelly_stake_keeps_entry_threshold_signal_at_base_size() -> None:
    stake, context = resolve_tiered_kelly_stake(
        probability=0.60,
        entry_price=0.49,
        base_stake=2.0,
        max_stake=10.0,
        enabled=True,
    )

    assert stake == pytest.approx(2.0)
    assert context["stake_source"] == "kelly_tier_base"


def test_tiered_kelly_stake_respects_max_stake_cap() -> None:
    stake, context = resolve_tiered_kelly_stake(
        probability=0.65,
        entry_price=0.49,
        base_stake=2.0,
        max_stake=6.0,
        enabled=True,
    )

    assert stake == pytest.approx(6.0)
    assert context["stake_source"] == "kelly_tier_strong"
