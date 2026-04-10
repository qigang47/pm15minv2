from __future__ import annotations

from dataclasses import dataclass

from pm15min.data.layout.helpers import normalize_cycle


@dataclass(frozen=True)
class CycleContract:
    cycle: str
    cycle_minutes: int
    entry_offsets: tuple[int, ...]
    first_half_anchor_offset: int
    regime_return_columns: tuple[str, str]


def resolve_cycle_contract(cycle: str | int) -> CycleContract:
    normalized = normalize_cycle(cycle)
    if normalized == "5m":
        return CycleContract(
            cycle="5m",
            cycle_minutes=5,
            entry_offsets=(2, 3, 4),
            first_half_anchor_offset=2,
            regime_return_columns=("ret_5m", "ret_15m"),
        )
    return CycleContract(
        cycle="15m",
        cycle_minutes=15,
        entry_offsets=(7, 8, 9),
        first_half_anchor_offset=7,
        regime_return_columns=("ret_15m", "ret_30m"),
    )
