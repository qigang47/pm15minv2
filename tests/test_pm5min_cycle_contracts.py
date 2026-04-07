from pm15min.core.cycle_contracts import resolve_cycle_contract


def test_resolve_cycle_contract_for_15m_and_5m() -> None:
    contract_15m = resolve_cycle_contract("15m")
    assert contract_15m.cycle == "15m"
    assert contract_15m.cycle_minutes == 15
    assert contract_15m.entry_offsets == (7, 8, 9)
    assert contract_15m.first_half_anchor_offset == 7
    assert contract_15m.regime_return_columns == ("ret_15m", "ret_30m")

    contract_5m = resolve_cycle_contract("5m")
    assert contract_5m.cycle == "5m"
    assert contract_5m.cycle_minutes == 5
    assert contract_5m.entry_offsets == (2, 3, 4)
    assert contract_5m.first_half_anchor_offset == 2
    assert contract_5m.regime_return_columns == ("ret_5m", "ret_15m")
