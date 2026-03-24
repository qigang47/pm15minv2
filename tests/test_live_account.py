from __future__ import annotations

from pathlib import Path

from pm15min.core.config import LiveConfig
from pm15min.live.account import (
    build_account_state_snapshot,
    load_latest_open_orders_snapshot,
    load_latest_positions_snapshot,
)
from pm15min.live.trading.contracts import OpenOrderRecord, PositionRecord


class FakeGateway:
    adapter_name = "fake"

    def __init__(
        self,
        *,
        open_orders: list[OpenOrderRecord] | None = None,
        positions: list[PositionRecord] | None = None,
        cash_balance: float | None = None,
    ) -> None:
        self._open_orders = list(open_orders or [])
        self._positions = list(positions or [])
        self._cash_balance = cash_balance

    def list_open_orders(self) -> list[OpenOrderRecord]:
        return list(self._open_orders)

    def list_positions(self) -> list[PositionRecord]:
        return list(self._positions)

    def get_cash_balance(self) -> float | None:
        return self._cash_balance


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def test_account_state_snapshot_persists_open_orders_and_positions(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    gateway = FakeGateway(
        open_orders=[
            OpenOrderRecord(
                order_id="order-1",
                market_id="market-1",
                token_id="token-up",
                side="BUY",
                status=None,
                price=0.20,
                size=5.0,
                created_at=None,
                raw={"id": "order-1"},
            )
        ],
        positions=[
            PositionRecord(
                market_id=None,
                condition_id="cond-1",
                token_id=None,
                size=3.5,
                redeemable=True,
                outcome_index=0,
                index_set=1,
                current_value=1.2,
                cash_pnl=0.0,
                raw={"conditionId": "cond-1"},
            )
        ],
        cash_balance=212.5,
    )

    payload = build_account_state_snapshot(cfg, persist=True, gateway=gateway)

    assert payload["open_orders"]["status"] == "ok"
    assert payload["open_orders"]["summary"]["total_orders"] == 1
    assert payload["open_orders"]["summary"]["total_notional_usd"] == 1.0
    assert payload["positions"]["status"] == "ok"
    assert payload["positions"]["cash_balance_usd"] == 212.5
    assert payload["positions"]["cash_balance_status"] == "ok"
    assert payload["positions"]["redeem_plan"]["cond-1"]["index_sets"] == [1]
    assert payload["positions"]["summary"]["current_value_sum"] == 1.2
    assert payload["summary"]["visible_open_order_notional_usd"] == 1.0
    assert payload["summary"]["visible_position_mark_usd"] == 1.2
    assert payload["summary"]["visible_capital_usage_usd"] == 2.2
    assert payload["summary"]["cash_balance_usd"] == 212.5
    assert payload["summary"]["cash_balance_available"] is True
    assert payload["trading_gateway"]["adapter"] == "fake"
    assert payload["open_orders"]["trading_gateway"]["adapter"] == "fake"
    assert payload["positions"]["trading_gateway"]["adapter"] == "fake"

    orders = load_latest_open_orders_snapshot(rewrite_root=root, market="sol")
    positions = load_latest_positions_snapshot(rewrite_root=root, market="sol")
    assert orders is not None
    assert positions is not None
    assert orders["orders"][0]["order_id"] == "order-1"
    assert positions["redeem_plan"]["cond-1"]["positions_count"] == 1


def test_account_state_snapshot_reports_missing_env_prerequisites(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", "")
    monkeypatch.setenv("POLYMARKET_USER_ADDRESS", "")

    payload = build_account_state_snapshot(cfg, persist=False, gateway=None)

    assert payload["open_orders"]["status"] == "skipped"
    assert payload["open_orders"]["reason"] == "missing_polymarket_private_key"
    assert payload["positions"]["status"] == "skipped"
    assert payload["positions"]["reason"] == "missing_polymarket_user_address"
    assert payload["summary"]["visible_capital_usage_usd"] == 0.0
    assert payload["summary"]["cash_balance_available"] is False
