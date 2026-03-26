"""DecisionEngine side-selection parity helpers for backtest frames.

The emitted reject reasons mirror `live_trading.core.trade_logic.DecisionEngine`
for this narrow surface, except for `input_missing`, which is adapter
housekeeping when a backtest row lacks probabilities or prices.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Sequence

import pandas as pd

from pm15min.live.profiles.spec import LiveProfileSpec


DECISION_ENGINE_PARITY_COLUMNS: tuple[str, ...] = (
    "decision_engine_action",
    "decision_engine_reason",
    "decision_engine_side",
    "decision_engine_rationale",
    "decision_engine_entry_price",
    "decision_engine_prob",
    "decision_engine_probability_gap",
    "decision_engine_edge",
    "decision_engine_roi",
    "decision_engine_roi_net",
)


@dataclass(frozen=True)
class DecisionEngineParityConfig:
    supported_offsets: tuple[int, ...] | None = None
    enforce_price_bounds: bool = False
    price_floor: float = 0.0
    price_cap: float = 1.0
    enforce_entry_price_band: bool = False
    entry_price_min: float | None = None
    entry_price_max: float | None = None
    min_dir_prob_default: float = 0.5
    min_dir_prob_by_offset: dict[int, float] = field(default_factory=dict)
    min_net_edge_default: float = 0.0
    min_net_edge_by_offset: dict[int, float] = field(default_factory=dict)
    min_net_edge_entry_price_le_0p10_bonus: float = 0.0
    min_net_edge_entry_price_le_0p05_bonus: float = 0.0
    roi_threshold_default: float = 0.0
    roi_threshold_by_offset: dict[int, float] = field(default_factory=dict)
    roi_max: float | None = None
    slippage_bps: float = 0.0
    fee_model: str = "flat_bps"
    fee_bps: float = 0.0
    fee_curve_k: float = 0.0

    def min_dir_prob_for(self, *, offset: int, boost: float = 0.0) -> float | None:
        if self.supported_offsets is not None and int(offset) not in self.supported_offsets:
            return None
        value = self.min_dir_prob_by_offset.get(int(offset), self.min_dir_prob_default)
        return min(1.0, max(0.0, float(value) + max(0.0, float(boost))))

    def min_net_edge_for(self, *, offset: int, entry_price: float) -> float:
        value = float(self.min_net_edge_by_offset.get(int(offset), self.min_net_edge_default))
        if entry_price <= 0.05:
            value += float(self.min_net_edge_entry_price_le_0p05_bonus)
        elif entry_price <= 0.10:
            value += float(self.min_net_edge_entry_price_le_0p10_bonus)
        return max(0.0, value)

    def roi_threshold_for(self, *, offset: int) -> float:
        return max(0.0, float(self.roi_threshold_by_offset.get(int(offset), self.roi_threshold_default)))

    def fee_rate(self, *, price: float) -> float:
        model = str(self.fee_model or "flat_bps").strip().lower()
        if model == "polymarket_curve":
            bounded_price = max(0.0, min(float(price), 1.0))
            return max(0.0, float(self.fee_curve_k) * (bounded_price * (1.0 - bounded_price)) ** 2)
        return max(0.0, float(self.fee_bps)) / 10_000.0


@dataclass(frozen=True)
class DecisionEngineParityDecision:
    action: str
    reason: str
    side: str | None = None
    rationale: str = ""
    entry_price: float | None = None
    selected_prob: float | None = None
    probability_gap: float | None = None
    raw_edge: float | None = None
    roi: float | None = None
    roi_net: float | None = None

    def as_record(self) -> dict[str, object]:
        return {
            "decision_engine_action": self.action,
            "decision_engine_reason": self.reason,
            "decision_engine_side": self.side,
            "decision_engine_rationale": self.rationale,
            "decision_engine_entry_price": self.entry_price,
            "decision_engine_prob": self.selected_prob,
            "decision_engine_probability_gap": self.probability_gap,
            "decision_engine_edge": self.raw_edge,
            "decision_engine_roi": self.roi,
            "decision_engine_roi_net": self.roi_net,
        }


def build_profile_decision_engine_parity_config(
    *,
    market: str,
    profile_spec: LiveProfileSpec,
) -> DecisionEngineParityConfig:
    offsets = tuple(int(offset) for offset in profile_spec.offsets)
    min_dir_prob_by_offset = {
        int(offset): float(profile_spec.threshold_for(market=market, offset=int(offset)))
        for offset in offsets
    }
    return DecisionEngineParityConfig(
        supported_offsets=(offsets or None),
        enforce_entry_price_band=(profile_spec.entry_price_min is not None or profile_spec.entry_price_max is not None),
        entry_price_min=profile_spec.entry_price_min,
        entry_price_max=profile_spec.entry_price_max,
        min_dir_prob_default=float(profile_spec.min_dir_prob_default),
        min_dir_prob_by_offset=min_dir_prob_by_offset,
        min_net_edge_default=float(profile_spec.min_net_edge_default),
        min_net_edge_by_offset={
            int(offset): float(value) for offset, value in profile_spec.min_net_edge_by_offset.items()
        },
        min_net_edge_entry_price_le_0p10_bonus=float(profile_spec.min_net_edge_entry_price_le_0p10_bonus),
        min_net_edge_entry_price_le_0p05_bonus=float(profile_spec.min_net_edge_entry_price_le_0p05_bonus),
        roi_threshold_default=float(profile_spec.roi_threshold_default),
        roi_threshold_by_offset={
            int(offset): float(value) for offset, value in profile_spec.roi_threshold_by_offset.items()
        },
        slippage_bps=float(profile_spec.slippage_bps),
        fee_model=str(profile_spec.fee_model),
        fee_bps=float(profile_spec.fee_bps),
        fee_curve_k=float(profile_spec.fee_curve_k),
    )


def evaluate_decision_engine_side(
    *,
    offset: int,
    p_up: float | None,
    p_down: float | None,
    up_price: float | None,
    down_price: float | None,
    config: DecisionEngineParityConfig | None = None,
    min_dir_prob_boost: float = 0.0,
) -> DecisionEngineParityDecision:
    """Resolve the live-like trade side, rationale, or reject reason for one row."""
    cfg = config or DecisionEngineParityConfig()
    resolved = _resolve_inputs(
        p_up=p_up,
        p_down=p_down,
        up_price=up_price,
        down_price=down_price,
    )
    if resolved is None:
        return DecisionEngineParityDecision(action="reject", reason="input_missing")

    p_up_eff, p_down_eff, c_up, c_down = resolved
    probability_gap = abs(p_up_eff - p_down_eff)

    min_dir_prob = cfg.min_dir_prob_for(offset=int(offset), boost=min_dir_prob_boost)
    if min_dir_prob is None:
        return DecisionEngineParityDecision(
            action="reject",
            reason="offset_unsupported",
            probability_gap=probability_gap,
        )

    in_up, in_down, reject = _check_price_bounds(config=cfg, up_price=c_up, down_price=c_down)
    if reject:
        return DecisionEngineParityDecision(
            action="reject",
            reason=reject,
            probability_gap=probability_gap,
        )

    in_band_up, in_band_down, reject = _check_entry_band(config=cfg, up_price=c_up, down_price=c_down)
    if reject:
        return DecisionEngineParityDecision(
            action="reject",
            reason=reject,
            probability_gap=probability_gap,
        )

    slip = max(0.0, float(cfg.slippage_bps)) / 10_000.0
    c_up_eff = c_up * (1.0 + slip)
    c_down_eff = c_down * (1.0 + slip)
    fee_up = cfg.fee_rate(price=c_up_eff)
    fee_down = cfg.fee_rate(price=c_down_eff)
    roi_threshold = cfg.roi_threshold_for(offset=int(offset))

    up_edge = p_up_eff - c_up
    down_edge = p_down_eff - c_down
    roi_up = _roi(edge=up_edge, price=c_up) if in_up else float("-inf")
    roi_down = _roi(edge=down_edge, price=c_down) if in_down else float("-inf")
    roi_up_net = _roi_net(prob=p_up_eff, effective_price=c_up_eff, fee_rate=fee_up) if in_up else float("-inf")
    roi_down_net = _roi_net(prob=p_down_eff, effective_price=c_down_eff, fee_rate=fee_down) if in_down else float("-inf")
    min_net_edge_up = cfg.min_net_edge_for(offset=int(offset), entry_price=c_up)
    min_net_edge_down = cfg.min_net_edge_for(offset=int(offset), entry_price=c_down)

    candidates: list[dict[str, float | str]] = []
    if (
        in_up
        and in_band_up
        and p_up_eff > min_dir_prob
        and up_edge >= min_net_edge_up
        and roi_up_net >= roi_threshold
        and (cfg.roi_max is None or roi_up_net <= float(cfg.roi_max))
    ):
        candidates.append(
            {
                "side": "UP",
                "price": c_up,
                "prob": p_up_eff,
                "edge": up_edge,
                "roi": roi_up,
                "roi_net": roi_up_net,
            }
        )
    if (
        in_down
        and in_band_down
        and p_down_eff > min_dir_prob
        and down_edge >= min_net_edge_down
        and roi_down_net >= roi_threshold
        and (cfg.roi_max is None or roi_down_net <= float(cfg.roi_max))
    ):
        candidates.append(
            {
                "side": "DOWN",
                "price": c_down,
                "prob": p_down_eff,
                "edge": down_edge,
                "roi": roi_down,
                "roi_net": roi_down_net,
            }
        )

    if not candidates:
        reason = _classify_no_candidate_reject(
            in_up=in_up,
            in_down=in_down,
            in_band_up=in_band_up,
            in_band_down=in_band_down,
            p_up_eff=p_up_eff,
            p_down_eff=p_down_eff,
            min_dir_prob=min_dir_prob,
            up_edge=up_edge,
            down_edge=down_edge,
            min_net_edge_up=min_net_edge_up,
            min_net_edge_down=min_net_edge_down,
        )
        return DecisionEngineParityDecision(
            action="reject",
            reason=reason,
            probability_gap=probability_gap,
        )

    chosen = max(candidates, key=lambda candidate: (float(candidate["roi_net"]), float(candidate["prob"])))
    side = str(chosen["side"])
    if side == "UP":
        rationale = f"p_up_eff={p_up_eff:.4f} vs price={c_up:.4f}"
    else:
        rationale = f"p_down_eff={p_down_eff:.4f} vs price={c_down:.4f}"
    return DecisionEngineParityDecision(
        action="trade",
        reason="trade",
        side=side,
        rationale=rationale,
        entry_price=float(chosen["price"]),
        selected_prob=float(chosen["prob"]),
        probability_gap=probability_gap,
        raw_edge=float(chosen["edge"]),
        roi=float(chosen["roi"]),
        roi_net=float(chosen["roi_net"]),
    )


def apply_decision_engine_parity(
    rows: pd.DataFrame,
    *,
    config: DecisionEngineParityConfig | None = None,
    offset_column: str = "offset",
    p_up_column: str = "p_up",
    p_down_column: str = "p_down",
    up_price_columns: Sequence[str] = ("quote_up_ask", "quote_prob_up", "p_up"),
    down_price_columns: Sequence[str] = ("quote_down_ask", "quote_prob_down", "p_down"),
    min_dir_prob_boost_column: str | None = None,
) -> pd.DataFrame:
    """Annotate a frame with DecisionEngine parity columns using quote-style fallbacks."""
    out = rows.copy()
    cfg = config or DecisionEngineParityConfig()
    if out.empty:
        for column in DECISION_ENGINE_PARITY_COLUMNS:
            out[column] = pd.Series(dtype="object" if column.endswith(("action", "reason", "side", "rationale")) else "float64")
        return out

    column_positions = {str(column): idx for idx, column in enumerate(out.columns)}
    offset_idx = column_positions.get(offset_column)
    p_up_idx = column_positions.get(p_up_column)
    p_down_idx = column_positions.get(p_down_column)
    boost_idx = column_positions.get(min_dir_prob_boost_column) if min_dir_prob_boost_column else None

    records: list[dict[str, object]] = []
    append_record = records.append
    for row in out.itertuples(index=False, name=None):
        boost = _float_or_none(row[boost_idx]) if boost_idx is not None else 0.0
        decision = evaluate_decision_engine_side(
            offset=int(_tuple_value(row, offset_idx) or 0),
            p_up=_float_or_none(_tuple_value(row, p_up_idx)),
            p_down=_float_or_none(_tuple_value(row, p_down_idx)),
            up_price=_resolve_first_numeric_tuple(row, column_positions, up_price_columns),
            down_price=_resolve_first_numeric_tuple(row, column_positions, down_price_columns),
            config=cfg,
            min_dir_prob_boost=float(boost or 0.0),
        )
        append_record(decision.as_record())

    parity = pd.DataFrame.from_records(records, index=out.index)
    return pd.concat([out, parity], axis=1)


def _resolve_inputs(
    *,
    p_up: float | None,
    p_down: float | None,
    up_price: float | None,
    down_price: float | None,
) -> tuple[float, float, float, float] | None:
    resolved_up = _float_or_none(p_up)
    resolved_down = _float_or_none(p_down)
    resolved_up_price = _float_or_none(up_price)
    resolved_down_price = _float_or_none(down_price)
    if resolved_up is None and resolved_down is None:
        return None
    if resolved_up is None:
        resolved_up = 1.0 - float(resolved_down)
    if resolved_down is None:
        resolved_down = 1.0 - float(resolved_up)
    if resolved_up_price is None or resolved_down_price is None:
        return None
    return float(resolved_up), float(resolved_down), float(resolved_up_price), float(resolved_down_price)


def _check_price_bounds(
    *,
    config: DecisionEngineParityConfig,
    up_price: float,
    down_price: float,
) -> tuple[bool, bool, str | None]:
    if not config.enforce_price_bounds:
        return True, True, None
    in_up = float(config.price_floor) <= up_price <= float(config.price_cap)
    in_down = float(config.price_floor) <= down_price <= float(config.price_cap)
    if not in_up and not in_down:
        return in_up, in_down, "price_bounds"
    return in_up, in_down, None


def _check_entry_band(
    *,
    config: DecisionEngineParityConfig,
    up_price: float,
    down_price: float,
) -> tuple[bool, bool, str | None]:
    if not config.enforce_entry_price_band:
        return True, True, None

    def in_band(price: float) -> bool:
        if config.entry_price_min is not None and price < float(config.entry_price_min):
            return False
        if config.entry_price_max is not None and price > float(config.entry_price_max):
            return False
        return True

    in_band_up = in_band(up_price)
    in_band_down = in_band(down_price)
    if not in_band_up and not in_band_down:
        if config.entry_price_min is not None and (
            up_price < float(config.entry_price_min) or down_price < float(config.entry_price_min)
        ):
            return in_band_up, in_band_down, "entry_price_min"
        return in_band_up, in_band_down, "entry_price_max"
    return in_band_up, in_band_down, None


def _classify_no_candidate_reject(
    *,
    in_up: bool,
    in_down: bool,
    in_band_up: bool,
    in_band_down: bool,
    p_up_eff: float,
    p_down_eff: float,
    min_dir_prob: float,
    up_edge: float,
    down_edge: float,
    min_net_edge_up: float,
    min_net_edge_down: float,
) -> str:
    dir_ok_up = in_up and in_band_up and p_up_eff > min_dir_prob
    dir_ok_down = in_down and in_band_down and p_down_eff > min_dir_prob
    if not (dir_ok_up or dir_ok_down):
        return "direction_prob"
    edge_ok_up = dir_ok_up and up_edge >= min_net_edge_up
    edge_ok_down = dir_ok_down and down_edge >= min_net_edge_down
    if not (edge_ok_up or edge_ok_down):
        return "net_edge"
    return "roi_or_price"


def _resolve_first_numeric(row: pd.Series, columns: Sequence[str]) -> float | None:
    for column in columns:
        value = _float_or_none(row.get(column))
        if value is not None:
            return value
    return None


def _resolve_first_numeric_tuple(
    row: tuple[object, ...],
    column_positions: dict[str, int],
    columns: Sequence[str],
) -> float | None:
    for column in columns:
        idx = column_positions.get(str(column))
        if idx is None:
            continue
        value = _float_or_none(row[idx])
        if value is not None:
            return value
    return None


def _tuple_value(row: tuple[object, ...], idx: int | None) -> object:
    if idx is None:
        return None
    return row[idx]


def _float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        parsed = float(value)
    except Exception:
        return None
    if math.isnan(parsed):
        return None
    return parsed


def _roi(*, edge: float, price: float) -> float:
    return edge / max(price, 1e-6)


def _roi_net(*, prob: float, effective_price: float, fee_rate: float) -> float:
    return prob / max(effective_price, 1e-6) - 1.0 - fee_rate
