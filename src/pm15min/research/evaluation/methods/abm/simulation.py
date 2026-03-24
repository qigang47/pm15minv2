from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ABMConfig:
    true_prob: float = 0.65
    init_price: float = 0.50
    n_informed: int = 10
    n_noise: int = 50
    n_mm: int = 5
    informed_signal_sigma: float = 0.02
    informed_threshold: float = 0.01
    informed_max_size: float = 0.10
    informed_sensitivity: float = 2.0
    noise_size_scale: float = 0.02
    noise_max_size: float = 0.20
    mm_min_spread: float = 0.02
    mm_base_spread: float = 0.05
    mm_decay_volume_scale: float = 120.0
    impact_floor: float = 0.02
    impact_noise_scale: float = 0.10
    price_floor: float = 0.01
    price_cap: float = 0.99

    def __post_init__(self) -> None:
        if not (0.0 < float(self.true_prob) < 1.0):
            raise ValueError(f"true_prob must be in (0,1), got {self.true_prob}")
        if not (0.0 < float(self.init_price) < 1.0):
            raise ValueError(f"init_price must be in (0,1), got {self.init_price}")
        for name in ["n_informed", "n_noise", "n_mm"]:
            if int(getattr(self, name)) < 0:
                raise ValueError(f"{name} must be >=0, got {getattr(self, name)}")
        if int(self.n_informed + self.n_noise + self.n_mm) <= 0:
            raise ValueError("Total agents must be positive")


class PredictionMarketABM:
    """Agent-based prediction market simulator with informed, noise, and market-maker agents."""

    def __init__(self, config: ABMConfig, seed: int | None = None):
        self.cfg = config
        self.rng = np.random.default_rng(seed)

        self.true_prob = float(config.true_prob)
        self.price = float(config.init_price)

        self.best_bid = max(config.price_floor, self.price - config.mm_base_spread / 2.0)
        self.best_ask = min(config.price_cap, self.price + config.mm_base_spread / 2.0)

        self.volume = 0.0
        self.informed_pnl = 0.0
        self.noise_pnl = 0.0
        self.mm_pnl = 0.0

        self.steps = 0
        self.trade_count = 0
        self.informed_trades = 0
        self.noise_trades = 0
        self.mm_updates = 0

        self.price_history = [self.price]
        self.bid_history = [self.best_bid]
        self.ask_history = [self.best_ask]
        self.spread_history = [self.best_ask - self.best_bid]
        self.error_history = [abs(self.price - self.true_prob)]
        self.volume_history = [self.volume]

    def _clip_price(self, value: float) -> float:
        return float(np.clip(float(value), self.cfg.price_floor, self.cfg.price_cap))

    def _kyle_lambda(self) -> float:
        sigma_v = abs(self.true_prob - self.price) + float(self.cfg.impact_floor)
        sigma_u = float(self.cfg.impact_noise_scale) * math.sqrt(max(1.0, float(self.cfg.n_noise)))
        return max(1e-6, sigma_v / (2.0 * sigma_u))

    def _update_book_around_price(self, spread: float | None = None) -> None:
        if spread is None:
            spread = max(float(self.cfg.mm_min_spread), float(self.best_ask - self.best_bid))
        book_spread = max(float(self.cfg.mm_min_spread), float(spread))
        self.best_bid = self._clip_price(self.price - book_spread / 2.0)
        self.best_ask = self._clip_price(self.price + book_spread / 2.0)
        if self.best_ask <= self.best_bid:
            self.best_ask = self._clip_price(self.best_bid + float(self.cfg.mm_min_spread))

    def _informed_trade(self) -> None:
        signal = self.true_prob + self.rng.normal(0.0, float(self.cfg.informed_signal_sigma))
        kyle_lambda = self._kyle_lambda()
        traded = False

        if signal > self.best_ask + float(self.cfg.informed_threshold):
            size = min(float(self.cfg.informed_max_size), (signal - self.price) * float(self.cfg.informed_sensitivity))
            size = max(0.0, size)
            if size > 0.0:
                exec_price = self.best_ask
                self.price = self._clip_price(self.price + size * kyle_lambda)
                self.volume += size
                self.informed_pnl += (self.true_prob - exec_price) * size
                self.mm_pnl += (exec_price - self.price) * size
                self.trade_count += 1
                self.informed_trades += 1
                traded = True
        elif signal < self.best_bid - float(self.cfg.informed_threshold):
            size = min(float(self.cfg.informed_max_size), (self.price - signal) * float(self.cfg.informed_sensitivity))
            size = max(0.0, size)
            if size > 0.0:
                exec_price = self.best_bid
                self.price = self._clip_price(self.price - size * kyle_lambda)
                self.volume += size
                self.informed_pnl += (exec_price - self.true_prob) * size
                self.mm_pnl += (self.price - exec_price) * size
                self.trade_count += 1
                self.informed_trades += 1
                traded = True

        self._update_book_around_price()
        if not traded:
            self.mm_updates += 1

    def _noise_trade(self) -> None:
        direction = int(self.rng.choice([-1, 1]))
        size = min(float(self.cfg.noise_max_size), float(self.rng.exponential(float(self.cfg.noise_size_scale))))
        size = max(0.0, size)
        if size <= 0.0:
            self.mm_updates += 1
            return

        kyle_lambda = self._kyle_lambda()
        exec_price = self.best_ask if direction > 0 else self.best_bid
        self.price = self._clip_price(self.price + direction * size * kyle_lambda)
        self.volume += size
        self.trade_count += 1
        self.noise_trades += 1

        self.noise_pnl -= abs(exec_price - self.true_prob) * size
        self.mm_pnl += abs(exec_price - self.price) * size * 0.5

        self._update_book_around_price()

    def _mm_update(self) -> None:
        decay = math.exp(-self.volume / max(1.0, float(self.cfg.mm_decay_volume_scale)))
        spread = float(self.cfg.mm_min_spread) + float(self.cfg.mm_base_spread) * decay
        self._update_book_around_price(spread=spread)
        self.mm_updates += 1

    def step(self) -> None:
        total_agents = int(self.cfg.n_informed + self.cfg.n_noise + self.cfg.n_mm)
        random_draw = float(self.rng.random())
        p_informed = float(self.cfg.n_informed) / total_agents
        p_noise = float(self.cfg.n_noise) / total_agents

        if random_draw < p_informed:
            self._informed_trade()
        elif random_draw < p_informed + p_noise:
            self._noise_trade()
        else:
            self._mm_update()

        self.steps += 1
        self.price_history.append(self.price)
        self.bid_history.append(self.best_bid)
        self.ask_history.append(self.best_ask)
        self.spread_history.append(self.best_ask - self.best_bid)
        self.error_history.append(abs(self.price - self.true_prob))
        self.volume_history.append(self.volume)

    def run(self, n_steps: int = 1000) -> np.ndarray:
        n_steps = int(n_steps)
        if n_steps <= 0:
            raise ValueError(f"n_steps must be positive, got {n_steps}")
        for _ in range(n_steps):
            self.step()
        return np.asarray(self.price_history, dtype=float)

    def convergence_time(self, epsilon: float = 0.02, hold_steps: int = 30) -> int | None:
        errors = np.asarray(self.error_history, dtype=float)
        if errors.size < hold_steps:
            return None
        eps = float(epsilon)
        window = int(hold_steps)
        for index in range(window - 1, errors.size):
            if np.all(errors[index - window + 1 : index + 1] <= eps):
                return int(index - window + 1)
        return None

    def to_frame(self) -> pd.DataFrame:
        steps = np.arange(len(self.price_history), dtype=int)
        return pd.DataFrame(
            {
                "t": steps,
                "price": np.asarray(self.price_history, dtype=float),
                "best_bid": np.asarray(self.bid_history, dtype=float),
                "best_ask": np.asarray(self.ask_history, dtype=float),
                "spread": np.asarray(self.spread_history, dtype=float),
                "abs_error": np.asarray(self.error_history, dtype=float),
                "cum_volume": np.asarray(self.volume_history, dtype=float),
            }
        )

    def summary(self, epsilon: float = 0.02, hold_steps: int = 30) -> dict[str, float | int | str]:
        convergence = self.convergence_time(epsilon=epsilon, hold_steps=hold_steps)
        return {
            "true_prob": float(self.true_prob),
            "start_price": float(self.price_history[0]),
            "final_price": float(self.price_history[-1]),
            "final_abs_error": float(abs(self.price_history[-1] - self.true_prob)),
            "mean_abs_error": float(np.mean(self.error_history)),
            "convergence_time": -1 if convergence is None else int(convergence),
            "total_steps": int(self.steps),
            "total_volume": float(self.volume),
            "trade_count": int(self.trade_count),
            "informed_trades": int(self.informed_trades),
            "noise_trades": int(self.noise_trades),
            "mm_updates": int(self.mm_updates),
            "informed_pnl": float(self.informed_pnl),
            "noise_pnl": float(self.noise_pnl),
            "mm_pnl": float(self.mm_pnl),
        }


def run_abm_simulation(
    *,
    config: ABMConfig,
    n_steps: int,
    seed: int | None = None,
) -> tuple[pd.DataFrame, dict[str, float | int | str]]:
    simulator = PredictionMarketABM(config=config, seed=seed)
    simulator.run(n_steps=int(n_steps))
    return simulator.to_frame(), simulator.summary()


def sweep_informed_noise_ratio(
    *,
    true_prob: float,
    init_price: float,
    informed_values: Sequence[int],
    noise_values: Sequence[int],
    n_mm: int,
    n_steps: int,
    seeds: Iterable[int],
) -> pd.DataFrame:
    rows: list[dict[str, float | int | str]] = []
    for n_informed in informed_values:
        for n_noise in noise_values:
            for seed in seeds:
                config = ABMConfig(
                    true_prob=float(true_prob),
                    init_price=float(init_price),
                    n_informed=int(n_informed),
                    n_noise=int(n_noise),
                    n_mm=int(n_mm),
                )
                _, summary = run_abm_simulation(config=config, n_steps=int(n_steps), seed=int(seed))
                rows.append(
                    {
                        "n_informed": int(n_informed),
                        "n_noise": int(n_noise),
                        "n_mm": int(n_mm),
                        "seed": int(seed),
                        **summary,
                    }
                )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["informed_to_noise"] = out["n_informed"] / np.maximum(1, out["n_noise"])
    return out
