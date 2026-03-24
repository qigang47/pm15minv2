from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats

from pm15min.research.evaluation.methods.abm import ABMConfig, run_abm_simulation
from pm15min.research.evaluation.methods.binary_metrics import brier_score
from pm15min.research.evaluation.methods.copula_risk import CopulaRiskConfig, run_copula_tail_risk
from pm15min.research.evaluation.methods.smc.particle_filter import ParticleFilterConfig, run_particle_filter


@dataclass(frozen=True)
class ProductionStackConfig:
    true_prob: float = 0.62
    init_price: float = 0.50
    n_steps: int = 1200
    seed: int = 42
    n_informed: int = 12
    n_noise: int = 60
    n_mm: int = 6
    n_particles: int = 3000
    process_sigma: float = 0.12
    obs_sigma: float = 0.05
    ensemble_w_pf: float = 0.55
    ensemble_w_news: float = 0.20
    ensemble_w_onchain: float = 0.15
    ensemble_w_jump: float = 0.10
    copula_family: str = "t"
    copula_tail: str = "lower"
    copula_n_sim: int = 120_000
    copula_quantile: float = 0.05
    risk_alpha: float = 0.99
    drawdown_alert: float = 0.08


@dataclass(frozen=True)
class ProductionStackResult:
    layer1_feed: pd.DataFrame
    layer2_probs: pd.DataFrame
    layer3_pairwise_tail: pd.DataFrame
    layer_summaries: dict[str, dict[str, float | int | str]]


def _clip_prob(x: np.ndarray) -> np.ndarray:
    return np.clip(np.asarray(x, dtype=float), 1e-6, 1.0 - 1e-6)


def _max_drawdown(pnl_path: np.ndarray) -> float:
    x = np.asarray(pnl_path, dtype=float)
    if x.size == 0:
        return 0.0
    running_max = np.maximum.accumulate(x)
    dd = running_max - x
    return float(np.max(dd))


def _evt_var_es(losses: np.ndarray, *, alpha: float, threshold_q: float = 0.95) -> tuple[float, float, dict[str, float]]:
    """GPD POT estimator with empirical fallback."""

    arr = np.asarray(losses, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size < 200:
        var = float(np.quantile(arr, alpha))
        es = float(np.mean(arr[arr >= var])) if np.any(arr >= var) else var
        return var, es, {"method": "empirical", "threshold": math.nan, "xi": math.nan, "beta": math.nan}

    u = float(np.quantile(arr, threshold_q))
    exceed = arr[arr > u] - u
    p_u = float(exceed.size / arr.size)

    if exceed.size < 80 or p_u <= 0.0:
        var = float(np.quantile(arr, alpha))
        es = float(np.mean(arr[arr >= var])) if np.any(arr >= var) else var
        return var, es, {"method": "empirical_fallback", "threshold": u, "xi": math.nan, "beta": math.nan}

    xi, _, beta = stats.genpareto.fit(exceed, floc=0.0)
    xi = float(xi)
    beta = float(beta)

    tail_prob = (1.0 - float(alpha)) / p_u
    if tail_prob <= 0.0:
        var = float(np.quantile(arr, alpha))
        es = float(np.mean(arr[arr >= var])) if np.any(arr >= var) else var
        return var, es, {"method": "empirical_fallback2", "threshold": u, "xi": xi, "beta": beta}

    if abs(xi) < 1e-6:
        var = u + beta * math.log(1.0 / tail_prob)
    else:
        var = u + (beta / xi) * (tail_prob ** (-xi) - 1.0)

    if xi < 1.0:
        es = (var + beta - xi * u) / max(1e-12, 1.0 - xi)
    else:
        es = float(np.mean(arr[arr >= var])) if np.any(arr >= var) else float(var)

    return float(var), float(es), {"method": "evt_gpd", "threshold": u, "xi": xi, "beta": beta}


def _build_multi_asset_returns(feed: pd.DataFrame, seed: int) -> pd.DataFrame:
    """Create synthetic multi-asset returns with shared factors for dependency modeling."""

    rng = np.random.default_rng(seed)
    base = pd.to_numeric(feed["ret_1"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    n = len(base)

    e1 = rng.normal(0.0, np.std(base) * 0.8 + 1e-6, size=n)
    e2 = rng.normal(0.0, np.std(base) * 0.9 + 1e-6, size=n)

    xrp = base
    sol = 0.68 * base + math.sqrt(max(1e-8, 1.0 - 0.68**2)) * e1
    btc = 0.42 * base + math.sqrt(max(1e-8, 1.0 - 0.42**2)) * e2

    return pd.DataFrame({"xrp_ret": xrp, "sol_ret": sol, "btc_ret": btc})


def run_production_stack_demo(config: ProductionStackConfig) -> ProductionStackResult:
    cfg = config

    abm_cfg = ABMConfig(
        true_prob=float(cfg.true_prob),
        init_price=float(cfg.init_price),
        n_informed=int(cfg.n_informed),
        n_noise=int(cfg.n_noise),
        n_mm=int(cfg.n_mm),
    )
    feed, abm_summary = run_abm_simulation(config=abm_cfg, n_steps=int(cfg.n_steps), seed=int(cfg.seed))

    t0 = pd.Timestamp("2026-03-01T00:00:00Z")
    feed["timestamp"] = pd.date_range(t0, periods=len(feed), freq="min", tz="UTC")
    feed["ret_1"] = feed["price"].diff().fillna(0.0)

    rng = np.random.default_rng(int(cfg.seed) + 100)
    feed["news_signal"] = _clip_prob(float(cfg.true_prob) + rng.normal(0.0, 0.06, size=len(feed)))
    feed["onchain_signal"] = _clip_prob(float(cfg.true_prob) + rng.normal(0.0, 0.04, size=len(feed)))
    feed["depth_proxy"] = 1.0 / np.maximum(1e-4, feed["spread"])

    layer1 = {
        "rows": int(len(feed)),
        "avg_spread": float(feed["spread"].mean()),
        "p95_spread": float(feed["spread"].quantile(0.95)),
        "avg_depth_proxy": float(feed["depth_proxy"].mean()),
        "abm_final_price": float(abm_summary["final_price"]),
        "abm_final_abs_error": float(abm_summary["final_abs_error"]),
        "abm_total_volume": float(abm_summary["total_volume"]),
    }

    pf_cfg = ParticleFilterConfig(
        n_particles=int(cfg.n_particles),
        process_sigma=float(cfg.process_sigma),
        obs_sigma=float(cfg.obs_sigma),
        resample_ess_ratio=0.5,
        prior_yes_prob=float(cfg.init_price),
        prior_logit_std=1.2,
    )
    pf = run_particle_filter(observations=feed["price"].to_numpy(dtype=float), config=pf_cfg, seed=int(cfg.seed) + 200)

    jump_score = np.clip(np.abs(feed["ret_1"].to_numpy(dtype=float)) / max(1e-6, np.std(feed["ret_1"])), 0.0, 4.0)
    jump_prob = _clip_prob(0.5 + 0.12 * np.tanh(jump_score - 1.0))

    w = np.array(
        [
            float(cfg.ensemble_w_pf),
            float(cfg.ensemble_w_news),
            float(cfg.ensemble_w_onchain),
            float(cfg.ensemble_w_jump),
        ],
        dtype=float,
    )
    w = w / np.sum(w)

    ensemble = _clip_prob(
        w[0] * pf["posterior_mean"].to_numpy(dtype=float)
        + w[1] * feed["news_signal"].to_numpy(dtype=float)
        + w[2] * feed["onchain_signal"].to_numpy(dtype=float)
        + w[3] * jump_prob
    )

    probs_df = pd.DataFrame(
        {
            "timestamp": feed["timestamp"],
            "market_price": feed["price"].to_numpy(dtype=float),
            "pf_prob": pf["posterior_mean"].to_numpy(dtype=float),
            "news_prob": feed["news_signal"].to_numpy(dtype=float),
            "onchain_prob": feed["onchain_signal"].to_numpy(dtype=float),
            "jump_prob": jump_prob,
            "ensemble_prob": ensemble,
            "pf_ess": pf["ess"].to_numpy(dtype=float),
        }
    )

    outcomes = rng.binomial(1, float(cfg.true_prob), size=len(probs_df)).astype(float)
    probs_df["outcome"] = outcomes

    layer2 = {
        "mean_pf_prob": float(np.mean(probs_df["pf_prob"])),
        "mean_ensemble_prob": float(np.mean(probs_df["ensemble_prob"])),
        "mean_pf_ess": float(np.mean(probs_df["pf_ess"])),
        "resample_count": int(np.sum(pf["resampled"].to_numpy(dtype=int))),
    }

    dep_df = _build_multi_asset_returns(feed, seed=int(cfg.seed) + 300)
    cop_cfg = CopulaRiskConfig(
        family=str(cfg.copula_family),
        tail=str(cfg.copula_tail),
        n_sim=int(cfg.copula_n_sim),
        quantile=float(cfg.copula_quantile),
        alpha=float(cfg.risk_alpha),
        tail_q=0.95,
        seed=int(cfg.seed) + 301,
    )
    cop = run_copula_tail_risk(
        data=dep_df.to_numpy(dtype=float),
        col_names=list(dep_df.columns),
        config=cop_cfg,
    )

    layer3 = {
        "copula_family": str(cop.summary["family"]),
        "all_bad_prob": float(cop.summary["all_bad_prob"]),
        "at_least_2_bad_prob": float(cop.summary["at_least_2_bad_prob"]),
        "VaR": float(cop.summary["VaR"]),
        "CVaR": float(cop.summary["CVaR"]),
    }
    if "nu" in cop.summary:
        layer3["nu"] = float(cop.summary["nu"])

    losses = -dep_df["xrp_ret"].to_numpy(dtype=float)
    var_evt, es_evt, evt_meta = _evt_var_es(losses, alpha=float(cfg.risk_alpha), threshold_q=0.95)

    stress_df = dep_df.copy()
    common = dep_df["xrp_ret"].to_numpy(dtype=float)
    eps = rng.normal(0.0, np.std(common), size=len(common))
    stress_df["sol_ret"] = 0.88 * common + math.sqrt(max(1e-8, 1.0 - 0.88**2)) * eps
    stress_cfg = CopulaRiskConfig(
        family="t",
        tail=str(cfg.copula_tail),
        n_sim=max(20_000, int(cfg.copula_n_sim // 4)),
        quantile=float(cfg.copula_quantile),
        alpha=float(cfg.risk_alpha),
        tail_q=0.95,
        seed=int(cfg.seed) + 302,
    )
    stress_cop = run_copula_tail_risk(
        data=stress_df.to_numpy(dtype=float),
        col_names=list(stress_df.columns),
        config=stress_cfg,
    )

    layer4 = {
        "evt_VaR": float(var_evt),
        "evt_ES": float(es_evt),
        "evt_method": str(evt_meta["method"]),
        "corr_stress_all_bad_prob": float(stress_cop.summary["all_bad_prob"]),
        "corr_stress_vs_base_ratio": float(stress_cop.summary["all_bad_prob"] / max(1e-9, cop.summary["all_bad_prob"])),
        "liquidity_low_depth_count": int(np.sum(feed["depth_proxy"] < np.quantile(feed["depth_proxy"], 0.10))),
    }

    brier = brier_score(probs_df["ensemble_prob"].to_numpy(dtype=float), probs_df["outcome"].to_numpy(dtype=float))

    edge = 0.03
    yes_trade = (probs_df["ensemble_prob"] > probs_df["market_price"] + edge).to_numpy(dtype=bool)
    no_trade = (probs_df["ensemble_prob"] < probs_df["market_price"] - edge).to_numpy(dtype=bool)

    yes_pnl = (float(cfg.true_prob) - probs_df["market_price"].to_numpy(dtype=float)) * yes_trade.astype(float)
    no_pnl = (probs_df["market_price"].to_numpy(dtype=float) - float(cfg.true_prob)) * no_trade.astype(float)
    strat_pnl = np.cumsum(yes_pnl + no_pnl)

    resid = probs_df["ensemble_prob"].to_numpy(dtype=float) - float(cfg.true_prob)
    mid = len(resid) // 2
    ks = stats.ks_2samp(resid[:mid], resid[mid:]) if mid >= 10 else None

    layer5 = {
        "brier": float(brier),
        "trades_yes": int(np.sum(yes_trade)),
        "trades_no": int(np.sum(no_trade)),
        "strategy_final_pnl": float(strat_pnl[-1]) if len(strat_pnl) else 0.0,
        "strategy_max_drawdown": float(_max_drawdown(strat_pnl)),
        "drawdown_alert_triggered": int(float(_max_drawdown(strat_pnl)) >= float(cfg.drawdown_alert)),
        "drift_ks_stat": float(ks.statistic) if ks is not None else math.nan,
        "drift_ks_pvalue": float(ks.pvalue) if ks is not None else math.nan,
    }

    summaries = {
        "layer1_data_ingestion": layer1,
        "layer2_probability_engine": layer2,
        "layer3_dependency_modeling": layer3,
        "layer4_risk_management": layer4,
        "layer5_monitoring": layer5,
    }

    return ProductionStackResult(
        layer1_feed=feed,
        layer2_probs=probs_df,
        layer3_pairwise_tail=cop.pairwise_tail,
        layer_summaries=summaries,
    )


def render_production_stack_markdown(result: ProductionStackResult) -> str:
    lines = ["# Production Stack Demo", ""]

    for layer_name in [
        "layer1_data_ingestion",
        "layer2_probability_engine",
        "layer3_dependency_modeling",
        "layer4_risk_management",
        "layer5_monitoring",
    ]:
        lines.append(f"## {layer_name}")
        lines.append("")
        info = result.layer_summaries[layer_name]
        for key in sorted(info.keys()):
            value = info[key]
            if isinstance(value, float):
                lines.append(f"- {key}: `{value:.6f}`")
            else:
                lines.append(f"- {key}: `{value}`")
        lines.append("")

    return "\n".join(lines)
