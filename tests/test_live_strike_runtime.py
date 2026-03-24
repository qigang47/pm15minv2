from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm15min.core.config import LiveConfig
from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.live.oracle.strike_cache import StrikeCache, StrikeCacheRecord
from pm15min.live.oracle.strike_runtime import (
    LiveRuntimeStrikeResolver,
    build_live_runtime_oracle_prices,
)
import pm15min.live.oracle.strike_runtime as strike_runtime_module
from pm15min.live.signal.utils import build_live_feature_frame


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def _clear_runtime_caches() -> None:
    strike_runtime_module._OPEN_PRICE_CACHE.clear()
    strike_runtime_module._OPEN_PRICE_LAST_ATTEMPT.clear()
    strike_runtime_module._STREAMS_BOUNDARY_CACHE.clear()
    strike_runtime_module._RTDS_PROVIDER_CACHE.clear()


def _raw_klines(start: str, *, periods: int = 4) -> pd.DataFrame:
    ts = pd.date_range(start, periods=periods, freq="min", tz="UTC")
    close = [100.2 + idx * 0.02 + ((idx % 5) - 2) * 0.3 for idx in range(periods)]
    return pd.DataFrame(
        {
            "open_time": ts,
            "open": [value - 0.1 for value in close],
            "high": [value + 0.3 for value in close],
            "low": [value - 0.3 for value in close],
            "close": close,
            "volume": [10.0] * periods,
            "quote_asset_volume": [1000.0] * periods,
            "taker_buy_quote_volume": [500.0] * periods,
            "number_of_trades": [100 + idx for idx in range(periods)],
            "symbol": ["SOLUSDT"] * periods,
        }
    )


class _FakeOracleClient:
    def __init__(self, payloads: list[dict[str, object]]) -> None:
        self.payloads = list(payloads)

    def fetch_crypto_price(self, *, symbol: str, cycle_start_ts: int, cycle_seconds: int) -> dict[str, object]:
        if not self.payloads:
            return {}
        return dict(self.payloads.pop(0))


class _FakeRTDSProvider:
    def __init__(self, quote, *, enforce_skew: bool = False):
        self.quote = quote
        self.enforce_skew = enforce_skew
        self.started = 0

    def start(self) -> None:
        self.started += 1

    def price_near(self, cycle_start_ts, *, max_skew_ms: int):
        if self.quote is None:
            return None
        if self.enforce_skew:
            target_ms = int(pd.Timestamp(cycle_start_ts).timestamp() * 1000)
            if abs(int(self.quote.ts_ms) - int(target_ms)) > int(max_skew_ms):
                return None
        return self.quote


def test_live_runtime_strike_resolver_prefers_open_price_over_streams_and_cache(tmp_path: Path) -> None:
    _clear_runtime_caches()
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    cycle_start_ts = pd.Timestamp("2026-03-23T10:00:00Z")
    streams_path = cfg.layout.streams_source_root / "year=2026" / "month=03" / "data.parquet"
    streams_path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "extra_ts": int(cycle_start_ts.timestamp()),
                    "price": 111.0,
                    "tx_hash": "0x1",
                    "perform_idx": 1,
                    "value_idx": 1,
                }
            ]
        ),
        streams_path,
    )
    cache_path = cfg.layout.cache_root / "live_oracle" / "strike_cache_sol.csv"
    cache = StrikeCache(path=cache_path, asset_slug="sol")
    cache.put(
        StrikeCacheRecord(
            cycle_start_ts=int(cycle_start_ts.timestamp()),
            strike_price=105.0,
            observed_ts_ms=int(cycle_start_ts.timestamp() * 1000),
            source="legacy_cache",
        )
    )

    resolver = LiveRuntimeStrikeResolver(
        data_cfg=cfg,
        market_slug="sol",
        oracle_client=_FakeOracleClient([{"openPrice": 120.0}]),
        cache_path=cache_path,
    )

    quote = resolver.strike_at(cycle_start_ts)

    assert quote is not None
    assert quote.price == 120.0
    assert quote.source == "polymarket_open_price_api"


def test_live_runtime_strike_resolver_falls_back_to_streams_then_cache(tmp_path: Path) -> None:
    _clear_runtime_caches()
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    cycle_start_ts = pd.Timestamp("2026-03-23T10:00:00Z")
    cache_path = cfg.layout.cache_root / "live_oracle" / "strike_cache_sol.csv"
    cache = StrikeCache(path=cache_path, asset_slug="sol")
    cache.put(
        StrikeCacheRecord(
            cycle_start_ts=int(cycle_start_ts.timestamp()),
            strike_price=105.0,
            observed_ts_ms=int(cycle_start_ts.timestamp() * 1000),
            source="legacy_cache",
        )
    )

    streams_path = cfg.layout.streams_source_root / "year=2026" / "month=03" / "data.parquet"
    streams_path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "extra_ts": int(cycle_start_ts.timestamp()),
                    "price": 111.0,
                    "tx_hash": "0x1",
                    "perform_idx": 1,
                    "value_idx": 1,
                }
            ]
        ),
        streams_path,
    )

    resolver = LiveRuntimeStrikeResolver(
        data_cfg=cfg,
        market_slug="sol",
        oracle_client=_FakeOracleClient([{}]),
        cache_path=cache_path,
    )
    quote = resolver.strike_at(cycle_start_ts)
    assert quote is not None
    assert quote.price == 111.0
    assert quote.source == "streams_parquet"

    streams_path.unlink()
    _clear_runtime_caches()
    fallback_resolver = LiveRuntimeStrikeResolver(
        data_cfg=cfg,
        market_slug="sol",
        oracle_client=_FakeOracleClient([{}]),
        cache_path=cache_path,
    )
    fallback_quote = fallback_resolver.strike_at(cycle_start_ts)
    assert fallback_quote is not None
    assert fallback_quote.price == 111.0
    assert fallback_quote.source == "streams_parquet"

    fresh_root = tmp_path / "fresh"
    fresh_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=fresh_root / "v2")
    fresh_cache_path = fresh_cfg.layout.cache_root / "live_oracle" / "strike_cache_sol.csv"
    fresh_cache = StrikeCache(path=fresh_cache_path, asset_slug="sol")
    fresh_cache.put(
        StrikeCacheRecord(
            cycle_start_ts=int(cycle_start_ts.timestamp()),
            strike_price=105.0,
            observed_ts_ms=int(cycle_start_ts.timestamp() * 1000),
            source="legacy_cache",
        )
    )
    _clear_runtime_caches()
    cache_only_resolver = LiveRuntimeStrikeResolver(
        data_cfg=fresh_cfg,
        market_slug="sol",
        oracle_client=_FakeOracleClient([{}]),
        cache_path=fresh_cache_path,
    )
    cache_only_quote = cache_only_resolver.strike_at(cycle_start_ts)
    assert cache_only_quote is not None
    assert cache_only_quote.price == 105.0
    assert cache_only_quote.source == "strike_cache:legacy_cache"


def test_build_live_runtime_oracle_prices_falls_back_to_existing_table_when_unresolved(tmp_path: Path) -> None:
    _clear_runtime_caches()
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    raw_klines = _raw_klines("2026-03-23T10:01:00Z", periods=2)
    base = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": int(pd.Timestamp("2026-03-23T10:00:00Z").timestamp()),
                "cycle_end_ts": int(pd.Timestamp("2026-03-23T10:15:00Z").timestamp()),
                "price_to_beat": 95.0,
                "final_price": pd.NA,
                "source_price_to_beat": "oracle_prices_table",
                "source_final_price": "",
                "has_price_to_beat": True,
                "has_final_price": False,
                "has_both": False,
            }
        ]
    )

    out = build_live_runtime_oracle_prices(
        data_cfg=cfg,
        market_slug="sol",
        raw_klines=raw_klines,
        oracle_prices_table=base,
        oracle_client=_FakeOracleClient([{}]),
        cache_path=cfg.layout.cache_root / "live_oracle" / "strike_cache_sol.csv",
    )

    assert float(out.iloc[0]["price_to_beat"]) == 95.0
    assert out.iloc[0]["source_price_to_beat"] == "oracle_prices_table"


def test_live_runtime_strike_resolver_uses_rtds_after_cache_miss(tmp_path: Path) -> None:
    _clear_runtime_caches()
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    cycle_start_ts = pd.Timestamp("2026-03-23T10:00:00Z")
    rtds = _FakeRTDSProvider(
        strike_runtime_module.StrikeQuote(
            price=118.0,
            ts_ms=int(cycle_start_ts.timestamp() * 1000),
            source="rtds_chainlink_boundary",
        )
    )

    resolver = LiveRuntimeStrikeResolver(
        data_cfg=cfg,
        market_slug="sol",
        oracle_client=_FakeOracleClient([{}]),
        cache_path=cfg.layout.cache_root / "live_oracle" / "strike_cache_sol.csv",
        rtds_provider=rtds,
    )
    quote = resolver.strike_at(cycle_start_ts)

    assert quote is not None
    assert quote.price == 118.0
    assert quote.source == "rtds_chainlink_boundary"
    assert rtds.started == 1

    cached = resolver.cache.get(int(cycle_start_ts.timestamp()))
    assert cached is not None
    assert cached.source == "rtds_chainlink_boundary"


def test_live_runtime_strike_resolver_prefers_valid_cached_rtds_before_live_rtds(tmp_path: Path) -> None:
    _clear_runtime_caches()
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    cycle_start_ts = pd.Timestamp("2026-03-23T10:00:00Z")
    cache_path = cfg.layout.cache_root / "live_oracle" / "strike_cache_sol.csv"
    cache = StrikeCache(path=cache_path, asset_slug="sol")
    cache.put(
        StrikeCacheRecord(
            cycle_start_ts=int(cycle_start_ts.timestamp()),
            strike_price=117.0,
            observed_ts_ms=int(cycle_start_ts.timestamp() * 1000) + 500,
            source="rtds_chainlink_boundary",
        )
    )
    rtds = _FakeRTDSProvider(
        strike_runtime_module.StrikeQuote(
            price=119.0,
            ts_ms=int(cycle_start_ts.timestamp() * 1000),
            source="rtds_chainlink_boundary",
        )
    )

    resolver = LiveRuntimeStrikeResolver(
        data_cfg=cfg,
        market_slug="sol",
        oracle_client=_FakeOracleClient([{}]),
        cache_path=cache_path,
        rtds_provider=rtds,
    )
    quote = resolver.strike_at(cycle_start_ts)

    assert quote is not None
    assert quote.price == 117.0
    assert quote.source == "strike_cache:rtds_chainlink_boundary"


def test_live_runtime_strike_resolver_rejects_stale_cached_rtds_and_skewed_live_rtds(tmp_path: Path) -> None:
    _clear_runtime_caches()
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    cycle_start_ts = pd.Timestamp("2026-03-23T10:00:00Z")
    cache_path = cfg.layout.cache_root / "live_oracle" / "strike_cache_sol.csv"
    cache = StrikeCache(path=cache_path, asset_slug="sol")
    cache.put(
        StrikeCacheRecord(
            cycle_start_ts=int(cycle_start_ts.timestamp()),
            strike_price=117.0,
            observed_ts_ms=int(cycle_start_ts.timestamp() * 1000) + 5_000,
            source="rtds_chainlink_boundary",
        )
    )
    rtds = _FakeRTDSProvider(
        strike_runtime_module.StrikeQuote(
            price=121.0,
            ts_ms=int(cycle_start_ts.timestamp() * 1000) + 5_000,
            source="rtds_chainlink_boundary",
        ),
        enforce_skew=True,
    )

    resolver = LiveRuntimeStrikeResolver(
        data_cfg=cfg,
        market_slug="sol",
        oracle_client=_FakeOracleClient([{}]),
        cache_path=cache_path,
        rtds_provider=rtds,
    )
    quote = resolver.strike_at(cycle_start_ts)

    assert quote is None


def test_build_live_feature_frame_prefers_runtime_strike_overlay(tmp_path: Path, monkeypatch) -> None:
    _clear_runtime_caches()
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    raw_klines = _raw_klines("2026-03-23T10:01:00Z", periods=3)
    btc_klines = _raw_klines("2026-03-23T10:01:00Z", periods=3)
    base_oracle = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": int(pd.Timestamp("2026-03-23T10:00:00Z").timestamp()),
                "cycle_end_ts": int(pd.Timestamp("2026-03-23T10:15:00Z").timestamp()),
                "price_to_beat": 90.0,
                "final_price": pd.NA,
                "source_price_to_beat": "oracle_prices_table",
                "source_final_price": "",
                "has_price_to_beat": True,
                "has_final_price": False,
                "has_both": False,
            }
        ]
    )
    streams_path = (
        DataConfig.build(market="sol", cycle="15m", surface="live", root=root).layout.streams_source_root
        / "year=2026"
        / "month=03"
        / "data.parquet"
    )
    streams_path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "extra_ts": int(pd.Timestamp("2026-03-23T10:00:00Z").timestamp()),
                    "price": 111.0,
                    "tx_hash": "0x1",
                    "perform_idx": 1,
                    "value_idx": 1,
                }
            ]
        ),
        streams_path,
    )
    captured: dict[str, pd.DataFrame] = {}

    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_binance_klines_1m",
        lambda data_cfg, symbol=None: btc_klines if str(symbol or "").upper() == "BTCUSDT" else raw_klines,
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_oracle_prices_table",
        lambda data_cfg: base_oracle.copy(),
    )
    monkeypatch.setattr(
        "pm15min.data.sources.polymarket_oracle_api.PolymarketOracleApiClient.fetch_crypto_price",
        lambda self, *, symbol, cycle_start_ts, cycle_seconds: {"openPrice": 120.0},
    )

    def _capture_feature_frame(raw_klines_arg, *, feature_set, oracle_prices, btc_klines=None, cycle="15m"):
        captured["oracle_prices"] = oracle_prices.copy()
        return pd.DataFrame([{"decision_ts": "2026-03-23T10:03:00Z", "offset": 7}])

    monkeypatch.setattr("pm15min.live.signal.utils.build_feature_frame_df", _capture_feature_frame)

    out = build_live_feature_frame(cfg, feature_set="v6_user_core")

    assert len(out) == 1
    oracle_prices = captured["oracle_prices"]
    latest = oracle_prices.loc[pd.to_numeric(oracle_prices["cycle_start_ts"], errors="coerce").eq(int(pd.Timestamp("2026-03-23T10:00:00Z").timestamp()))]
    assert len(latest) == 1
    assert float(latest.iloc[0]["price_to_beat"]) == 120.0
    assert latest.iloc[0]["source_price_to_beat"] == "polymarket_open_price_api"


def test_build_live_feature_frame_exposes_legacy_live_features(tmp_path: Path, monkeypatch) -> None:
    _clear_runtime_caches()
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    raw_klines = _raw_klines("2026-03-23T00:00:00Z", periods=480)
    btc_klines = _raw_klines("2026-03-23T00:00:00Z", periods=480)
    btc_klines["symbol"] = "BTCUSDT"
    cycle_starts = pd.date_range("2026-03-23T00:00:00Z", periods=40, freq="15min", tz="UTC")
    oracle_prices = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": int(ts.timestamp()),
                "cycle_end_ts": int((ts + pd.Timedelta(minutes=15)).timestamp()),
                "price_to_beat": 100.0 + idx * 0.25,
                "final_price": pd.NA,
                "source_price_to_beat": "oracle_prices_table",
                "source_final_price": "",
                "has_price_to_beat": True,
                "has_final_price": False,
                "has_both": False,
            }
            for idx, ts in enumerate(cycle_starts)
        ]
    )

    monkeypatch.setattr(
        "pm15min.live.signal.utils.load_binance_klines_1m",
        lambda data_cfg, symbol=None: btc_klines if str(symbol or "").upper() == "BTCUSDT" else raw_klines,
    )
    monkeypatch.setattr(
        "pm15min.live.signal.utils.build_live_runtime_oracle_prices",
        lambda **kwargs: oracle_prices.copy(),
    )

    out = build_live_feature_frame(cfg, feature_set="v6_user_core")
    latest = out.sort_values("decision_ts").tail(1).iloc[0]
    for column in (
        "ret_60m",
        "ma_gap_15",
        "ema_gap_12",
        "ma_15_slope",
        "gk_vol_30",
        "rs_vol_30",
        "rr_30",
        "macd_hist",
        "rsi_14",
        "median_gap_20",
        "vwap_gap_60",
        "regime_trend",
        "obv_z",
        "donch_pos_20",
        "trade_intensity",
        "hour_sin",
        "hour_cos",
        "dow_sin",
        "dow_cos",
        "z_ret_60m",
        "first_half_ret",
        "second_half_ret_proxy",
        "ret_1m_lag2",
        "ret_15m_lag1",
        "rsi_14_lag1",
        "taker_buy_ratio_lag1",
        "macd_extreme",
        "momentum_agree",
        "q_bs_up_strike",
    ):
        assert column in out.columns
        assert pd.notna(latest[column]), column
