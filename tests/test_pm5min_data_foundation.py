from __future__ import annotations

import importlib
import json
from pathlib import Path

from pm5min.data.config import DataConfig


def _load_pm5min_pipeline(module_name: str):
    module_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "pm5min"
        / "data"
        / "pipelines"
        / f"{module_name}.py"
    )
    assert module_path.exists(), f"Expected local pm5min pipeline module at {module_path}"
    return importlib.import_module(f"pm5min.data.pipelines.{module_name}")


def test_pm5min_foundation_runtime_reports_5m_cycle(tmp_path, monkeypatch) -> None:
    module = _load_pm5min_pipeline("foundation_runtime")
    cfg = DataConfig.build(market="sol", cycle="5m", surface="live", root=tmp_path)
    monkeypatch.setattr(module, "_build_foundation_task_specs", lambda *args, **kwargs: [])

    payload = module.run_live_data_foundation(cfg, iterations=1, loop=False)

    assert payload["cycle"] == "5m"


def test_pm5min_foundation_runtime_uses_pm5min_local_steps(tmp_path, monkeypatch) -> None:
    module = _load_pm5min_pipeline("foundation_runtime")
    cfg = DataConfig.build(market="sol", cycle="5m", surface="live", root=tmp_path)

    monkeypatch.setattr(module, "sync_market_catalog", lambda *args, **kwargs: {"dataset": "market_catalog"})
    monkeypatch.setattr(module, "sync_binance_klines_1m", lambda *args, **kwargs: {"dataset": "binance_klines_1m"})
    monkeypatch.setattr(module, "sync_streams_from_rpc", lambda *args, **kwargs: {"dataset": "streams_rpc"})
    monkeypatch.setattr(
        module,
        "sync_polymarket_oracle_price_window",
        lambda *args, **kwargs: {
            "dataset": "polymarket_direct_oracle_price_window",
            "rows_imported": 1,
            "canonical_rows": 1,
            "target_path": str(cfg.layout.direct_oracle_source_path),
        },
    )
    monkeypatch.setattr(module, "build_oracle_prices_15m", lambda *args, **kwargs: {"dataset": "oracle_prices_5m"})
    monkeypatch.setattr(module, "run_orderbook_recorder", lambda *args, **kwargs: {"dataset": "orderbook_depth", "status": "ok"})

    payload = module.run_live_data_foundation(
        cfg,
        iterations=1,
        loop=False,
        include_direct_oracle=True,
        include_streams=True,
        include_orderbooks=True,
    )

    assert "oracle_prices_5m" in json.dumps(payload)

