from __future__ import annotations

from ..config import DataConfig


def dataset_audit_rules(cfg: DataConfig) -> dict[str, dict[str, object]]:
    if cfg.surface == "live":
        return {
            "binance_klines_1m_source": {"severity": "error", "min_row_count": 2, "max_age_seconds": 90 * 60},
            "market_catalog_table": {"severity": "error", "min_row_count": 1, "max_age_seconds": 12 * 3600},
            "direct_oracle_source": {"severity": "error", "min_row_count": 1, "max_age_seconds": 12 * 3600},
            "oracle_prices_table": {"severity": "error", "min_row_count": 1},
            "settlement_truth_source": {"severity": "warning", "min_row_count": 1},
            "truth_table": {"severity": "warning", "min_row_count": 1},
            "chainlink_streams_source": {"severity": "warning", "min_row_count": 1},
            "chainlink_datafeeds_source": {"severity": "warning", "min_row_count": 1},
            "orderbook_index_table": {"severity": "warning", "min_row_count": 1, "max_age_seconds": 90 * 60},
            "orderbook_depth_source": {"severity": "error", "min_partition_count": 1, "max_partition_age_days": 1},
        }
    return {
        "binance_klines_1m_source": {"severity": "error", "min_row_count": 2},
        "market_catalog_table": {"severity": "warning", "min_row_count": 1},
        "direct_oracle_source": {"severity": "warning", "min_row_count": 1},
        "settlement_truth_source": {"severity": "warning", "min_row_count": 1},
        "oracle_prices_table": {"severity": "error", "min_row_count": 1},
        "truth_table": {"severity": "error", "min_row_count": 1},
        "chainlink_streams_source": {"severity": "warning", "min_row_count": 1},
        "chainlink_datafeeds_source": {"severity": "warning", "min_row_count": 1},
        "orderbook_index_table": {"severity": "warning", "min_row_count": 1},
        "orderbook_depth_source": {"severity": "warning", "min_partition_count": 1},
    }


def critical_dataset_names(*, surface: str) -> list[str]:
    token = str(surface or "backtest").strip().lower()
    if token == "live":
        return [
            "binance_klines_1m_source",
            "market_catalog_table",
            "direct_oracle_source",
            "oracle_prices_table",
            "orderbook_depth_source",
        ]
    return [
        "binance_klines_1m_source",
        "oracle_prices_table",
        "truth_table",
    ]
