from __future__ import annotations

from pathlib import Path

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.queries.loaders import load_binance_klines_1m, load_oracle_prices_table
from pm15min.research.config import ResearchConfig
from pm15min.research.features.builders import build_feature_frame as build_feature_frame_df
from pm15min.research.features.registry import feature_schema, feature_set_columns
from pm15min.research.manifests import build_manifest, write_manifest


def build_feature_frame_dataset(cfg: ResearchConfig) -> dict[str, object]:
    data_cfg = DataConfig.build(
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        surface=cfg.source_surface,
        root=cfg.layout.storage.rewrite_root,
    )
    raw_klines = load_binance_klines_1m(data_cfg)
    if raw_klines.empty:
        raise FileNotFoundError(
            f"Missing Binance 1m source dataset: {data_cfg.layout.binance_klines_path()}. "
            "Expected canonical source under v2/data/<surface>/sources/binance/klines_1m/."
        )
    oracle_prices = load_oracle_prices_table(data_cfg)
    btc_klines = None
    if cfg.asset.slug != "btc":
        btc_cfg = DataConfig.build(
            market="btc",
            cycle=cfg.cycle,
            surface=cfg.source_surface,
            root=cfg.layout.storage.rewrite_root,
        )
        btc_klines = load_binance_klines_1m(btc_cfg, symbol="BTCUSDT")

    frame = build_feature_frame_df(
        raw_klines,
        feature_set=cfg.feature_set,
        oracle_prices=oracle_prices,
        btc_klines=btc_klines,
        cycle=cfg.cycle,
    )

    data_path = cfg.layout.feature_frame_path(cfg.feature_set, source_surface=cfg.source_surface)
    manifest_path = cfg.layout.feature_frame_manifest_path(cfg.feature_set, source_surface=cfg.source_surface)
    write_parquet_atomic(frame, data_path)
    manifest = build_manifest(
        object_type="feature_frame",
        object_id=f"feature_frame:{cfg.asset.slug}:{cfg.feature_set}:{cfg.source_surface}",
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        path=data_path,
        spec={
            "feature_set": cfg.feature_set,
            "source_surface": cfg.source_surface,
            "feature_columns": list(feature_set_columns(cfg.feature_set)),
            "feature_schema": feature_schema(cfg.feature_set),
        },
        inputs=[
            {"path": str(data_cfg.layout.binance_klines_path()), "kind": "binance_klines_1m"},
            {"path": str(data_cfg.layout.oracle_prices_table_path), "kind": "oracle_prices_15m"},
        ]
        + (
            [{"path": str(DataConfig.build(market="btc", cycle=cfg.cycle, surface=cfg.source_surface, root=cfg.layout.storage.rewrite_root).layout.binance_klines_path()), "kind": "btc_klines_1m"}]
            if cfg.asset.slug != "btc"
            else []
        ),
        outputs=[
            {"path": str(data_path), "kind": "feature_frame_parquet"},
            {"path": str(manifest_path), "kind": "manifest"},
        ],
        metadata={
            "row_count": int(len(frame)),
            "column_count": int(len(frame.columns)),
        },
    )
    write_manifest(manifest_path, manifest)
    return {
        "dataset": "feature_frame",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "source_surface": cfg.source_surface,
        "feature_set": cfg.feature_set,
        "rows_written": int(len(frame)),
        "target_path": str(data_path),
        "manifest_path": str(manifest_path),
    }
