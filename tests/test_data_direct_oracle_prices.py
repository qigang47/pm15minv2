from __future__ import annotations

from pm15min.data.config import DataConfig
from pm15min.data.pipelines.direct_oracle_prices import _resolve_label_frame_rebuild_summary


def test_resolve_label_frame_rebuild_summary_passes_skip_freshness_when_supported(tmp_path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=tmp_path / "v2")
    captured: dict[str, object] = {}

    def _rebuild(data_cfg, *, skip_freshness=False):
        captured["market"] = data_cfg.asset.slug
        captured["skip_freshness"] = bool(skip_freshness)
        return {"status": "ok"}

    payload = _resolve_label_frame_rebuild_summary(
        cfg,
        rebuild_label_frame_fn=_rebuild,
        skip_freshness=True,
    )

    assert payload == {"status": "ok"}
    assert captured == {
        "market": "sol",
        "skip_freshness": True,
    }
