from __future__ import annotations

from pm15min.research.layout import ResearchLayout, window_label


def test_research_layout_market_paths() -> None:
    layout = ResearchLayout.discover()
    market = layout.for_market("sol", cycle="15m")

    assert str(layout.research_root).endswith("pm15min/v2/research")
    assert str(layout.var_root).endswith("pm15min/v2/var/research")
    assert market.feature_frame_path("deep_otm_v1").name == "data.parquet"
    assert "feature_set=deep_otm_v1" in str(market.feature_frame_path("deep_otm_v1"))
    assert "label_set=truth" in str(market.label_frame_path("truth"))
    assert "target=reversal" in str(
        market.training_set_path(
            feature_set="deep_otm_v1",
            label_set="truth",
            target="reversal",
            window="2025-10-27_2026-03-05",
            offset=7,
        )
    )
    assert "profile=deep_otm" in str(
        market.bundle_manifest_path(profile="deep_otm", target="direction", bundle_label_text="planned")
    )
    assert "active_bundles" in str(
        market.active_bundle_selection_path(profile="deep_otm", target="direction")
    )
    assert str(
        market.active_bundle_selection_path(profile="deep_otm", target="direction")
    ).endswith("selection.json")


def test_window_label_helper() -> None:
    assert window_label("2025-10-27", "2026-03-05") == "2025-10-27_2026-03-05"


def test_window_label_helper_supports_precise_timestamps() -> None:
    assert window_label("2025-10-27T19:30:00Z", "2026-03-05") == "2025-10-27T19-30-00Z_2026-03-05"
