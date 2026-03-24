from __future__ import annotations

from pathlib import Path


def cycle_asset_root(base: Path, cycle: str, asset_slug: str, *prefix: str) -> Path:
    return base.joinpath(*prefix, f"cycle={cycle}", f"asset={asset_slug}")


def cycle_asset_file(
    base: Path,
    cycle: str,
    asset_slug: str,
    *prefix: str,
    filename: str = "data.parquet",
) -> Path:
    return cycle_asset_root(base, cycle, asset_slug, *prefix) / filename


def year_month_file(base: Path, year: int, month: int, *, filename: str = "data.parquet") -> Path:
    return base / f"year={int(year):04d}" / f"month={int(month):02d}" / filename


def snapshot_file(base: Path, snapshot_ts: str, *, filename: str) -> Path:
    return base / f"snapshot_ts={snapshot_ts}" / filename


def snapshot_history_file(base: Path, snapshot_ts: str, *, filename: str) -> Path:
    return base / "snapshots" / f"snapshot_ts={snapshot_ts}" / filename
