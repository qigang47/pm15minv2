from __future__ import annotations

from pathlib import Path


def state_scope_with_target(
    *,
    state_root: Path,
    group: str,
    market: str,
    cycle: str,
    profile: str,
    target: str,
) -> Path:
    return (
        state_root
        / group
        / f"cycle={cycle}"
        / f"asset={market}"
        / f"profile={profile}"
        / f"target={target}"
    )


def state_scope_with_profile(
    *,
    state_root: Path,
    group: str,
    market: str,
    cycle: str,
    profile: str,
) -> Path:
    return state_root / group / f"cycle={cycle}" / f"asset={market}" / f"profile={profile}"


def state_scope_asset_only(
    *,
    state_root: Path,
    group: str,
    market: str,
) -> Path:
    return state_root / group / f"asset={market}"


def latest_payload_path(scope_dir: Path, *, filename: str = "latest.json") -> Path:
    return scope_dir / filename


def snapshot_payload_path(scope_dir: Path, *, snapshot_ts: str, filename: str) -> Path:
    return scope_dir / "snapshots" / f"snapshot_ts={snapshot_ts}" / filename


def runner_log_scope(
    *,
    logs_root: Path,
    market: str,
    cycle: str,
    profile: str,
    target: str,
) -> Path:
    return (
        logs_root
        / "runner"
        / f"cycle={cycle}"
        / f"asset={market}"
        / f"profile={profile}"
        / f"target={target}"
    )


def profile_log_scope(
    *,
    logs_root: Path,
    group: str,
    market: str,
    cycle: str,
    profile: str,
) -> Path:
    return (
        logs_root
        / group
        / f"cycle={cycle}"
        / f"asset={market}"
        / f"profile={profile}"
    )
