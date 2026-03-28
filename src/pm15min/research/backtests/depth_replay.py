from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.ndjson_zst import iter_ndjson_zst
from pm15min.data.queries.loaders import load_market_catalog
from pm15min.research.backtests.replay_loader import REPLAY_KEY_COLUMNS
from pm15min.research.backtests.data_surface_fallback import (
    load_market_catalog_with_fallback,
    resolve_orderbook_depth_path,
)


DEFAULT_DEPTH_REPLAY_TOLERANCE_MS = 120_000
DEPTH_REPLAY_HEARTBEAT_INTERVAL_RECORDS = 50_000
LEGACY_RAW_DECISION_TS_BACKSHIFT_MS = 60_000
DEPTH_REPLAY_SURFACE_COLUMNS = [
    "depth_snapshot_rank",
    "depth_candidate_total_count",
    "depth_snapshot_ts",
    "depth_snapshot_ts_ms",
    "depth_snapshot_status",
    "depth_snapshot_reason",
    "depth_match_strategy",
    "depth_source_path",
    "depth_up_record",
    "depth_down_record",
    "depth_up_snapshot_ts_ms",
    "depth_down_snapshot_ts_ms",
]


@dataclass(frozen=True)
class DepthReplaySummary:
    market_rows_loaded: int
    replay_rows: int
    source_files_scanned: int
    raw_records_scanned: int
    raw_record_matches: int
    snapshot_rows: int
    complete_snapshot_rows: int
    partial_snapshot_rows: int
    decision_key_snapshot_rows: int
    token_window_snapshot_rows: int
    mixed_strategy_snapshot_rows: int
    replay_rows_with_snapshots: int
    replay_rows_without_snapshots: int

    def to_dict(self) -> dict[str, int]:
        return {
            "market_rows_loaded": self.market_rows_loaded,
            "replay_rows": self.replay_rows,
            "source_files_scanned": self.source_files_scanned,
            "raw_records_scanned": self.raw_records_scanned,
            "raw_record_matches": self.raw_record_matches,
            "snapshot_rows": self.snapshot_rows,
            "complete_snapshot_rows": self.complete_snapshot_rows,
            "partial_snapshot_rows": self.partial_snapshot_rows,
            "decision_key_snapshot_rows": self.decision_key_snapshot_rows,
            "token_window_snapshot_rows": self.token_window_snapshot_rows,
            "mixed_strategy_snapshot_rows": self.mixed_strategy_snapshot_rows,
            "replay_rows_with_snapshots": self.replay_rows_with_snapshots,
            "replay_rows_without_snapshots": self.replay_rows_without_snapshots,
        }


def build_raw_depth_replay_frame(
    *,
    replay: pd.DataFrame,
    data_cfg: DataConfig,
    snapshot_tolerance_ms: int = DEFAULT_DEPTH_REPLAY_TOLERANCE_MS,
    allow_token_window_fallback: bool = True,
    max_snapshots_per_replay_row: int | None = None,
    heartbeat: Callable[[str], None] | None = None,
) -> tuple[pd.DataFrame, DepthReplaySummary]:
    tolerance_ms = max(0, int(snapshot_tolerance_ms))
    snapshot_cap = None if max_snapshots_per_replay_row is None else max(1, int(max_snapshots_per_replay_row))
    prepared, market_rows_loaded = _prepare_replay_frame(replay=replay, data_cfg=data_cfg)
    if prepared.empty:
        return _empty_depth_replay_frame(prepared), DepthReplaySummary(
            market_rows_loaded=market_rows_loaded,
            replay_rows=0,
            source_files_scanned=0,
            raw_records_scanned=0,
            raw_record_matches=0,
            snapshot_rows=0,
            complete_snapshot_rows=0,
            partial_snapshot_rows=0,
            decision_key_snapshot_rows=0,
            token_window_snapshot_rows=0,
            mixed_strategy_snapshot_rows=0,
            replay_rows_with_snapshots=0,
            replay_rows_without_snapshots=0,
        )

    date_lookups = _build_date_lookups(
        prepared,
        snapshot_tolerance_ms=tolerance_ms,
    )
    buckets: dict[tuple[int, int], dict[str, Any]] = {}
    seen_bucket_keys: set[tuple[int, int]] = set()
    row_candidate_total_counts: dict[int, int] = {}
    row_stored_candidate_counts: dict[int, int] = {}
    raw_records_scanned = 0
    raw_record_matches = 0
    source_files_scanned = 0

    depth_sources = [
        (date_str, resolve_orderbook_depth_path(data_cfg, date_str))
        for date_str in sorted(date_lookups)
    ]
    existing_sources = [(date_str, depth_path) for date_str, depth_path in depth_sources if depth_path.exists()]

    for source_idx, (date_str, depth_path) in enumerate(existing_sources, start=1):
        source_files_scanned += 1
        if heartbeat is not None:
            heartbeat(f"Scanning depth replay file {source_idx}/{len(existing_sources)}: {date_str}")
        date_lookup = date_lookups[date_str]
        for raw in iter_ndjson_zst(depth_path):
            raw_records_scanned += 1
            if heartbeat is not None and raw_records_scanned % DEPTH_REPLAY_HEARTBEAT_INTERVAL_RECORDS == 0:
                heartbeat(
                    f"Scanning depth replay file {source_idx}/{len(existing_sources)}: "
                    f"{raw_records_scanned:,} raw records"
                )
            market_id = str(raw.get("market_id") or "").strip()
            if not market_id or market_id not in date_lookup["market_ids"]:
                continue
            raw_offset = _int_or_none(raw.get("offset"))
            if raw_offset is not None and date_lookup["offsets"] and raw_offset not in date_lookup["offsets"]:
                continue
            matched_rows, strategy = _match_replay_rows(
                raw,
                decision_lookup=date_lookup["decision_lookup"],
                token_lookup=date_lookup["token_lookup"],
                snapshot_tolerance_ms=tolerance_ms,
                allow_token_window_fallback=allow_token_window_fallback,
            )
            if not matched_rows:
                continue
            side = str(raw.get("side") or "").strip().lower()
            if side not in {"up", "down"}:
                continue
            bucket_ts_ms = _replay_snapshot_ts_ms(raw)
            if bucket_ts_ms is None:
                continue
            record_ts_ms = _record_snapshot_ts_ms(raw)
            raw_record_matches += len(matched_rows)
            for row_idx in matched_rows:
                bucket_key = (row_idx, bucket_ts_ms)
                if bucket_key not in seen_bucket_keys:
                    seen_bucket_keys.add(bucket_key)
                    row_candidate_total_counts[int(row_idx)] = row_candidate_total_counts.get(int(row_idx), 0) + 1
                    stored_count = row_stored_candidate_counts.get(int(row_idx), 0)
                    if snapshot_cap is not None and stored_count >= snapshot_cap:
                        continue
                    row_stored_candidate_counts[int(row_idx)] = stored_count + 1
                    buckets[bucket_key] = {
                        "source_path": str(depth_path),
                        "up": None,
                        "down": None,
                        "match_strategies": set(),
                        "up_snapshot_ts_ms": None,
                        "down_snapshot_ts_ms": None,
                    }
                bucket = buckets.get(bucket_key)
                if bucket is None:
                    continue
                bucket[side] = raw
                bucket[f"{side}_snapshot_ts_ms"] = record_ts_ms
                bucket["match_strategies"].add(strategy)

    out = _build_depth_replay_output(
        prepared=prepared,
        buckets=buckets,
        row_candidate_total_counts=row_candidate_total_counts,
    )
    if out.empty:
        strategy_counts: dict[str, int] = {}
        replay_rows_with_snapshots = 0
    else:
        strategy_counts = {
            str(index): int(value)
            for index, value in out["depth_match_strategy"].astype("string").value_counts().items()
        }
        replay_rows_with_snapshots = int(out["_replay_row_idx"].nunique())

    summary = DepthReplaySummary(
        market_rows_loaded=market_rows_loaded,
        replay_rows=int(len(prepared)),
        source_files_scanned=source_files_scanned,
        raw_records_scanned=raw_records_scanned,
        raw_record_matches=raw_record_matches,
        snapshot_rows=int(len(out)),
        complete_snapshot_rows=int(out.get("depth_snapshot_status", pd.Series(dtype="string")).astype("string").eq("ok").sum()),
        partial_snapshot_rows=int(out.get("depth_snapshot_status", pd.Series(dtype="string")).astype("string").eq("partial").sum()),
        decision_key_snapshot_rows=int(strategy_counts.get("decision_key", 0)),
        token_window_snapshot_rows=int(strategy_counts.get("token_window", 0)),
        mixed_strategy_snapshot_rows=int(strategy_counts.get("mixed", 0)),
        replay_rows_with_snapshots=replay_rows_with_snapshots,
        replay_rows_without_snapshots=int(len(prepared) - replay_rows_with_snapshots),
    )
    if "_replay_row_idx" in out.columns:
        out = out.drop(columns=["_replay_row_idx"])
    return out, summary


def _empty_depth_replay_frame(replay: pd.DataFrame) -> pd.DataFrame:
    frame = replay.iloc[0:0].copy()
    for column in DEPTH_REPLAY_SURFACE_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.Series(dtype="object")
    return frame


def _prepare_replay_frame(
    *,
    replay: pd.DataFrame,
    data_cfg: DataConfig,
) -> tuple[pd.DataFrame, int]:
    frame = replay.copy().reset_index(drop=True)
    if "decision_ts" not in frame.columns:
        frame["decision_ts"] = pd.Series(index=frame.index, dtype="datetime64[ns, UTC]")
    else:
        frame["decision_ts"] = pd.to_datetime(frame["decision_ts"], utc=True, errors="coerce")
    for column in ("cycle_start_ts", "cycle_end_ts"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    if "offset" in frame.columns:
        frame["offset"] = pd.to_numeric(frame["offset"], errors="coerce").astype("Int64")
    market_table = _prepare_market_table(load_market_catalog_with_fallback(data_cfg))
    frame = _attach_market_metadata(frame, market_table)
    for column in ("market_id", "condition_id", "token_up", "token_down", "question"):
        if column not in frame.columns:
            frame[column] = pd.Series(index=frame.index, dtype="object")
    return frame, int(len(market_table))


def _build_date_lookups(
    replay: pd.DataFrame,
    *,
    snapshot_tolerance_ms: int,
) -> dict[str, dict[str, Any]]:
    by_date: dict[str, dict[str, Any]] = {}
    for row in replay.itertuples(index=True):
        decision_ts = getattr(row, "decision_ts", None)
        if decision_ts is None or pd.isna(decision_ts):
            continue
        window_start_ts = _coalesce_window_start_ts(
            getattr(row, "window_start_ts", None),
            decision_ts,
        )
        window_end_ts = _coalesce_window_end_ts(
            getattr(row, "window_end_ts", None),
            window_start_ts,
            decision_ts,
            cycle_end_ts=getattr(row, "cycle_end_ts", None),
            snapshot_tolerance_ms=snapshot_tolerance_ms,
        )
        market_id = str(getattr(row, "market_id", "") or "").strip()
        if not market_id:
            continue
        decision_ts_ms = int(pd.Timestamp(decision_ts).timestamp() * 1000)
        scan_dates = _scan_dates_for_decision_ts(
            pd.Timestamp(window_start_ts),
            window_end_ts=pd.Timestamp(window_end_ts),
        )
        offset = _int_or_none(getattr(row, "offset", None))
        token_pairs = [
            ("up", str(getattr(row, "token_up", "") or "").strip()),
            ("down", str(getattr(row, "token_down", "") or "").strip()),
        ]
        for date_str in scan_dates:
            entry = by_date.setdefault(
                date_str,
                {
                    "decision_lookup": {},
                    "token_lookup": {},
                    "market_ids": set(),
                    "offsets": set(),
                },
            )
            entry["market_ids"].add(market_id)
            if offset is not None:
                entry["offsets"].add(int(offset))
                for key in _decision_lookup_keys(market_id=market_id, decision_ts_ms=decision_ts_ms, offset=offset):
                    entry["decision_lookup"].setdefault(key, []).append(int(row.Index))
            for side, token_id in token_pairs:
                if not token_id:
                    continue
                token_key = (market_id, token_id, side)
                entry["token_lookup"].setdefault(token_key, []).append(
                    (
                        int(row.Index),
                        int(pd.Timestamp(window_start_ts).timestamp() * 1000),
                        int(pd.Timestamp(window_end_ts).timestamp() * 1000),
                    )
                )
    return by_date


def _scan_dates_for_decision_ts(
    decision_ts: pd.Timestamp,
    *,
    window_end_ts: pd.Timestamp,
) -> set[str]:
    dates = {decision_ts.strftime("%Y-%m-%d")}
    backshift_start_ts = decision_ts - pd.Timedelta(milliseconds=LEGACY_RAW_DECISION_TS_BACKSHIFT_MS)
    if backshift_start_ts.strftime("%Y-%m-%d") != decision_ts.strftime("%Y-%m-%d"):
        dates.add(backshift_start_ts.strftime("%Y-%m-%d"))
    if window_end_ts.strftime("%Y-%m-%d") != decision_ts.strftime("%Y-%m-%d"):
        dates.add(window_end_ts.strftime("%Y-%m-%d"))
    return dates


def _decision_lookup_keys(*, market_id: str, decision_ts_ms: int, offset: int) -> tuple[tuple[str, int, int], ...]:
    keys = [(market_id, int(decision_ts_ms), int(offset))]
    shifted_decision_ts_ms = int(decision_ts_ms) - LEGACY_RAW_DECISION_TS_BACKSHIFT_MS
    if shifted_decision_ts_ms >= 0:
        keys.append((market_id, shifted_decision_ts_ms, int(offset)))
    return tuple(dict.fromkeys(keys))


def _match_replay_rows(
    raw: dict[str, Any],
    *,
    decision_lookup: dict[tuple[str, int, int], list[int]],
    token_lookup: dict[tuple[str, str, str], list[tuple[int, int, int]]],
    snapshot_tolerance_ms: int,
    allow_token_window_fallback: bool,
) -> tuple[list[int], str | None]:
    market_id = str(raw.get("market_id") or "").strip()
    if not market_id:
        return [], None
    side = str(raw.get("side") or "").strip().lower()
    if side not in {"up", "down"}:
        return [], None

    decision_ts_ms = _parse_ts_ms(raw.get("decision_ts"))
    offset = _int_or_none(raw.get("offset"))
    if decision_ts_ms is not None and offset is not None:
        matches = decision_lookup.get((market_id, decision_ts_ms, offset), [])
        if matches:
            return matches, "decision_key"

    if not allow_token_window_fallback:
        return [], None
    token_id = str(raw.get("token_id") or "").strip()
    snapshot_ts_ms = _replay_snapshot_ts_ms(raw)
    if not token_id or snapshot_ts_ms is None:
        return [], None
    matches = [
        row_idx
        for row_idx, row_window_start_ts_ms, row_window_end_ts_ms in token_lookup.get((market_id, token_id, side), [])
        if int(row_window_start_ts_ms) <= int(snapshot_ts_ms) < int(row_window_end_ts_ms)
    ]
    if matches:
        return matches, "token_window"
    return [], None


def _coalesce_window_start_ts(value: object, decision_ts: object) -> pd.Timestamp:
    window_start_ts = pd.to_datetime(value, utc=True, errors="coerce")
    if window_start_ts is None or pd.isna(window_start_ts):
        window_start_ts = pd.to_datetime(decision_ts, utc=True, errors="coerce")
    if window_start_ts is None or pd.isna(window_start_ts):
        raise ValueError("window_start_ts/decision_ts missing for depth replay row")
    return pd.Timestamp(window_start_ts)


def _coalesce_window_end_ts(
    value: object,
    window_start_ts: pd.Timestamp,
    decision_ts: object,
    *,
    cycle_end_ts: object,
    snapshot_tolerance_ms: int,
) -> pd.Timestamp:
    window_end_ts = pd.to_datetime(value, utc=True, errors="coerce")
    if window_end_ts is None or pd.isna(window_end_ts):
        duration_seconds = 60.0
        decision_dt = pd.to_datetime(decision_ts, utc=True, errors="coerce")
        if decision_dt is not None and not pd.isna(decision_dt):
            window_end_ts = pd.Timestamp(decision_dt) + pd.to_timedelta(duration_seconds, unit="s")
        else:
            window_end_ts = window_start_ts + pd.to_timedelta(duration_seconds, unit="s")
    cycle_end_dt = pd.to_datetime(cycle_end_ts, utc=True, errors="coerce")
    if cycle_end_dt is not None and not pd.isna(cycle_end_dt):
        window_end_ts = min(pd.Timestamp(window_end_ts), pd.Timestamp(cycle_end_dt))
    if window_end_ts <= window_start_ts:
        fallback_end = window_start_ts + pd.to_timedelta(max(1, int(snapshot_tolerance_ms)), unit="ms")
        window_end_ts = max(pd.Timestamp(window_end_ts), fallback_end)
    return pd.Timestamp(window_end_ts)


def _build_depth_replay_output(
    *,
    prepared: pd.DataFrame,
    buckets: dict[tuple[int, int], dict[str, Any]],
    row_candidate_total_counts: dict[int, int],
) -> pd.DataFrame:
    if not buckets:
        return _empty_depth_replay_frame(prepared)

    rows: list[dict[str, Any]] = []
    for (row_idx, snapshot_ts_ms), bucket in buckets.items():
        replay_row = prepared.iloc[int(row_idx)].to_dict()
        up_record = bucket.get("up")
        down_record = bucket.get("down")
        missing: list[str] = []
        if up_record is None:
            missing.append("up_snapshot_missing")
        if down_record is None:
            missing.append("down_snapshot_missing")
        rows.append(
            {
                **replay_row,
                "_replay_row_idx": int(row_idx),
                "depth_snapshot_rank": 0,
                "depth_candidate_total_count": int(row_candidate_total_counts.get(int(row_idx), 0)),
                "depth_snapshot_ts": _iso_from_ts_ms(snapshot_ts_ms),
                "depth_snapshot_ts_ms": int(snapshot_ts_ms),
                "depth_snapshot_status": "ok" if not missing else "partial",
                "depth_snapshot_reason": ",".join(missing),
                "depth_match_strategy": _collapse_match_strategies(bucket.get("match_strategies") or set()),
                "depth_source_path": str(bucket.get("source_path") or ""),
                "depth_up_record": up_record,
                "depth_down_record": down_record,
                "depth_up_snapshot_ts_ms": _int_or_none(bucket.get("up_snapshot_ts_ms")),
                "depth_down_snapshot_ts_ms": _int_or_none(bucket.get("down_snapshot_ts_ms")),
            }
        )
    out = pd.DataFrame(rows)
    sort_columns = [column for column in [*REPLAY_KEY_COLUMNS, "market_id", "depth_snapshot_ts_ms"] if column in out.columns]
    if sort_columns:
        out = out.sort_values(sort_columns).reset_index(drop=True)
    rank_group_columns = [column for column in [*REPLAY_KEY_COLUMNS, "market_id", "_replay_row_idx"] if column in out.columns]
    if rank_group_columns:
        out["depth_snapshot_rank"] = out.groupby(rank_group_columns, dropna=False).cumcount() + 1
    return out


def _collapse_match_strategies(strategies: set[str]) -> str:
    cleaned = {str(strategy) for strategy in strategies if str(strategy)}
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return next(iter(cleaned))
    return "mixed"


def _prepare_market_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for column in ("cycle_start_ts", "cycle_end_ts"):
        if column in out.columns:
            numeric = pd.to_numeric(out[column], errors="coerce")
            out[column] = pd.to_datetime(numeric, unit="s", utc=True, errors="coerce")
    return out


def _attach_market_metadata(replay: pd.DataFrame, market_table: pd.DataFrame) -> pd.DataFrame:
    if market_table.empty:
        return replay
    out = replay.copy()
    metadata_columns = [
        column
        for column in ("market_id", "condition_id", "token_up", "token_down", "question")
        if column in market_table.columns
    ]
    if "market_id" in replay.columns and "market_id" in metadata_columns:
        merged = out.merge(
            market_table[metadata_columns].drop_duplicates(subset=["market_id"], keep="last"),
            on="market_id",
            how="left",
            suffixes=("", "_catalog"),
        )
        out = _coalesce_catalog_columns(merged)

    join_columns = [column for column in ("cycle_start_ts", "cycle_end_ts") if column in out.columns and column in market_table.columns]
    if len(join_columns) != 2:
        return out
    fallback_mask = pd.Series(True, index=out.index)
    if "market_id" in out.columns:
        fallback_mask = out["market_id"].astype("string").fillna("").eq("")
    if not bool(fallback_mask.any()):
        return out
    selected_columns = [*join_columns, *[column for column in metadata_columns if column not in join_columns]]
    fallback_rows = out.loc[fallback_mask].copy()
    fallback_rows["_row_idx"] = fallback_rows.index
    merged = fallback_rows.merge(
        market_table[selected_columns].drop_duplicates(subset=join_columns, keep="last"),
        on=join_columns,
        how="left",
        suffixes=("", "_catalog"),
    )
    merged = _coalesce_catalog_columns(merged).set_index("_row_idx")
    for column in merged.columns:
        if column not in out.columns:
            out[column] = pd.Series(index=out.index, dtype="object")
        out.loc[merged.index, column] = merged[column]
    return out


def _coalesce_catalog_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in ("market_id", "condition_id", "token_up", "token_down", "question"):
        catalog_column = f"{column}_catalog"
        if catalog_column not in out.columns:
            continue
        if column not in out.columns:
            out[column] = out[catalog_column]
        else:
            out[column] = out[column].where(out[column].notna() & out[column].astype("string").ne(""), out[catalog_column])
        out = out.drop(columns=[catalog_column])
    return out


def _parse_ts_ms(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        out = int(value)
        return out if out > 0 else None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.isdigit():
            out = int(text)
            return out if out > 0 else None
        return _parse_ts_ms_text(text)
    except Exception:
        return None


@lru_cache(maxsize=262_144)
def _parse_ts_ms_text(text: str) -> int | None:
    dt = pd.to_datetime(text, utc=True, errors="coerce")
    if dt is None or pd.isna(dt):
        return None
    return int(dt.timestamp() * 1000)


def _int_or_none(value: object) -> int | None:
    try:
        if value is None or pd.isna(value):
            return None
        return int(value)
    except Exception:
        return None


def _iso_from_ts_ms(value: int | None) -> str | None:
    if value is None:
        return None
    return pd.Timestamp(int(value), unit="ms", tz="UTC").isoformat()


def _record_snapshot_ts_ms(raw: dict[str, Any] | None) -> int | None:
    if not isinstance(raw, dict):
        return None
    for key in ("captured_ts_ms", "orderbook_ts", "logged_at", "source_ts_ms"):
        ts_ms = _parse_ts_ms(raw.get(key))
        if ts_ms is not None:
            return ts_ms
    return None


def _replay_snapshot_ts_ms(raw: dict[str, Any]) -> int | None:
    # Match legacy replay semantics first so same-bucket up/down records pair on logged_at.
    for key in ("logged_at", "orderbook_ts", "captured_ts_ms", "source_ts_ms"):
        ts_ms = _parse_ts_ms(raw.get(key))
        if ts_ms is not None:
            return ts_ms
    return None
