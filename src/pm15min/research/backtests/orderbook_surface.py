from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.queries.loaders import load_market_catalog
from pm15min.live.quotes.orderbook import load_orderbook_index_frame, resolve_orderbook_row


QUOTE_SURFACE_COLUMNS = [
    "token_up",
    "token_down",
    "question",
    "quote_up_ask",
    "quote_down_ask",
    "quote_up_bid",
    "quote_down_bid",
    "quote_up_ask_size_1",
    "quote_down_ask_size_1",
    "quote_up_bid_size_1",
    "quote_down_bid_size_1",
    "quote_captured_ts_ms_up",
    "quote_captured_ts_ms_down",
    "quote_age_ms_up",
    "quote_age_ms_down",
    "quote_source_path",
    "quote_status",
    "quote_reason",
]


@dataclass(frozen=True)
class QuoteSurfaceSummary:
    market_rows_loaded: int
    replay_rows: int
    quote_ready_rows: int
    quote_missing_rows: int

    def to_dict(self) -> dict[str, int]:
        return {
            "market_rows_loaded": self.market_rows_loaded,
            "replay_rows": self.replay_rows,
            "quote_ready_rows": self.quote_ready_rows,
            "quote_missing_rows": self.quote_missing_rows,
        }


def attach_canonical_quote_surface(
    *,
    replay: pd.DataFrame,
    data_cfg: DataConfig,
) -> tuple[pd.DataFrame, QuoteSurfaceSummary]:
    if replay.empty:
        frame = replay.copy()
        for column in QUOTE_SURFACE_COLUMNS:
            if column not in frame.columns:
                frame[column] = pd.Series(dtype="object")
        return frame, QuoteSurfaceSummary(
            market_rows_loaded=0,
            replay_rows=0,
            quote_ready_rows=0,
            quote_missing_rows=0,
        )

    market_table = _prepare_market_table(load_market_catalog(data_cfg))
    frame = _attach_market_metadata(replay.copy(), market_table)
    surface_rows = [_build_quote_surface_row(frame.iloc[idx], data_cfg=data_cfg) for idx in range(len(frame))]
    surface = pd.DataFrame(surface_rows, index=frame.index)
    surface = surface[[column for column in surface.columns if column not in frame.columns]]
    out = pd.concat([frame, surface], axis=1)
    quote_ready = int(out.get("quote_status", pd.Series(index=out.index, dtype="string")).astype("string").eq("ok").sum())
    return out, QuoteSurfaceSummary(
        market_rows_loaded=int(len(market_table)),
        replay_rows=int(len(out)),
        quote_ready_rows=quote_ready,
        quote_missing_rows=int(len(out) - quote_ready),
    )


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
    for column in ("decision_ts", "cycle_start_ts", "cycle_end_ts"):
        if column in replay.columns:
            replay[column] = pd.to_datetime(replay[column], utc=True, errors="coerce")
    if market_table.empty:
        for column in ("token_up", "token_down", "question"):
            if column not in replay.columns:
                replay[column] = pd.Series(index=replay.index, dtype="object")
        return replay

    metadata_columns = [column for column in ("market_id", "condition_id", "token_up", "token_down", "question") if column in market_table.columns]
    if "market_id" in replay.columns and "market_id" in metadata_columns:
        merged = replay.merge(
            market_table[metadata_columns].drop_duplicates(subset=["market_id"], keep="last"),
            on="market_id",
            how="left",
            suffixes=("", "_catalog"),
        )
        return _coalesce_catalog_columns(merged)

    join_columns = [column for column in ("cycle_start_ts", "cycle_end_ts") if column in replay.columns and column in market_table.columns]
    if len(join_columns) == 2:
        merged = replay.merge(
            market_table[metadata_columns].drop_duplicates(subset=join_columns, keep="last"),
            on=join_columns,
            how="left",
            suffixes=("", "_catalog"),
        )
        return _coalesce_catalog_columns(merged)
    return replay


def _coalesce_catalog_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in ("condition_id", "token_up", "token_down", "question"):
        catalog_column = f"{column}_catalog"
        if catalog_column not in out.columns:
            continue
        if column not in out.columns:
            out[column] = out[catalog_column]
        else:
            out[column] = out[column].where(out[column].notna() & out[column].astype("string").ne(""), out[catalog_column])
        out = out.drop(columns=[catalog_column])
    return out


def _build_quote_surface_row(row: pd.Series, *, data_cfg: DataConfig) -> dict[str, object]:
    decision_ts = pd.to_datetime(row.get("decision_ts"), utc=True, errors="coerce")
    market_id = str(row.get("market_id") or "").strip()
    token_up = str(row.get("token_up") or "").strip()
    token_down = str(row.get("token_down") or "").strip()
    if pd.isna(decision_ts) or not market_id:
        return _empty_quote_surface(reason="market_or_decision_missing")

    date_str = decision_ts.strftime("%Y-%m-%d")
    index_path = data_cfg.layout.orderbook_index_path(date_str)
    index_frame = load_orderbook_index_frame(index_path=index_path)
    if index_frame.empty:
        return _empty_quote_surface(
            reason="orderbook_index_missing",
            source_path=str(index_path),
        )

    decision_ts_ms = int(decision_ts.timestamp() * 1000)
    up_row = _resolve_side_row(
        index_frame,
        market_id=market_id,
        token_id=token_up,
        side="up",
        decision_ts_ms=decision_ts_ms,
    )
    down_row = _resolve_side_row(
        index_frame,
        market_id=market_id,
        token_id=token_down,
        side="down",
        decision_ts_ms=decision_ts_ms,
    )
    if up_row is None or down_row is None:
        missing_reasons: list[str] = []
        if up_row is None:
            missing_reasons.append("up_quote_missing")
        if down_row is None:
            missing_reasons.append("down_quote_missing")
        return _empty_quote_surface(
            reason=",".join(missing_reasons),
            source_path=str(index_path),
            token_up=token_up or None,
            token_down=token_down or None,
        )
    return {
        "token_up": token_up or None,
        "token_down": token_down or None,
        "question": str(row.get("question") or "") or None,
        "quote_up_ask": _float_or_none(up_row.get("best_ask")),
        "quote_down_ask": _float_or_none(down_row.get("best_ask")),
        "quote_up_bid": _float_or_none(up_row.get("best_bid")),
        "quote_down_bid": _float_or_none(down_row.get("best_bid")),
        "quote_up_ask_size_1": _float_or_none(up_row.get("ask_size_1")),
        "quote_down_ask_size_1": _float_or_none(down_row.get("ask_size_1")),
        "quote_up_bid_size_1": _float_or_none(up_row.get("bid_size_1")),
        "quote_down_bid_size_1": _float_or_none(down_row.get("bid_size_1")),
        "quote_captured_ts_ms_up": int(up_row["captured_ts_ms"]),
        "quote_captured_ts_ms_down": int(down_row["captured_ts_ms"]),
        "quote_age_ms_up": int(decision_ts_ms - int(up_row["captured_ts_ms"])),
        "quote_age_ms_down": int(decision_ts_ms - int(down_row["captured_ts_ms"])),
        "quote_source_path": str(index_path),
        "quote_status": "ok",
        "quote_reason": "",
    }


def _resolve_side_row(
    frame: pd.DataFrame,
    *,
    market_id: str,
    token_id: str,
    side: str,
    decision_ts_ms: int,
) -> dict[str, object] | None:
    if token_id:
        row = resolve_orderbook_row(
            frame,
            market_id=market_id,
            token_id=token_id,
            side=side,
            decision_ts_ms=decision_ts_ms,
        )
        if row is not None:
            return row

    df = frame.copy()
    df = df[
        (df["market_id"].astype(str) == str(market_id))
        & (df["side"].astype(str).str.lower() == str(side).lower())
    ]
    if df.empty:
        return None
    df["captured_ts_ms"] = pd.to_numeric(df["captured_ts_ms"], errors="coerce")
    df = df.dropna(subset=["captured_ts_ms"]).copy()
    if df.empty:
        return None
    past = df[df["captured_ts_ms"] <= int(decision_ts_ms)]
    if not past.empty:
        return past.sort_values("captured_ts_ms").iloc[-1].to_dict()
    future = df[df["captured_ts_ms"] > int(decision_ts_ms)]
    if not future.empty:
        return future.sort_values("captured_ts_ms").iloc[0].to_dict()
    return None


def _empty_quote_surface(
    *,
    reason: str,
    source_path: str | None = None,
    token_up: str | None = None,
    token_down: str | None = None,
) -> dict[str, object]:
    return {
        "token_up": token_up,
        "token_down": token_down,
        "question": None,
        "quote_up_ask": None,
        "quote_down_ask": None,
        "quote_up_bid": None,
        "quote_down_bid": None,
        "quote_up_ask_size_1": None,
        "quote_down_ask_size_1": None,
        "quote_up_bid_size_1": None,
        "quote_down_bid_size_1": None,
        "quote_captured_ts_ms_up": None,
        "quote_captured_ts_ms_down": None,
        "quote_age_ms_up": None,
        "quote_age_ms_down": None,
        "quote_source_path": source_path,
        "quote_status": "missing_quote_inputs",
        "quote_reason": str(reason or "quote_missing"),
    }


def _float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out
