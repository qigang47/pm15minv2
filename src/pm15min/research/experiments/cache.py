from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pm15min.data.io.json_files import write_json_atomic
from pm15min.data.io.parquet import read_parquet_if_exists, write_parquet_atomic
from pm15min.research.layout import ResearchLayout


_CACHE_SCHEMA_VERSION = "pm15min.research.experiments.cache.v1"
_PREPARED_DATASET_COLUMNS = [
    "market",
    "cycle",
    "profile",
    "feature_set",
    "label_set",
    "rewrite_root",
    "source_suite_name",
    "source_run_label",
    "source_run_dir",
    "prepared_at",
]
_TRAINING_REUSE_COLUMNS = [
    "cache_key",
    "market",
    "profile",
    "model_family",
    "feature_set",
    "label_set",
    "target",
    "window",
    "offsets_json",
    "run_label",
    "run_dir",
    "source_suite_name",
    "source_run_label",
    "updated_at",
]
_BUNDLE_REUSE_COLUMNS = [
    "cache_key",
    "market",
    "profile",
    "target",
    "offsets_json",
    "training_run_label",
    "bundle_label",
    "bundle_dir",
    "source_suite_name",
    "source_run_label",
    "updated_at",
]


def utc_cache_timestamp(now: datetime | None = None) -> str:
    ts = datetime.now(timezone.utc) if now is None else now.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def prepared_dataset_cache_key(
    *,
    market: str,
    cycle: str,
    profile: str,
    feature_set: str,
    label_set: str,
    rewrite_root: str,
) -> tuple[str, ...]:
    return (
        str(market).strip(),
        str(cycle).strip(),
        str(profile).strip(),
        str(feature_set).strip(),
        str(label_set).strip(),
        str(rewrite_root).strip(),
    )


def training_cache_key(
    *,
    market: str,
    profile: str,
    model_family: str,
    feature_set: str,
    label_set: str,
    target: str,
    window_label: str,
    offsets: tuple[int, ...],
) -> str:
    return stable_key(
        {
            "market": str(market).strip(),
            "profile": str(profile).strip(),
            "model_family": str(model_family).strip(),
            "feature_set": str(feature_set).strip(),
            "label_set": str(label_set).strip(),
            "target": str(target).strip(),
            "window": str(window_label).strip(),
            "offsets": [int(value) for value in offsets],
        }
    )


def bundle_cache_key(
    *,
    market: str,
    profile: str,
    target: str,
    offsets: tuple[int, ...],
    training_run_label: str,
) -> str:
    return stable_key(
        {
            "market": str(market).strip(),
            "profile": str(profile).strip(),
            "target": str(target).strip(),
            "offsets": [int(value) for value in offsets],
            "training_run_label": str(training_run_label).strip(),
        }
    )


def stable_key(payload: dict[str, object]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def normalize_offsets(raw: object) -> tuple[int, ...]:
    if raw is None:
        return ()
    values = raw.tolist() if hasattr(raw, "tolist") else raw
    if isinstance(values, (str, bytes)):
        return ()
    try:
        return tuple(int(value) for value in values)
    except TypeError:
        try:
            return (int(values),)
        except Exception:
            return ()
    except Exception:
        return ()


@dataclass(frozen=True)
class ExperimentSharedCachePaths:
    root: Path
    prepared_datasets_path: Path
    training_reuse_path: Path
    bundle_reuse_path: Path
    manifest_path: Path

    @classmethod
    def for_storage(cls, storage: ResearchLayout) -> "ExperimentSharedCachePaths":
        root = storage.experiments_root / "cache"
        return cls(
            root=root,
            prepared_datasets_path=root / "prepared_datasets.parquet",
            training_reuse_path=root / "training_reuse.parquet",
            bundle_reuse_path=root / "bundle_reuse.parquet",
            manifest_path=root / "manifest.json",
        )


@dataclass
class ExperimentSharedCache:
    paths: ExperimentSharedCachePaths
    prepared_datasets: dict[tuple[str, ...], dict[str, object]] = field(default_factory=dict)
    training_reuse: dict[str, dict[str, object]] = field(default_factory=dict)
    bundle_reuse: dict[str, dict[str, object]] = field(default_factory=dict)

    @classmethod
    def load_for_storage(cls, storage: ResearchLayout) -> "ExperimentSharedCache":
        return cls.load(ExperimentSharedCachePaths.for_storage(storage))

    @classmethod
    def load(cls, paths: ExperimentSharedCachePaths) -> "ExperimentSharedCache":
        prepared_frame = _read_table(paths.prepared_datasets_path, columns=_PREPARED_DATASET_COLUMNS)
        training_frame = _read_table(paths.training_reuse_path, columns=_TRAINING_REUSE_COLUMNS)
        bundle_frame = _read_table(paths.bundle_reuse_path, columns=_BUNDLE_REUSE_COLUMNS)
        return cls(
            paths=paths,
            prepared_datasets=_prepared_datasets_from_frame(prepared_frame),
            training_reuse=_training_reuse_from_frame(training_frame),
            bundle_reuse=_bundle_reuse_from_frame(bundle_frame),
        )

    def counts(self) -> dict[str, int]:
        return {
            "prepared_datasets": len(self.prepared_datasets),
            "training_reuse": len(self.training_reuse),
            "bundle_reuse": len(self.bundle_reuse),
        }

    def get_prepared_dataset(
        self,
        *,
        market: str,
        cycle: str,
        profile: str,
        feature_set: str,
        label_set: str,
        rewrite_root: str,
    ) -> dict[str, object] | None:
        key = prepared_dataset_cache_key(
            market=market,
            cycle=cycle,
            profile=profile,
            feature_set=feature_set,
            label_set=label_set,
            rewrite_root=rewrite_root,
        )
        cached = self.prepared_datasets.get(key)
        return None if cached is None else dict(cached)

    def has_prepared_dataset(
        self,
        *,
        market: str,
        cycle: str,
        profile: str,
        feature_set: str,
        label_set: str,
        rewrite_root: str,
    ) -> bool:
        return (
            self.get_prepared_dataset(
                market=market,
                cycle=cycle,
                profile=profile,
                feature_set=feature_set,
                label_set=label_set,
                rewrite_root=rewrite_root,
            )
            is not None
        )

    def remember_prepared_dataset(
        self,
        *,
        market: str,
        cycle: str,
        profile: str,
        feature_set: str,
        label_set: str,
        rewrite_root: str,
        source_suite_name: str | None = None,
        source_run_label: str | None = None,
        source_run_dir: str | None = None,
        prepared_at: str | None = None,
    ) -> dict[str, object]:
        record = {
            "market": str(market).strip(),
            "cycle": str(cycle).strip(),
            "profile": str(profile).strip(),
            "feature_set": str(feature_set).strip(),
            "label_set": str(label_set).strip(),
            "rewrite_root": str(rewrite_root).strip(),
            "source_suite_name": _clean_optional_text(source_suite_name),
            "source_run_label": _clean_optional_text(source_run_label),
            "source_run_dir": _clean_optional_text(source_run_dir),
            "prepared_at": str(prepared_at or utc_cache_timestamp()),
        }
        key = prepared_dataset_cache_key(
            market=record["market"],
            cycle=record["cycle"],
            profile=record["profile"],
            feature_set=record["feature_set"],
            label_set=record["label_set"],
            rewrite_root=record["rewrite_root"],
        )
        self.prepared_datasets[key] = record
        return dict(record)

    def get_training(
        self,
        *,
        market: str,
        profile: str,
        model_family: str,
        feature_set: str,
        label_set: str,
        target: str,
        window_label: str,
        offsets: tuple[int, ...],
    ) -> dict[str, object] | None:
        key = training_cache_key(
            market=market,
            profile=profile,
            model_family=model_family,
            feature_set=feature_set,
            label_set=label_set,
            target=target,
            window_label=window_label,
            offsets=offsets,
        )
        cached = self.training_reuse.get(key)
        return None if cached is None else dict(cached)

    def remember_training(
        self,
        *,
        market: str,
        profile: str,
        model_family: str,
        feature_set: str,
        label_set: str,
        target: str,
        window_label: str,
        offsets: tuple[int, ...],
        run_dir: str,
        run_label: str | None = None,
        source_suite_name: str | None = None,
        source_run_label: str | None = None,
        updated_at: str | None = None,
    ) -> dict[str, object]:
        normalized_offsets = tuple(int(value) for value in offsets)
        normalized_run_dir = str(run_dir).strip()
        cache_key = training_cache_key(
            market=market,
            profile=profile,
            model_family=model_family,
            feature_set=feature_set,
            label_set=label_set,
            target=target,
            window_label=window_label,
            offsets=normalized_offsets,
        )
        normalized_run_label = str(run_label or Path(normalized_run_dir).name).strip()
        previous = self.training_reuse.get(cache_key)
        if previous is not None:
            previous_run_label = str(previous.get("run_label") or "").strip()
            if previous_run_label and previous_run_label != normalized_run_label:
                stale_bundle_key = bundle_cache_key(
                    market=market,
                    profile=profile,
                    target=target,
                    offsets=normalized_offsets,
                    training_run_label=previous_run_label,
                )
                self.bundle_reuse.pop(stale_bundle_key, None)
        record = {
            "cache_key": cache_key,
            "market": str(market).strip(),
            "profile": str(profile).strip(),
            "model_family": str(model_family).strip(),
            "feature_set": str(feature_set).strip(),
            "label_set": str(label_set).strip(),
            "target": str(target).strip(),
            "window": str(window_label).strip(),
            "offsets": normalized_offsets,
            "run_label": normalized_run_label,
            "run_dir": normalized_run_dir,
            "source_suite_name": _clean_optional_text(source_suite_name),
            "source_run_label": _clean_optional_text(source_run_label),
            "updated_at": str(updated_at or utc_cache_timestamp()),
        }
        self.training_reuse[str(record["cache_key"])] = record
        return dict(record)

    def get_bundle(
        self,
        *,
        market: str,
        profile: str,
        target: str,
        offsets: tuple[int, ...],
        training_run_label: str,
    ) -> dict[str, object] | None:
        key = bundle_cache_key(
            market=market,
            profile=profile,
            target=target,
            offsets=offsets,
            training_run_label=training_run_label,
        )
        cached = self.bundle_reuse.get(key)
        return None if cached is None else dict(cached)

    def remember_bundle(
        self,
        *,
        market: str,
        profile: str,
        target: str,
        offsets: tuple[int, ...],
        training_run_label: str,
        bundle_dir: str,
        bundle_label: str | None = None,
        source_suite_name: str | None = None,
        source_run_label: str | None = None,
        updated_at: str | None = None,
    ) -> dict[str, object]:
        normalized_offsets = tuple(int(value) for value in offsets)
        normalized_bundle_dir = str(bundle_dir).strip()
        normalized_training_run_label = str(training_run_label).strip()
        record = {
            "cache_key": bundle_cache_key(
                market=market,
                profile=profile,
                target=target,
                offsets=normalized_offsets,
                training_run_label=normalized_training_run_label,
            ),
            "market": str(market).strip(),
            "profile": str(profile).strip(),
            "target": str(target).strip(),
            "offsets": normalized_offsets,
            "training_run_label": normalized_training_run_label,
            "bundle_label": str(bundle_label or Path(normalized_bundle_dir).name).strip(),
            "bundle_dir": normalized_bundle_dir,
            "source_suite_name": _clean_optional_text(source_suite_name),
            "source_run_label": _clean_optional_text(source_run_label),
            "updated_at": str(updated_at or utc_cache_timestamp()),
        }
        self.bundle_reuse[str(record["cache_key"])] = record
        return dict(record)

    def ingest_training_runs(
        self,
        training_runs: pd.DataFrame,
        *,
        cycle: str,
        rewrite_root: str,
        source_suite_name: str | None = None,
        source_run_label: str | None = None,
    ) -> None:
        if training_runs.empty:
            return
        for row in training_runs.to_dict(orient="records"):
            market = _clean_optional_text(row.get("market"))
            profile = _clean_optional_text(row.get("profile"))
            feature_set = _clean_optional_text(row.get("feature_set"))
            label_set = _clean_optional_text(row.get("label_set"))
            if market and profile and feature_set and label_set:
                self.remember_prepared_dataset(
                    market=market,
                    cycle=cycle,
                    profile=profile,
                    feature_set=feature_set,
                    label_set=label_set,
                    rewrite_root=rewrite_root,
                    source_suite_name=source_suite_name,
                    source_run_label=source_run_label,
                    source_run_dir=_clean_optional_text(row.get("training_run_dir")),
                )

            offsets = normalize_offsets(row.get("offsets"))
            training_run_dir = _clean_optional_text(row.get("training_run_dir"))
            model_family = _clean_optional_text(row.get("model_family"))
            target = _clean_optional_text(row.get("target"))
            window_label = _clean_optional_text(row.get("window"))
            if market and profile and feature_set and label_set and model_family and target and window_label and offsets and training_run_dir:
                training_record = self.remember_training(
                    market=market,
                    profile=profile,
                    model_family=model_family,
                    feature_set=feature_set,
                    label_set=label_set,
                    target=target,
                    window_label=window_label,
                    offsets=offsets,
                    run_dir=training_run_dir,
                    source_suite_name=source_suite_name,
                    source_run_label=source_run_label,
                )
                bundle_dir = _clean_optional_text(row.get("bundle_dir"))
                if bundle_dir:
                    self.remember_bundle(
                        market=market,
                        profile=profile,
                        target=target,
                        offsets=offsets,
                        training_run_label=str(training_record["run_label"]),
                        bundle_dir=bundle_dir,
                        source_suite_name=source_suite_name,
                        source_run_label=source_run_label,
                    )

    def save(self) -> dict[str, str]:
        self.paths.root.mkdir(parents=True, exist_ok=True)
        prepared_frame = _merge_and_write_table(
            path=self.paths.prepared_datasets_path,
            incoming=self._prepared_datasets_frame(),
            columns=_PREPARED_DATASET_COLUMNS,
            key_columns=["market", "cycle", "profile", "feature_set", "label_set", "rewrite_root"],
            order_columns=["market", "cycle", "profile", "feature_set", "label_set", "rewrite_root"],
        )
        training_frame = _merge_and_write_table(
            path=self.paths.training_reuse_path,
            incoming=self._training_reuse_frame(),
            columns=_TRAINING_REUSE_COLUMNS,
            key_columns=["cache_key"],
            order_columns=["market", "profile", "target", "window", "run_label"],
        )
        bundle_frame = _merge_and_write_table(
            path=self.paths.bundle_reuse_path,
            incoming=self._bundle_reuse_frame(),
            columns=_BUNDLE_REUSE_COLUMNS,
            key_columns=["cache_key"],
            order_columns=["market", "profile", "target", "training_run_label", "bundle_label"],
        )
        self.prepared_datasets = _prepared_datasets_from_frame(prepared_frame)
        self.training_reuse = _training_reuse_from_frame(training_frame)
        self.bundle_reuse = _bundle_reuse_from_frame(bundle_frame)
        write_json_atomic(
            {
                "schema_version": _CACHE_SCHEMA_VERSION,
                "root": str(self.paths.root),
                "counts": self.counts(),
                "tables": {
                    "prepared_datasets": str(self.paths.prepared_datasets_path),
                    "training_reuse": str(self.paths.training_reuse_path),
                    "bundle_reuse": str(self.paths.bundle_reuse_path),
                },
                "updated_at": utc_cache_timestamp(),
            },
            self.paths.manifest_path,
        )
        return {
            "root": str(self.paths.root),
            "prepared_datasets_path": str(self.paths.prepared_datasets_path),
            "training_reuse_path": str(self.paths.training_reuse_path),
            "bundle_reuse_path": str(self.paths.bundle_reuse_path),
            "manifest_path": str(self.paths.manifest_path),
        }

    def _prepared_datasets_frame(self) -> pd.DataFrame:
        rows = list(self.prepared_datasets.values())
        return _frame_from_records(rows, columns=_PREPARED_DATASET_COLUMNS)

    def _training_reuse_frame(self) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for record in self.training_reuse.values():
            row = {column: record.get(column) for column in _TRAINING_REUSE_COLUMNS if column != "offsets_json"}
            row["offsets_json"] = json.dumps([int(value) for value in record.get("offsets", ())], ensure_ascii=False)
            rows.append(row)
        return _frame_from_records(rows, columns=_TRAINING_REUSE_COLUMNS)

    def _bundle_reuse_frame(self) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for record in self.bundle_reuse.values():
            row = {column: record.get(column) for column in _BUNDLE_REUSE_COLUMNS if column != "offsets_json"}
            row["offsets_json"] = json.dumps([int(value) for value in record.get("offsets", ())], ensure_ascii=False)
            rows.append(row)
        return _frame_from_records(rows, columns=_BUNDLE_REUSE_COLUMNS)


def _clean_optional_text(value: object) -> str | None:
    if _is_missing(value):
        return None
    token = str(value).strip()
    return token or None


def _read_table(path: Path, *, columns: list[str]) -> pd.DataFrame:
    frame = read_parquet_if_exists(path)
    if frame is None:
        return pd.DataFrame(columns=columns)
    return _ensure_columns(frame, columns=columns)


def _frame_from_records(rows: list[dict[str, object]], *, columns: list[str]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=columns)
    return _ensure_columns(pd.DataFrame(rows), columns=columns)


def _ensure_columns(frame: pd.DataFrame, *, columns: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = None
    return out[columns]


def _merge_and_write_table(
    *,
    path: Path,
    incoming: pd.DataFrame,
    columns: list[str],
    key_columns: list[str],
    order_columns: list[str],
) -> pd.DataFrame:
    existing = _read_table(path, columns=columns)
    combined = pd.concat([existing, incoming], ignore_index=True, sort=False)
    combined = _ensure_columns(combined, columns=columns)
    if key_columns:
        combined = combined.drop_duplicates(subset=key_columns, keep="last")
    if order_columns:
        combined = combined.sort_values(order_columns, kind="stable", na_position="last")
    combined = combined.reset_index(drop=True)
    write_parquet_atomic(combined, path)
    return combined


def _prepared_datasets_from_frame(frame: pd.DataFrame) -> dict[tuple[str, ...], dict[str, object]]:
    records: dict[tuple[str, ...], dict[str, object]] = {}
    for row in frame.to_dict(orient="records"):
        key = prepared_dataset_cache_key(
            market=_clean_optional_text(row.get("market")) or "",
            cycle=_clean_optional_text(row.get("cycle")) or "",
            profile=_clean_optional_text(row.get("profile")) or "",
            feature_set=_clean_optional_text(row.get("feature_set")) or "",
            label_set=_clean_optional_text(row.get("label_set")) or "",
            rewrite_root=_clean_optional_text(row.get("rewrite_root")) or "",
        )
        if all(key):
            records[key] = {
                "market": key[0],
                "cycle": key[1],
                "profile": key[2],
                "feature_set": key[3],
                "label_set": key[4],
                "rewrite_root": key[5],
                "source_suite_name": _clean_optional_text(row.get("source_suite_name")),
                "source_run_label": _clean_optional_text(row.get("source_run_label")),
                "source_run_dir": _clean_optional_text(row.get("source_run_dir")),
                "prepared_at": _clean_optional_text(row.get("prepared_at")) or "",
            }
    return records


def _training_reuse_from_frame(frame: pd.DataFrame) -> dict[str, dict[str, object]]:
    records: dict[str, dict[str, object]] = {}
    for row in frame.to_dict(orient="records"):
        cache_key = _clean_optional_text(row.get("cache_key")) or ""
        run_dir = _clean_optional_text(row.get("run_dir")) or ""
        if not cache_key or not run_dir:
            continue
        records[cache_key] = {
            "cache_key": cache_key,
            "market": _clean_optional_text(row.get("market")) or "",
            "profile": _clean_optional_text(row.get("profile")) or "",
            "model_family": _clean_optional_text(row.get("model_family")) or "",
            "feature_set": _clean_optional_text(row.get("feature_set")) or "",
            "label_set": _clean_optional_text(row.get("label_set")) or "",
            "target": _clean_optional_text(row.get("target")) or "",
            "window": _clean_optional_text(row.get("window")) or "",
            "offsets": _parse_offsets_json(row.get("offsets_json")),
            "run_label": _clean_optional_text(row.get("run_label")) or Path(run_dir).name,
            "run_dir": run_dir,
            "source_suite_name": _clean_optional_text(row.get("source_suite_name")),
            "source_run_label": _clean_optional_text(row.get("source_run_label")),
            "updated_at": _clean_optional_text(row.get("updated_at")) or "",
        }
    return records


def _bundle_reuse_from_frame(frame: pd.DataFrame) -> dict[str, dict[str, object]]:
    records: dict[str, dict[str, object]] = {}
    for row in frame.to_dict(orient="records"):
        cache_key = _clean_optional_text(row.get("cache_key")) or ""
        bundle_dir = _clean_optional_text(row.get("bundle_dir")) or ""
        if not cache_key or not bundle_dir:
            continue
        records[cache_key] = {
            "cache_key": cache_key,
            "market": _clean_optional_text(row.get("market")) or "",
            "profile": _clean_optional_text(row.get("profile")) or "",
            "target": _clean_optional_text(row.get("target")) or "",
            "offsets": _parse_offsets_json(row.get("offsets_json")),
            "training_run_label": _clean_optional_text(row.get("training_run_label")) or "",
            "bundle_label": _clean_optional_text(row.get("bundle_label")) or Path(bundle_dir).name,
            "bundle_dir": bundle_dir,
            "source_suite_name": _clean_optional_text(row.get("source_suite_name")),
            "source_run_label": _clean_optional_text(row.get("source_run_label")),
            "updated_at": _clean_optional_text(row.get("updated_at")) or "",
        }
    return records


def _parse_offsets_json(raw: object) -> tuple[int, ...]:
    token = _clean_optional_text(raw) or ""
    if not token:
        return ()
    try:
        payload = json.loads(token)
    except Exception:
        return ()
    return normalize_offsets(payload)


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False
    except ValueError:
        return False
