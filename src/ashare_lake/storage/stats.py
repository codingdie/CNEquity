"""Lake self-measurement — what each partition actually holds.

``list_datasets()`` answers coverage from directory names alone, which is what
makes it cheap. It cannot answer how many rows a partition holds, how big it
is, or which source wrote it, because none of that is in a directory name. Those
are the questions a lake dashboard asks on every page, and answering them by
scanning curated on each request does not survive contact with a ten-year lake.

So they are answered once, into two small parquet tables under ``meta/stats``:

``partition_stats``
    One row per (dataset, partition): rows, files, bytes, and the period the
    directory covers. The coverage-and-size view.

``provenance_stats``
    One row per (dataset, partition, source, data_version): rows and the
    ``fetched_at`` span. The "which source wrote this, and when" view — a
    ``source`` mix that shifts on a date is visible here and nowhere else.

Two tables rather than one because ``bytes`` and ``file_count`` are properties
of a directory while ``row_count`` splits by source: carrying the file-level
numbers on the finer grain would make them look summable when summing them
double-counts.

**Registry fields are deliberately absent.** No ``tier``, ``layer`` or
``history_mode`` columns — those live in ``domain.datasets`` and are one import
away, and a copy inside a data file is a copy that goes stale. These tables hold
measurements only.

Parquet rather than a DuckDB file, deliberately: the writer replaces it
atomically and readers are never blocked, whereas a DuckDB file takes an
exclusive write lock that would put ``asl serve`` and the nightly run in each
other's way.

Rebuilds are whole-dataset. Incremental refresh is possible — the partitions a
run touched are recoverable from ``ingestion_batches.window_start/window_end``
— but a full pass over a 1.3GB lake reads two low-cardinality string columns and
a timestamp, and is not worth pre-optimising against.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import pyarrow.parquet as pq

from ashare_lake.config import Config
from ashare_lake.domain.datasets import DATASETS, DatasetSpec
from ashare_lake.domain.partitions import granularity_of, parse_partition
from ashare_lake.file_lock import LockUnavailable, exclusive_lock
from ashare_lake.query.parquet_scan import list_partitions, partition_dir
from ashare_lake.storage.atomic import write_parquet_atomic

PARTITION_STATS_FILE = "partition_stats.parquet"
PROVENANCE_STATS_FILE = "provenance_stats.parquet"
STATS_SUMMARY_FILE = "stats-latest.json"
_REBUILD_LOCK = ".rebuild.lock"
# Above this row count a dataset is considered too large for the historical
# Polars whole-dataset aggregation path. Large datasets get footer-based
# partition row counts and a DuckDB column-pruned provenance aggregation.
LARGE_STATS_ROW_THRESHOLD = 2_000_000

# Column polars fills with the source file of each row. Underscored so it cannot
# collide with a dataset column.
_PATH_COL = "__source_file"

PARTITION_STATS_SCHEMA: dict[str, pl.DataType] = {
    "dataset": pl.Utf8,
    # None for rows sitting directly under the dataset root: the whole table for
    # a merge-style dataset (instruments), stray files for a partitioned one.
    "partition": pl.Utf8,
    "granularity": pl.Utf8,
    "period_start": pl.Date,
    "period_end": pl.Date,
    "row_count": pl.Int64,
    "file_count": pl.Int64,
    "bytes": pl.Int64,
}

PROVENANCE_STATS_SCHEMA: dict[str, pl.DataType] = {
    "dataset": pl.Utf8,
    "partition": pl.Utf8,
    "source": pl.Utf8,
    "data_version": pl.Utf8,
    "row_count": pl.Int64,
    "fetched_at_min": pl.Datetime("us", "UTC"),
    "fetched_at_max": pl.Datetime("us", "UTC"),
}

_PROVENANCE_KEYS = ("source", "data_version")


@dataclass
class StatsResult:
    """What one rebuild covered."""

    datasets: list[str] = field(default_factory=list)
    empty: list[str] = field(default_factory=list)
    partitions: int = 0
    rows: int = 0
    files: int = 0
    bytes: int = 0
    elapsed_seconds: float = 0.0

    def as_dict(self) -> dict:
        return {
            "datasets": len(self.datasets),
            "empty_datasets": sorted(self.empty),
            "partitions": self.partitions,
            "rows": self.rows,
            "files": self.files,
            "bytes": self.bytes,
            "elapsed_seconds": round(self.elapsed_seconds, 2),
        }


def stats_root(config: Config) -> Path:
    return config.meta_root / "stats"


def partition_stats_path(config: Config) -> Path:
    return stats_root(config) / PARTITION_STATS_FILE


def provenance_stats_path(config: Config) -> Path:
    return stats_root(config) / PROVENANCE_STATS_FILE


def summary_path(config: Config) -> Path:
    return stats_root(config) / STATS_SUMMARY_FILE


def dataset_root(config: Config, spec: DatasetSpec) -> Path:
    base = config.derived_root if spec.layer == "derived" else config.curated_root
    return base / spec.name


def _partition_of(file_path: str, partition_col: str | None) -> str | None:
    """Partition value owning *file_path*, read back from its directory name.

    Derived from the path polars hands back rather than from a lookup keyed on
    the path we passed in, because the two need not be spelled identically once
    the scanner has resolved them.
    """
    if partition_col is None:
        return None
    prefix = f"{partition_col}="
    parent = Path(file_path).parent.name
    return parent[len(prefix) :] if parent.startswith(prefix) else None


def _file_groups(root: Path, spec: DatasetSpec) -> dict[str | None, list[Path]]:
    """Parquet files under *root*, grouped by the partition value owning them."""
    groups: dict[str | None, list[Path]] = {}
    if spec.partition_col:
        for part in list_partitions(root, spec.partition_col):
            files = sorted(partition_dir(root, spec.partition_col, part.value).rglob("*.parquet"))
            if files:
                groups[part.value] = files
    loose = sorted(root.glob("*.parquet"))
    if loose:
        groups[None] = loose
    return groups


def _empty(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(schema=schema)


def _sql_ident(name: str) -> str:
    """Quote a SQL identifier; all callers pass trusted dataset column names."""
    return '"' + name.replace('"', '""') + '"'


def _parquet_row_count(path: Path) -> int:
    """Read one file's row count from its footer, never from its data rows."""
    try:
        return int(pq.ParquetFile(path).metadata.num_rows)
    except Exception:
        # A damaged file already fails normal reads. Stats should still be
        # able to describe the rest of the lake.
        return 0


def _duckdb_provenance(
    config: Config, files: list[Path], spec: DatasetSpec
) -> tuple[pl.DataFrame, dict[str | None, int]]:
    """Column-pruned provenance aggregation for very large datasets.

    The historical Polars path materialises the same columns plus the source
    file name and can exhaust a normal container on datasets with hundreds of
    millions of rows. DuckDB can perform the same grouped aggregation while
    spilling to disk, so the process keeps the dashboard alive instead of
    becoming a one-off high-memory job.
    """
    scratch = stats_root(config) / "tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"SET memory_limit='{config.duckdb_memory_limit}'")
        con.execute(f"SET threads={config.duckdb_threads}")
        con.execute(
            f"PRAGMA temp_directory='{scratch.as_posix().replace(chr(39), chr(39) * 2)}'"
        )
        paths = [str(path) for path in files]
        describe = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true)", [paths]
        ).fetchall()
        names = {str(row[0]) for row in describe}
        keys = [col for col in _PROVENANCE_KEYS if col in names]

        selected = ["filename AS __source_file"]
        selected += [_sql_ident(key) for key in keys]
        selected += ["count(*) AS row_count"]
        if "fetched_at" in names:
            selected += [
                "min(fetched_at) AS fetched_at_min",
                "max(fetched_at) AS fetched_at_max",
            ]

        grouped = ["__source_file"]
        grouped += [_sql_ident(key) for key in keys]
        query = (
            f"SELECT {', '.join(selected)} "
            "FROM read_parquet(?, filename=true, union_by_name=true) "
            f"GROUP BY {', '.join(grouped)}"
        )
        per_file = pl.from_arrow(con.execute(query, [paths]).arrow())
    finally:
        con.close()

    per_file = per_file.with_columns(
        pl.col("__source_file")
        .map_elements(
            lambda path: _partition_of(path, spec.partition_col),
            return_dtype=pl.Utf8,
        )
        .alias("partition")
    )

    rows_by_partition = {
        row["partition"]: int(row["row_count"])
        for row in per_file.group_by("partition")
        .agg(pl.col("row_count").sum())
        .iter_rows(named=True)
    }

    if not keys:
        return _empty(PROVENANCE_STATS_SCHEMA), rows_by_partition

    rollup = [pl.col("row_count").sum().alias("row_count")]
    if "fetched_at_min" in per_file.columns:
        rollup += [
            pl.col("fetched_at_min").min().alias("fetched_at_min"),
            pl.col("fetched_at_max").max().alias("fetched_at_max"),
        ]
    provenance = (
        per_file.group_by(["partition", *keys])
        .agg(rollup)
        .with_columns(pl.lit(spec.name, dtype=pl.Utf8).alias("dataset"))
    )
    for col, dtype in PROVENANCE_STATS_SCHEMA.items():
        if col not in provenance.columns:
            provenance = provenance.with_columns(pl.lit(None, dtype=dtype).alias(col))
    provenance = provenance.select(list(PROVENANCE_STATS_SCHEMA)).cast(PROVENANCE_STATS_SCHEMA)
    return provenance, rows_by_partition


def _partition_stats_from_footers(
    spec: DatasetSpec, groups: dict[str | None, list[Path]]
) -> pl.DataFrame:
    """Partition measurements from Parquet footers only."""
    rows = []
    for value, files in groups.items():
        part = parse_partition(value) if value is not None else None
        rows.append(
            {
                "dataset": spec.name,
                "partition": value,
                "granularity": granularity_of(part) if part else None,
                "period_start": part.start if part else None,
                "period_end": part.end if part else None,
                "row_count": sum(_parquet_row_count(path) for path in files),
                "file_count": len(files),
                "bytes": sum(path.stat().st_size for path in files if path.is_file()),
            }
        )
    return pl.DataFrame(rows, schema=PARTITION_STATS_SCHEMA)


def _provenance_for_dataset(
    files: list[Path], spec: DatasetSpec
) -> tuple[pl.DataFrame, dict[str | None, int]]:
    """Per-(partition, source, data_version) rows, and rows per partition.

    One scan for the whole dataset, not one per partition: a day-partitioned
    decade is ~4,000 directories, and 4,000 query plans over one file each cost
    far more than one plan over 4,000.
    """
    lf = pl.scan_parquet([str(f) for f in files], include_file_paths=_PATH_COL)
    names = set(lf.collect_schema().names())
    keys = [col for col in _PROVENANCE_KEYS if col in names]

    aggs = [pl.len().alias("row_count")]
    if "fetched_at" in names:
        aggs += [
            pl.col("fetched_at").min().alias("fetched_at_min"),
            pl.col("fetched_at").max().alias("fetched_at_max"),
        ]

    per_file = lf.group_by([_PATH_COL, *keys]).agg(aggs).collect()
    per_file = per_file.with_columns(
        pl.col(_PATH_COL)
        .map_elements(
            lambda p: _partition_of(p, spec.partition_col),
            return_dtype=pl.Utf8,
        )
        .alias("partition")
    )

    rows_by_partition = {
        row["partition"]: int(row["row_count"])
        for row in per_file.group_by("partition")
        .agg(pl.col("row_count").sum())
        .iter_rows(named=True)
    }

    if not keys:
        # Nothing to attribute a row to; the partition totals still stand.
        return _empty(PROVENANCE_STATS_SCHEMA), rows_by_partition

    rollup = [pl.col("row_count").sum().alias("row_count")]
    if "fetched_at" in names:
        rollup += [
            pl.col("fetched_at_min").min().alias("fetched_at_min"),
            pl.col("fetched_at_max").max().alias("fetched_at_max"),
        ]
    provenance = (
        per_file.group_by(["partition", *keys])
        .agg(rollup)
        .with_columns(pl.lit(spec.name, dtype=pl.Utf8).alias("dataset"))
    )
    for col, dtype in PROVENANCE_STATS_SCHEMA.items():
        if col not in provenance.columns:
            provenance = provenance.with_columns(pl.lit(None, dtype=dtype).alias(col))
    provenance = provenance.select(list(PROVENANCE_STATS_SCHEMA)).cast(PROVENANCE_STATS_SCHEMA)
    return provenance, rows_by_partition


def _stats_for_dataset(
    config: Config, spec: DatasetSpec
) -> tuple[pl.DataFrame, pl.DataFrame] | None:
    """Both tables for one dataset, or None when it holds no parquet."""
    root = dataset_root(config, spec)
    groups = _file_groups(root, spec)
    if not groups:
        return None

    all_files = [f for files in groups.values() for f in files]
    footer_partitions = _partition_stats_from_footers(spec, groups)
    estimated_rows = int(footer_partitions["row_count"].sum())

    if estimated_rows > LARGE_STATS_ROW_THRESHOLD:
        provenance, rows_by_partition = _duckdb_provenance(config, all_files, spec)
        return footer_partitions, provenance

    provenance, rows_by_partition = _provenance_for_dataset(all_files, spec)

    partition_rows = []
    for value, files in groups.items():
        part = parse_partition(value) if value is not None else None
        partition_rows.append(
            {
                "dataset": spec.name,
                "partition": value,
                "granularity": granularity_of(part) if part else None,
                "period_start": part.start if part else None,
                "period_end": part.end if part else None,
                "row_count": rows_by_partition.get(value, 0),
                "file_count": len(files),
                "bytes": sum(f.stat().st_size for f in files if f.is_file()),
            }
        )
    partitions = pl.DataFrame(partition_rows, schema=PARTITION_STATS_SCHEMA)
    return partitions, provenance


def _merge(existing: pl.DataFrame, fresh: pl.DataFrame, rebuilt: set[str]) -> pl.DataFrame:
    """Replace the rebuilt datasets' rows, leave every other dataset alone.

    A ``--dataset`` rebuild must not delete the datasets it did not look at.
    """
    if existing.is_empty():
        return fresh
    kept = existing.filter(~pl.col("dataset").is_in(list(rebuilt)))
    if fresh.is_empty():
        return kept
    return pl.concat([kept, fresh], how="vertical_relaxed")


def _read(path: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    if not path.exists():
        return _empty(schema)
    try:
        return pl.read_parquet(path)
    except Exception:
        # A truncated or half-written table is regenerated, not fatal.
        return _empty(schema)


def rebuild_stats(config: Config, *, datasets: list[str] | None = None) -> StatsResult:
    """Recompute the stats tables and replace them atomically.

    *datasets* limits the pass; the remaining datasets keep the rows they
    already had.
    """
    started = time.monotonic()
    names = sorted(datasets) if datasets else sorted(DATASETS)
    unknown = [name for name in names if name not in DATASETS]
    if unknown:
        raise ValueError(f"unknown dataset(s): {', '.join(unknown)}")

    result = StatsResult()
    partition_frames: list[pl.DataFrame] = []
    provenance_frames: list[pl.DataFrame] = []

    for name in names:
        computed = _stats_for_dataset(config, DATASETS[name])
        if computed is None:
            result.empty.append(name)
            continue
        partitions, provenance = computed
        partition_frames.append(partitions)
        if not provenance.is_empty():
            provenance_frames.append(provenance)
        result.datasets.append(name)
        result.partitions += partitions.height
        result.rows += int(partitions["row_count"].sum())
        result.files += int(partitions["file_count"].sum())
        result.bytes += int(partitions["bytes"].sum())

    fresh_partitions = (
        pl.concat(partition_frames, how="vertical_relaxed")
        if partition_frames
        else _empty(PARTITION_STATS_SCHEMA)
    )
    fresh_provenance = (
        pl.concat(provenance_frames, how="vertical_relaxed")
        if provenance_frames
        else _empty(PROVENANCE_STATS_SCHEMA)
    )

    rebuilt = set(names)
    stats_root(config).mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(
        partition_stats_path(config),
        _merge(
            _read(partition_stats_path(config), PARTITION_STATS_SCHEMA), fresh_partitions, rebuilt
        ),
    )
    write_parquet_atomic(
        provenance_stats_path(config),
        _merge(
            _read(provenance_stats_path(config), PROVENANCE_STATS_SCHEMA), fresh_provenance, rebuilt
        ),
    )

    result.elapsed_seconds = time.monotonic() - started
    _write_summary(config, result, rebuilt)
    return result


def _write_summary(config: Config, result: StatsResult, rebuilt: set[str]) -> None:
    """Sidecar recording when the tables were built and against which run.

    A reader comparing ``latest_run_id`` against the manifest can tell whether
    the tables predate the last ingestion without reading the parquet at all.
    """
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "latest_run_id": _latest_run_id(config),
        "rebuilt_datasets": sorted(rebuilt),
        **result.as_dict(),
    }
    with open(summary_path(config), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2, default=str)


def _latest_run_id(config: Config) -> str | None:
    """Newest run in the manifest, or None when there is no readable manifest."""
    try:
        from ashare_lake.orchestrator.manifest import Manifest

        latest = Manifest(config.manifest_path).latest_run()
        return latest["run_id"] if latest else None
    except Exception:
        # Staleness is a convenience signal and the summary is a sidecar; a
        # missing or locked manifest must not fail a rebuild that has already
        # written both tables.
        return None


@dataclass(frozen=True)
class StatsFreshness:
    """Whether the tables still describe the lake as it stands."""

    stale: bool
    reason: str | None
    generated_at: datetime | None
    stats_run_id: str | None
    latest_run_id: str | None


def stats_freshness(config: Config) -> StatsFreshness:
    """Compare the stats sidecar against the manifest's newest run.

    Reads one small JSON and one SQLite row — cheap enough for a dashboard to
    call on every request, which is the point: the tables do not refresh
    themselves, so somebody has to notice.

    The run id is the signal rather than a wall-clock age. Ingestion is what
    changes the lake, so tables built after the last run are current however
    old they are, and tables built before it are stale however recent.
    """
    latest_run_id = _latest_run_id(config)
    summary = load_summary(config)
    if summary is None:
        return StatsFreshness(True, "no stats yet", None, None, latest_run_id)

    generated_at = None
    raw = summary.get("generated_at")
    if isinstance(raw, str):
        try:
            generated_at = datetime.fromisoformat(raw)
        except ValueError:
            generated_at = None

    stats_run_id = summary.get("latest_run_id")
    if latest_run_id is not None and stats_run_id != latest_run_id:
        return StatsFreshness(
            True,
            f"ingestion run {latest_run_id} landed after the stats were built",
            generated_at,
            stats_run_id,
            latest_run_id,
        )
    return StatsFreshness(False, None, generated_at, stats_run_id, latest_run_id)


def refresh_stats_if_stale(config: Config, *, force: bool = False) -> StatsResult | None:
    """Rebuild only when the lake has moved on. Returns None when it has not.

    Guarded by a non-blocking lock so the callers that share a lake — a
    dashboard request, a cron fallback, a nightly run — collapse into one
    rebuild instead of racing. A caller that loses the lock returns None rather
    than waiting: another rebuild is already producing the answer, and blocking
    a web request behind a full scan is worse than serving numbers one run old.

    Threading policy stays with the caller. A server wanting this off the
    request path runs it in its own background thread.
    """
    if not force and not stats_freshness(config).stale:
        return None
    try:
        with exclusive_lock(stats_root(config) / _REBUILD_LOCK, blocking=False):
            # Re-check under the lock: whoever held it may have just finished
            # the very rebuild this call was about to repeat.
            if not force and not stats_freshness(config).stale:
                return None
            return rebuild_stats(config)
    except LockUnavailable:
        return None


def load_partition_stats(config: Config) -> pl.DataFrame:
    return _read(partition_stats_path(config), PARTITION_STATS_SCHEMA)


def load_provenance_stats(config: Config) -> pl.DataFrame:
    return _read(provenance_stats_path(config), PROVENANCE_STATS_SCHEMA)


def load_summary(config: Config) -> dict | None:
    path = summary_path(config)
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
