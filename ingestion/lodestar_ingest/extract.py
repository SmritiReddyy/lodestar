"""Extract stage: source CSVs -> Parquet in the partitioned landing zone.

Design notes worth defending in an interview:

* **Schema-on-write at the boundary.** CSV types are declared once, in
  `sources.py`, and enforced here. Downstream stages never re-guess whether
  `price` is a string.
* **Idempotent partitions.** A run targeting `ingest_date=D` deletes that
  partition before writing it, so any historical day can be re-run safely and
  cannot double-count.
* **Watermarking.** Incremental tables filter on their event timestamp, so a
  daily run lands only that day's slice.
* **History fans out in one pass.** The initial load derives `ingest_date`
  from each row's own watermark and writes every day partition in a single
  DuckDB `COPY ... PARTITION_BY`, instead of looping over hundreds of days.
  That leaves the landing zone laid out exactly as if it had been filled one
  day at a time, so subsequent daily runs line up with it.
"""

from __future__ import annotations

import logging
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path

import duckdb

from .config import Config
from .sources import SOURCE_TABLES, SourceTable
from .storage import LandingZone, object_key, partition_key, table_key

log = logging.getLogger(__name__)

# Logical type -> DuckDB read_csv type.
DUCKDB_TYPES = {
    "string": "VARCHAR",
    "integer": "BIGINT",
    "float": "DOUBLE",
    "timestamp": "TIMESTAMP",
    "date": "DATE",
    "boolean": "BOOLEAN",
}


@dataclass
class ExtractResult:
    table: str
    rows: int
    uri: str
    bytes_written: int
    partitions: int
    replaced_objects: int


def _column_spec(table: SourceTable) -> str:
    """DuckDB struct literal declaring every column's type."""
    pairs = ", ".join(
        f"'{col}': '{DUCKDB_TYPES[logical]}'" for col, logical in table.columns.items()
    )
    return "{" + pairs + "}"


def _select_list(table: SourceTable) -> str:
    """Project columns explicitly so column order is pinned, not inherited."""
    return ", ".join(f'"{c}"' for c in table.column_names)


def _read_expr(table: SourceTable, source_path: Path) -> str:
    return (
        f"read_csv('{source_path.as_posix()}', header=true, "
        f"columns={_column_spec(table)}, nullstr='', sample_size=-1)"
    )


def _source_path(table: SourceTable, config: Config) -> Path:
    path = config.source_dir / table.filename
    if not path.exists():
        raise FileNotFoundError(
            f"Source file missing for table {table.name!r}: {path}. "
            "Run `lodestar-ingest seed` first, or point LODESTAR_SOURCE_DIR at "
            "an unpacked Olist export."
        )
    return path


def _window_clause(table: SourceTable, window_start: str | None, window_end: str | None) -> str:
    """Watermark filter for one day's slice, `[start, end)`."""
    if table.load_strategy != "incremental" or not table.watermark_column or not window_start:
        return ""
    col = f'"{table.watermark_column}"'
    bounds = [f"{col} >= TIMESTAMP '{window_start}'"]
    if window_end:
        bounds.append(f"{col} < TIMESTAMP '{window_end}'")
    # Rows with a null watermark belong to no window and would otherwise be
    # invisible forever; keep them so the history load still sees them.
    return f"WHERE ({' AND '.join(bounds)}) OR {col} IS NULL"


def extract_table(
    table: SourceTable,
    *,
    config: Config,
    zone: LandingZone,
    ingest_date: str,
    run_id: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
) -> ExtractResult:
    """Land one table's daily slice as a single Parquet object."""
    run_id = run_id or uuid.uuid4().hex[:12]
    source_path = _source_path(table, config)

    con = duckdb.connect(":memory:")
    try:
        query = f"""
            SELECT
                {_select_list(table)},
                CAST('{ingest_date}' AS DATE) AS ingest_date,
                now()                         AS _ingested_at,
                '{table.filename}'            AS _source_file,
                '{run_id}'                    AS _run_id
            FROM {_read_expr(table, source_path)}
            {_window_clause(table, window_start, window_end)}
        """
        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp) / f"{table.name}-{run_id}.parquet"
            con.execute(
                f"COPY ({query}) TO '{local.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            rows = con.execute(
                f"SELECT count(*) FROM read_parquet('{local.as_posix()}')"
            ).fetchone()[0]
            size = local.stat().st_size
            replaced = zone.delete_prefix(partition_key(table.name, ingest_date))
            uri = zone.put_file(local, object_key(table.name, ingest_date, run_id))
    finally:
        con.close()

    log.info(
        "extracted %s: %s rows -> %s (%.1f KiB, replaced %s object(s))",
        table.name,
        f"{rows:,}",
        uri,
        size / 1024,
        replaced,
    )
    return ExtractResult(table.name, rows, uri, size, 1, replaced)


def extract_table_history(
    table: SourceTable,
    *,
    config: Config,
    zone: LandingZone,
    fallback_date: str,
    run_id: str | None = None,
) -> ExtractResult:
    """Land a table's entire history, fanned out into per-day partitions.

    Incremental tables derive `ingest_date` from their own watermark, so the
    landing zone ends up identical to what N daily runs would have produced.
    Full-snapshot tables have no event time and land in one partition dated
    `fallback_date`.
    """
    run_id = run_id or uuid.uuid4().hex[:12]
    source_path = _source_path(table, config)

    if table.load_strategy == "incremental" and table.watermark_column:
        # COALESCE keeps null-watermark rows in the fallback partition rather
        # than dropping them into a null-named directory.
        date_expr = (
            f"COALESCE(CAST(\"{table.watermark_column}\" AS DATE), CAST('{fallback_date}' AS DATE))"
        )
    else:
        date_expr = f"CAST('{fallback_date}' AS DATE)"

    con = duckdb.connect(":memory:")
    try:
        query = f"""
            SELECT
                {_select_list(table)},
                {date_expr}        AS ingest_date,
                now()              AS _ingested_at,
                '{table.filename}' AS _source_file,
                '{run_id}'         AS _run_id
            FROM {_read_expr(table, source_path)}
        """
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / table.name
            con.execute(
                f"COPY ({query}) TO '{out_dir.as_posix()}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (ingest_date), "
                f"OVERWRITE_OR_IGNORE, FILENAME_PATTERN '{table.name}-{run_id}')"
            )
            rows = con.execute(
                f"SELECT count(*) FROM read_parquet('{out_dir.as_posix()}/*/*.parquet')"
            ).fetchone()[0]
            size = sum(p.stat().st_size for p in out_dir.rglob("*.parquet"))
            n_parts = len(list(out_dir.glob("ingest_date=*")))

            # Rewriting the whole history replaces the whole table prefix.
            replaced = zone.delete_prefix(table_key(table.name))
            zone.put_tree(out_dir, table_key(table.name))
            uri = zone.partition_glob(table.name)
    finally:
        con.close()

    log.info(
        "extracted history for %s: %s rows across %s partition(s) (%.1f KiB)",
        table.name,
        f"{rows:,}",
        n_parts,
        size / 1024,
    )
    return ExtractResult(table.name, rows, uri, size, n_parts, replaced)


def _select_tables(tables: list[str] | None) -> tuple[SourceTable, ...]:
    if not tables:
        return SOURCE_TABLES
    wanted = set(tables)
    return tuple(t for t in SOURCE_TABLES if t.name in wanted)


def extract_all(
    *,
    config: Config,
    zone: LandingZone,
    ingest_date: str,
    tables: list[str] | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
    history: bool = False,
) -> list[ExtractResult]:
    run_id = uuid.uuid4().hex[:12]
    selected = _select_tables(tables)
    if history:
        return [
            extract_table_history(
                t, config=config, zone=zone, fallback_date=ingest_date, run_id=run_id
            )
            for t in selected
        ]
    return [
        extract_table(
            t,
            config=config,
            zone=zone,
            ingest_date=ingest_date,
            run_id=run_id,
            window_start=window_start,
            window_end=window_end,
        )
        for t in selected
    ]
