"""Load stage: landing-zone Parquet -> raw warehouse tables.

Two backends behind one interface:

* **DuckDB** for local development and CI. The whole warehouse is a single
  file, so a PR can build and test the full model graph in seconds with no
  cloud credentials and no spend.
* **BigQuery** for the deployed pipeline.

Both honour the same contract: loading `ingest_date=D` twice produces the same
table state as loading it once.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

from .config import Config
from .sources import SourceTable
from .storage import LandingZone

log = logging.getLogger(__name__)


@dataclass
class LoadResult:
    table: str
    rows_loaded: int
    rows_total: int
    mode: str
    destination: str


class Warehouse(ABC):
    @abstractmethod
    def ensure_dataset(self, dataset: str) -> None: ...

    @abstractmethod
    def load_partition(
        self,
        table: SourceTable,
        dataset: str,
        source_uri: str,
        ingest_date: str,
        full_refresh: bool = False,
    ) -> LoadResult: ...

    @abstractmethod
    def row_count(self, dataset: str, table: str) -> int: ...

    def close(self) -> None:  # noqa: B027 - optional hook, not every backend holds a handle
        """Release any connection. DuckDB overrides this; BigQuery has nothing to free."""


class DuckDBWarehouse(Warehouse):
    """File-backed warehouse used for local runs and CI."""

    def __init__(self, config: Config) -> None:
        import duckdb  # noqa: PLC0415

        config.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        self.path = config.duckdb_path
        self.con = duckdb.connect(str(config.duckdb_path))

    def ensure_dataset(self, dataset: str) -> None:
        self.con.execute(f'CREATE SCHEMA IF NOT EXISTS "{dataset}"')

    def load_partition(
        self,
        table: SourceTable,
        dataset: str,
        source_uri: str,
        ingest_date: str,
        full_refresh: bool = False,
    ) -> LoadResult:
        if source_uri.startswith("gs://"):
            raise ValueError(
                "DuckDBWarehouse reads local Parquet only. Use LODESTAR_TARGET=gcp "
                "with a BigQuery destination for a gs:// landing zone."
            )
        self.ensure_dataset(dataset)
        fq = f'"{dataset}"."{table.name}"'
        read = f"read_parquet('{source_uri}', union_by_name=true)"

        exists = self.con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [dataset, table.name],
        ).fetchone()[0]

        if full_refresh or table.load_strategy == "full" or not exists:
            self.con.execute(f"CREATE OR REPLACE TABLE {fq} AS SELECT * FROM {read}")
            mode = "replace"
        else:
            # Delete-then-insert makes the partition rewrite idempotent.
            self.con.execute(f"DELETE FROM {fq} WHERE ingest_date = DATE '{ingest_date}'")
            self.con.execute(f"INSERT INTO {fq} BY NAME SELECT * FROM {read}")
            mode = "delete+insert"

        total = self.row_count(dataset, table.name)
        loaded = (
            total
            if mode == "replace"
            else self.con.execute(
                f"SELECT count(*) FROM {fq} WHERE ingest_date = DATE '{ingest_date}'"
            ).fetchone()[0]
        )
        return LoadResult(table.name, loaded, total, mode, f"{self.path}:{dataset}.{table.name}")

    def row_count(self, dataset: str, table: str) -> int:
        return self.con.execute(f'SELECT count(*) FROM "{dataset}"."{table}"').fetchone()[0]

    def close(self) -> None:
        self.con.close()


class BigQueryWarehouse(Warehouse):
    """Deployed warehouse. Raw tables are partitioned on `ingest_date`."""

    def __init__(self, config: Config) -> None:
        try:
            from google.cloud import bigquery  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover - exercised only on GCP
            raise ImportError(
                "google-cloud-bigquery is required for LODESTAR_TARGET=gcp. "
                "Install with: pip install -e '.[gcp]'"
            ) from exc

        if not config.gcp_project:
            raise ValueError("LODESTAR_GCP_PROJECT must be set when LODESTAR_TARGET=gcp")

        self.bigquery = bigquery
        self.project = config.gcp_project
        self.location = config.bq_location
        self.client = bigquery.Client(project=config.gcp_project)

    def ensure_dataset(self, dataset: str) -> None:
        ref = self.bigquery.Dataset(f"{self.project}.{dataset}")
        ref.location = self.location
        self.client.create_dataset(ref, exists_ok=True)

    def load_partition(
        self,
        table: SourceTable,
        dataset: str,
        source_uri: str,
        ingest_date: str,
        full_refresh: bool = False,
    ) -> LoadResult:
        self.ensure_dataset(dataset)
        table_id = f"{self.project}.{dataset}.{table.name}"
        replace = full_refresh or table.load_strategy == "full"

        if not replace and self._table_exists(table_id):
            # Clear the target partition first so re-runs stay idempotent.
            self.client.query(
                f"DELETE FROM `{table_id}` WHERE ingest_date = DATE(@d)",
                job_config=self.bigquery.QueryJobConfig(
                    query_parameters=[
                        self.bigquery.ScalarQueryParameter("d", "STRING", ingest_date)
                    ]
                ),
            ).result()
            write_disposition = self.bigquery.WriteDisposition.WRITE_APPEND
            mode = "delete+insert"
        else:
            write_disposition = self.bigquery.WriteDisposition.WRITE_TRUNCATE
            mode = "replace"

        job_config = self.bigquery.LoadJobConfig(
            source_format=self.bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
            time_partitioning=self.bigquery.TimePartitioning(field="ingest_date"),
            # Pruning on the partition column is what keeps daily runs cheap.
            schema_update_options=[self.bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )
        job = self.client.load_table_from_uri(
            source_uri, table_id, job_config=job_config, location=self.location
        )
        job.result()

        total = self.row_count(dataset, table.name)
        return LoadResult(table.name, job.output_rows or 0, total, mode, table_id)

    def _table_exists(self, table_id: str) -> bool:
        from google.cloud.exceptions import NotFound  # noqa: PLC0415

        try:
            self.client.get_table(table_id)
        except NotFound:
            return False
        return True

    def row_count(self, dataset: str, table: str) -> int:
        rows = self.client.query(
            f"SELECT count(*) AS n FROM `{self.project}.{dataset}.{table}`"
        ).result()
        return next(iter(rows)).n


def get_warehouse(config: Config) -> Warehouse:
    return BigQueryWarehouse(config) if config.is_gcp else DuckDBWarehouse(config)


def load_tables(
    tables: list[SourceTable],
    *,
    config: Config,
    zone: LandingZone,
    ingest_date: str,
    warehouse: Warehouse | None = None,
    all_partitions: bool = False,
) -> list[LoadResult]:
    """Load one landed partition per table, or the whole history at once.

    `all_partitions=True` pairs with the history extract: it reads every day
    partition and rebuilds each raw table from scratch.
    """
    wh = warehouse or get_warehouse(config)
    owns = warehouse is None
    try:
        results = []
        for table in tables:
            # A daily run reads only the partition just landed. The write mode
            # differs by strategy: a full table's partition is a complete
            # snapshot and replaces the target, while an incremental table's
            # partition is a slice and is merged in by date.
            uri = zone.partition_glob(table.name, None if all_partitions else ingest_date)
            result = wh.load_partition(
                table, config.raw_dataset, uri, ingest_date, full_refresh=all_partitions
            )
            log.info(
                "loaded %s: %s rows in partition, %s total (%s)",
                result.table,
                f"{result.rows_loaded:,}",
                f"{result.rows_total:,}",
                result.mode,
            )
            results.append(result)
        return results
    finally:
        if owns:
            wh.close()
