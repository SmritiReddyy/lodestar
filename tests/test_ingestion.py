"""Unit tests for the extract/load layer.

These target the properties that are expensive to discover in production:
idempotency, watermark boundaries, and the landing-zone layout contract.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from lodestar_ingest.config import Config
from lodestar_ingest.extract import extract_all, extract_table, extract_table_history
from lodestar_ingest.generate import OlistGenerator, apply_seller_drift
from lodestar_ingest.sources import SOURCE_TABLES, get_table
from lodestar_ingest.storage import LocalLandingZone, partition_key
from lodestar_ingest.warehouse import DuckDBWarehouse, load_tables

ORDERS = 400
SEED = 4242


@pytest.fixture(scope="module")
def source_dir(tmp_path_factory) -> Path:
    """A small synthetic export, generated once for the whole module."""
    target = tmp_path_factory.mktemp("source")
    OlistGenerator(n_orders=ORDERS, seed=SEED).write_csvs(target)
    return target


@pytest.fixture
def config(source_dir: Path, tmp_path: Path) -> Config:
    return Config(
        target="local",
        source_dir=source_dir,
        landing_uri=str(tmp_path / "landing"),
        gcp_project=None,
        bq_location="US",
        raw_dataset="lodestar_raw",
        duckdb_path=tmp_path / "test.duckdb",
        synthetic_orders=ORDERS,
        synthetic_seed=SEED,
    )


@pytest.fixture
def zone(config: Config) -> LocalLandingZone:
    return LocalLandingZone(config.landing_uri)


# --------------------------------------------------------------- generator


def test_generator_is_deterministic(tmp_path: Path) -> None:
    """Same seed, same bytes — otherwise no test downstream can be stable."""
    a, b = tmp_path / "a", tmp_path / "b"
    OlistGenerator(n_orders=200, seed=99).write_csvs(a)
    OlistGenerator(n_orders=200, seed=99).write_csvs(b)

    for table in SOURCE_TABLES:
        assert (a / table.filename).read_bytes() == (b / table.filename).read_bytes()


def test_generator_emits_declared_columns(source_dir: Path) -> None:
    """The CSV headers must match `sources.py` exactly, typos included."""
    for table in SOURCE_TABLES:
        header = (source_dir / table.filename).read_text().splitlines()[0]
        assert header.split(",") == table.column_names


def test_generated_orders_reference_real_customers(source_dir: Path) -> None:
    con = duckdb.connect(":memory:")
    orphans = con.execute(
        f"""
        select count(*)
        from read_csv_auto('{(source_dir / "olist_orders_dataset.csv").as_posix()}') o
        left join read_csv_auto('{(source_dir / "olist_customers_dataset.csv").as_posix()}') c
          on o.customer_id = c.customer_id
        where c.customer_id is null
        """
    ).fetchone()[0]
    assert orphans == 0


def test_seller_drift_changes_only_attributes(tmp_path: Path) -> None:
    """Drift must move sellers, never invent or drop keys — otherwise the SCD2
    demo would be testing an insert, not a change."""
    OlistGenerator(n_orders=300, seed=11).write_csvs(tmp_path)
    path = tmp_path / "olist_sellers_dataset.csv"

    before = path.read_text().splitlines()[1:]
    keys_before = {line.split(",")[0] for line in before}

    moved = apply_seller_drift(tmp_path, fraction=0.25, seed=5)

    after = path.read_text().splitlines()[1:]
    keys_after = {line.split(",")[0] for line in after}

    assert moved > 0
    assert keys_before == keys_after
    assert len(before) == len(after)
    assert sum(1 for a, b in zip(sorted(before), sorted(after), strict=True) if a != b) > 0


# ----------------------------------------------------------------- extract


def test_extract_writes_expected_partition_path(config: Config, zone: LocalLandingZone) -> None:
    table = get_table("customers")
    result = extract_table(table, config=config, zone=zone, ingest_date="2024-05-01")

    expected_dir = Path(config.landing_uri) / partition_key("customers", "2024-05-01")
    assert expected_dir.is_dir()
    assert result.rows == ORDERS
    assert Path(result.uri).suffix == ".parquet"


def test_extract_is_idempotent(config: Config, zone: LocalLandingZone) -> None:
    """Re-landing a partition replaces it; it never accumulates objects."""
    table = get_table("customers")
    for _ in range(3):
        extract_table(table, config=config, zone=zone, ingest_date="2024-05-01")

    objects = zone.list_prefix(partition_key("customers", "2024-05-01"))
    assert len(objects) == 1


def test_watermark_window_is_half_open(config: Config, zone: LocalLandingZone) -> None:
    """`[start, end)` — a row at exactly midnight belongs to one day only."""
    table = get_table("orders")
    day = "2024-03-15"
    next_day = "2024-03-16"

    first = extract_table(
        table,
        config=config,
        zone=zone,
        ingest_date=day,
        window_start=f"{day} 00:00:00",
        window_end=f"{next_day} 00:00:00",
    )
    second = extract_table(
        table,
        config=config,
        zone=zone,
        ingest_date=next_day,
        window_start=f"{next_day} 00:00:00",
        window_end="2024-03-17 00:00:00",
    )

    con = duckdb.connect(":memory:")
    overlap = con.execute(
        f"""
        select count(*) from (
            select order_id from read_parquet('{first.uri}')
            intersect
            select order_id from read_parquet('{second.uri}')
        )
        """
    ).fetchone()[0]
    assert overlap == 0


def test_history_extract_fans_out_by_event_date(config: Config, zone: LocalLandingZone) -> None:
    """Each row lands in the partition its own watermark belongs to."""
    table = get_table("orders")
    result = extract_table_history(table, config=config, zone=zone, fallback_date="2025-01-01")

    assert result.partitions > 1
    assert result.rows == ORDERS

    con = duckdb.connect(":memory:")
    mismatched = con.execute(
        f"""
        select count(*)
        from read_parquet('{zone.partition_glob("orders")}')
        where ingest_date <> cast(order_purchase_timestamp as date)
        """
    ).fetchone()[0]
    assert mismatched == 0


def test_extract_adds_lineage_columns(config: Config, zone: LocalLandingZone) -> None:
    result = extract_table(get_table("sellers"), config=config, zone=zone, ingest_date="2024-05-01")
    con = duckdb.connect(":memory:")
    described = con.execute(f"describe select * from read_parquet('{result.uri}')").fetchall()
    columns = {row[0] for row in described}
    assert {"ingest_date", "_ingested_at", "_source_file", "_run_id"} <= columns


# -------------------------------------------------------------------- load


def _load_history(config: Config, zone: LocalLandingZone) -> DuckDBWarehouse:
    extract_all(config=config, zone=zone, ingest_date="2025-01-01", history=True)
    warehouse = DuckDBWarehouse(config)
    load_tables(
        list(SOURCE_TABLES),
        config=config,
        zone=zone,
        ingest_date="2025-01-01",
        warehouse=warehouse,
        all_partitions=True,
    )
    return warehouse


def test_history_load_populates_every_table(config: Config, zone: LocalLandingZone) -> None:
    warehouse = _load_history(config, zone)
    try:
        assert warehouse.row_count("lodestar_raw", "orders") == ORDERS
        assert warehouse.row_count("lodestar_raw", "order_items") > ORDERS
        for table in SOURCE_TABLES:
            assert warehouse.row_count("lodestar_raw", table.name) > 0
    finally:
        warehouse.close()


def test_daily_load_is_idempotent(config: Config, zone: LocalLandingZone) -> None:
    """The property the whole backfill story rests on: re-running a day is a
    no-op, not a duplicate."""
    warehouse = _load_history(config, zone)
    try:
        baseline = warehouse.row_count("lodestar_raw", "orders")

        con = duckdb.connect(str(config.duckdb_path))
        busiest = con.execute(
            "select ingest_date from lodestar_raw.orders group by 1 order by count(*) desc limit 1"
        ).fetchone()[0]
        con.close()

        day = busiest.isoformat()
        next_day = (busiest + timedelta(days=1)).isoformat()

        for _ in range(3):
            extract_all(
                config=config,
                zone=zone,
                ingest_date=day,
                window_start=f"{day} 00:00:00",
                window_end=f"{next_day} 00:00:00",
            )
            load_tables(
                list(SOURCE_TABLES),
                config=config,
                zone=zone,
                ingest_date=day,
                warehouse=warehouse,
            )

        assert warehouse.row_count("lodestar_raw", "orders") == baseline

        con = duckdb.connect(str(config.duckdb_path))
        duplicates = con.execute(
            "select count(*) from (select order_id from lodestar_raw.orders "
            "group by 1 having count(*) > 1)"
        ).fetchone()[0]
        con.close()
        assert duplicates == 0
    finally:
        warehouse.close()


def test_full_snapshot_tables_replace_rather_than_append(
    config: Config, zone: LocalLandingZone
) -> None:
    """A dimension re-landed on a later date replaces the snapshot; it must not
    accumulate a second copy of every row."""
    warehouse = _load_history(config, zone)
    try:
        baseline = warehouse.row_count("lodestar_raw", "sellers")

        for day in ("2025-01-02", "2025-01-03"):
            extract_all(config=config, zone=zone, ingest_date=day, tables=["sellers"])
            load_tables(
                [get_table("sellers")],
                config=config,
                zone=zone,
                ingest_date=day,
                warehouse=warehouse,
            )

        assert warehouse.row_count("lodestar_raw", "sellers") == baseline
    finally:
        warehouse.close()


# ------------------------------------------------------------------ config


def test_gcp_target_rejects_local_landing_path(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LODESTAR_TARGET", "gcp")
    monkeypatch.setenv("LODESTAR_LANDING_URI", str(tmp_path))
    with pytest.raises(ValueError, match="gs://"):
        Config.from_env()


def test_unknown_target_is_rejected(monkeypatch) -> None:
    monkeypatch.setenv("LODESTAR_TARGET", "redshift")
    with pytest.raises(ValueError, match="local"):
        Config.from_env()


def test_duckdb_warehouse_refuses_gcs_uri(config: Config) -> None:
    warehouse = DuckDBWarehouse(config)
    try:
        with pytest.raises(ValueError, match="local Parquet"):
            warehouse.load_partition(
                get_table("sellers"), "lodestar_raw", "gs://bucket/x/*.parquet", "2024-01-01"
            )
    finally:
        warehouse.close()


def test_unknown_source_table_raises() -> None:
    with pytest.raises(KeyError, match="Unknown source table"):
        get_table("nope")
