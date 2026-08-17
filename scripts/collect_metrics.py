#!/usr/bin/env python
"""Measure the pipeline and print the numbers used in docs/METRICS.md.

Resume bullets should come from a measurement, not a guess. This reads the
warehouse and the artefacts a run leaves behind, so every figure quoted in the
docs can be regenerated with one command.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "ingestion"))

import duckdb  # noqa: E402

from lodestar_ingest.config import get_config  # noqa: E402


def _schema(layer: str) -> str:
    target = os.environ.get("DBT_TARGET", "dev")
    schema = os.environ.get("DBT_SCHEMA", "lodestar")
    return layer if target == "prod" else f"{schema}_{layer}"


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def _count_files(path: Path, pattern: str = "*.parquet") -> int:
    return len(list(path.rglob(pattern))) if path.exists() else 0


def collect() -> dict:
    config = get_config()
    metrics: dict = {"target": config.target}

    # --- landing zone -----------------------------------------------------
    landing = Path(config.landing_uri)
    metrics["landing_zone"] = {
        "objects": _count_files(landing),
        "partitions": len(list(landing.glob("raw/*/ingest_date=*"))),
        "bytes": _dir_size(landing),
        "source_bytes": _dir_size(config.source_dir),
    }

    if not config.duckdb_path.exists():
        metrics["warehouse"] = {"error": "no warehouse built yet"}
        return metrics

    con = duckdb.connect(str(config.duckdb_path), read_only=True)
    try:
        raw = config.raw_dataset
        marts = _schema("marts")
        staging = _schema("staging")

        def scalar(sql: str, default=None):
            try:
                return con.execute(sql).fetchone()[0]
            except duckdb.Error:
                return default

        raw_tables = con.execute(
            "select table_name from information_schema.tables where table_schema = ? "
            "order by table_name",
            [raw],
        ).fetchall()
        raw_counts = {t[0]: scalar(f'select count(*) from "{raw}"."{t[0]}"', 0) for t in raw_tables}

        mart_tables = con.execute(
            "select table_name from information_schema.tables where table_schema = ? "
            "order by table_name",
            [marts],
        ).fetchall()
        mart_counts = {
            t[0]: scalar(f'select count(*) from "{marts}"."{t[0]}"', 0) for t in mart_tables
        }

        metrics["warehouse"] = {
            "file_bytes": config.duckdb_path.stat().st_size,
            "raw_row_counts": raw_counts,
            "raw_total_rows": sum(raw_counts.values()),
            "mart_row_counts": mart_counts,
            "mart_total_rows": sum(mart_counts.values()),
            "staging_models": len(
                con.execute(
                    "select table_name from information_schema.tables where table_schema = ?",
                    [staging],
                ).fetchall()
            ),
        }

        # --- business measures -------------------------------------------
        metrics["business"] = {
            "orders": scalar(f'select count(*) from "{marts}"."fct_orders"', 0),
            "order_lines": scalar(f'select count(*) from "{marts}"."fct_order_items"', 0),
            "gross_revenue": float(
                scalar(f'select sum(item_total) from "{marts}"."fct_order_items"', 0) or 0
            ),
            "date_range": [
                str(scalar(f'select min(purchased_date) from "{marts}"."fct_orders"')),
                str(scalar(f'select max(purchased_date) from "{marts}"."fct_orders"')),
            ],
            "late_delivery_rate": float(
                scalar(
                    f'select 1.0 * sum(case when is_late then 1 else 0 end) / count(*) '
                    f'from "{marts}"."fct_orders" where is_delivered',
                    0,
                )
                or 0
            ),
            "scd2_versions": scalar(f'select count(*) from "{marts}"."dim_sellers_history"', 0),
            "scd2_sellers": scalar(
                f'select count(distinct seller_key) from "{marts}"."dim_sellers_history"', 0
            ),
        }
    finally:
        con.close()

    # --- test counts, straight from the dbt manifest ---------------------
    manifest_path = REPO_ROOT / "dbt_project" / "target" / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        nodes = manifest.get("nodes", {})
        tests = [n for n in nodes.values() if n.get("resource_type") == "test"]
        models = [n for n in nodes.values() if n.get("resource_type") == "model"]
        metrics["dbt"] = {
            "models": len(models),
            "models_by_layer": {
                layer: sum(1 for m in models if f"/{layer}/" in m.get("original_file_path", ""))
                for layer in ("staging", "intermediate", "marts")
            },
            "tests": len(tests),
            "generic_tests": sum(1 for t in tests if t.get("test_metadata")),
            "singular_tests": sum(1 for t in tests if not t.get("test_metadata")),
            "sources": len(manifest.get("sources", {})),
        }

    # --- last run's timings ----------------------------------------------
    run_results = REPO_ROOT / "dbt_project" / "target" / "run_results.json"
    if run_results.exists():
        results = json.loads(run_results.read_text())
        metrics["last_dbt_run"] = {
            "elapsed_seconds": round(results.get("elapsed_time", 0), 2),
            "nodes": len(results.get("results", [])),
            "slowest": sorted(
                (
                    {
                        "node": r["unique_id"].split(".")[-1],
                        "seconds": round(r.get("execution_time", 0), 3),
                    }
                    for r in results.get("results", [])
                ),
                key=lambda r: r["seconds"],
                reverse=True,
            )[:5],
        }

    # --- data quality report ---------------------------------------------
    latest = REPO_ROOT / "quality" / "results" / "latest.json"
    if latest.exists():
        report = json.loads(latest.read_text())
        metrics["great_expectations"] = {
            "suites": len(report["suites"]),
            "expectations": sum(s["evaluated"] for s in report["suites"]),
            "passed": sum(s["passed"] for s in report["suites"]),
            "rows_validated": sum(s["rows_validated"] for s in report["suites"]),
        }

    return metrics


def _mib(n: int) -> str:
    return f"{n / 1024 / 1024:.1f} MiB"


def render(metrics: dict) -> None:
    print("\nLodestar — pipeline metrics")
    print("=" * 68)

    lz = metrics["landing_zone"]
    print("\nLanding zone")
    print(f"  source CSVs            {_mib(lz['source_bytes']):>14}")
    print(f"  parquet objects        {lz['objects']:>14,}")
    print(f"  date partitions        {lz['partitions']:>14,}")
    print(f"  landed size            {_mib(lz['bytes']):>14}")
    if lz["source_bytes"]:
        ratio = lz["source_bytes"] / max(lz["bytes"], 1)
        print(f"  compression vs CSV     {ratio:>13.2f}x")

    wh = metrics.get("warehouse", {})
    if "error" in wh:
        print(f"\nWarehouse: {wh['error']}")
        return

    print("\nWarehouse")
    print(f"  raw rows               {wh['raw_total_rows']:>14,}")
    print(f"  mart rows              {wh['mart_total_rows']:>14,}")
    print(f"  duckdb file            {_mib(wh['file_bytes']):>14}")

    biz = metrics.get("business", {})
    if biz:
        print("\nBusiness")
        print(f"  orders                 {biz['orders']:>14,}")
        print(f"  order lines            {biz['order_lines']:>14,}")
        print(f"  gross revenue          {biz['gross_revenue']:>14,.2f}")
        print(f"  date range             {biz['date_range'][0]} .. {biz['date_range'][1]}")
        print(f"  late delivery rate     {biz['late_delivery_rate'] * 100:>13.2f}%")
        print(f"  SCD2 versions/sellers  {biz['scd2_versions']:>7,} / {biz['scd2_sellers']:,}")

    dbt = metrics.get("dbt", {})
    if dbt:
        by_layer = dbt["models_by_layer"]
        print("\ndbt")
        print(
            f"  models                 {dbt['models']:>14}  "
            f"(staging {by_layer['staging']}, intermediate {by_layer['intermediate']}, "
            f"marts {by_layer['marts']})"
        )
        print(
            f"  tests                  {dbt['tests']:>14}  "
            f"({dbt['generic_tests']} generic, {dbt['singular_tests']} singular)"
        )

    run = metrics.get("last_dbt_run", {})
    if run:
        print(f"\nLast dbt run           {run['elapsed_seconds']:>14.2f}s "
              f"across {run['nodes']} nodes")
        for slow in run["slowest"][:3]:
            print(f"    {slow['node']:<38} {slow['seconds']:>7.3f}s")

    ge = metrics.get("great_expectations", {})
    if ge:
        print("\nGreat Expectations")
        print(f"  suites / expectations  {ge['suites']:>7} / {ge['passed']}/{ge['expectations']}")
        print(f"  rows validated         {ge['rows_validated']:>14,}")

    print("\n" + "=" * 68)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Emit raw JSON instead of a table.")
    args = parser.parse_args()

    metrics = collect()
    if args.json:
        print(json.dumps(metrics, indent=2, default=str))
    else:
        render(metrics)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
