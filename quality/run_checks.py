"""Run the Great Expectations suites against the built marts.

Usage:
    python -m quality.run_checks                    # local DuckDB
    LODESTAR_TARGET=gcp python -m quality.run_checks # BigQuery

Exits non-zero if any expectation fails, so Airflow and CI both treat a data
quality regression as a broken build rather than a log line — which is the
whole point of running it.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import great_expectations as gx
import pandas as pd
from great_expectations.data_context.types.base import ProgressBarsConfig

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

from lodestar_ingest.config import get_config  # noqa: E402

from .suites import SuiteSpec, build_suites, to_gx_suite  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = REPO_ROOT / "quality" / "results"


def _mart_schema(layer: str) -> str:
    """Mirror the routing in `macros/generate_schema_name.sql`.

    dbt's prod target writes bare layer names; every other target prefixes them
    with the target schema. The checks have to look in the same place dbt wrote.
    """
    dbt_target = os.environ.get("DBT_TARGET", "dev")
    dbt_schema = os.environ.get("DBT_SCHEMA", "lodestar" if dbt_target != "ci" else "ci")
    return layer if dbt_target == "prod" else f"{dbt_schema}_{layer}"


def _normalise_dtypes(frame: pd.DataFrame) -> pd.DataFrame:
    """Make the frame safe for GE's JSON result serializer.

    Warehouse drivers hand back numpy scalars (`numpy.bool_`, `Decimal`), which
    land in GE's `observed_value` and `partial_unexpected_list` and then fail to
    serialize. Coercing here keeps that plumbing detail out of the suites.
    """
    for column in frame.columns:
        kind = frame[column].dtype.kind
        if kind == "b":
            frame[column] = (
                frame[column].astype("object").map(lambda v: None if pd.isna(v) else bool(v))
            )
        elif kind == "O" and len(frame) and isinstance(frame[column].iloc[0], Decimal):
            frame[column] = frame[column].astype("float64")
    return frame


def fetch_dataframe(spec: SuiteSpec) -> pd.DataFrame:
    """Pull just the columns a suite needs out of the warehouse."""
    config = get_config()
    schema = _mart_schema(spec.schema)
    columns = ", ".join(f'"{c}"' for c in spec.columns) if spec.columns else "*"
    where = f" where {spec.where}" if spec.where else ""

    if config.is_gcp:
        from google.cloud import bigquery  # noqa: PLC0415

        client = bigquery.Client(project=config.gcp_project)
        sql = f"select {columns} from `{config.gcp_project}.{schema}.{spec.table}`{where}"
        return _normalise_dtypes(client.query(sql).to_dataframe())

    import duckdb  # noqa: PLC0415

    con = duckdb.connect(str(config.duckdb_path), read_only=True)
    try:
        sql = f'select {columns} from "{schema}"."{spec.table}"{where}'
        return _normalise_dtypes(con.execute(sql).df())
    finally:
        con.close()


def run_suite(context, spec: SuiteSpec) -> dict:
    """Validate one table, returning a plain-dict summary.

    Validates the batch directly rather than going through a
    ValidationDefinition. The latter persists results through GE's store, whose
    JSON serializer rejects the `numpy.bool_` values pandas hands back under
    numpy >= 2. We only need the result object, not GE's own history, so the
    store is simply not in the path.
    """
    frame = fetch_dataframe(spec)

    data_source = context.data_sources.add_or_update_pandas(name=f"lodestar_{spec.name}")
    asset = data_source.add_dataframe_asset(name=spec.table)
    batch_definition = asset.add_batch_definition_whole_dataframe("all_rows")

    batch = batch_definition.get_batch(batch_parameters={"dataframe": frame})
    result = batch.validate(to_gx_suite(spec))

    failures = [
        {
            "expectation": r["expectation_config"]["type"],
            "kwargs": {
                k: v for k, v in r["expectation_config"]["kwargs"].items() if k not in {"batch_id"}
            },
            "observed": r["result"].get("observed_value"),
            "unexpected_count": r["result"].get("unexpected_count"),
            "unexpected_percent": r["result"].get("unexpected_percent"),
        }
        for r in result["results"]
        if not r["success"]
    ]

    stats = result["statistics"]
    return {
        "suite": spec.name,
        "table": f"{_mart_schema(spec.schema)}.{spec.table}",
        "rows_validated": len(frame),
        "success": bool(result["success"]),
        "evaluated": stats["evaluated_expectations"],
        "passed": stats["successful_expectations"],
        "failed": stats["unsuccessful_expectations"],
        "failures": failures,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Lodestar data quality checks")
    parser.add_argument(
        "--scale",
        type=int,
        default=int(os.environ.get("LODESTAR_SYNTHETIC_ORDERS", "100000")),
        help="Expected order count; volume bounds are derived from it.",
    )
    parser.add_argument("--suite", nargs="*", help="Only run these suites.")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=RESULTS_DIR,
        help="Where to write the JSON report.",
    )
    args = parser.parse_args(argv)

    specs = build_suites(scale=args.scale)
    if args.suite:
        wanted = set(args.suite)
        specs = [s for s in specs if s.name in wanted]
        if not specs:
            print(f"error: no suites matched {sorted(wanted)}", file=sys.stderr)
            return 2

    context = gx.get_context(mode="ephemeral")
    # Progress bars are noise in a CI log.
    context.variables.progress_bars = ProgressBarsConfig(globally=False, metric_calculations=False)
    reports = [run_suite(context, spec) for spec in specs]

    args.results_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report = {
        "generated_at": stamp,
        "target": get_config().target,
        "suites": reports,
    }
    out_path = args.results_dir / f"validation-{stamp}.json"
    out_path.write_text(json.dumps(report, indent=2, default=str))
    (args.results_dir / "latest.json").write_text(json.dumps(report, indent=2, default=str))

    # --- human-readable summary ------------------------------------------
    total_failed = 0
    print(f"\nGreat Expectations — {len(reports)} suite(s)\n" + "=" * 62)
    for r in reports:
        status = "PASS" if r["success"] else "FAIL"
        print(
            f"{status:>4}  {r['suite']:<24} {r['passed']}/{r['evaluated']} expectations  "
            f"({r['rows_validated']:,} rows)"
        )
        for f in r["failures"]:
            total_failed += 1
            observed = f["observed"]
            print(f"        - {f['expectation']}({_describe(f['kwargs'])})")
            print(f"          observed: {observed}")
    print("=" * 62)
    print(f"Report: {out_path}")

    if total_failed:
        print(f"\n{total_failed} expectation(s) failed.", file=sys.stderr)
        return 1
    print("\nAll expectations passed.")
    return 0


def _describe(kwargs: dict) -> str:
    parts = [f"{k}={v!r}" for k, v in kwargs.items() if v is not None]
    return ", ".join(parts)


if __name__ == "__main__":
    raise SystemExit(main())
