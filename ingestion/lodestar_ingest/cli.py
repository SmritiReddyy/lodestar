"""Command-line entry point: `lodestar-ingest <command>`.

The Airflow DAG shells out to these same commands, so anything the scheduler
does can be reproduced by hand from a terminal.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta

from .config import Config, get_config
from .extract import extract_all
from .generate import apply_seller_drift, ensure_source_data
from .sources import SOURCE_TABLES, get_table
from .storage import get_landing_zone
from .warehouse import get_warehouse, load_tables


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def _resolve_tables(names: list[str] | None):
    if not names:
        return list(SOURCE_TABLES)
    return [get_table(n) for n in names]


def _default_window(ingest_date: str) -> tuple[str, str]:
    """Daily window [D, D+1) for incremental tables."""
    day = datetime.strptime(ingest_date, "%Y-%m-%d").date()
    return f"{day} 00:00:00", f"{day + timedelta(days=1)} 00:00:00"


def cmd_seed(args: argparse.Namespace, config: Config) -> int:
    generated = ensure_source_data(
        config.source_dir,
        n_orders=args.orders or config.synthetic_orders,
        seed=args.seed or config.synthetic_seed,
    )
    if generated:
        print(f"Generated synthetic Olist source data in {config.source_dir}")
    else:
        print(f"Source data already present in {config.source_dir}; nothing to do")

    if args.drift:
        moved = apply_seller_drift(config.source_dir, fraction=args.drift_fraction)
        print(
            f"Relocated {moved} seller(s) in place. Re-run `lodestar-ingest run` "
            "then `dbt snapshot` to open new Type 2 versions."
        )
    for table in SOURCE_TABLES:
        path = config.source_dir / table.filename
        size = path.stat().st_size if path.exists() else 0
        print(f"  {table.name:<28} {table.filename:<38} {size / 1024:>9.1f} KiB")
    return 0


def cmd_extract(args: argparse.Namespace, config: Config) -> int:
    zone = get_landing_zone(config.landing_uri)
    history = getattr(args, "full_history", False)
    window_start, window_end = None, None
    if not history:
        window_start = args.window_start
        window_end = args.window_end
        if window_start is None:
            window_start, window_end = _default_window(args.ingest_date)

    results = extract_all(
        config=config,
        zone=zone,
        ingest_date=args.ingest_date,
        tables=args.tables,
        window_start=window_start,
        window_end=window_end,
        history=history,
    )
    total = sum(r.rows for r in results)
    parts = sum(r.partitions for r in results)
    print(
        f"Extracted {total:,} rows across {len(results)} table(s) / {parts} partition(s) "
        f"into {config.landing_uri}"
    )
    for r in results:
        print(
            f"  {r.table:<28} {r.rows:>9,} rows  {r.partitions:>4} part  "
            f"{r.bytes_written / 1024:>9.1f} KiB"
        )
    if args.json:
        print(json.dumps([r.__dict__ for r in results], indent=2))
    return 0


def cmd_load(args: argparse.Namespace, config: Config) -> int:
    zone = get_landing_zone(config.landing_uri)
    tables = _resolve_tables(args.tables)
    results = load_tables(
        tables,
        config=config,
        zone=zone,
        ingest_date=args.ingest_date,
        all_partitions=getattr(args, "full_history", False),
    )
    print(f"Loaded {len(results)} table(s) into {config.raw_dataset}")
    for r in results:
        print(f"  {r.table:<28} {r.rows_loaded:>9,} loaded {r.rows_total:>10,} total  ({r.mode})")
    return 0


def cmd_run(args: argparse.Namespace, config: Config) -> int:
    """seed (if needed) -> extract -> load, for one logical day."""
    ensure_source_data(config.source_dir, config.synthetic_orders, config.synthetic_seed)
    rc = cmd_extract(args, config)
    if rc:
        return rc
    return cmd_load(args, config)


def cmd_backfill(args: argparse.Namespace, config: Config) -> int:
    """Re-land and re-load a closed date range, one partition per day."""
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    if end < start:
        print("--end must not precede --start", file=sys.stderr)
        return 2

    ensure_source_data(config.source_dir, config.synthetic_orders, config.synthetic_seed)
    zone = get_landing_zone(config.landing_uri)
    warehouse = get_warehouse(config)
    tables = _resolve_tables(args.tables)
    try:
        day = start
        grand_total = 0
        while day <= end:
            ingest_date = day.isoformat()
            window_start, window_end = _default_window(ingest_date)
            results = extract_all(
                config=config,
                zone=zone,
                ingest_date=ingest_date,
                tables=args.tables,
                window_start=window_start,
                window_end=window_end,
            )
            load_tables(
                tables,
                config=config,
                zone=zone,
                ingest_date=ingest_date,
                warehouse=warehouse,
            )
            landed = sum(r.rows for r in results)
            grand_total += landed
            print(f"  {ingest_date}: {landed:,} rows")
            day += timedelta(days=1)
    finally:
        warehouse.close()
    print(f"Backfill complete: {grand_total:,} rows across {(end - start).days + 1} day(s)")
    return 0


def cmd_info(_args: argparse.Namespace, config: Config) -> int:
    print(
        json.dumps(
            {
                "target": config.target,
                "source_dir": str(config.source_dir),
                "landing_uri": config.landing_uri,
                "raw_dataset": config.raw_dataset,
                "duckdb_path": str(config.duckdb_path),
                "gcp_project": config.gcp_project,
                "tables": [t.name for t in SOURCE_TABLES],
            },
            indent=2,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lodestar-ingest", description="Lodestar extract/load pipeline"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_date(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--ingest-date",
            default=date.today().isoformat(),
            help="Logical partition date (YYYY-MM-DD). Defaults to today.",
        )
        p.add_argument("--tables", nargs="*", help="Subset of tables; default all.")

    p_seed = sub.add_parser("seed", help="Create synthetic source CSVs if absent")
    p_seed.add_argument("--orders", type=int, help="Number of orders to synthesise")
    p_seed.add_argument("--seed", type=int, help="RNG seed")
    p_seed.add_argument(
        "--drift",
        action="store_true",
        help="Relocate a sample of sellers in place, to exercise SCD Type 2 history.",
    )
    p_seed.add_argument(
        "--drift-fraction", type=float, default=0.05, help="Share of sellers to move."
    )
    p_seed.set_defaults(func=cmd_seed)

    p_extract = sub.add_parser("extract", help="Source CSVs -> landing zone Parquet")
    add_date(p_extract)
    p_extract.add_argument("--window-start", help="Inclusive watermark lower bound")
    p_extract.add_argument("--window-end", help="Exclusive watermark upper bound")
    p_extract.add_argument(
        "--full-history",
        action="store_true",
        help=(
            "Land every row, fanning incremental tables out into one partition "
            "per event day. Use for the initial history load."
        ),
    )
    p_extract.add_argument("--json", action="store_true", help="Emit results as JSON")
    p_extract.set_defaults(func=cmd_extract)

    p_load = sub.add_parser("load", help="Landing zone -> raw warehouse tables")
    add_date(p_load)
    p_load.add_argument(
        "--full-history",
        action="store_true",
        help="Read every landed partition and rebuild each raw table.",
    )
    p_load.set_defaults(func=cmd_load)

    p_run = sub.add_parser("run", help="seed + extract + load for one date")
    add_date(p_run)
    p_run.add_argument("--window-start")
    p_run.add_argument("--window-end")
    p_run.add_argument("--full-history", action="store_true")
    p_run.add_argument("--json", action="store_true")
    p_run.set_defaults(func=cmd_run)

    p_bf = sub.add_parser("backfill", help="Re-run a date range, one partition per day")
    p_bf.add_argument("--start", required=True)
    p_bf.add_argument("--end", required=True)
    p_bf.add_argument("--tables", nargs="*")
    p_bf.set_defaults(func=cmd_backfill)

    p_info = sub.add_parser("info", help="Print resolved configuration")
    p_info.set_defaults(func=cmd_info)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _setup_logging(args.verbose)
    try:
        return args.func(args, get_config())
    except (FileNotFoundError, ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
