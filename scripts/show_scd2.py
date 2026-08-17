#!/usr/bin/env python
"""Print the Type 2 history that `make drift-demo` just produced.

Exists so the SCD2 demonstration ends in visible evidence rather than a
"snapshot completed" log line.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))

import duckdb  # noqa: E402

from lodestar_ingest.config import get_config  # noqa: E402


def mart_schema() -> str:
    target = os.environ.get("DBT_TARGET", "dev")
    schema = os.environ.get("DBT_SCHEMA", "lodestar")
    return "marts" if target == "prod" else f"{schema}_marts"


def main() -> int:
    config = get_config()
    if config.is_gcp:
        print("This helper reads the local DuckDB warehouse only.", file=sys.stderr)
        return 2
    if not config.duckdb_path.exists():
        print(f"No warehouse at {config.duckdb_path}. Run `make demo` first.", file=sys.stderr)
        return 1

    schema = mart_schema()
    con = duckdb.connect(str(config.duckdb_path), read_only=True)
    try:
        totals = con.execute(f"""
            select
                count(*)                                        as versions,
                count(distinct seller_key)                       as sellers,
                sum(case when is_current then 1 else 0 end)      as current_versions,
                sum(case when version_number > 1 then 1 else 0 end) as superseding_versions
            from "{schema}"."dim_sellers_history"
        """).fetchone()

        print("\nSCD Type 2 — dim_sellers_history")
        print("=" * 72)
        print(f"  total versions       {totals[0]:>8,}")
        print(f"  distinct sellers     {totals[1]:>8,}")
        print(f"  currently active     {totals[2]:>8,}")
        print(f"  changed at least once{totals[3]:>8,}")

        changed = con.execute(f"""
            select seller_key
            from "{schema}"."dim_sellers_history"
            group by seller_key
            having count(*) > 1
            order by seller_key
            limit 3
        """).fetchall()

        if not changed:
            print("\n  No seller has changed yet. Run `make drift-demo` to create history.")
            return 0

        for (seller_key,) in changed:
            print(f"\n  seller {seller_key}")
            rows = con.execute(f"""
                select version_number, seller_city, seller_state,
                       valid_from, valid_to, is_current
                from "{schema}"."dim_sellers_history"
                where seller_key = ?
                order by version_number
            """, [seller_key]).fetchall()
            for v, city, state, vfrom, vto, current in rows:
                window = f"{vfrom:%Y-%m-%d %H:%M:%S} -> " + (
                    f"{vto:%Y-%m-%d %H:%M:%S}" if vto else "(open)"
                )
                marker = "  <- current" if current else ""
                print(f"    v{v}  {city+', '+state:<28} {window}{marker}")

        print("\n" + "=" * 72)
        print("  Point-in-time join: match a fact on")
        print("    purchased_at >= valid_from and (purchased_at < valid_to or valid_to is null)")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
