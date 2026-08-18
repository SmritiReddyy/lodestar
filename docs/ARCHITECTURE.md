# Architecture

## Layer contract

Each layer has exactly one job. Keeping the boundaries strict is what makes the
model graph reviewable — you can tell where a bug lives from its symptom.

| Layer | Materialisation | Owns | Never does |
|---|---|---|---|
| **Landing zone** | Parquet on FS/GCS | Immutable, partitioned copy of the source | Any transformation |
| **Raw** | Warehouse tables | Typed load of the landing zone, plus lineage columns | Cleaning, renaming |
| **Staging** | Views | Rename, cast, trim, dedup, one model per source table | Joins across sources |
| **Intermediate** | Views | Joins, business logic, grain changes | Being consumed by BI |
| **Marts** | Tables | The dimensional model exposed to consumers | Referencing sources directly |

Nothing skips a layer. `fct_orders` cannot read `source('olist', ...)`, and BI
cannot read `intermediate`.

## Why a landing zone at all

The raw warehouse tables could be loaded straight from the source. The landing
zone exists because it makes the warehouse *rebuildable*: every table can be
dropped and reconstructed from objects in the bucket, at any historical date,
without re-reading the source system.

Its layout is the contract:

```
raw/<table>/ingest_date=YYYY-MM-DD/<table>-<run_id>.parquet
```

`ingest_date` is both the directory key and a real column inside every file, so
a reader recovers the partition value whether or not it understands Hive path
conventions. BigQuery reads the column; DuckDB reads either.

## Idempotency

The property everything else rests on: **loading `ingest_date=D` twice produces
the same state as loading it once.**

Three mechanisms enforce it:

1. **Extract** deletes the target partition prefix before writing it, so a
   re-run replaces rather than appends.
2. **Load** is delete-then-insert keyed on `ingest_date` for incremental
   tables, and full replace for snapshot dimensions.
3. **Staging** applies a defensive `qualify row_number() over (partition by
   <pk> order by _ingested_at desc) = 1`, so even a duplicate that somehow
   reaches the raw layer cannot reach a mart.

`tests/test_ingestion.py::test_daily_load_is_idempotent` runs the same day three
times and asserts row counts and duplicate counts do not move.

## Incremental strategy

Both facts are incremental on `purchased_date` with a **7-day lookback**:

```sql
{% if is_incremental() %}
    where purchased_date >= (
        select dateadd(day, -7, coalesce(max(purchased_date), '1900-01-01'))
        from {{ this }}
    )
{% endif %}
```

The lookback is the interesting part. Reading only "since the last max date"
would be cheaper, and wrong: payments and reviews arrive days after the order
is placed, so an order's row keeps changing after its purchase date passes.
Seven days is wide enough to catch that and narrow enough to stay cheap.

`unique_key` lets each adapter pick its native strategy — `merge` on BigQuery,
`delete+insert` on DuckDB — without the model knowing which.

## Cross-warehouse portability

Two mechanisms, both in `macros/`:

**`adapter.dispatch`** for dialect gaps. BigQuery has `INITCAP`; DuckDB does
not. `title_case()` dispatches to `duckdb__title_case` or
`default__title_case`, so models call one macro and never branch.

**Config helpers** for physical layout. `partition_by` takes a dict on BigQuery
and is rejected outright by DuckDB. `partition_config()` and `cluster_config()`
return the config on BigQuery and `none` elsewhere, so partitioning and
clustering are applied where they mean something and silently skipped where
they do not.

Everything else stays ANSI: `qualify`, `extract(... from ...)`, and dbt's
cross-database `datediff` / `dateadd` / `date_trunc` / `type_numeric` macros.
Day-of-week goes through `dbt_date.day_of_week(..., isoweek=True)` because
BigQuery numbers Sunday as 1 and DuckDB as 0.

## Schema routing

`macros/generate_schema_name.sql`:

- `prod` → bare layer names: `staging`, `marts`
- everything else → prefixed: `lodestar_marts`, `ci_pr_142_marts`

That prefix is what lets each pull request build the entire graph into its own
namespace, and lets that namespace be dropped in one statement when the PR
closes.

## Data quality: three tiers

**Source tests** run against the raw layer before any modelling, so a bad load
is caught before it propagates. Plus `dbt source freshness` to detect a pipeline
that has silently stopped delivering.

**Model tests** — 148 generic (unique, not_null, relationships, accepted_values,
`dbt_expectations` range checks) and 7 singular tests encoding invariants that
cannot be expressed declaratively:

| Test | Invariant |
|---|---|
| `assert_payments_reconcile_to_orders` | payments settle the order total |
| `assert_order_items_roll_up_to_orders` | summing the line fact reproduces the order fact — catches fan-out |
| `assert_agg_reconciles_to_fact` | the BI aggregate agrees with its source |
| `assert_no_future_dated_orders` | no future purchases, no delivery before purchase |
| `assert_delivery_sla_is_plausible` | the SLA metric itself is in a sane range |
| `assert_scd2_history_is_well_formed` | one open version per key, no overlaps |
| `assert_order_completeness_within_tolerance` | known messiness stays within bounds (warn/error thresholds) |

`store_failures: true` writes offending rows to a table, so a failure can be
inspected rather than merely counted.

**Great Expectations** owns distribution and volume — the failures that pass
every row-level test. See `quality/suites.py`.

## Orchestration

```
extract → load → source_freshness → dbt run → dbt snapshot
   → dbt test → great_expectations → dbt docs → end
                        └→ publish_run_metrics (ALL_DONE)
```

- `catchup=False`, `max_active_runs=1` — turning the DAG on does not queue a
  year of runs; backfills are explicit.
- Retries are exponential with a 20-minute cap, covering transient warehouse
  and network failures without hammering a warehouse under load.
- `dbt test` and the GE task fail the DAG. A quality regression stops the line
  rather than reaching the BI layer.
- `publish_run_metrics` uses `TriggerRule.ALL_DONE`, so a *failed* run still
  reports how far it got and how long that took.
- Every bash task starts with `set -euo pipefail`. Without it a failing command
  inside a pipeline is swallowed and the task reports success.

## Infrastructure

Terraform manages ~28 resources. Notable choices:

- **State in GCS**, so the environment is reproducible from any machine and two
  applies cannot race. The state bucket itself is deliberately unmanaged — a
  backend cannot create the bucket that stores its own state.
- **Uniform bucket-level access** and enforced public access prevention: all
  permissions come from IAM, with no per-object ACLs to drift.
- **Lifecycle rules** move landing objects to Nearline at 30 days and delete at
  365. That retention window is the real bound on how far back a rebuild can go.
- **Two service accounts** (pipeline, BI) with dataset-scoped bindings. The BI
  identity has no binding at all for `raw`/`staging`/`intermediate`.
- **Composer off by default** — see the comment at the top of `composer.tf`.
