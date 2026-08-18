# Metrics and how they were measured

Every figure here is reproducible. Nothing is estimated, and the method is
stated so the numbers can be argued with.

**Measurement environment:** Apple Silicon laptop, 8 cores, 8 GB RAM, macOS.
DuckDB 1.5.5 target, dbt 1.12.4, Python 3.13. Single-user, warm page cache.

Regenerate everything with:

```bash
make clean && make demo && make metrics
```

---

## Volume

| Measure | Value | Method |
|---|---|---|
| Orders | 100,000 | `count(*)` on `fct_orders` |
| Order lines | 151,040 | `count(*)` on `fct_order_items` |
| Total rows through the pipeline | 595,219 | sum of raw-layer row counts |
| Mart rows | 469,881 | sum of mart row counts |
| Landing-zone partitions | 1,477 | `raw/*/ingest_date=*` directory count |
| Landing-zone objects | 3,714 | Parquet file count |
| Source CSV size | 62.8 MiB | recursive byte count of `data/source/` |
| Landed Parquet size | 35.7 MiB | recursive byte count of `data/landing/` |
| **Compression ratio** | **1.76×** | CSV bytes ÷ Parquet bytes |
| Simulated history | 2023-01-01 → 2024-12-30 | min/max `purchased_date` |

The compression figure is honest but modest, and worth understanding: the
landing zone stores *more* than the CSVs do — four lineage columns per row, plus
1,477 separate files each carrying its own Parquet footer and dictionary. ZSTD
still nets a 1.76× win. On a single large file the ratio would be considerably
better; the partition count is a deliberate trade of bytes for pruning.

## Speed

Wall-clock, measured with `/usr/bin/time -p`.

| Stage | Time |
|---|---|
| Generate 100k-order synthetic export | 8.9 s |
| Extract full history → 1,477 partitions | 4.5 s |
| Load landing zone → raw layer | 2.3 s |
| `dbt build --full-refresh` (179 nodes) | 10.5 s |
| `dbt build` incremental (179 nodes) | 6.4 s |
| **End-to-end, empty directory → tested warehouse** | **~26 s** |

### Incremental vs full refresh

The comparison that matters is on the fact models, since those are the ones the
daily DAG rebuilds:

| Model | `--full-refresh` | incremental | Speedup |
|---|---|---|---|
| `fct_order_items` | 0.79 s | 0.44 s | 1.80× |
| `fct_orders` | 0.97 s | 0.43 s | 2.26× |
| **Combined** | **1.76 s** | **0.87 s** | **2.02×** |

At 100k orders the absolute saving is under a second. The point is the *shape*
of the curve: the full refresh scales with total history while the incremental
run scales with the lookback window, so the gap widens with every day of data
retained. The mechanism is what is being demonstrated, not the second saved.

## Query performance

Median of 12 runs after a warm-up, comparing dashboard queries written against
the lowest-grain fact vs the pre-aggregate `agg_daily_performance`.

| Dashboard query | `fct_order_items` (151k rows) | `agg_daily_performance` (70k rows) | Speedup |
|---|---|---|---|
| Monthly revenue trend | 6.78 ms | 3.51 ms | 1.93× |
| Revenue by region × category | 17.58 ms | 4.79 ms | 3.67× |
| Top 10 states by revenue, 2024 | 5.12 ms | 2.83 ms | 1.81× |
| **Total** | **29.48 ms** | **11.13 ms** | **2.65×** |

Read this as a demonstration of the technique, not a claim about production
scale. DuckDB is a vectorised columnar engine on a small local dataset, so both
sides are fast in absolute terms — the aggregate wins by scanning 70k rows
instead of 151k and by pre-computing the grouping. The same model on BigQuery
gains additionally from partition pruning on `purchased_date` and clustering on
`customer_state` / `category_name`, which cut *bytes billed* rather than
milliseconds. That is the number worth quoting on a cloud warehouse, and it is
not measurable locally.

## Testing

| Measure | Value |
|---|---|
| dbt tests | **155** — 148 generic, 7 singular |
| dbt test pass rate | 100% (154 pass, 1 intentional threshold warning) |
| Models covered | 23 of 23 |
| Great Expectations expectations | **25** across 3 suites |
| Rows validated by GE | 320,865 |
| Python unit tests | 16 |
| Airflow DAGs import-validated | 2 (18 tasks total) |

The single warning is `assert_order_completeness_within_tolerance` reporting 419
orders with known upstream messiness — configured to warn above 0 and fail above
1,500. It is working as designed; see the test's own comment for why a threshold
beats a binary assertion here.

## Data quality outcomes

| Measure | Value | Meaning |
|---|---|---|
| Payment reconciliation variance | 0.00 on every complete order | payments settle order totals exactly |
| Fact roll-up consistency | exact | `sum(fct_order_items)` == `fct_orders` totals |
| Aggregate reconciliation | exact | `agg_daily_performance` == its source fact |
| Orders missing payment rows | 321 (0.32%) | known upstream quirk, flagged not dropped |
| Orders `delivered` with no timestamp | 98 (0.10%) | known upstream quirk, flagged not dropped |
| Late delivery rate | **7.25%** of delivered orders | the SLA headline |

## SCD Type 2

After `make drift-demo` relocates 5% of sellers:

| Measure | Value |
|---|---|
| Total versions | 3,281 |
| Distinct sellers | 3,125 |
| Sellers with >1 version | 156 |
| Open versions | 3,125 (exactly one per seller) |
| Overlapping validity windows | 0 |

The last two rows are asserted by `assert_scd2_history_is_well_formed`, not just
observed. They are what makes a point-in-time join safe: without exactly one
open version and zero overlaps, joining a fact to history silently returns two
rows and doubles whatever it touched.

## Orchestration, verified on the running stack

The DAGs were executed on the containerised Airflow stack (Airflow 2.10.5,
LocalExecutor, Postgres metadata DB), not just import-validated.

| Check | Result |
|---|---|
| DAGs loaded by a real scheduler | 2, **0 import errors** |
| `lodestar_initial_load` | **7/7 tasks succeeded**, built the 80 MB warehouse from an empty directory |
| `lodestar_elt` via `airflow dags test` | **11/11 tasks succeeded** |
| `lodestar_elt` via the **scheduler** (unpaused, triggered) | **11/11 tasks succeeded** |
| `dbt test` inside Airflow | **PASS=154 WARN=1 ERROR=0** — identical to the host |
| Great Expectations inside Airflow | 25/25 expectations, 100,000 orders / 151,040 lines |
| Webserver | HTTP 200, scheduler heartbeat healthy |

Per-task timings from the real scheduled run:

| Task | Seconds |
|---|---|
| `extract_to_landing_zone` | 1.55 |
| `load_to_warehouse` | 1.36 |
| `dbt_source_freshness` | 2.47 |
| `dbt_run` | 5.09 |
| `dbt_snapshot` | 2.49 |
| `dbt_test` | 7.56 |
| `great_expectations` | 4.47 |
| `dbt_docs_generate` | 3.93 |
| **End-to-end** | **~29 s** |

`publish_run_metrics` reported 24.99 s across the tasks that had finished when
it ran, and listed the two still in flight under `tasks_without_timing` rather
than scoring them as zero.

## Infrastructure

| Measure | Value |
|---|---|
| Terraform-managed resources | ~28 |
| Buckets | 2 (landing, artefacts) |
| BigQuery datasets | 7 |
| Service accounts | 2 |
| IAM bindings | 11 |
| Manual console steps to reproduce the environment | **0** |
| Time to provision from scratch | ~3 min (`terraform apply`, Composer disabled) |

---

## Turning these into resume bullets

Numbers are only useful attached to a decision. Some framings that stay honest:

> Built an ELT platform ingesting 595k rows across 9 source tables into a
> partitioned lakehouse landing zone and a BigQuery star schema, orchestrated
> with Airflow and provisioned entirely via Terraform (~28 resources, zero
> console steps).

> Modelled a dimensional warehouse in dbt — 23 models across staging /
> intermediate / mart layers with 155 tests including cross-grain
> reconciliation — and made it dual-target (DuckDB / BigQuery) so every pull
> request builds and tests the full graph in ~10s at zero cloud cost.

> Cut fact-table rebuild time ~2× by converting full-refresh models to
> incremental with a 7-day late-arriving-data lookback, and cut dashboard query
> time 2.65× by introducing a pre-aggregated mart — with a reconciliation test
> asserting the aggregate never drifts from its source fact.

> Implemented SCD Type 2 history with dbt snapshots and structural invariant
> tests (exactly one open version per key, zero overlapping validity windows),
> making point-in-time joins provably safe.

Avoid quoting the raw millisecond figures as though they were production
numbers — they are laptop-scale. Quote the ratio and the mechanism.
