# Runbook

What to do when something breaks, and how to do the routine operations.

## Routine operations

### Re-run a single day

Safe to repeat. The extract deletes the partition before rewriting it and the
load is delete-then-insert on `ingest_date`.

```bash
lodestar-ingest run --ingest-date 2024-06-15
cd dbt_project && dbt build
```

### Backfill a range

```bash
lodestar-ingest backfill --start 2024-06-01 --end 2024-06-30
cd dbt_project && dbt build --full-refresh --select fct_orders+ fct_order_items+
```

Use `--full-refresh` after a backfill that reaches further back than the
incremental lookback window (7 days). Without it the facts will not re-read the
older partitions you just rewrote.

In Airflow:

```bash
airflow dags backfill lodestar_elt --start-date 2024-06-01 --end-date 2024-06-30
```

### Rebuild everything from the landing zone

The warehouse is disposable; the landing zone is not.

```bash
rm -f data/lodestar.duckdb
lodestar-ingest load --full-history --ingest-date 2025-01-01
cd dbt_project && dbt build --full-refresh
```

### Rebuild the landing zone too

```bash
make clean && make demo
```

### Drop a CI schema after a PR closes

```sql
-- BigQuery
DROP SCHEMA `project.ci_pr_142_marts` CASCADE;
```

---

## Failure playbook

### `dbt test` fails

Every test has `store_failures: true`, so the offending rows are in a table
rather than gone.

```bash
cd dbt_project
dbt test --select fct_orders          # narrow it down
# then query the failure table:
#   <schema>_test_failures.<test_name>
```

Start with the singular tests — they encode invariants, so a failure there
usually means a real modelling bug rather than bad source data:

| Failing test | Most likely cause |
|---|---|
| `assert_order_items_roll_up_to_orders` | a join in `fct_orders` fanned out |
| `assert_payments_reconcile_to_orders` | payment splits lost or double counted in staging |
| `assert_agg_reconciles_to_fact` | grouping keys in `agg_daily_performance` drifted |
| `assert_scd2_history_is_well_formed` | snapshot ran twice concurrently, or `check_cols` changed |
| `assert_no_future_dated_orders` | timezone handling or a bad parse in extract |
| `assert_delivery_sla_is_plausible` | the SLA logic broke, not the business |

### `assert_order_completeness_within_tolerance` errors (not warns)

It warns above 0 and **errors above 1,500**. Crossing that means the count of
orders with missing payments or delivery timestamps jumped well beyond the known
upstream messiness. Check whether the extract dropped rows:

```sql
select ingest_date, count(*)
from lodestar_raw.order_payments
group by 1 order by 1 desc limit 10;
```

If the source genuinely changed, raise the ceiling in the test — deliberately,
with a commit message saying why.

### Great Expectations fails

```bash
python -m quality.run_checks          # prints the failing expectation + observed value
cat quality/results/latest.json       # full detail
```

| Failing expectation | Meaning |
|---|---|
| `expect_table_row_count_to_be_between` | a load truncated or doubled the table |
| `expect_column_mean_to_be_between` | a units or currency change upstream |
| `expect_column_quantile_values_to_be_between` | distribution shifted — often a new product category or pricing change |
| `expect_column_values_to_be_between` on `payment_variance` | reconciliation broke; also check the dbt test |

A distribution failure is not automatically a bug. If the business genuinely
changed, widen the bound in `quality/suites.py` and say so in the commit.

### The Airflow DAG will not appear

```bash
python tests/validate_dags.py         # import errors, cycles, missing owners
cd airflow && docker compose logs airflow-scheduler | tail -50
```

### An Airflow task fails

Every task shells out to the same CLI you can run by hand. Copy the command from
the task log and reproduce it locally — no Airflow needed.

### `dbt run` fails on an incremental model only

Usually a schema change the incremental strategy cannot absorb. `on_schema_change`
is `append_new_columns`, which handles additions but not renames or type changes.

```bash
dbt run --select fct_orders --full-refresh
```

### DuckDB "database is locked"

DuckDB allows one writer. Close the `duckdb` CLI, stop `dbt docs serve`, and
stop the Airflow scheduler if it is mid-run.

### Terraform wants to destroy every dataset

Check `bq_location`. It cannot change after creation — a different value makes
Terraform plan a replacement of every dataset. Revert the variable.

---

## Monitoring

**Is the pipeline still receiving data?**

```bash
cd dbt_project && dbt source freshness
```

Warns at 24 h, errors at 48 h since the newest `_ingested_at`.

**How long did recent runs take?**

`dbt_project/target/run_results.json` holds per-node timings for the last run.
`make metrics` summarises it, including the five slowest nodes.

On GCP, the `publish_run_metrics` task writes to
`lodestar_observability.pipeline_runs`, partitioned by `run_date` and clustered
on `dag_id`/`task_id` — so "is the DAG getting slower?" is a SQL question:

```sql
select run_date, task_id, avg(duration_seconds)
from `project.lodestar_observability.pipeline_runs`
where run_date >= current_date() - 30
group by 1, 2
order by 1 desc, 3 desc;
```

---

## Cost control (GCP)

- `maximum_bytes_billed` is set to 100 GB in the `prod` profile. A runaway query
  fails instead of billing.
- Landing objects move to Nearline at 30 days and are deleted at 365. That
  retention window bounds how far back a rebuild can reach — shorten it to save
  money, but know what you are giving up.
- `test_failures` tables expire after 30 days.
- **Composer is the expensive thing.** `enable_composer = false` by default. If
  you turn it on to demo a managed deployment, run `terraform destroy` after.
