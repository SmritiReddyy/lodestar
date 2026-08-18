# Lodestar — ELT Analytics Platform

A production-shaped modern data stack: raw e-commerce data landed in a
partitioned object store, modelled into a dimensional warehouse with dbt,
tested at every layer, orchestrated by Airflow, and provisioned with Terraform.

**It runs end to end on a laptop with no cloud account and no credentials.**
The same code targets GCS + BigQuery when you point it there.

```bash
git clone <this repo> && cd lodestar
make demo
```

That takes about 30 seconds and produces a fully built, fully tested warehouse.

---

## What it actually does

```
 ┌──────────────┐   extract    ┌────────────────────────┐   load    ┌──────────────┐
 │ Source CSVs  │ ───────────► │  Landing zone (Parquet)│ ────────► │  Raw layer   │
 │ (Olist)      │              │  ingest_date=YYYY-MM-DD│           │              │
 └──────────────┘              └────────────────────────┘           └──────┬───────┘
                                  local FS  ⇄  GCS                         │
                                                                           ▼
   ┌───────────────────────────────────────────────────────────┐    ┌─────────────┐
   │  dbt:  staging (9) ──► intermediate (5) ──► marts (9)     │ ◄──│  DuckDB or  │
   │        views            views                tables       │    │  BigQuery   │
   └───────────────────────────┬───────────────────────────────┘    └─────────────┘
                               │
         ┌─────────────────────┼──────────────────────┐
         ▼                     ▼                      ▼
   155 dbt tests      25 GE expectations       BI / dashboards
   (schema + singular) (distribution + volume)  (agg_daily_performance)

   All of it orchestrated by Airflow; all infra defined in Terraform.
```

### The dimensional model

Marts are a star schema at two grains, with conformed dimensions:

| Table | Grain | Notes |
|---|---|---|
| `fct_orders` | one row per order | incremental, 7-day lookback |
| `fct_order_items` | one row per order line | incremental, lowest grain |
| `dim_customers` | one row per `customer_id` | person-level lifetime measures |
| `dim_products` | one row per product | sales rollups, weight class |
| `dim_sellers` | one row per seller | fulfilment performance |
| `dim_sellers_history` | one row per seller *version* | **SCD Type 2** |
| `dim_geography` | one row per zip prefix | centroid + modal city |
| `dim_date` | one row per day | conformed calendar |
| `agg_daily_performance` | date × state × category | pre-aggregated for BI |

---

## Measured results

Every number below comes from `make metrics`, which reads the warehouse and the
dbt/GE artefacts directly. Regenerate them any time; nothing here is estimated.

**Volume**
| | |
|---|---|
| Orders / order lines | 100,000 / 151,040 |
| Total rows through the pipeline | 595,219 |
| Date partitions in the landing zone | 1,477 across 3,714 Parquet objects |
| Landed size vs source CSV | 35.7 MiB vs 62.8 MiB (**1.76× compression**) |
| Simulated history | 2023-01-01 → 2024-12-30 |

**Speed** (8-core laptop, DuckDB target)
| Stage | Time |
|---|---|
| Extract full history (fan-out into 1,477 partitions) | 4.5 s |
| Load to raw layer | 2.3 s |
| `dbt build --full-refresh` — 179 nodes | 10.5 s |
| `dbt build` incremental — 179 nodes | 6.4 s |
| Fact models: full refresh vs incremental | 1.76 s → 0.87 s (**2.0× faster**) |

**Query performance** — median of 12 runs, dashboard queries against the
lowest-grain fact vs the pre-aggregate:

| Dashboard query | `fct_order_items` | `agg_daily_performance` | Speedup |
|---|---|---|---|
| Monthly revenue trend | 6.78 ms | 3.51 ms | 1.93× |
| Revenue by region × category | 17.58 ms | 4.79 ms | 3.67× |
| Top 10 states by revenue, 2024 | 5.12 ms | 2.83 ms | 1.81× |
| **Total** | **29.48 ms** | **11.13 ms** | **2.65×** |

**Testing**
| | |
|---|---|
| dbt tests | **155** (148 generic, 7 singular) — 100% pass |
| Great Expectations | **25 expectations**, 3 suites, 320,865 rows validated |
| Python unit tests | 16 |
| Airflow DAGs validated on every PR | 2 (18 tasks) |

**Orchestration** — run on the containerised Airflow stack, not just validated
| | |
|---|---|
| `lodestar_initial_load` | 7/7 tasks, built the warehouse from an empty directory |
| `lodestar_elt` (real scheduler run) | 11/11 tasks, ~29 s end to end |
| `dbt test` inside Airflow | PASS=154 WARN=1 ERROR=0 — identical to the host |
| Great Expectations inside Airflow | 25/25 on 100,000 orders |

**Infrastructure**
| | |
|---|---|
| Terraform-managed resources | ~28 (buckets, 7 datasets, 2 service accounts, 11 IAM bindings, 4 API enablements) |
| Console clicks required | 0 |

**Business measures the model produces**
| | |
|---|---|
| Gross revenue modelled | 28,123,629.25 |
| Late delivery rate | **7.25%** of delivered orders |
| SCD2 seller versions | 3,281 versions across 3,125 sellers (156 relocated) |

---

## Engineering decisions worth explaining

**Two warehouse targets, one codebase.** `dev`/`ci` run against DuckDB, `prod`
against BigQuery. This is not a toy convenience — it is what makes a *full*
build-and-test affordable on every pull request. CI builds all 23 models and
runs all 155 tests in seconds for zero cloud spend, so no untested model
reaches `main`. Portability is enforced with `adapter.dispatch` (see
[`macros/title_case.sql`](dbt_project/macros/title_case.sql), which works
around DuckDB having no `INITCAP`) and
[`macros/warehouse_config.sql`](dbt_project/macros/warehouse_config.sql), which
applies BigQuery partitioning and clustering only where it means something.

**Idempotency is a tested property, not an aspiration.** Every partition write
deletes before it writes, and the daily load is delete-then-insert keyed on
`ingest_date`. `tests/test_ingestion.py` re-runs the same day three times and
asserts row counts and duplicate counts are unchanged. This is what makes
`airflow dags backfill` safe.

**The history load fans out in one pass.** Rather than looping over 730 days,
the initial load derives `ingest_date` from each row's own event timestamp and
writes every day partition in a single DuckDB `COPY ... PARTITION_BY`. The
landing zone ends up laid out exactly as if the daily DAG had been running since
the first order — which is what stops the first daily run after a bulk load
from double-counting.

**dbt tests and Great Expectations do different jobs.** dbt owns row-level
structure (uniqueness, not-null, referential integrity) close to the model
definitions. GE owns *shape* — distribution, quantiles, volume — the failures
that pass every row-level test while quietly making a dashboard wrong. Neither
duplicates the other.

**One test warns instead of failing, deliberately.**
[`assert_order_completeness_within_tolerance`](dbt_project/tests/assert_order_completeness_within_tolerance.sql)
warns above 0 and fails above 1,500. Some orders genuinely arrive without
payment rows — that quirk is in the real Olist export. Asserting zero would mean
a permanently red pipeline everyone learns to ignore; asserting nothing would
hide the day the loader starts dropping payments. A threshold keeps the signal.

**Type 2 history tracks sellers, not customers.** In the Olist schema
`customer_id` is minted per order, so a customer row is immutable by
construction and a Type 2 dimension over it would record changes that cannot
happen. Sellers have a real 1:1 natural key whose city and state genuinely
change. `dim_customers` instead rolls up at the person-level
`customer_unique_id` to answer the repeat-purchase questions Type 2 would
otherwise serve. See [`snapshots/snap_sellers.sql`](dbt_project/snapshots/snap_sellers.sql).

**Least privilege in the IAM.** Two service accounts: the pipeline writes the
landing zone and builds the warehouse; the BI identity can read `marts` and
nothing else. A compromised dashboard credential cannot reach raw data or drop
a dataset.

**Managed Airflow is off by default.** Cloud Composer bills ~USD 300/month
whether or not a DAG runs. `enable_composer = false` is the honest default; the
docker-compose stack runs the identical DAG for free.

---

## Running it

### Local — no cloud account needed

```bash
make setup      # venv + dependencies + dbt packages
make demo       # seed → land → load → build → test → validate
make metrics    # the numbers above, regenerated
```

Useful individual targets:

```bash
make build          # dbt build (models + tests interleaved)
make quality        # Great Expectations suites
make drift-demo     # relocate sellers, show SCD Type 2 open new versions
make test           # everything CI runs
make docs           # dbt lineage/docs site
make query          # DuckDB shell against the warehouse
```

### Airflow

```bash
make airflow-up     # http://localhost:8080  (airflow / airflow)
```

Two DAGs: `lodestar_initial_load` (one-shot, seeds history) and `lodestar_elt`
(daily). LocalExecutor rather than Celery — no Redis, no worker containers,
comfortable on 8 GB.

### Deploying to GCP

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # set project_id
terraform init -backend-config="bucket=<your-tfstate-bucket>"
terraform apply
terraform output pipeline_env                  # env vars for the pipeline
```

Then point the pipeline at it:

```bash
export LODESTAR_TARGET=gcp
export LODESTAR_GCP_PROJECT=<project>
export LODESTAR_LANDING_URI=gs://<bucket>
export DBT_TARGET=prod
pip install -e ".[gcp]"
```

### Using the real Olist data

The pipeline ships a deterministic generator that emits CSVs with byte-identical
column names to the [Kaggle Olist export](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
— including the two misspelled `_lenght` columns — so a clone-and-run works with
no Kaggle login. To use the real thing, unzip it into `data/source/` and the
generator is skipped automatically. Nothing downstream changes.

---

## Repository layout

```
lodestar/
├── infra/                    Terraform: buckets, datasets, IAM, optional Composer
├── ingestion/                Extract & load (Python package + CLI)
│   └── lodestar_ingest/      config, sources, generate, extract, storage, warehouse
├── dbt_project/
│   ├── models/               staging (9) → intermediate (5) → marts (9)
│   ├── snapshots/            SCD Type 2 source
│   ├── macros/               schema routing, cross-warehouse portability
│   └── tests/                7 singular tests encoding business invariants
├── quality/                  Great Expectations suites + runner
├── airflow/                  DAGs, Dockerfile, docker-compose (LocalExecutor)
├── tests/                    pytest suite + DAG import validation
├── scripts/                  metrics collection, SCD2 demo
└── docs/                     architecture, metrics methodology, runbook
```

Further reading: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) ·
[docs/METRICS.md](docs/METRICS.md) · [docs/RUNBOOK.md](docs/RUNBOOK.md) ·
[docs/BI.md](docs/BI.md)

---

## CI

Every pull request runs four jobs: lint (ruff + `terraform fmt`), Terraform
validate, Airflow DAG import, and a full warehouse build. The build job also
runs the incremental models **twice in a row and then tests them** — a model
that only works with `--full-refresh` is a model that breaks in production on
day two. Each PR builds into its own schema (`ci_pr_<number>`), so concurrent
PRs cannot clobber each other.
