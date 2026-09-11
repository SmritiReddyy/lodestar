# Running Lodestar

Everything here works without a cloud account. The GCP section at the end is
optional.

## Requirements

- Python 3.11+
- Docker, only if you want to run Airflow
- A GCP project, only if you want to deploy it

## First run

```bash
make setup     # creates .venv, installs dependencies and dbt packages
make demo      # seed → land → load → build → test → validate
make metrics   # prints the numbers quoted in the README
```

`make demo` starts from nothing and ends with a built, tested warehouse in
`data/lodestar.duckdb`. It takes about 30 seconds.

## Day-to-day

```bash
make build        # dbt build (models and tests interleaved)
make rebuild      # dbt build --full-refresh
make quality      # Great Expectations suites
make test         # everything CI runs
make docs         # dbt lineage site, served locally
make query        # DuckDB shell against the warehouse
make clean        # delete generated data and artefacts
```

Individual pipeline stages, if you want to drive them by hand:

```bash
lodestar-ingest seed                                  # generate source CSVs
lodestar-ingest extract --ingest-date 2024-06-15      # land one day
lodestar-ingest load --ingest-date 2024-06-15         # load that day
lodestar-ingest backfill --start 2024-06-01 --end 2024-06-30
lodestar-ingest info                                  # resolved config
```

Re-running any of these for the same date is safe — that's tested, not hoped
for.

## The SCD Type 2 demo

The source data is a static export, so nothing ever changes and a Type 2
dimension would sit at one version per key forever. This mutates it the way a
real source would:

```bash
make drift-demo
```

It relocates 5% of sellers, re-runs the pipeline, takes a new snapshot, and
prints the resulting version history with `valid_from` / `valid_to` boundaries.

## Airflow

```bash
make airflow-up      # http://localhost:8080, login airflow / airflow
make airflow-logs    # follow the scheduler
make airflow-down    # stop and wipe the metadata DB
```

Two DAGs:

- **`lodestar_initial_load`** — manual trigger, one shot. Seeds the source,
  lands the full history fanned out by event date, and builds everything.
  Run this first against an empty warehouse.
- **`lodestar_elt`** — the daily pipeline. Extract, load, freshness check,
  dbt run, snapshot, test, quality checks, docs, metrics.

Both are paused on arrival. Unpause `lodestar_elt` in the UI, or:

```bash
docker exec airflow-airflow-scheduler-1 airflow dags unpause lodestar_elt
docker exec airflow-airflow-scheduler-1 airflow dags trigger lodestar_elt
```

It uses LocalExecutor rather than Celery — no Redis, no worker containers, and
it fits comfortably on a laptop.

## Backfilling

```bash
airflow dags backfill lodestar_elt --start-date 2024-06-01 --end-date 2024-06-30
```

Safe to repeat. If you backfill further back than the incremental models'
7-day lookback window, follow it with a `--full-refresh` so the facts re-read
the older partitions:

```bash
cd dbt_project && dbt build --full-refresh --select fct_orders+ fct_order_items+
```

## Deploying to GCP

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # set project_id
terraform init -backend-config="bucket=<your-tfstate-bucket>"
terraform apply
terraform output pipeline_env
```

The state bucket is deliberately not managed by this config — a backend can't
create the bucket that stores its own state, so make that one by hand.

Then point the pipeline at what Terraform built:

```bash
pip install -e ".[gcp]"
export LODESTAR_TARGET=gcp
export LODESTAR_GCP_PROJECT=<project>
export LODESTAR_LANDING_URI=gs://<bucket>
export LODESTAR_RAW_DATASET=<raw dataset>
export DBT_TARGET=prod
```

**Cloud Composer is off by default.** It bills roughly USD 300/month whether or
not a DAG runs. Set `enable_composer = true` only to demonstrate a managed
deployment, and destroy it afterwards.

## Using the real Olist data

The repo ships a generator that emits CSVs with byte-identical column names to
the [Kaggle Olist export](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
— including the two misspelled `_lenght` columns — so a clone-and-run works
with no Kaggle login.

To use the real thing, unzip it into `data/source/`. The generator detects the
files and skips itself. Nothing downstream changes.

## Configuration

Everything is environment-driven, so the same code runs locally, in CI and in
the cloud.

| Variable | Default | Purpose |
|---|---|---|
| `LODESTAR_TARGET` | `local` | `local` (DuckDB) or `gcp` (BigQuery) |
| `LODESTAR_SOURCE_DIR` | `data/source` | Where source CSVs are read from |
| `LODESTAR_LANDING_URI` | `data/landing` | Landing zone path or `gs://` URI |
| `LODESTAR_DUCKDB_PATH` | `data/lodestar.duckdb` | Local warehouse file |
| `LODESTAR_RAW_DATASET` | `lodestar_raw` | Raw layer schema/dataset |
| `LODESTAR_GCP_PROJECT` | — | Required when target is `gcp` |
| `LODESTAR_SYNTHETIC_ORDERS` | `100000` | Generator scale |
| `DBT_TARGET` | `dev` | `dev`, `ci` or `prod` |
| `DBT_SCHEMA` | `lodestar` | Schema prefix; `prod` uses bare layer names |

## Repository layout

```
lodestar/
├── infra/                    Terraform: buckets, datasets, IAM, optional Composer
├── ingestion/                Extract and load (Python package + CLI)
├── dbt_project/
│   ├── models/               staging (9) → intermediate (5) → marts (9)
│   ├── snapshots/            SCD Type 2 source
│   ├── macros/               schema routing, cross-warehouse portability
│   └── tests/                singular tests encoding business invariants
├── quality/                  Great Expectations suites and runner
├── airflow/                  DAGs, Dockerfile, docker-compose
├── tests/                    pytest suite and DAG import validation
├── scripts/                  metrics collection, SCD2 demo
└── docs/                     architecture, metrics, runbook, BI guide
```

## CI

Four jobs on every pull request: lint (ruff), Terraform validate and format,
Airflow DAG import, and a full warehouse build with all tests.

The build job also runs the incremental models **twice in a row and then tests
them** — a model that only works with `--full-refresh` is a model that breaks
in production on day two. Each PR builds into its own schema (`ci_pr_<number>`)
so concurrent PRs can't clobber each other.
