# Lodestar

A batch data platform that takes messy e-commerce data and turns it into a
warehouse you can actually build a dashboard on.

Raw CSVs land in a partitioned object store, dbt models them into a star
schema, and everything gets tested on the way through. Airflow runs it daily,
Terraform provisions it.

**It runs on your laptop with no cloud account.**

```bash
git clone https://github.com/SmritiReddyy/lodestar && cd lodestar
make demo
```

About 30 seconds later you have a fully built, fully tested warehouse.

---

## The shape of it

```
Olist CSVs → landing zone (Parquet, partitioned by date) → raw tables
                                    ↓
              dbt: staging → intermediate → marts (star schema)
                                    ↓
                 155 dbt tests + 25 Great Expectations checks
                                    ↓
                              dashboards
```

The marts are a normal star schema: `fct_orders` and `fct_order_items` at two
grains, with conformed `dim_customers`, `dim_products`, `dim_sellers`,
`dim_geography` and `dim_date` around them. There's a Type 2 slowly-changing
dimension for seller history, and a pre-aggregated table for the BI layer.

## What came out of it

- **100,000 orders / 151,040 line items** through the pipeline
- **155 dbt tests**, all passing — including cross-grain reconciliation that
  catches the classic fan-out bug
- Runs against **DuckDB locally and BigQuery in production** from one codebase,
  so CI builds and tests the whole thing in ~10 seconds for free
- Incremental facts rebuild **2× faster** than a full refresh
- A pre-aggregated mart made dashboard queries **2.65× faster**
- **~28 Terraform resources**, zero clicking around in a console

Full numbers, and how each was measured, are in [docs/METRICS.md](docs/METRICS.md).

## A few decisions I'd defend in an interview

**Two warehouses, one codebase.** Local runs hit DuckDB, production hits
BigQuery. That's not a convenience — it's what makes it affordable to build and
test the *entire* model graph on every pull request instead of hoping.

**Idempotency is tested, not assumed.** The test suite re-runs the same day
three times and asserts nothing duplicates. That's what makes a backfill safe.

**One test warns instead of failing, on purpose.** Some orders genuinely arrive
with no payment rows — that quirk is in the real data. Failing on it would mean
a permanently red pipeline everyone learns to ignore, so it warns below a
threshold and fails above it.

**Type 2 history tracks sellers, not customers.** In this schema `customer_id`
is minted fresh per order, so a customer row can't change. Sellers have a real
key whose city and state actually move.

More of these in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Running it

[INSTRUCTIONS.md](INSTRUCTIONS.md) covers setup, the Airflow stack, deploying
to GCP, and using the real Olist dataset.

Quick version:

```bash
make setup     # venv + dependencies
make demo      # build everything
make test      # everything CI runs
make airflow-up   # Airflow at localhost:8080
```

## Docs

| | |
|---|---|
| [INSTRUCTIONS.md](INSTRUCTIONS.md) | Setup, running, deploying |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | How the layers fit together and why |
| [docs/METRICS.md](docs/METRICS.md) | Every number, and how it was measured |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | What to do when it breaks |
| [docs/BI.md](docs/BI.md) | Connecting dashboards to the marts |

Its streaming counterpart is [Tideline](https://github.com/SmritiReddyy/tideline).
