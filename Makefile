# Lodestar — common tasks.
#
#   make setup     one-time: venv, deps, dbt packages
#   make demo      the whole thing, from nothing to a tested warehouse
#   make test      everything CI runs
#
# Every target works with no cloud credentials.

SHELL := /bin/bash
.DEFAULT_GOAL := help

VENV        := .venv
PY          := $(VENV)/bin/python
PIP         := $(VENV)/bin/pip
INGEST      := $(VENV)/bin/lodestar-ingest
DBT         := cd dbt_project && DBT_PROFILES_DIR=. ../$(VENV)/bin/dbt
RUFF        := $(VENV)/bin/ruff
PYTEST      := $(VENV)/bin/pytest

# The date the initial history load files dimension snapshots under.
LOAD_DATE   ?= 2025-01-01
ORDERS      ?= 100000

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# --- setup ----------------------------------------------------------------

$(VENV)/bin/activate: pyproject.toml
	python3 -m venv $(VENV)
	$(PIP) install --quiet --upgrade pip
	$(PIP) install --quiet -e ".[dev]"
	@touch $(VENV)/bin/activate

.PHONY: setup
setup: $(VENV)/bin/activate deps ## Create the venv, install deps and dbt packages

.PHONY: deps
deps: ## Install dbt packages
	@$(DBT) deps

# --- pipeline -------------------------------------------------------------

.PHONY: seed
seed: ## Generate the synthetic Olist source export
	$(INGEST) seed --orders $(ORDERS)

.PHONY: extract
extract: ## Land one day's slice (DATE=YYYY-MM-DD)
	$(INGEST) extract --ingest-date $(or $(DATE),$(shell date +%F))

.PHONY: load
load: ## Load one day's landed partition (DATE=YYYY-MM-DD)
	$(INGEST) load --ingest-date $(or $(DATE),$(shell date +%F))

.PHONY: history
history: ## Land and load the full history, fanned out by event date
	$(INGEST) extract --full-history --ingest-date $(LOAD_DATE)
	$(INGEST) load    --full-history --ingest-date $(LOAD_DATE)

.PHONY: build
build: ## dbt build (models + tests together)
	@$(DBT) build

.PHONY: rebuild
rebuild: ## dbt build --full-refresh
	@$(DBT) build --full-refresh

.PHONY: snapshot
snapshot: ## Capture SCD Type 2 history
	@$(DBT) snapshot

.PHONY: quality
quality: ## Great Expectations suites
	$(PY) -m quality.run_checks --scale $(ORDERS)

.PHONY: docs
docs: ## Generate and serve the dbt docs site
	@$(DBT) docs generate && DBT_PROFILES_DIR=. ../$(VENV)/bin/dbt docs serve

# --- demo -----------------------------------------------------------------

.PHONY: demo
demo: setup seed history rebuild snapshot quality ## Nothing -> fully tested warehouse
	@echo ""
	@echo "Warehouse built at data/lodestar.duckdb"
	@echo "Explore it:  make query"

.PHONY: drift-demo
drift-demo: ## Relocate sellers and show SCD Type 2 opening new versions
	$(INGEST) seed --drift
	$(INGEST) run --ingest-date $(LOAD_DATE) --tables sellers
	@$(DBT) snapshot
	@$(DBT) run --select dim_sellers_history
	@$(PY) scripts/show_scd2.py

.PHONY: query
query: ## Open a DuckDB shell against the warehouse
	$(PY) -c "import duckdb,sys; duckdb.connect('data/lodestar.duckdb'); print('Use: duckdb data/lodestar.duckdb')"
	@command -v duckdb >/dev/null && duckdb data/lodestar.duckdb || \
		echo "Install the DuckDB CLI (brew install duckdb) for an interactive shell."

.PHONY: metrics
metrics: ## Print the pipeline metrics reported in docs/METRICS.md
	$(PY) scripts/collect_metrics.py

# --- airflow --------------------------------------------------------------

.PHONY: airflow-up
airflow-up: ## Start local Airflow (http://localhost:8080, airflow/airflow)
	cd airflow && docker compose up -d --build
	@echo "Airflow starting at http://localhost:8080 (airflow / airflow)"

.PHONY: airflow-down
airflow-down: ## Stop Airflow and wipe its metadata DB
	cd airflow && docker compose down -v

.PHONY: airflow-logs
airflow-logs: ## Follow the scheduler logs
	cd airflow && docker compose logs -f airflow-scheduler

# --- checks ---------------------------------------------------------------

.PHONY: lint
lint: ## ruff + terraform fmt
	$(RUFF) check ingestion quality tests
	$(RUFF) format --check ingestion quality tests
	terraform fmt -check -recursive infra

.PHONY: fmt
fmt: ## Auto-format everything
	$(RUFF) check --fix ingestion quality tests
	$(RUFF) format ingestion quality tests
	terraform fmt -recursive infra

.PHONY: unit
unit: ## Unit tests
	$(PYTEST) -q

.PHONY: tf-validate
tf-validate: ## Validate the Terraform configuration
	cd infra && terraform init -backend=false -input=false >/dev/null && terraform validate

.PHONY: test
test: lint unit build quality tf-validate ## Everything CI runs

# --- housekeeping ---------------------------------------------------------

.PHONY: clean
clean: ## Remove generated data and build artefacts
	rm -rf data dbt_project/target dbt_project/logs quality/results
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

.PHONY: clean-all
clean-all: clean ## Also remove the venv and dbt packages
	rm -rf $(VENV) dbt_project/dbt_packages
