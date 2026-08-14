"""Lodestar daily ELT DAG.

    extract -> load -> dbt run -> dbt test -> data quality -> publish metrics

Design decisions worth defending:

* **Idempotent and backfill-safe.** Every task is keyed on the run's logical
  date, and the extract/load layer rewrites that date's partition rather than
  appending. `airflow dags backfill` over any historical range is therefore
  safe to run twice.
* **No catchup storm.** `catchup=False` plus `max_active_runs=1` means turning
  the DAG on does not immediately queue a year of runs; backfills are explicit.
* **Failures stop the line.** dbt test and the Great Expectations suite both
  fail the task rather than logging a warning, so a data quality regression
  never reaches the BI layer unnoticed.
* **The DAG shells out to the same CLI a human uses.** Nothing here is
  reachable only through Airflow, which makes local reproduction of a failed
  task a copy-paste rather than an investigation.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Repo root as seen from inside the Airflow container / Composer bucket.
PROJECT_ROOT = Path(os.environ.get("LODESTAR_HOME", "/opt/lodestar"))
DBT_DIR = PROJECT_ROOT / "dbt_project"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    # Retries cover transient warehouse/network failures. The delay is
    # exponential so a warehouse under load is not hammered.
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "email_on_failure": False,
}

# Shared by every shell task. `set -euo pipefail` matters: without it a failing
# command inside a pipeline is silently swallowed and the task reports success.
#
# LODESTAR_BIN_PATH lets the same DAG run in two places: in the container the
# tools are already on PATH, while a bare-metal Airflow points it at the repo's
# virtualenv.
BASH_PREAMBLE = f"""
set -euo pipefail
cd {PROJECT_ROOT}
export PATH="${{LODESTAR_BIN_PATH:-{PROJECT_ROOT}/.venv/bin}}:$PATH"
"""

DBT_PREAMBLE = f"""
set -euo pipefail
cd {DBT_DIR}
export PATH="${{LODESTAR_BIN_PATH:-{PROJECT_ROOT}/.venv/bin}}:$PATH"
export DBT_PROFILES_DIR={DBT_DIR}
"""


def publish_run_metrics(**context) -> dict:
    """Record what this run actually did, for the metrics table and the README.

    Emitting run statistics as data — rather than leaving them in task logs —
    is what makes "how long does the pipeline take?" and "is it getting slower?"
    answerable in SQL months later.
    """
    import json
    import sys

    sys.path.insert(0, str(PROJECT_ROOT / "ingestion"))
    from lodestar_ingest.config import get_config  # noqa: PLC0415
    from lodestar_ingest.warehouse import get_warehouse  # noqa: PLC0415

    dag_run = context["dag_run"]
    logical_date = context["logical_date"]

    task_instances = dag_run.get_task_instances()

    def _seconds(ti) -> float | None:
        """Task runtime, or None when Airflow has not recorded one.

        `ti.duration` is null for a task that is still running, and for every
        task under `airflow dags test`, which does not persist timings the way
        the scheduler does. Falling back to end - start recovers the real
        number where those are set; returning None where they are not keeps a
        missing measurement distinguishable from a genuine zero.
        """
        if ti.duration is not None:
            return round(float(ti.duration), 2)
        if ti.start_date and ti.end_date:
            return round((ti.end_date - ti.start_date).total_seconds(), 2)
        return None

    durations = {
        ti.task_id: _seconds(ti)
        for ti in task_instances
        if ti.task_id != context["task"].task_id
    }
    measured = [v for v in durations.values() if v is not None]
    total_seconds = round(sum(measured), 2) if measured else None

    config = get_config()
    warehouse = get_warehouse(config)
    try:
        row_counts = {
            table: warehouse.row_count(config.raw_dataset, table)
            for table in ("orders", "order_items")
        }
    finally:
        warehouse.close()

    summary = {
        "run_date": logical_date.date().isoformat(),
        "dag_id": dag_run.dag_id,
        "run_id": dag_run.run_id,
        "total_seconds": total_seconds,
        "task_seconds": durations,
        "tasks_without_timing": [k for k, v in durations.items() if v is None],
        "raw_row_counts": row_counts,
    }
    print(json.dumps(summary, indent=2))
    return summary


with DAG(
    dag_id="lodestar_elt",
    description="Olist ELT: land raw data, model it, test it, publish it.",
    default_args=DEFAULT_ARGS,
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    # Explicit backfills only — switching the DAG on should not queue a year of runs.
    catchup=False,
    max_active_runs=1,
    # Alert if the whole run has not finished within four hours.
    dagrun_timeout=timedelta(hours=4),
    tags=["lodestar", "elt", "dbt", "olist"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # --- extract ----------------------------------------------------------
    # `{{ ds }}` is the run's logical date, so a backfill of 2024-03-04 lands
    # exactly that day's slice into exactly that day's partition.
    extract = BashOperator(
        task_id="extract_to_landing_zone",
        bash_command=BASH_PREAMBLE + """
        lodestar-ingest extract --ingest-date {{ ds }}
        """,
        doc_md="Source CSVs -> partitioned Parquet in the landing zone.",
    )

    # --- load -------------------------------------------------------------
    load = BashOperator(
        task_id="load_to_warehouse",
        bash_command=BASH_PREAMBLE + """
        lodestar-ingest load --ingest-date {{ ds }}
        """,
        doc_md="Landing zone -> raw warehouse tables. Rewrites the day's partition.",
    )

    # --- source freshness -------------------------------------------------
    # Runs before the models so a stale source is caught early, but does not
    # block the build — a warning here is informational.
    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=DBT_PREAMBLE + """
        dbt source freshness --target "${DBT_TARGET:-dev}" || true
        """,
        doc_md="Warn if the raw layer has stopped receiving data.",
    )

    # --- transform --------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=DBT_PREAMBLE + """
        dbt run --target "${DBT_TARGET:-dev}"
        """,
        doc_md="Build staging, intermediate and mart models.",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=DBT_PREAMBLE + """
        dbt snapshot --target "${DBT_TARGET:-dev}"
        """,
        doc_md="Capture Type 2 history before the dimensions are rebuilt on it.",
    )

    # --- test -------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_PREAMBLE + """
        dbt test --target "${DBT_TARGET:-dev}"
        """,
        doc_md=(
            "Schema and singular tests. A non-zero exit fails the task, so bad "
            "data stops here rather than reaching the BI layer."
        ),
    )

    data_quality = BashOperator(
        task_id="great_expectations",
        bash_command=BASH_PREAMBLE + """
        python -m quality.run_checks
        """,
        doc_md=(
            "Distribution and volume checks that row-level tests cannot express. "
            "Exits non-zero on failure."
        ),
    )

    # --- docs & metrics ---------------------------------------------------
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=DBT_PREAMBLE + """
        dbt docs generate --target "${DBT_TARGET:-dev}"
        """,
        doc_md="Refresh the lineage/documentation site from the run's manifest.",
    )

    metrics = PythonOperator(
        task_id="publish_run_metrics",
        python_callable=publish_run_metrics,
        # Runs even when an upstream task failed, so a failed run still reports
        # how far it got and how long that took.
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)

    (
        start
        >> extract
        >> load
        >> source_freshness
        >> dbt_run
        >> dbt_snapshot
        >> dbt_test
        >> data_quality
        >> dbt_docs
        >> end
    )

    # Metrics hang off the end of the line but do not gate it.
    data_quality >> metrics
