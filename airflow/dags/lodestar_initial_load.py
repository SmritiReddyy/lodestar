"""One-shot DAG that seeds an empty warehouse with the full history.

Manual trigger only. The daily `lodestar_elt` DAG lands one day at a time and
assumes history already exists; this is what creates it.

The history extract fans every row out into the day partition its own event
timestamp belongs to, so the landing zone ends up laid out exactly as if the
daily DAG had been running since the first order. That alignment is the point:
without it, the first daily run after a bulk load would double-count.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

PROJECT_ROOT = Path(os.environ.get("LODESTAR_HOME", "/opt/lodestar"))
DBT_DIR = PROJECT_ROOT / "dbt_project"

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

with DAG(
    dag_id="lodestar_initial_load",
    description="One-shot full-history load. Run once against an empty warehouse.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    tags=["lodestar", "backfill", "one-shot"],
    doc_md=__doc__,
    params={
        "fallback_date": Param(
            default="2025-01-01",
            type="string",
            description=(
                "Partition date for tables with no event timestamp (dimensions), "
                "and for rows whose watermark is null."
            ),
        ),
    },
    default_args={
        "owner": "data-engineering",
        # A bulk load is expensive to half-repeat; fail loudly instead.
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    start = EmptyOperator(task_id="start")

    seed = BashOperator(
        task_id="seed_source_data",
        bash_command=BASH_PREAMBLE + """
        lodestar-ingest seed
        """,
        doc_md=(
            "Generate the synthetic Olist export if no real one is present. "
            "A no-op when `data/source/` already holds the Kaggle CSVs."
        ),
    )

    extract_history = BashOperator(
        task_id="extract_full_history",
        bash_command=BASH_PREAMBLE + """
        lodestar-ingest extract --full-history \
            --ingest-date {{ params.fallback_date }}
        """,
        doc_md="Fan the whole source out into one landing partition per event day.",
    )

    load_history = BashOperator(
        task_id="load_full_history",
        bash_command=BASH_PREAMBLE + """
        lodestar-ingest load --full-history \
            --ingest-date {{ params.fallback_date }}
        """,
        doc_md="Read every landed partition and rebuild each raw table from scratch.",
    )

    dbt_build = BashOperator(
        task_id="dbt_build_full_refresh",
        bash_command=DBT_PREAMBLE + """
        dbt build --full-refresh --target "${DBT_TARGET:-dev}"
        """,
        doc_md=(
            "Full refresh so the incremental facts are built from the complete "
            "history rather than one day's slice. `build` runs models and their "
            "tests together, stopping a broken model from feeding a downstream one."
        ),
    )

    data_quality = BashOperator(
        task_id="great_expectations",
        bash_command=BASH_PREAMBLE + """
        python -m quality.run_checks
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> seed >> extract_history >> load_history >> dbt_build >> data_quality >> end
