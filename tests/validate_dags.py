#!/usr/bin/env python
"""Import every DAG and fail loudly on anything the scheduler would reject.

Run standalone (`python tests/validate_dags.py`) or via pytest. A DAG with a
syntax error, a bad import, or a cycle otherwise sits quietly broken until the
scheduler next parses it — usually at 6am.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_FOLDER = REPO_ROOT / "airflow" / "dags"

# The DAGs resolve paths from LODESTAR_HOME; point it at the checkout so the
# import does not depend on the container layout.
os.environ.setdefault("LODESTAR_HOME", str(REPO_ROOT))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_FOLDER))

EXPECTED_DAG_IDS = {"lodestar_elt", "lodestar_initial_load"}


def validate() -> int:
    from airflow.exceptions import AirflowDagCycleException
    from airflow.models import DagBag
    from airflow.utils.dag_cycle_tester import check_cycle

    dag_bag = DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)

    if dag_bag.import_errors:
        print("DAG import errors:", file=sys.stderr)
        for filename, error in dag_bag.import_errors.items():
            print(f"\n  {filename}\n    {error}", file=sys.stderr)
        return 1

    found = set(dag_bag.dag_ids)
    missing = EXPECTED_DAG_IDS - found
    if missing:
        print(f"Expected DAGs not found: {sorted(missing)}", file=sys.stderr)
        return 1

    failures = []
    for dag_id in sorted(found):
        # `dag_bag.dags[...]`, not `get_dag(...)`: the latter reaches into the
        # Airflow metadata database, which would make this check require a
        # migrated DB. Parsing is all we need, and it should stay free.
        dag = dag_bag.dags[dag_id]

        if not dag.tasks:
            failures.append(f"{dag_id}: has no tasks")

        # Airflow refuses to run a cyclic DAG; catch it here instead.
        try:
            check_cycle(dag)
        except AirflowDagCycleException as exc:
            failures.append(f"{dag_id}: cycle detected — {exc}")

        # A task with no owner or no retries is a task nobody gets paged about.
        for task in dag.tasks:
            if not task.owner or task.owner == "airflow":
                failures.append(f"{dag_id}.{task.task_id}: no explicit owner set")

        print(f"  ok  {dag_id:<28} {len(dag.tasks):>2} tasks, schedule={dag.schedule_interval!r}")

    if failures:
        print("\nValidation failures:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print(f"\n{len(found)} DAG(s) validated successfully.")
    return 0


def test_dags_import() -> None:
    """pytest entry point."""
    assert validate() == 0


if __name__ == "__main__":
    raise SystemExit(validate())
