# Airflow image with the Lodestar pipeline and its tooling baked in.
#
# Building an image beats `_PIP_ADDITIONAL_REQUIREMENTS`: dependencies resolve
# once at build time instead of on every container start, and the scheduler and
# workers are guaranteed to be running identical code.

FROM apache/airflow:2.10.5-python3.11

USER root

# git is needed by `dbt deps` to fetch packages from the hub.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy only the dependency manifest first so the layer caches across code edits.
COPY --chown=airflow:root pyproject.toml /tmp/lodestar/pyproject.toml
COPY --chown=airflow:root README.md /tmp/lodestar/README.md
COPY --chown=airflow:root ingestion /tmp/lodestar/ingestion

# The constraint file keeps pip from upgrading Airflow's own pinned deps out
# from under it — the single most common way an Airflow image breaks.
RUN pip install --no-cache-dir \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
        "apache-airflow[postgres]==2.10.5" \
    && pip install --no-cache-dir \
        "dbt-core>=1.9,<2.0" \
        "dbt-duckdb>=1.9,<2.0" \
        "duckdb>=1.1" \
        "great_expectations>=1.3" \
        "pandas>=2.2" \
    && pip install --no-cache-dir --no-deps -e /tmp/lodestar

ENV PYTHONPATH=/opt/lodestar/ingestion:/opt/lodestar \
    LODESTAR_HOME=/opt/lodestar \
    LODESTAR_BIN_PATH=/home/airflow/.local/bin
