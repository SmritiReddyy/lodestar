"""Runtime configuration for the Lodestar ingestion layer.

Everything is driven by environment variables so the same code runs
identically from a laptop, from an Airflow worker, and from CI.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    if value is not None and value.strip() == "":
        return default
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    """Resolved settings for one ingestion run."""

    # "local" runs against the filesystem + DuckDB, "gcp" against GCS + BigQuery.
    target: str

    # Where the untouched source CSVs are read from.
    source_dir: Path

    # Landing zone root: a local path, or a gs:// URI when target == "gcp".
    landing_uri: str

    # Warehouse coordinates.
    gcp_project: str | None
    bq_location: str
    raw_dataset: str
    duckdb_path: Path

    # Synthetic-data knobs, used when the real Olist export is not present.
    synthetic_orders: int
    synthetic_seed: int

    @property
    def is_gcp(self) -> bool:
        return self.target == "gcp"

    @classmethod
    def from_env(cls) -> Config:
        target = (_env("LODESTAR_TARGET", "local") or "local").lower()
        if target not in {"local", "gcp"}:
            raise ValueError(f"LODESTAR_TARGET must be 'local' or 'gcp', got {target!r}")

        source_dir = Path(
            _env("LODESTAR_SOURCE_DIR", str(REPO_ROOT / "data" / "source"))
        ).expanduser()

        default_landing = str(REPO_ROOT / "data" / "landing")
        landing_uri = _env("LODESTAR_LANDING_URI", default_landing) or default_landing

        if target == "gcp" and not landing_uri.startswith("gs://"):
            raise ValueError("LODESTAR_LANDING_URI must be a gs:// URI when LODESTAR_TARGET=gcp")

        duckdb_path = Path(
            _env("LODESTAR_DUCKDB_PATH", str(REPO_ROOT / "data" / "lodestar.duckdb"))
        ).expanduser()

        return cls(
            target=target,
            source_dir=source_dir,
            landing_uri=landing_uri,
            gcp_project=_env("LODESTAR_GCP_PROJECT"),
            bq_location=_env("LODESTAR_BQ_LOCATION", "US") or "US",
            raw_dataset=_env("LODESTAR_RAW_DATASET", "lodestar_raw") or "lodestar_raw",
            duckdb_path=duckdb_path,
            # Defaults to roughly the real Olist export's scale (~99k orders).
            synthetic_orders=int(_env("LODESTAR_SYNTHETIC_ORDERS", "100000") or 100000),
            synthetic_seed=int(_env("LODESTAR_SYNTHETIC_SEED", "20240501") or 20240501),
        )


def get_config() -> Config:
    return Config.from_env()
