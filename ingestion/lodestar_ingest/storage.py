"""Landing-zone abstraction.

One interface, two backends: the local filesystem for laptop/CI runs and
Google Cloud Storage for the deployed pipeline. Callers never branch on the
target — they ask the zone for a partition URI and hand it a file.

Layout is Hive-style so BigQuery, DuckDB and Spark can all discover it:

    <root>/raw/<table>/ingest_date=YYYY-MM-DD/<table>-<run_id>.parquet

`ingest_date` is both the directory key and a real column inside every file,
so a reader recovers the partition value whether or not it understands Hive
path conventions.
"""

from __future__ import annotations

import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import urlparse


def table_key(table: str) -> str:
    return f"raw/{table}"


def partition_key(table: str, ingest_date: str) -> str:
    return f"{table_key(table)}/ingest_date={ingest_date}"


def object_key(table: str, ingest_date: str, run_id: str) -> str:
    return f"{partition_key(table, ingest_date)}/{table}-{run_id}.parquet"


class LandingZone(ABC):
    """Write-side interface over the raw landing area."""

    @abstractmethod
    def put_file(self, local_path: Path, key: str) -> str:
        """Upload/move `local_path` to `key`. Returns the resulting URI."""

    @abstractmethod
    def uri_for(self, key: str) -> str:
        """Absolute URI for a key, without touching the object."""

    @abstractmethod
    def delete_prefix(self, prefix: str) -> int:
        """Remove everything under `prefix`. Returns objects removed.

        Used to make a partition rewrite idempotent: a re-run of the same
        logical day replaces that day rather than appending a second copy.
        """

    @abstractmethod
    def list_prefix(self, prefix: str) -> list[str]:
        """URIs of every object under `prefix`."""

    def partition_glob(self, table: str, ingest_date: str | None = None) -> str:
        """Glob a reader can pass straight to DuckDB or BigQuery.

        Passing `ingest_date=None` matches every partition, which is what the
        initial history load reads.
        """
        if ingest_date is None:
            return self.uri_for(f"{table_key(table)}/*/*.parquet")
        return self.uri_for(f"{partition_key(table, ingest_date)}/*.parquet")

    def put_tree(self, local_dir: Path, key_prefix: str) -> int:
        """Upload a locally partitioned directory tree. Returns files written.

        Used by the history extract, which fans one pass over the source into
        many day partitions at once.
        """
        count = 0
        for path in sorted(local_dir.rglob("*.parquet")):
            rel = path.relative_to(local_dir).as_posix()
            self.put_file(path, f"{key_prefix}/{rel}")
            count += 1
        return count


class LocalLandingZone(LandingZone):
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / key

    def put_file(self, local_path: Path, key: str) -> str:
        dest = self._path(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(local_path), dest)
        return str(dest)

    def uri_for(self, key: str) -> str:
        return str(self._path(key))

    def delete_prefix(self, prefix: str) -> int:
        target = self._path(prefix)
        if not target.exists():
            return 0
        removed = sum(1 for p in target.rglob("*") if p.is_file())
        shutil.rmtree(target)
        return removed

    def list_prefix(self, prefix: str) -> list[str]:
        target = self._path(prefix)
        if not target.exists():
            return []
        return sorted(str(p) for p in target.rglob("*") if p.is_file())


class GCSLandingZone(LandingZone):
    def __init__(self, uri: str) -> None:
        parsed = urlparse(uri)
        if parsed.scheme != "gs":
            raise ValueError(f"Expected a gs:// URI, got {uri!r}")
        self.bucket_name = parsed.netloc
        self.prefix = parsed.path.strip("/")

        try:
            from google.cloud import storage  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover - exercised only on GCP
            raise ImportError(
                "google-cloud-storage is required for LODESTAR_TARGET=gcp. "
                "Install with: pip install -e '.[gcp]'"
            ) from exc

        self._client = storage.Client()
        self._bucket = self._client.bucket(self.bucket_name)

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def put_file(self, local_path: Path, key: str) -> str:
        blob = self._bucket.blob(self._key(key))
        blob.upload_from_filename(str(local_path))
        local_path.unlink(missing_ok=True)
        return self.uri_for(key)

    def uri_for(self, key: str) -> str:
        return f"gs://{self.bucket_name}/{self._key(key)}"

    def delete_prefix(self, prefix: str) -> int:
        blobs = list(self._client.list_blobs(self.bucket_name, prefix=self._key(prefix)))
        for blob in blobs:
            blob.delete()
        return len(blobs)

    def list_prefix(self, prefix: str) -> list[str]:
        blobs = self._client.list_blobs(self.bucket_name, prefix=self._key(prefix))
        return sorted(f"gs://{self.bucket_name}/{b.name}" for b in blobs)


def get_landing_zone(uri: str) -> LandingZone:
    """Pick a backend from the URI scheme."""
    return GCSLandingZone(uri) if uri.startswith("gs://") else LocalLandingZone(uri)
