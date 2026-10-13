"""MinIO access via Airflow Connections (no hardcoded credentials).

Reads ``minio_s3`` connection (login/password/extra endpoint_url).
Falls back to env vars for local scripts/tests.
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

MINIO_CONN_ID = "minio_s3"
MINIO_BUCKET_DEFAULT = "organizacao"


def _conn_kwargs(conn_id: str = MINIO_CONN_ID) -> dict[str, Any]:
    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(conn_id)
        extra = conn.extra_dejson
        return {
            "endpoint": extra.get("endpoint_url", "http://minio:9000")
            .replace("http://", "")
            .replace("https://", ""),
            "access_key": conn.login or "",
            "secret_key": conn.password or "",
            "secure": str(extra.get("secure", "false")).lower() == "true",
        }
    except Exception:
        # Test fallback only: no real defaults (fail-fast without a Connection).
        return {
            "endpoint": os.environ.get("MINIO_LOCAL_DOMAIN", "minio:9000"),
            "access_key": os.environ.get("MINIO_ROOT_USER", ""),
            "secret_key": os.environ.get("MINIO_ROOT_PASSWORD", ""),
            "secure": os.environ.get("MINIO_SECURE", "False").lower() == "true",
        }


def get_client(conn_id: str = MINIO_CONN_ID) -> Any:
    """Return a MinIO client authenticated from the Airflow connection."""
    from minio import Minio

    kw = _conn_kwargs(conn_id)
    return Minio(
        kw["endpoint"], access_key=kw["access_key"], secret_key=kw["secret_key"], secure=kw["secure"]
    )


def bucket_name() -> str:
    """Lake bucket (Variable > env > default parity with Mage)."""
    value = ""
    try:
        try:
            # Airflow 3 SDK: get(key, default=...).
            from airflow.sdk import Variable

            fetched = Variable.get("minio_bucket_name", default="")
        except ImportError:
            # Airflow 2 / SDK antigo: get(key, default_var=...).
            from airflow.models import Variable as ClassicVariable

            fetched = ClassicVariable.get("minio_bucket_name", default_var="")
        if fetched:
            value = str(fetched).strip()
    except Exception:
        value = ""
    if value:
        return value
    return os.environ.get("MINIO_BUCKET_NAME", MINIO_BUCKET_DEFAULT)


def ensure_bucket(client: Any, bucket: str) -> None:
    """Create bucket if missing."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def file_exists(client: Any, bucket: str, key: str) -> bool:
    """Check object existence without raising."""
    try:
        client.stat_object(bucket, key)
        return True
    except Exception:
        return False


def has_success(client: Any, bucket: str, prefix: str, date_suffix: str) -> bool:
    """Check ``_control/_SUCCESS_<date>`` marker (Iceberg-safe subfolder)."""
    return file_exists(client, bucket, f"{prefix}/_control/_SUCCESS_{date_suffix}")


def write_success(client: Any, bucket: str, prefix: str, date_suffix: str) -> None:
    """Write ``_control/_SUCCESS_<date>`` marker."""
    ensure_bucket(client, bucket)
    fd, tmp = tempfile.mkstemp(suffix=".txt")
    try:
        with open(fd, "w") as handle:
            handle.write(f"completed_at={datetime.now().isoformat()}\n")
        client.fput_object(bucket, f"{prefix}/_control/_SUCCESS_{date_suffix}", tmp)
        log.info("marker=ok %s", prefix)
    finally:
        Path(tmp).unlink(missing_ok=True)


class MarkerStoreAdapter:
    """Adapt a MinIO client to the ``MarkerStore`` protocol (config module)."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def has_success(self, bucket: str, prefix: str, date_suffix: str) -> bool:
        """Delegate to :func:`has_success`."""
        return has_success(self._client, bucket, prefix, date_suffix)
