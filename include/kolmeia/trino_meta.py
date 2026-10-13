"""Trino metadata registration: silver/gold Parquet -> Iceberg via PyIceberg.

Plain Parquet written by DuckDB becomes queryable in Trino without rewrite:
table is created once (explicit ``s3a://`` location so HMS + Trino share it)
and new files are appended. Failures warn (Mage non-strict parity).
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

WAREHOUSE_PREFIX = "s3a://organizacao/warehouse"


def warehouse_prefix() -> str:
    """Warehouse Iceberg derivado do bucket (fonte única: Variable minio_bucket_name)."""
    try:
        from kolmeia.io_minio import bucket_name

        return f"s3a://{bucket_name()}/warehouse"
    except Exception:
        return WAREHOUSE_PREFIX


def _catalog() -> Any:
    """Hive-backed PyIceberg catalog (Connection minio_s3 + Variable, env fallback for tests)."""
    import os

    from pyiceberg.catalog import load_catalog

    from kolmeia.io_minio import MINIO_CONN_ID

    try:
        from airflow.hooks.base import BaseHook

        try:
            # Airflow 3 SDK: get(key, default=...).
            from airflow.sdk import Variable

            uri = Variable.get("hive_metastore_uri", default="thrift://hive-metastore:9083")
        except ImportError:
            # Airflow 2 / SDK antigo: get(key, default_var=...).
            from airflow.models import Variable as ClassicVariable

            uri = ClassicVariable.get("hive_metastore_uri", default_var="thrift://hive-metastore:9083")
        minio_conn = BaseHook.get_connection(MINIO_CONN_ID)
        extra = minio_conn.extra_dejson
        endpoint = extra.get("endpoint_url", "http://minio:9000")
        access_key = minio_conn.login or ""
        secret_key = minio_conn.password or ""
    except Exception:
        uri = os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
        endpoint = os.environ.get("S3_ENDPOINT_URL", "http://minio:9000")
        access_key = os.environ.get("MINIO_ROOT_USER", "")
        secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "")

    return load_catalog(
        "hive",
        **{
            "uri": uri,
            "s3.endpoint": endpoint,
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
        },
    )


def register_parquet_prefix(
    schema: str, table: str, location: str, conn_id: str = "trino_default"
) -> dict[str, str]:
    """Register ``s3a://...`` prefix as Iceberg table (legacy entrypoint).

    ``location`` is kept for signature compatibility but the canonical table
    location lives under the warehouse; local files under the prefix are
    appended. Prefer :func:`register_local_parquets`.
    """
    _ = (location, conn_id)
    return {"status": "skipped", "reason": "use register_local_parquets"}


def pending_keys(registered_csv: str, keys: list[str]) -> list[str]:
    """Return keys not yet registered (pure, unit-tested)."""
    registered = {k.strip() for k in registered_csv.split(",") if k.strip()}
    seen: set[str] = set()
    pending: list[str] = []
    for key in keys:
        if key not in registered and key not in seen:
            seen.add(key)
            pending.append(key)
    return pending


def register_local_parquets(
    schema: str,
    table: str,
    local_paths: list[str],
    columns: list[str] | None = None,
    source_keys: list[str] | None = None,
) -> dict[str, Any]:
    """Create Iceberg table if missing and append only unregistered files.

    Idempotency comes from the ``kolmeia_sources`` table property listing
    already-registered MinIO keys — re-runs append nothing.
    Returns ``{"status": "success"|"skipped", ...}``.
    """
    try:
        import pyarrow.parquet as pq
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType

        cat = _catalog()
        full = f"{schema}.{table}"
        prefix = warehouse_prefix()
        if columns is None:
            columns = list(pq.read_schema(local_paths[0]).names)
        try:
            tbl = cat.load_table(full)
        except Exception:
            cat.create_namespace_if_not_exists(schema, {"location": f"{prefix}/{schema}/"})
            tbl = cat.create_table(
                full,
                schema=Schema(
                    *[
                        NestedField(i + 1, name, StringType(), required=False)
                        for i, name in enumerate(columns)
                    ]
                ),
                location=f"{prefix}/{schema}/{table}/",
            )
        total = 0
        pending_idx = [i for i, _ in enumerate(local_paths)]
        if source_keys is not None:
            registered = str(tbl.metadata.properties.get("kolmeia_sources", ""))
            keep = set(pending_keys(registered, source_keys))
            pending_idx = [i for i, k in enumerate(source_keys) if k in keep]
            if not pending_idx:
                return {"status": "skipped", "table": f"iceberg.{full}", "rows": 0}
        for i in pending_idx:
            arrow = pq.read_table(local_paths[i])
            tbl.append(arrow)
            total += arrow.num_rows
        if source_keys is not None:
            registered = str(tbl.metadata.properties.get("kolmeia_sources", ""))
            merged = sorted({*registered.split(","), *source_keys} - {""})
            tbl.transaction().set_properties({"kolmeia_sources": ",".join(merged)}).commit_transaction()
        return {"status": "success", "table": f"iceberg.{full}", "rows": total}
    except Exception as err:  # noqa: BLE001 - non-strict by design
        log.warning("trino_meta=skip table=%s.%s motivo=%s", schema, table, err)
        return {"status": "skipped", "reason": str(err)[:200]}
