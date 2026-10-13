"""Postgres access via Airflow Connections (no hardcoded credentials)."""

from __future__ import annotations

from typing import Any

ORGANIZACAO_CONN_ID = "postgres_organizacao"


def pg_kwargs(conn_id: str = ORGANIZACAO_CONN_ID) -> dict[str, Any]:
    """Return psycopg2 kwargs from the Airflow connection (env fallback for tests)."""
    import os

    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(conn_id)
        return {
            "host": conn.host or "postgres",
            "port": int(conn.port or 5432),
            "dbname": conn.schema or "organizacaodb",
            "user": conn.login or "",
            "password": conn.password or "",
        }
    except Exception:
        # Test fallback only: no real defaults (fail-fast without a Connection).
        return {
            "host": os.environ.get("PG_HOST", "postgres"),
            "port": int(os.environ.get("PG_PORT", "5432")),
            "dbname": os.environ.get("PG_DB", "organizacaodb"),
            "user": os.environ.get("PG_USER", ""),
            "password": os.environ.get("PG_PASSWORD", ""),
        }


def connect(conn_id: str = ORGANIZACAO_CONN_ID) -> Any:
    """Open a psycopg2 connection from the Airflow connection."""
    import psycopg2

    return psycopg2.connect(**pg_kwargs(conn_id))
