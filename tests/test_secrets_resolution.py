"""Secrets hygiene: execution reads Connections/Variables, never hardcoded secrets.

Scope (2026-10-11): only what DAG execution consumes — minio_s3,
postgres_organizacao, trino_default + minio_bucket_name/hive_metastore_uri/
processing_* Variables. Code fallbacks carry no real defaults (fail-fast).
"""

import sys
from pathlib import Path


def test_no_secret_defaults_in_execution_code():
    """No real credential defaults in dags/ or include/ (fail-fast instead)."""
    banned = ["minioadmin123", "organizacao_password"]
    roots = [Path("dags"), Path("include")]
    offenders = []
    for root in roots:
        for path in sorted(root.rglob("*.py")):
            text = path.read_text()
            for secret in banned:
                if secret in text:
                    offenders.append(f"{path}:{secret}")
    assert not offenders, f"secret defaults em código: {offenders}"


def test_bronze_uses_conn_not_legacy_variables():
    """Bronze container env comes from minio_s3 Connection (no minio_root_* Variables)."""
    text = Path("dags/bronze_rf.py").read_text()
    assert "conn.minio_s3.login" in text
    assert "conn.minio_s3.password" in text
    assert "var.value.minio_root_user" not in text
    assert "var.value.minio_root_password" not in text


def test_resolve_ufs_env_fallback(monkeypatch):
    """Without airflow installed, PROCESSING_ESTADO env still resolves (fallback)."""
    sys.path.insert(0, "include")
    assert "airflow" not in sys.modules, "teste espera airflow ausente"
    from kolmeia.config import resolve_ufs

    monkeypatch.setenv("PROCESSING_ESTADO", "MA")
    assert resolve_ufs({}) == ["MA"]
    monkeypatch.delenv("PROCESSING_ESTADO")
    from kolmeia.config import DEFAULT_UFS_BRASIL

    assert resolve_ufs({}) == list(DEFAULT_UFS_BRASIL)


def test_variable_wins_over_env(monkeypatch):
    """Airflow Variable overrides env when airflow is importable (fake module)."""
    import types

    sys.path.insert(0, "include")

    fake_sdk = types.ModuleType("airflow.sdk")

    class FakeVariable:
        @staticmethod
        def get(key: str, default: str = "") -> str:
            assert (key, default) == ("processing_estado", "")
            return "SP"

    fake_sdk.Variable = FakeVariable  # type: ignore[attr-defined]
    fake_airflow = types.ModuleType("airflow")
    monkeypatch.setitem(sys.modules, "airflow", fake_airflow)
    monkeypatch.setitem(sys.modules, "airflow.sdk", fake_sdk)
    monkeypatch.setenv("PROCESSING_ESTADO", "MA")

    import importlib

    import kolmeia.config as config

    importlib.reload(config)
    try:
        assert config.resolve_ufs({}) == ["SP"]
    finally:
        importlib.reload(config)


def test_bucket_name_env_fallback(monkeypatch):
    """bucket_name honors env when airflow is absent."""
    sys.path.insert(0, "include")
    assert "airflow" not in sys.modules, "teste espera airflow ausente"
    from kolmeia.io_minio import bucket_name

    monkeypatch.setenv("MINIO_BUCKET_NAME", "meu_bucket")
    assert bucket_name() == "meu_bucket"
    monkeypatch.delenv("MINIO_BUCKET_NAME")
    assert bucket_name() == "organizacao"


def test_no_hardcoded_execution_hosts():
    """Fonte única: sem host/bucket/warehouse literais no código de execução."""
    bronze = Path("dags/bronze_rf.py").read_text()
    assert '"minio:9000"' not in bronze, "endpoint deve vir de conn.minio_s3.extra_dejson"
    assert "conn.minio_s3.extra_dejson.endpoint_url" in bronze
    meta = Path("include/kolmeia/trino_meta.py").read_text()
    assert "warehouse_prefix()" in meta
    assert meta.count("WAREHOUSE_PREFIX") <= 3, "usos diretos devem migrar p/ warehouse_prefix()"


def test_warehouse_prefix_derives_from_bucket(monkeypatch):
    """warehouse_prefix espelha o bucket (troca o bucket e o prefixo acompanha)."""
    import types

    sys.path.insert(0, "include")

    fake_sdk = types.ModuleType("airflow.sdk")

    class FakeVariable:
        @staticmethod
        def get(key: str, default: str = "") -> str:
            return {"minio_bucket_name": "outro_bucket"}.get(key, default)

    fake_sdk.Variable = FakeVariable  # type: ignore[attr-defined]
    fake_airflow = types.ModuleType("airflow")
    monkeypatch.setitem(sys.modules, "airflow", fake_airflow)
    monkeypatch.setitem(sys.modules, "airflow.sdk", fake_sdk)

    import importlib

    import kolmeia.trino_meta as meta

    importlib.reload(meta)
    try:
        assert meta.warehouse_prefix() == "s3a://outro_bucket/warehouse"
    finally:
        importlib.reload(meta)


def test_duckdb_settings_defaults_and_overrides(monkeypatch):
    """duckdb_settings: defaults 2GB/2, aceita '2.0'/'2GB', Variable vence env."""
    import types

    sys.path.insert(0, "include")
    assert "airflow" not in sys.modules, "teste espera airflow ausente"

    import importlib

    import kolmeia.config as config

    importlib.reload(config)
    monkeypatch.delenv("DUCKDB_MEMORY_LIMIT_GB", raising=False)
    monkeypatch.delenv("DUCKDB_THREADS", raising=False)
    assert config.duckdb_settings() == ("2GB", 2)
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT_GB", "4.0")
    monkeypatch.setenv("DUCKDB_THREADS", "4")
    assert config.duckdb_settings() == ("4GB", 4)
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT_GB", "lixo")
    assert config.duckdb_settings()[0] == "2GB"

    fake_sdk = types.ModuleType("airflow.sdk")

    class FakeVariable:
        @staticmethod
        def get(key: str, default: str = "") -> str:
            return {"duckdb_memory_limit": "8GB", "duckdb_threads": "6"}.get(key, default)

    fake_sdk.Variable = FakeVariable  # type: ignore[attr-defined]
    fake_airflow = types.ModuleType("airflow")
    monkeypatch.setitem(sys.modules, "airflow", fake_airflow)
    monkeypatch.setitem(sys.modules, "airflow.sdk", fake_sdk)
    importlib.reload(config)
    try:
        assert config.duckdb_settings() == ("8GB", 6)
    finally:
        importlib.reload(config)
