"""Kolmeia gates: DAG integrity + unit tests (fake stores, no infra)."""

import ast
from pathlib import Path


def _dags() -> list[Path]:
    dags = sorted(Path("dags").glob("*.py"))
    assert dags, "sem DAGs"
    return dags


def test_dags_parse_and_ids_stable():
    """All DAGs parse; dag_ids unchanged (RBAC perms survive refactor)."""
    import re

    expected = {
        "bronze_rf_to_minio",
        "silver_zip_to_parquet",
        "postgres_carga",
        "gold_marts",
        "financeiro_dummy",
    }
    found = set()
    for path in _dags():
        src = path.read_text()
        ast.parse(src)
        assert "SubDAG" not in src
        found.update(re.findall(r'dag_id="([^"]+)"', src))
    assert expected <= found, f"dag_ids pulados: {expected - found}"


def test_dags_no_hardcoded_secrets_and_wired_triggers():
    """No plaintext creds in DAGs; chain triggers are wired (>>), not orphan."""
    for path in _dags():
        src = path.read_text()
        assert "organizacao_password" not in src, f"{path} tem senha hardcoded"
        assert "minioadmin123" not in src, f"{path} tem secret hardcoded"
    for dag_id in ("bronze_rf", "silver_parquet", "postgres_carga"):
        src = (Path("dags") / f"{dag_id}.py").read_text()
        assert ">> trigger_" in src or ">>trigger_" in src, f"{dag_id} com trigger órfão"


def test_runparams_smoke_full():
    """Smoke pins small slice; full expands (periodo validated)."""
    import sys

    sys.path.insert(0, "include")
    from kolmeia.config import RunParams

    smoke = RunParams.from_dag_params({"modo": "smoke", "periodo": "2026-09", "estado": "MA"})
    assert smoke.datasets == ("cnaes",)
    assert smoke.ufs == ("MA",)
    assert smoke.periodo == "2026-09"
    full = RunParams.from_dag_params({"modo": "full", "periodo": "", "estado": "MA,SP"})
    assert set(full.datasets) == {"cnaes", "empresas", "estabelecimentos", "socios"}
    assert full.ufs == ("MA", "SP")
    try:
        RunParams.from_dag_params({"modo": "smoke", "periodo": "09-2026", "estado": "MA"})
    except ValueError:
        pass
    else:
        raise AssertionError("periodo inválido aceito")


def test_periodo_latest_fallback():
    """Empty periodo resolves latest _SUCCESS (regression: smoke sem env quebrava)."""

    import sys

    sys.path.insert(0, "include")
    from kolmeia.config import get_last_day_of_month, resolve_periodo_or_latest

    class FakeStore:
        def __init__(self, months: set[str]) -> None:
            self.months = months

        def has_success(self, bucket: str, prefix: str, date_suffix: str) -> bool:
            return date_suffix in {get_last_day_of_month(m) for m in self.months}

    store = FakeStore({"2026-07", "2026-09"})
    assert resolve_periodo_or_latest(store, "b", "bronze/cnaes", {"periodo": "2026-09"}) == "2026-09"
    assert resolve_periodo_or_latest(store, "b", "bronze/cnaes", {}) == "2026-09"
    try:
        resolve_periodo_or_latest(store, "b", "bronze/cnaes", {"periodo": "2026-10"})
    except ValueError:
        pass
    else:
        raise AssertionError("periodo sem marker aceito")


def test_pg_sql_builder_escapes_periodo():
    """SQL builder escapes periodo quotes (injection-safe literal)."""
    import sys

    sys.path.insert(0, "include")
    from kolmeia.pg_load import build_dados_empresas_copy_sql

    sql = build_dados_empresas_copy_sql(
        estabelecimentos_parquet="/tmp/e.parquet",
        empresas_parquet="/tmp/p.parquet",
        cnaes_parquet="/tmp/c.parquet",
        output_parquet="/tmp/o.parquet",
        periodo="2026-01'; DROP--",
    )
    assert "2026-01''; DROP--" in sql
    assert "read_parquet" in sql


def test_marker_key_format():
    """Markers live under _control/ (Iceberg add_files safety)."""
    import sys

    sys.path.insert(0, "include")
    from kolmeia import io_minio

    calls: list[tuple[str, str, str]] = []

    class FakeClient:
        def fput_object(self, bucket: str, key: str, path: str) -> None:
            calls.append((bucket, key, path))

        def bucket_exists(self, bucket: str) -> bool:
            return True

    io_minio.write_success(FakeClient(), "organizacao", "silver/cnaes", "20260930")  # type: ignore[arg-type]
    assert calls[0][1] == "silver/cnaes/_control/_SUCCESS_20260930"


def test_pending_keys_idempotent():
    """Re-runs register nothing already registered (snapshot hygiene)."""
    import sys

    sys.path.insert(0, "include")
    from kolmeia.trino_meta import pending_keys

    assert pending_keys("", ["a", "b"]) == ["a", "b"]
    assert pending_keys("a,b", ["a", "b"]) == []
    assert pending_keys("a", ["a", "b", "b"]) == ["b"]
