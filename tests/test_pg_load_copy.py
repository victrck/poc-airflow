"""pg_load COPY path: per-UF cadastro streams via CSV (no full materialization).

Regression (2026-10-11, postgres_carga full 27 UFs): ``to_pylist`` +
``executemany`` OOM-killed big UFs (SP ~8M rows as Python dicts).
``load_dados_empresas`` now writes CSV with DuckDB and streams it with
``COPY ... FROM STDIN``, keeping Python-side memory flat.
"""

import sys
from pathlib import Path


def _tiny_parquets(tmp_path: Path) -> dict[str, str]:
    """Minimal silver-like parquets covering every column the JOIN reads."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    sys.path.insert(0, "include")
    from kolmeia.silver import COLUMNS

    def table(dataset: str, n: int, fill: str) -> pa.Table:
        cols = {c: [f"{fill}-{c}-{i}" for i in range(n)] for c in COLUMNS[dataset]}
        return pa.table(cols)

    files = {}
    specs = {
        "estabelecimentos": ("est", 5, "e"),
        "empresas": ("emp", 3, "p"),
        "cnaes": ("cna", 1, "c"),  # 1 row: LEFT JOIN must not multiply (5x3x1=15)
    }
    for dataset, (tag, n, fill) in specs.items():
        path = tmp_path / f"{tag}.parquet"
        # JOIN keys must match so INNER JOIN keeps rows.
        t = table(dataset, n, fill)
        if dataset in ("estabelecimentos", "empresas"):
            idx = t.schema.get_field_index("cnpj_basico")
            t = t.set_column(idx, "cnpj_basico", pa.array(["00000001"] * n))
        if dataset == "estabelecimentos":
            for col, val in (("cnpj_ordem", "0001"), ("cnpj_dv", "00"), ("uf", "ZZ")):
                idx = t.schema.get_field_index(col)
                t = t.set_column(idx, col, pa.array([val] * n))
            idx = t.schema.get_field_index("cnae_fiscal_principal")
            t = t.set_column(idx, "cnae_fiscal_principal", pa.array(["0000000"] * n))
        if dataset == "cnaes":
            idx = t.schema.get_field_index("codigo")
            t = t.set_column(idx, "codigo", pa.array(["0000000"] * n))
        pq.write_table(t, path)
        files[dataset] = str(path)
    return files


class _FakeCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.copied_rows: int = 0
        self.delete_args: tuple = ()

    def execute(self, sql: str, args: tuple = ()) -> None:
        self.statements.append(sql)
        if sql.startswith("DELETE FROM"):
            self.delete_args = args

    def copy_expert(self, sql: str, handle) -> None:
        assert "FROM STDIN" in sql
        assert "DELIMITER ';'" in sql
        self.copied_rows = sum(1 for _ in handle)


class _FakeDB:
    def __init__(self) -> None:
        self.cur = _FakeCursor()
        self.committed = False

    def cursor(self) -> _FakeCursor:
        return self.cur

    def commit(self) -> None:
        self.committed = True

    def close(self) -> None:
        pass


def test_load_dados_empresas_copy_streams(tmp_path):
    sys.path.insert(0, "include")
    from kolmeia.pg_load import load_dados_empresas

    files = _tiny_parquets(tmp_path)
    fake = _FakeDB()

    n = load_dados_empresas("ZZ", files, "2026-09", lambda: fake)

    assert n == 15
    assert fake.cur.copied_rows == 15
    assert fake.cur.delete_args == ("ZZ", "2026-09")
    assert fake.committed


def test_csv_builder_escapes_periodo():
    """CSV variant escapes periodo quotes like the parquet builder."""
    sys.path.insert(0, "include")
    from kolmeia.pg_load import build_dados_empresas_csv_sql

    sql = build_dados_empresas_csv_sql(
        estabelecimentos_parquet="/tmp/e.parquet",
        empresas_parquet="/tmp/p.parquet",
        cnaes_parquet="/tmp/c.parquet",
        output_csv="/tmp/o.csv",
        periodo="2026-01'; DROP--",
    )
    assert "2026-01''; DROP--" in sql
    assert "HEADER false" in sql
