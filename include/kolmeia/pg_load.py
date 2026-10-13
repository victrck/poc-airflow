"""Postgres loaders: silver Parquet -> organizacao.* (typed, connection-based)."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

DADOS_EMPRESAS_COLS: list[str] = [
    "cnpj_basico",
    "cnpj",
    "identificador_matriz_filial",
    "razao_social",
    "nome_fantasia",
    "natureza_juridica",
    "porte_empresa",
    "capital_social",
    "qualificacao_responsavel",
    "ente_federativo",
    "situacao_cadastral",
    "data_inicio_atividade",
    "cnae_principal",
    "cnae_fiscal_secundaria",
    "uf",
    "qtd_estabelecimentos",
    "periodo",
]


def build_dados_empresas_copy_sql(
    *,
    estabelecimentos_parquet: str,
    empresas_parquet: str,
    cnaes_parquet: str,
    output_parquet: str,
    periodo: str,
) -> str:
    """Build per-UF cadastro SQL (ported verbatim from Mage empresas_sql)."""
    query = _dados_empresas_query(
        estabelecimentos_parquet=estabelecimentos_parquet,
        empresas_parquet=empresas_parquet,
        cnaes_parquet=cnaes_parquet,
        periodo=periodo,
    )
    return f"COPY ({query}) TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION SNAPPY)"


def build_dados_empresas_csv_sql(
    *,
    estabelecimentos_parquet: str,
    empresas_parquet: str,
    cnaes_parquet: str,
    output_csv: str,
    periodo: str,
) -> str:
    """Same SELECT as :func:`build_dados_empresas_copy_sql`, streamed as CSV.

    Column order matches ``DADOS_EMPRESAS_COLS``; ``HEADER false`` + ``;``
    delimiter pair with ``COPY ... FROM STDIN DELIMITER ';' CSV`` on the
    Postgres side (quoting/NULL defaults are compatible on both ends).
    """
    query = _dados_empresas_query(
        estabelecimentos_parquet=estabelecimentos_parquet,
        empresas_parquet=empresas_parquet,
        cnaes_parquet=cnaes_parquet,
        periodo=periodo,
    )
    return f"COPY ({query}) TO '{output_csv}' (HEADER false, DELIMITER ';')"


def _dados_empresas_query(
    *,
    estabelecimentos_parquet: str,
    empresas_parquet: str,
    cnaes_parquet: str,
    periodo: str,
) -> str:
    """Shared SELECT core (column order == DADOS_EMPRESAS_COLS)."""
    lit = "'" + periodo.replace("'", "''") + "'"
    date = "TRY_CAST(TRY_STRPTIME(NULLIF(et.data_inicio_atividade, ''), '%Y%m%d') AS DATE)"
    return f"""
        SELECT
            et.cnpj_basico,
            et.cnpj_basico || et.cnpj_ordem || et.cnpj_dv AS cnpj,
            et.identificador_matriz_filial,
            ep.razao_social,
            et.nome_fantasia,
            ep.natureza_juridica,
            ep.porte_empresa,
            TRY_CAST(REPLACE(ep.capital_social, ',', '.') AS DOUBLE) AS capital_social,
            ep.qualificacao_responsavel,
            ep.ente_federativo,
            et.situacao_cadastral,
            {date} AS data_inicio_atividade,
            cn.descricao AS cnae_principal,
            et.cnae_fiscal_secundaria,
            et.uf,
            CAST(count(*) OVER (PARTITION BY et.cnpj_basico) AS INTEGER) AS qtd_estabelecimentos,
            {lit} AS periodo
        FROM read_parquet('{estabelecimentos_parquet}') AS et
        INNER JOIN read_parquet('{empresas_parquet}') AS ep
            ON ep.cnpj_basico = et.cnpj_basico
        LEFT JOIN read_parquet('{cnaes_parquet}') AS cn
            ON cn.codigo = et.cnae_fiscal_principal
    """


def load_cnaes(silver_path: str, periodo: str, connect: Any) -> int:
    """Normalize silver cnaes Parquet and TRUNCATE+insert. Returns row count."""
    import duckdb
    import pyarrow.parquet as pq

    tmp = Path(tempfile.mkdtemp(prefix="cnaes_pg_"))
    out = tmp / "cnaes_norm.parquet"
    conn = duckdb.connect()
    try:
        conn.execute(
            "COPY (SELECT CAST(codigo AS VARCHAR) AS codigo, CAST(descricao AS VARCHAR) AS descricao "
            f"FROM read_parquet('{silver_path}') WHERE codigo IS NOT NULL) "
            f"TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
    finally:
        conn.close()
    rows = [(r["codigo"], r["descricao"], periodo) for r in pq.read_table(str(out)).to_pylist()]
    db = connect()
    try:
        cur = db.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS organizacao")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS organizacao.cnaes " "(codigo TEXT, descricao TEXT, periodo TEXT)"
        )
        cur.execute("TRUNCATE organizacao.cnaes")
        cur.executemany("INSERT INTO organizacao.cnaes VALUES (%s,%s,%s)", rows)
        db.commit()
    finally:
        db.close()
    return len(rows)


def load_dados_empresas(uf: str, files: dict[str, str], periodo: str, connect: Any) -> int:
    """Build per-UF cadastro CSV and DELETE+COPY. Returns row count.

    Streaming: DuckDB writes CSV to disk and Postgres ``COPY FROM STDIN``
    consumes it without ever materializing all rows in Python (the old
    ``to_pylist`` + ``executemany`` OOM-killed big UFs like SP).
    """
    import os

    import duckdb

    tmp = Path(tempfile.mkdtemp(prefix=f"dados_{uf}_"))
    out = tmp / f"dados_{uf}.csv"
    sql = build_dados_empresas_csv_sql(
        estabelecimentos_parquet=files["estabelecimentos"],
        empresas_parquet=files["empresas"],
        cnaes_parquet=files["cnaes"],
        output_csv=str(out),
        periodo=periodo,
    )
    spill = os.environ.get("DUCKDB_TEMP_DIR", str(Path(tempfile.gettempdir()) / "duckdb_tmp"))
    try:
        Path(spill).mkdir(parents=True, exist_ok=True)
    except OSError:
        spill = tempfile.gettempdir()
    conn = duckdb.connect()
    try:
        from kolmeia.config import duckdb_settings

        _mem, _thr = duckdb_settings()
        conn.execute(f"SET memory_limit='{_mem}'")
        conn.execute(f"SET threads={_thr}")
        conn.execute(f"SET temp_directory='{spill}'")
        conn.execute(sql)
        if out.stat().st_size == 0:
            count = 0
        else:
            row = conn.execute(f"SELECT count(*) FROM read_csv('{out}', header=false, delim=';')").fetchone()
            count = int(row[0]) if row else 0
    finally:
        conn.close()
    cols = ", ".join(DADOS_EMPRESAS_COLS)
    db = connect()
    try:
        cur = db.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS organizacao.dados_empresas (cnpj_basico TEXT, cnpj TEXT, "
            "identificador_matriz_filial TEXT, razao_social TEXT, nome_fantasia TEXT, "
            "natureza_juridica TEXT, porte_empresa TEXT, capital_social DOUBLE PRECISION, "
            "qualificacao_responsavel TEXT, ente_federativo TEXT, situacao_cadastral TEXT, "
            "data_inicio_atividade DATE, cnae_principal TEXT, cnae_fiscal_secundaria TEXT, "
            "uf TEXT, qtd_estabelecimentos INT, periodo TEXT)"
        )
        cur.execute("DELETE FROM organizacao.dados_empresas WHERE uf=%s AND periodo=%s", (uf, periodo))
        with open(out) as handle:
            cur.copy_expert(
                f"COPY organizacao.dados_empresas ({cols}) FROM STDIN DELIMITER ';' CSV",
                handle,
            )
        db.commit()
    finally:
        db.close()
    out.unlink(missing_ok=True)
    return count
