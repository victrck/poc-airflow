"""Gold MA mart: silver JOIN -> Postgres gold.* + denormalized MinIO Parquet."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def gold_ma(files: dict[str, str], periodo: str, date_suffix: str, connect: Any) -> dict[str, Any]:
    """Build MA mart. ``files`` keys: est, emp, soc, cna. Returns counts + gold key."""
    import duckdb

    tmpd = Path(tempfile.mkdtemp(prefix="gold_ma_"))
    est, emp, soc, cna = files["est"], files["emp"], files["soc"], files["cna"]
    from kolmeia.config import duckdb_settings

    mem, thr = duckdb_settings()
    conn = duckdb.connect()
    try:
        conn.execute(f"SET memory_limit='{mem}'")
        conn.execute(f"SET threads={thr}")
        conn.execute(f"CREATE TEMP TABLE ma_cnpj AS SELECT DISTINCT cnpj_basico FROM read_parquet('{est}')")
        conn.execute(
            "CREATE TEMP TABLE ma_empresas AS SELECT cnpj_basico, razao_social, natureza_juridica, "
            "qualificacao_responsavel, CAST(REPLACE(capital_social, ',', '.') AS DOUBLE) "
            f"AS capital_social, porte_empresa, ente_federativo FROM read_parquet('{emp}') e "
            "WHERE EXISTS (SELECT 1 FROM ma_cnpj m WHERE m.cnpj_basico = e.cnpj_basico)"
        )
        conn.execute(
            "CREATE TEMP TABLE ma_socios AS SELECT s.* "
            f"FROM read_parquet('{soc}') s "
            "WHERE EXISTS (SELECT 1 FROM ma_cnpj m WHERE m.cnpj_basico = s.cnpj_basico)"
        )
        emp_csv = tmpd / "emp.csv"
        conn.execute(f"COPY ma_empresas TO '{emp_csv}' (HEADER false, DELIMITER ';')")
        gold_pq = tmpd / f"gold_ma_{date_suffix}.parquet"
        conn.execute(
            "COPY (SELECT est.*, emp.razao_social, emp.natureza_juridica, emp.capital_social, "
            "emp.porte_empresa, emp.ente_federativo, cnae.descricao AS cnae_descricao "
            f"FROM read_parquet('{est}') est LEFT JOIN ma_empresas emp "
            "ON est.cnpj_basico = emp.cnpj_basico "
            f"LEFT JOIN read_parquet('{cna}') cnae ON est.cnae_fiscal_principal = cnae.codigo) "
            f"TO '{gold_pq}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        row = conn.execute("SELECT count(*) FROM ma_empresas").fetchone()
        n_emp = int(row[0]) if row else 0
    finally:
        conn.close()

    db = connect()
    try:
        cur = db.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS gold.empresas_ma (cnpj_basico TEXT, razao_social TEXT, "
            "natureza_juridica TEXT, qualificacao_responsavel TEXT, "
            "capital_social DOUBLE PRECISION, porte_empresa TEXT, ente_federativo TEXT)"
        )
        cur.execute("TRUNCATE gold.empresas_ma")
        with open(emp_csv) as handle:
            cur.copy_expert("COPY gold.empresas_ma FROM STDIN DELIMITER ';' CSV", handle)
        db.commit()
    finally:
        db.close()
    key = f"gold/ma/gold_ma_{date_suffix}.parquet"
    log.info("gold_ma periodo=%s emp=%d key=%s", periodo, n_emp, key)
    return {"periodo": periodo, "gold_key": key, "local_path": str(gold_pq), "n_emp": n_emp}
