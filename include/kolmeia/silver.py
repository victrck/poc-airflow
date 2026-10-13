"""Silver transform: bronze ZIP -> validated Parquet (ported from Mage bronze_to_silver_core).

Pure-ish functions taking explicit paths/clients — testable without MinIO.
DuckDB stays local (2GB/2 threads, fresh connection per UF split).
"""

from __future__ import annotations

import gc
import logging
import tempfile
import zipfile
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

COLUMNS: dict[str, list[str]] = {
    "cnaes": ["codigo", "descricao"],
    "empresas": [
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo",
    ],
    "estabelecimentos": [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ],
    "socios": [
        "cnpj_basico",
        "identificador_socio",
        "nome_socio",
        "cnpj_cpf_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "cpf_representante_legal",
        "nome_representante",
        "qualificacao_representante",
        "faixa_etaria",
    ],
}

REJECT_WHERE: dict[str, str] = {
    "cnaes": "length(codigo)=7",
    "empresas": "length(cnpj_basico)=8",
    "estabelecimentos": "length(cnpj_basico)=8 AND identificador_matriz_filial IN ('1','2')",
    "socios": "length(cnpj_basico)=8 AND identificador_socio IN ('1','2','3')",
}


def extract_zips_to_utf8(zip_paths: list[Path], workdir: Path) -> list[Path]:
    """Extract first CSV of each ZIP re-encoded ISO-8859-1 -> UTF-8."""
    csvs: list[Path] = []
    for zip_path in sorted(zip_paths):
        with zipfile.ZipFile(zip_path) as zf:
            inner = zf.namelist()[0]
            out = workdir / f"{inner}.utf8"
            with zf.open(inner) as src, open(out, "w", encoding="utf-8") as dst:
                while chunk := src.read(10 * 1024 * 1024):
                    dst.write(chunk.decode("iso-8859-1", errors="replace"))
        zip_path.unlink(missing_ok=True)
        csvs.append(out)
        gc.collect()
    return csvs


def csvs_to_parquet(csvs: list[Path], output: Path, columns: list[str], batch_rows: int = 250_000) -> int:
    """Convert CSVs to one Parquet (Polars batched + ZSTD). Returns row count."""
    import polars as pl
    import pyarrow.parquet as pq

    parts: list[Path] = []
    try:
        for idx, csv in enumerate(csvs):
            part = output.with_suffix(f".part{idx}.parquet")
            reader = pl.read_csv_batched(
                csv,
                separator=";",
                has_header=False,
                encoding="utf8-lossy",
                new_columns=columns,
                infer_schema_length=0,
                ignore_errors=True,
                batch_size=batch_rows,
            )
            writer = None
            try:
                while batches := reader.next_batches(4):
                    for batch in batches:
                        table = batch.to_arrow()
                        if writer is None:
                            writer = pq.ParquetWriter(str(part), table.schema, compression="zstd")
                        writer.write_table(table)
            finally:
                if writer is not None:
                    writer.close()
            csv.unlink(missing_ok=True)
            parts.append(part)
            gc.collect()
        import duckdb

        # Lista explícita: read_parquet('a','b',...) NÃO existe (só VARCHAR
        # único ou VARCHAR[]); multi-zip (full) quebrava com BinderException.
        union = "[" + ", ".join(f"'{p}'" for p in parts) + "]"
        import os

        spill = os.environ.get("DUCKDB_TEMP_DIR", "/tmp/mage/duckdb_tmp")
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
            conn.execute(f"COPY (SELECT * FROM read_parquet({union})) TO '{output}' (FORMAT PARQUET)")
        finally:
            conn.close()
        return int(pl.scan_parquet(str(output)).select(pl.len()).collect().item())
    finally:
        for part in parts:
            part.unlink(missing_ok=True)


def apply_reject_where(input_path: Path, output_path: Path, where: str) -> None:
    """Filter combined Parquet with DuckDB ``WHERE`` (cheap, columnar)."""
    import duckdb

    from kolmeia.config import duckdb_settings

    mem, thr = duckdb_settings()
    conn = duckdb.connect()
    try:
        conn.execute(f"SET memory_limit='{mem}'")
        conn.execute(f"SET threads={thr}")
        conn.execute(
            f"COPY (SELECT * FROM read_parquet('{input_path}') WHERE {where}) "
            f"TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        conn.close()


def split_by_uf(input_path: Path, outdir: Path, prefix: str, date_suffix: str, ufs: list[str]) -> list[Path]:
    """Split Parquet per UF with a fresh DuckDB connection each (resets buffer manager)."""
    import duckdb

    outdir.mkdir(parents=True, exist_ok=True)
    outs: list[Path] = []
    from kolmeia.config import duckdb_settings

    mem, thr = duckdb_settings()
    for uf in ufs:
        out = outdir / f"{prefix}_{uf}_{date_suffix}.parquet"
        conn = duckdb.connect()
        try:
            conn.execute(f"SET memory_limit='{mem}'")
            conn.execute(f"SET threads={thr}")
            conn.execute(
                f"COPY (SELECT * FROM read_parquet('{input_path}') WHERE uf = '{uf}') "
                f"TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        finally:
            conn.close()
        outs.append(out)
    return outs


def silver_transform(
    dataset: str,
    periodo: str,
    date_suffix: str,
    ufs: list[str],
    download_zips: Any,
    upload_file: Any,
) -> dict[str, Any]:
    """Full silver pipeline for one dataset.

    ``download_zips`` returns local ZIP paths; ``upload_file(local, key)`` uploads.
    Returns ``{"dataset", "periodo", "files"}``.
    """
    tmp = Path(tempfile.mkdtemp(prefix=f"{dataset}_silver_"))
    zips = download_zips(tmp)
    if not zips:
        raise ValueError(f"sem ZIP em bronze/{dataset}")
    csvs = extract_zips_to_utf8([Path(z) for z in zips], tmp)
    combined = tmp / f"{dataset}_all_{date_suffix}.parquet"
    rows = csvs_to_parquet(csvs, combined, COLUMNS[dataset])
    filtered = tmp / f"{dataset}_filt_{date_suffix}.parquet"
    apply_reject_where(combined, filtered, REJECT_WHERE[dataset])
    combined.unlink(missing_ok=True)

    keys: list[str] = []
    local_paths: list[str] = []
    if dataset == "estabelecimentos":
        for out in split_by_uf(filtered, tmp, "estabelecimentos", date_suffix, ufs):
            key = f"silver/estabelecimentos/{out.name}"
            upload_file(str(out), key)
            keys.append(key)
            local_paths.append(str(out))
    else:
        final = tmp / f"{dataset}_{date_suffix}.parquet"
        filtered.rename(final)
        key = f"silver/{dataset}/{final.name}"
        upload_file(str(final), key)
        keys.append(key)
        local_paths.append(str(final))
    log.info("silver=%s rows=%d files=%d", dataset, rows, len(keys))
    return {
        "dataset": dataset,
        "periodo": periodo,
        "files": len(keys),
        "rows": rows,
        "local_paths": local_paths,
        "keys": keys,
    }
