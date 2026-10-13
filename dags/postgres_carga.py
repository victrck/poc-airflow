"""DAG 3/4 Postgres: silver -> organizacao.* (modo smoke|full, creds via Connections)."""

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, task

with DAG(
    dag_id="postgres_carga",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"modo": "smoke", "periodo": "", "estado": "MA"},
    tags=["empresasbr", "postgres"],
) as dag:

    @task
    def load_cnaes(run_params: dict | None = None) -> dict:
        """Load cnaes silver (latest fallback) into organizacao.cnaes."""
        from kolmeia import io_minio, io_pg
        from kolmeia.config import RunParams, get_last_day_of_month, resolve_periodo_or_latest
        from kolmeia.pg_load import load_cnaes as _load

        run = RunParams.from_dag_params(run_params)
        cli = io_minio.get_client()
        bucket = io_minio.bucket_name()
        periodo = run.periodo or resolve_periodo_or_latest(
            io_minio.MarkerStoreAdapter(cli), bucket, "silver/cnaes", run_params
        )
        suffix = get_last_day_of_month(periodo)
        objs = [
            o.object_name
            for o in cli.list_objects(bucket, prefix="silver/cnaes/", recursive=True)
            if o.object_name.endswith(".parquet") and suffix in o.object_name
        ]
        if not objs:
            raise ValueError(f"sem silver cnaes para {periodo}")
        import tempfile

        local = str(Path(tempfile.mkdtemp(prefix="cnaes_dl_")) / "cnaes.parquet")
        cli.fget_object(bucket, sorted(objs)[-1], local)
        rows = _load(local, periodo, io_pg.connect)
        return {"rows": rows, "periodo": periodo}

    @task(pool="heavy_duckdb")
    def load_dados_empresas(uf: str, run_params: dict | None = None) -> dict:
        """Load per-UF cadastro (silver join) into organizacao.dados_empresas."""
        import tempfile
        from pathlib import Path as _Path

        from kolmeia import io_minio, io_pg
        from kolmeia.config import RunParams, get_last_day_of_month, resolve_periodo_or_latest
        from kolmeia.pg_load import load_dados_empresas as _load

        run = RunParams.from_dag_params(run_params)
        cli = io_minio.get_client()
        bucket = io_minio.bucket_name()
        periodo = run.periodo or resolve_periodo_or_latest(
            io_minio.MarkerStoreAdapter(cli), bucket, "silver/estabelecimentos", run_params
        )
        suffix = get_last_day_of_month(periodo)
        tmpd = _Path(tempfile.mkdtemp(prefix=f"dados_{uf}_"))

        def latest(prefix: str, frag: str) -> str:
            cands = [
                o.object_name
                for o in cli.list_objects(bucket, prefix=prefix, recursive=True)
                if o.object_name.endswith(".parquet") and frag in o.object_name and suffix in o.object_name
            ]
            if not cands:
                raise ValueError(f"sem {prefix}{frag} para {periodo}")
            dst = tmpd / f"{frag.strip('_')}.parquet"
            cli.fget_object(bucket, sorted(cands)[-1], str(dst))
            return str(dst)

        files = {
            "estabelecimentos": latest("silver/estabelecimentos/", f"estabelecimentos_{uf}_"),
            "empresas": latest("silver/empresas/", "empresas_"),
            "cnaes": latest("silver/cnaes/", "cnaes_"),
        }
        rows = _load(uf, files, periodo, io_pg.connect)
        return {"uf": uf, "rows": rows}

    @task
    def select_ufs(modo: str = "smoke", estado: str = "MA") -> list[str]:
        """UF slice: smoke skips dados_empresas (cnaes-only validation); full expands."""
        from kolmeia.config import resolve_ufs

        if modo == "smoke":
            return []
        return resolve_ufs({"estado": estado})

    RP = {"modo": "{{ params.modo }}", "periodo": "{{ params.periodo }}", "estado": "{{ params.estado }}"}
    cnaes = load_cnaes(RP)
    ufs = select_ufs("{{ params.modo }}", "{{ params.estado }}")
    mapped = load_dados_empresas.partial(run_params=RP).expand(uf=ufs)
    cnaes >> mapped

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_rule="none_failed",
        trigger_dag_id="gold_marts",
        conf=RP,
    )
    mapped >> trigger_gold
