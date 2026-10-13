"""DAG 4/4 Gold: silver -> gold/ma + Postgres gold.* (modo smoke|full)."""

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))

from airflow.sdk import DAG, task

with DAG(
    dag_id="gold_marts",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"modo": "smoke", "periodo": "", "estado": "MA"},
    tags=["empresasbr", "gold"],
) as dag:

    @task(pool="heavy_duckdb")
    def gold_ma(run_params: dict | None = None) -> dict:
        """Build MA mart (dual PG + MinIO). Full mode only."""
        import tempfile
        from pathlib import Path as _Path

        from kolmeia import io_minio, io_pg
        from kolmeia.config import RunParams, get_last_day_of_month, resolve_periodo_or_latest
        from kolmeia.gold import gold_ma as _gold
        from kolmeia.trino_meta import register_local_parquets

        run = RunParams.from_dag_params(run_params)
        cli = io_minio.get_client()
        bucket = io_minio.bucket_name()
        periodo = run.periodo or resolve_periodo_or_latest(
            io_minio.MarkerStoreAdapter(cli), bucket, "silver/estabelecimentos", run_params
        )
        suffix = get_last_day_of_month(periodo)
        tmpd = _Path(tempfile.mkdtemp(prefix="gold_dl_"))

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

        uf = (list(run.ufs) or ["MA"])[0]
        files = {
            "est": latest("silver/estabelecimentos/", f"estabelecimentos_{uf}_"),
            "emp": latest("silver/empresas/", "empresas_"),
            "soc": latest("silver/socios/", "socios_"),
            "cna": latest("silver/cnaes/", "cnaes_"),
        }
        result = _gold(files, periodo, suffix, io_pg.connect)
        cli.fput_object(bucket, result["gold_key"], result["local_path"])
        io_minio.write_success(cli, bucket, "gold/ma", suffix)
        register_local_parquets("gold", "meta_ma", [result["local_path"]], None, [result["gold_key"]])
        return {"periodo": periodo, "gold_key": result["gold_key"]}

    @task
    def check_silver_meta(run_params: dict | None = None) -> dict:
        """Smoke validation: silver meta tables readable in Trino with rows > 0."""
        from airflow.providers.trino.hooks.trino import TrinoHook
        from kolmeia.config import RunParams

        run = RunParams.from_dag_params(run_params)
        hook = TrinoHook(trino_conn_id="trino_default")
        rows = hook.get_records("SELECT count(*) FROM iceberg.silver.meta_cnaes")
        count = int(rows[0][0])
        if count <= 0:
            raise ValueError("meta_cnaes vazia")
        return {"modo": run.modo, "meta_cnaes_rows": count}

    @task.branch
    def choose_path(modo: str = "smoke") -> str:
        """Smoke runs the Trino check; full builds the MA mart."""
        return "check_silver_meta" if modo == "smoke" else "gold_ma"

    RP = {"modo": "{{ params.modo }}", "periodo": "{{ params.periodo }}", "estado": "{{ params.estado }}"}
    branch = choose_path("{{ params.modo }}")
    check = check_silver_meta(RP)
    mart = gold_ma(RP)
    branch >> [check, mart]
