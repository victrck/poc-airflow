"""DAG 2/4 Silver: bronze ZIP -> Parquet validado (modo smoke|full)."""

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, task

with DAG(
    dag_id="silver_zip_to_parquet",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"modo": "smoke", "periodo": "", "estado": "MA"},
    tags=["empresasbr", "silver"],
    max_active_tasks=4,
) as dag:

    @task(pool="heavy_duckdb")
    def silver_one(dataset: str, run_params: dict | None = None) -> dict:
        """Transform one dataset: resolve periodo (latest fallback) -> transform -> upload."""
        from kolmeia import io_minio
        from kolmeia.config import RunParams, get_last_day_of_month, resolve_periodo_or_latest
        from kolmeia.silver import silver_transform

        run = RunParams.from_dag_params(run_params)
        cli = io_minio.get_client()
        bucket = io_minio.bucket_name()
        periodo = run.periodo or resolve_periodo_or_latest(
            io_minio.MarkerStoreAdapter(cli), bucket, f"bronze/{dataset}", run_params
        )
        suffix = get_last_day_of_month(periodo)
        if io_minio.has_success(cli, bucket, f"silver/{dataset}", suffix):
            return {"dataset": dataset, "skipped": True, "periodo": periodo}

        def download_zips(tmpdir: Path) -> list[str]:
            outs = []
            for obj in cli.list_objects(bucket, prefix=f"bronze/{dataset}/", recursive=True):
                if obj.object_name.endswith(".zip"):
                    dst = tmpdir / obj.object_name.split("/")[-1]
                    cli.fget_object(bucket, obj.object_name, str(dst))
                    outs.append(str(dst))
            return outs

        def upload_file(local: str, key: str) -> None:
            cli.fput_object(bucket, key, local)

        from kolmeia.trino_meta import register_local_parquets

        result = silver_transform(dataset, periodo, suffix, list(run.ufs), download_zips, upload_file)
        from kolmeia.silver import COLUMNS

        register_local_parquets(
            "silver", f"meta_{dataset}", result["local_paths"], COLUMNS[dataset], result["keys"]
        )
        io_minio.write_success(cli, bucket, f"silver/{dataset}", suffix)
        return result

    @task
    def select_datasets(modo: str = "smoke") -> list[str]:
        """Slice by modo (shared with bronze)."""
        from kolmeia.config import FULL_DATASETS, SMOKE_DATASETS

        return list(SMOKE_DATASETS) if modo == "smoke" else list(FULL_DATASETS)

    chosen = select_datasets("{{ params.modo }}")
    done = silver_one.partial(
        run_params={
            "modo": "{{ params.modo }}",
            "periodo": "{{ params.periodo }}",
            "estado": "{{ params.estado }}",
        }
    ).expand(dataset=chosen)

    trigger_postgres = TriggerDagRunOperator(
        task_id="trigger_postgres",
        trigger_rule="none_failed",
        trigger_dag_id="postgres_carga",
        conf={
            "modo": "{{ params.modo }}",
            "periodo": "{{ params.periodo }}",
            "estado": "{{ params.estado }}",
        },
    )
    chosen >> done >> trigger_postgres
