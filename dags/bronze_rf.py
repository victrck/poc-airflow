"""DAG 1/4 Bronze: RF -> MinIO via DockerOperator efêmero (modo smoke|full)."""

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "include"))

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, task
from docker.types import Mount
from kolmeia.config import FULL_DATASETS, SMOKE_DATASETS

with DAG(
    dag_id="bronze_rf_to_minio",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"modo": "smoke", "periodo": "", "estado": "MA"},
    tags=["empresasbr", "bronze"],
    doc_md="Bronze RAW ZIPs da RF para s3://organizacao/bronze/<ds>/ + _SUCCESS.",
    max_active_tasks=4,
) as dag:

    @task
    def select_commands(modo: str = "smoke", periodo: str = "", estado: str = "MA") -> list[str]:
        """Build container args (image entrypoint is already python /app/script.py)."""
        datasets = list(SMOKE_DATASETS) if modo == "smoke" else list(FULL_DATASETS)
        per = f" --periodo {periodo}" if periodo.strip() else ""
        return [f"--dataset {ds}{per} --estado {estado or 'MA'}" for ds in datasets]

    chosen = select_commands("{{ params.modo }}", "{{ params.periodo }}", "{{ params.estado }}")

    bronze = DockerOperator.partial(
        task_id="bronze",
        image="poc_airflow-ingestao_rf:latest",
        docker_url="unix://var/run/docker.sock",
        # Rede com nome fixo (compose: networks.default.name=kolmeia-net),
        # portável entre máquinas (sem prefixo do projeto).
        network_mode="kolmeia-net",
        mounts=[Mount(source="mage-tmp", target="/tmp/mage", type="volume")],
        environment={
            # Fonte única: endpoint vem do extra da Connection; bucket de Variable.
            "MINIO_LOCAL_DOMAIN": (
                "{{ conn.minio_s3.extra_dejson.endpoint_url"
                ".replace('http://', '').replace('https://', '') }}"
            ),
            # Creds via Connection (file backend), não via Variables/DB.
            "MINIO_ROOT_USER": "{{ conn.minio_s3.login }}",
            "MINIO_ROOT_PASSWORD": "{{ conn.minio_s3.password }}",
            "MINIO_BUCKET_NAME": "{{ var.value.minio_bucket_name }}",
        },
        mount_tmp_dir=False,
        auto_remove="success",
        pool="heavy_duckdb",
    ).expand(command=chosen)

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_rule="none_failed",
        trigger_dag_id="silver_zip_to_parquet",
        conf={
            "modo": "{{ params.modo }}",
            "periodo": "{{ params.periodo }}",
            "estado": "{{ params.estado }}",
        },
    )
    chosen >> bronze >> trigger_silver
