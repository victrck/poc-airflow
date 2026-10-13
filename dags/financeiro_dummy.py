"""DAG dummy para demo RBAC (Usuário B)."""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="financeiro_dummy",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rbac-demo", "financeiro"],
    doc_md="DAG isolada para teste de RBAC — só role_financeiro enxerga.",
) as dag:
    BashOperator(task_id="echo", bash_command="echo financeiro-ok")
