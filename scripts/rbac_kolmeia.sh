#!/bin/bash
# WS1 RBAC Kolmeia — reset + 4 perfis (FabAuthManager).
# Idempotente. NUNCA toca em admin nem nas roles built-in.
set -e
cd "$(dirname "$0")"

DAGS="bronze_rf_to_minio silver_zip_to_parquet postgres_carga gold_marts financeiro_dummy"

echo "== reset: remove usuarios/roles legados =="
airflow users delete --username user_a 2>/dev/null || true
airflow users delete --username user_b 2>/dev/null || true
airflow users delete --username usr_read 2>/dev/null || true
airflow users delete --username usr_write 2>/dev/null || true
airflow users delete --username usr_ops 2>/dev/null || true
airflow roles delete role_empresasbr 2>/dev/null || true
airflow roles delete role_financeiro 2>/dev/null || true

echo "== cria roles =="
for r in kolmeia_read kolmeia_write kolmeia_ops; do
  airflow roles create "$r" 2>/dev/null || true
done

echo "== base read (UI + leitura) nas 3 roles =="
perm() { airflow roles add-perms "$1" -a "$2" -r "$3" || true; }
for r in kolmeia_read kolmeia_write kolmeia_ops; do
  perm "$r" can_read "Website"
  perm "$r" can_read "DAG Code"
  perm "$r" can_read "Task Instances"
  perm "$r" can_read "Task Logs"
  for d in $DAGS; do
    perm "$r" can_read "DAG:$d"
    perm "$r" can_read "DAG Run:$d"
  done
done

echo "== write: trigger/pause nas DAGs =="
for r in kolmeia_write kolmeia_ops; do
  for d in $DAGS; do
    perm "$r" can_edit "DAG:$d"
    perm "$r" can_create "DAG Run:$d"
  done
done

echo "== ops: escreve em secrets (Variables + Connections) =="
perm kolmeia_ops can_read "Variables"
perm kolmeia_ops can_edit "Variables"
perm kolmeia_ops can_create "Variables"
perm kolmeia_ops can_delete "Variables"
perm kolmeia_ops can_read "Connections"
perm kolmeia_ops can_edit "Connections"
perm kolmeia_ops can_create "Connections"
perm kolmeia_ops can_delete "Connections"

echo "== sync =="
airflow sync-perm || true

echo "== usuarios novos =="
airflow users create --username usr_read --password "${USR_READ_PW:-kolmeia_read123}" --firstname Kolmeia --lastname Read --role kolmeia_read --email read@kolmeia.local || true
airflow users create --username usr_write --password "${USR_WRITE_PW:-kolmeia_write123}" --firstname Kolmeia --lastname Write --role kolmeia_write --email write@kolmeia.local || true
airflow users create --username usr_ops --password "${USR_OPS_PW:-kolmeia_ops123}" --firstname Kolmeia --lastname Ops --role kolmeia_ops --email ops@kolmeia.local || true

echo "RBAC kolmeia ok"
