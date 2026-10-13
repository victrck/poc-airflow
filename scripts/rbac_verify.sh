#!/bin/bash
# Verifica a matriz RBAC kolmeia direto no metadata DB (determinístico).
# Uso: ./scripts/rbac_verify.sh
set -e
cd "$(dirname "$0")"
Q="SELECT r.name, v.name, p.name FROM ab_role r JOIN ab_permission_view_role rp ON rp.role_id=r.id JOIN ab_permission_view pv ON pv.id=rp.permission_view_id JOIN ab_view_menu v ON v.id=pv.view_menu_id JOIN ab_permission p ON p.id=pv.permission_id WHERE r.name LIKE 'kolmeia%' ORDER BY 1,2,3"
MATRIX=$(docker compose exec -T postgres psql -U airflow -d airflow_db -t -A -F'|' -c "$Q")
fail=0
check() { # check <role> <resource> <action> <must:yes|no>
  if echo "$MATRIX" | grep -qx "$1|$2|$3"; then got=yes; else got=no; fi
  if [ "$got" = "$4" ]; then echo "OK   $1 $2 $3 = $got";
  else echo "FAIL $1 $2 $3 esperado=$4 obtido=$got"; fail=1; fi
}
DAGS="bronze_rf_to_minio silver_zip_to_parquet postgres_carga gold_marts financeiro_dummy"
for d in $DAGS; do
  check kolmeia_read "DAG:$d" can_read yes
  check kolmeia_read "DAG:$d" can_edit no
  check kolmeia_write "DAG:$d" can_edit yes
  check kolmeia_write "DAG Run:$d" can_create yes
  check kolmeia_read "DAG Run:$d" can_create no
done
for a in can_read can_edit can_create can_delete; do
  check kolmeia_ops Variables "$a" yes
  check kolmeia_ops Connections "$a" yes
  check kolmeia_write Variables "$a" no
  check kolmeia_write Connections "$a" no
  check kolmeia_read Variables "$a" no
done
check kolmeia_read Website can_read yes
[ $fail -eq 0 ] && echo "RBAC-MATRIX-OK" || { echo "RBAC-MATRIX-FAIL"; exit 1; }
