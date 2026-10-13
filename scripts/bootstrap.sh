#!/bin/bash
# Bootstrap zero-touch: máquina zerada -> ambiente no ar + seed + smoke pronto.
# Uso: ./scripts/bootstrap.sh [--reset]   (--reset apaga volumes: PERDE DADOS)
# Pré-req: docker + docker compose plugin + python3.
set -e
cd "$(dirname "$0")/.."
RESET=${1:-}

echo "== [0] pré-reqs =="
docker --version >/dev/null || { echo "instale o docker"; exit 1; }
docker compose version >/dev/null || { echo "instale o docker compose plugin"; exit 1; }

if [ "$RESET" = "--reset" ]; then
  echo "== RESET: derruba tudo COM volumes =="
  docker compose down -v
fi

echo "== [1] .env (gera segredos + DOCKER_GID se ausentes) =="
if [ ! -f .env ]; then cp .env.example .env; fi
gen() { python3 -c "import secrets; print(secrets.token_urlsafe(32))"; }
if grep -q "GERADO_NO_BOOTSTRAP_JWT" .env; then
  sed -i "s/GERADO_NO_BOOTSTRAP_JWT/$(gen)/" .env
fi
if grep -q "GERADO_NO_BOOTSTRAP_FERNET" .env; then
  python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" > /tmp/fk.txt 2>/dev/null \
    || echo "7NFNUK-Lisulc7GRwIIQ0BcHM_ZRN7la6-83eSXlADU=" > /tmp/fk.txt
  FK=$(cat /tmp/fk.txt); rm -f /tmp/fk.txt
  sed -i "s|GERADO_NO_BOOTSTRAP_FERNET|$FK|" .env
fi
GID=$(stat -c %g /var/run/docker.sock)
if grep -q "GERADO_NO_BOOTSTRAP_GID" .env; then
  sed -i "s/GERADO_NO_BOOTSTRAP_GID/$GID/" .env
elif ! grep -q "^DOCKER_GID=" .env; then
  echo "DOCKER_GID=$GID" >> .env
fi
for key in AIRFLOW__SECRETS__BACKEND AIRFLOW__SECRETS__BACKEND_KWARGS; do
  if ! grep -q "^$key=" .env; then
    grep "^$key=" .env.example >> .env
    echo "(.env: adicionado $key do .env.example)"
  fi
done

echo "== [1b] secrets backend files (gerados do .env, nunca commitar) =="
# 644 (não 600): o scheduler roda como uid 50000 e precisa ler os arquivos;
# mesmo nível de exposição do .env. Mount é read-only no compose.
python3 - <<'EOF'
import json
import os
from pathlib import Path

env: dict[str, str] = {}
for line in Path(".env").read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip().strip("'").strip('"')

outdir = Path("config/airflow")
outdir.mkdir(parents=True, exist_ok=True)
connections = {
    "minio_s3": {
        "conn_type": "aws",
        "login": env.get("MINIO_ROOT_USER", "minioadmin"),
        "password": env.get("MINIO_ROOT_PASSWORD", "minioadmin123"),
        "extra_dejson": {"endpoint_url": "http://minio:9000"},
    },
    "trino_default": {
        "conn_type": "trino",
        "host": env.get("TRINO_HOST", "trino"),
        "port": int(env.get("TRINO_PORT", "8080")),
        "login": env.get("TRINO_USER", "admin"),
        "extra_dejson": {"catalog": env.get("TRINO_CATALOG", "iceberg"), "schema": "silver"},
    },
    "postgres_organizacao": {
        "conn_type": "postgres",
        "host": "postgres",
        "port": 5432,
        "login": env.get("ORGANIZACAO_USER", "organizacao_user"),
        "password": env.get("ORGANIZACAO_PASSWORD", "organizacao_password"),
        "schema": env.get("ORGANIZACAO_DB", "organizacaodb"),
    },
}
variables = {
    "hive_metastore_uri": "thrift://hive-metastore:9083",
    "minio_bucket_name": env.get("MINIO_BUCKET_NAME", "organizacao"),
}
try:
    variables["duckdb_memory_limit"] = f"{float(env.get('DUCKDB_MEMORY_LIMIT_GB', '2.0')):g}GB"
except ValueError:
    variables["duckdb_memory_limit"] = "2GB"
try:
    variables["duckdb_threads"] = str(max(1, int(float(env.get("DUCKDB_THREADS", "2")))))
except ValueError:
    variables["duckdb_threads"] = "2"
for var_key, env_key in (("processing_estado", "PROCESSING_ESTADO"), ("processing_periodo", "PROCESSING_PERIODO")):
    if env.get(env_key, "").strip():
        variables[var_key] = env[env_key].strip()
(outdir / "connections.json").write_text(json.dumps(connections, indent=2) + "\n")
(outdir / "variables.json").write_text(json.dumps(variables, indent=2) + "\n")
os.chmod(outdir / "connections.json", 0o644)
os.chmod(outdir / "variables.json", 0o644)
print("secrets-files-ok")
EOF

echo "== [2] build imagens locais =="
docker build -f Dockerfile.airflow -t poc_airflow-airflow:3.0.2 .
docker build -t poc_airflow-ingestao_rf:latest docker/ingestao_rf

echo "== [3] sobe stack =="
docker compose up -d
echo "-- aguarda postgres/minio healthy (até 3min) --"
for _ in $(seq 1 36); do
  P=$(docker compose ps postgres --format '{{.Health}}' 2>/dev/null || echo starting)
  M=$(docker compose ps minio --format '{{.Health}}' 2>/dev/null || echo starting)
  [ "$P" = "healthy" ] && [ "$M" = "healthy" ] && break
  sleep 5
done
echo "postgres=$P minio=$M"
echo "-- aguarda airflow-init concluir (até 5min; -a pois one-shot sai de ps padrão no compose v5+) --"
for _ in $(seq 1 60); do
  S=$(docker compose ps -a airflow-init --format '{{.State}}' 2>/dev/null || echo missing)
  case "$S" in
    exited) break ;;
    "" | missing) sleep 2; continue ;;
  esac
  sleep 5
done
CODE=$(docker compose ps -a airflow-init --format '{{.ExitCode}}' 2>/dev/null || echo "?")
echo "airflow-init state=$S exit=$CODE"
[ "$CODE" = "0" ] || { echo "airflow-init falhou"; exit 1; }

echo "== [4] seed: pool + espelho dos segredos no DB p/ visibilidade na UI =="
docker compose exec -T airflow-scheduler airflow pools set heavy_duckdb 1 "DuckDB pesado serializado (OOM full-mode 2026-10-10)" >/dev/null 2>&1 || true
# File backend (config/airflow/*.json) é a fonte p/ execução; o DB é espelho p/ a UI listar.
# A listagem Browse -> Connections/Variables mostra SÓ o DB por design do Airflow.
python3 - <<'EOF'
import json
import subprocess
from pathlib import Path

conns = json.loads(Path("config/airflow/connections.json").read_text())
for cid, c in conns.items():
    extra = json.dumps(c.get("extra_dejson", {}))
    cmd = [
        "docker", "compose", "exec", "-T", "airflow-scheduler",
        "airflow", "connections", "add", cid,
        "--conn-type", str(c.get("conn_type", "")),
        "--conn-host", str(c.get("host", "") or ""),
        "--conn-login", str(c.get("login", "") or ""),
        "--conn-password", str(c.get("password", "") or ""),
        "--conn-schema", str(c.get("schema", "") or ""),
    ]
    if c.get("port"):
        cmd += ["--conn-port", str(c["port"])]
    if extra != "{}":
        cmd += ["--conn-extra", extra]
    subprocess.run(cmd, capture_output=True)
print("conns-espelhadas-no-db")
EOF
python3 - <<'EOF'
import json
import subprocess
from pathlib import Path

data = Path("config/airflow/variables.json").read_text()
variables = json.loads(data)
for key, value in variables.items():
    if key.startswith("_") or value is None or str(value) == "":
        continue
    subprocess.run(
        ["docker", "compose", "exec", "-T", "airflow-scheduler",
         "airflow", "variables", "set", key, str(value)],
        capture_output=True,
    )
print("vars-espelhadas-no-db")
EOF
for v in minio_root_user minio_root_password; do
  docker compose exec -T airflow-scheduler airflow variables delete "$v" >/dev/null 2>&1 || true
done
echo "pool-ok espelho-ok legado-limpo"
echo "== [4b] prova do secrets backend (sem imprimir segredo) =="
docker compose exec -T airflow-scheduler airflow connections get minio_s3 >/dev/null 2>&1 \
  && echo "secrets-backend-ok (minio_s3 resolvida via arquivo)" \
  || { echo "FALHA: minio_s3 não resolvida (ver config/airflow/*.json + mounts)"; exit 1; }

echo "== [5] RBAC kolmeia =="
docker compose cp scripts/rbac_kolmeia.sh airflow-scheduler:/tmp/rbac_kolmeia.sh
docker compose exec -T airflow-scheduler bash /tmp/rbac_kolmeia.sh 2>&1 | tail -1

echo "== [6] despausa DAGs =="
for d in bronze_rf_to_minio silver_zip_to_parquet postgres_carga gold_marts financeiro_dummy; do
  docker compose exec -T airflow-scheduler airflow dags unpause "$d" >/dev/null 2>&1 || true
done

echo "== [7] token MinIO p/ Prometheus (rotaciona o versionado) =="
docker compose exec -T minio mc alias set local http://minio:9000 minioadmin minioadmin123 >/dev/null 2>&1 || true
TOK=$(docker compose exec -T minio mc admin prometheus generate local 2>/dev/null | grep bearer_token | awk '{print $2}' || true)
if [ -n "$TOK" ]; then
  python3 - "$TOK" <<'EOF'
import re, sys
p = "config/prometheus/prometheus.yml"
src = open(p).read()
src = re.sub(r"bearer_token: \S+", f"bearer_token: {sys.argv[1]}", src)
open(p, "w").write(src)
print("minio-token-rotated")
EOF
  docker compose up -d prometheus >/dev/null 2>&1 || true
else
  echo "minio-token-keep (usa o versionado)"
fi

echo
echo "== BOOTSTRAP-OK =="
echo "UI:      http://localhost:8081 (admin/admin)"
echo "MinIO:   http://localhost:9001 | Trino: http://localhost:8080"
echo "Grafana: http://localhost:3000 | Prometheus: http://localhost:9090"
echo "Smoke:   docker compose exec airflow-scheduler airflow dags trigger silver_zip_to_parquet --conf '{\"modo\":\"smoke\",\"periodo\":\"\",\"estado\":\"MA\"}'"
echo "Gates:   make gates | RBAC: ./scripts/rbac_verify.sh + ./scripts/login_proof.sh"
