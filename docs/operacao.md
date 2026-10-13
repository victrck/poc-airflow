# Operação — URLs de integração + CLI (fontes: docs Airflow 3.0/3.3 + FAB provider)

## 1. URLs que usamos / precisamos

| Serviço | URL interna (compose) | URL host | Uso |
|---|---|---|---|
| Airflow UI + API pública | `http://airflow-api-server:8081` | http://localhost:8081 | UI, `/api/v2/*`, `/openapi.json` |
| **Execution API** (tasks → scheduler) | `http://airflow-api-server:8081/execution/` | — (só intra-rede) | `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`; rotas em `api_fastapi/execution_api/routes/` (`health/ping` é pública, resto exige JWT) |
| Scheduler log server | `localhost:8793` (no container) | — | **NÃO** é a execution API (erro que nos custou 1h) |
| MinIO API / Console | `http://minio:9000` / `:9001` | :9000/:9001 | bronze/silver/gold/warehouse (`organizacao`) |
| Trino | `http://trino:8080` | :8080 | `iceberg.*` + `hive.*` (S3 nativo) |
| Hive Metastore (thrift) | `thrift://hive-metastore:9083` | :9083 | catálogo Iceberg (PyIceberg + Trino) |
| Postgres | `postgres:5432` | :5432 | `airflow_db`, `hive_metastore_db`, `organizacaodb` |
| Prometheus / StatSD-exp / node / cAdvisor | `prometheus:9090`, `:9102`, `:9100`, `:8080` | :9090/:9102/:9100/:8082 | `up{}`, `airflow_*`, `container_*`, `node_*` |
| Grafana | `http://grafana:3000` | :3000 | dashboard `Kolmeia SRE — Pipelines` (uid `kolmeia-sre`) |
| Receita Federal DAV | https://arquivos.receitafederal.gov.br/.../YggdBLfdninEJX9/YYYY-MM/ | — | PROPFIND discovery + GET (fallback 6 meses) |

## 2. CLI Airflow — comandos que executamos (sintaxe 3.0.2/FAB)

```bash
# infra/metadata
airflow db migrate
airflow users create --username X --password Y --firstname F --lastname L --role R --email E
airflow users delete --username X
airflow users list
# roles FAB: add-perms exige -a <action> -r <resource> (NÃO --permission/--resource-name)
airflow roles create R
airflow roles delete R
airflow roles list
airflow roles add-perms R -a can_read -r "DAG:<dag_id>"
airflow sync-perm                       # após mudar access_control/perms
# recursos descobertos no banco: DAG:<id>, DAG Run:<id>, DAGs, DAG Code,
# Task Instances, Task Logs, Variables, Connections, Website (+ can_read/can_edit/can_create/can_delete)
# DAGs
airflow dags list
airflow dags list-runs <dag_id>          # posicional, SEM --dag-id/-d
airflow dags trigger <dag_id> [--conf '{"modo":"smoke","periodo":"2026-09","estado":"MA"}']
airflow dags unpause <dag_id>
airflow dags list-import-errors
airflow tasks test <dag> <task>          # in-process (não usa executor)
# config/secrets (FAB não filtra Variables/Connections por item — só endpoint)
airflow config get-value <section> <key> # ex: api_auth jwt_secret
airflow connections add ID --conn-type T --conn-host H --conn-login U --conn-password P [--conn-schema S --conn-port N --conn-extra '{...}']
airflow variables set KEY VAL
airflow pools set heavy_duckdb 1 "DuckDB pesado serializado (default desde 2026-10-10)"
```
<!-- 2026-10-11: load_dados_empresas usa COPY ... FROM STDIN streaming
     (sem to_pylist) + pool heavy_duckdb; full 27 UFs validado (73,2M linhas).
     Órfãos de runs mortas acumulam em /tmp/mage/airflow_tmp — limpar com fila vazia. -->

## 2b. Secrets backend — só o que a execução consome (2026-10-11)

`LocalFilesystemBackend` (env `AIRFLOW__SECRETS__*` no `.env`): `config/airflow/connections.json`
(`minio_s3`, `trino_default`, `postgres_organizacao`) + `variables.json`
(`hive_metastore_uri`, `minio_bucket_name`, `processing_estado?`, `processing_periodo?`,
`duckdb_memory_limit`, `duckdb_threads`),
gerados do `.env` pelo bootstrap (`chmod 644` — scheduler roda como uid 50000 e precisa ler;
mount ro no scheduler e na api-server; sem o mount na api-server a UI/API não resolve o arquivo).
Ordem: arquivo > `AIRFLOW_VAR_*`/`AIRFLOW_CONN_*` > DB.
A UI lista só o DB por design, por isso o bootstrap espelha os mesmos valores no DB.
Código lê `BaseHook`/`Variable`/`{{ conn.* }}` (bronze); fallbacks de env sem defaults
sensíveis (fail-fast; só p/ testes). Fonte única: endpoint do container bronze vem do
`extra_dejson` da `minio_s3`, warehouse Iceberg deriva de `minio_bucket_name`,
tuning DuckDB via `duckdb_*` (`config.duckdb_settings()`). Validação: `connections get <id>` + smoke + `rbac_verify`
+ `login_proof`. Rollback: reverter mounts/env + religar seed CLI no bootstrap.

## 3. Armadilhas reais encontradas (com prova)

1. Executor apontava `localhost:8080` (default `{BASE_URL}/execution/`) — fix `EXECUTION_API_SERVER_URL=http://airflow-api-server:8081/execution/`.
2. `jwt_secret` auto-gerado **diferente por container** → tasks 403 com log vazio — fix `AIRFLOW__API_AUTH__JWT_SECRET` compartilhado.
3. `AIRFLOW__API__AUTH_BACKENDS=basic_auth` (AF2) removido — quebrava API interna.
4. `STATSD_PORT` 8125 vs exporter 9125 — zero métricas até corrigir.
5. Fernet key efêmera → connections ilegíveis após recreate — fix `FERNET_KEY` explícita + recriar secrets.
6. `basic_auth` não autentica usuários FAB na API — validação RBAC via matriz no banco (`scripts/rbac_verify.sh`), não via curl.
7. HMS sem S3A no classpath → `HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/*` + locations `s3a://`.
8. Trino 455 não tem `system.add_files` → registro Iceberg via PyIceberg (`create_table` + `append`); `add_files` do PyIceberg tem quirk de unicidade — não usado.
9. pyiceberg 0.12 exige pyarrow ≥19 (imagem tinha 16) → `pyarrow>=19`.
10. DockerOperator via scheduler: socket exige `group_add` com GID do host (`DOCKER_GID`, via bootstrap) + rede com nome fixo (`networks.default.name=kolmeia-net`, DAG usa `kolmeia-net` — prefixo do projeto muda por máquina).
11. Comando do container efêmero = só args (`--dataset ..`); imagem já tem entrypoint `python /app/script.py` — prefixo duplo dá exit 2.
12. Jinja NÃO renderiza dentro de `.map(lambda)` — comandos montados em `@task` Python puro.
13. Expand vazio propaga `skipped` e mata chain triggers — `trigger_rule="none_failed"` nos 3 triggers de cadeia.
14. Runs manuais em DAGs pausadas ficam `queued` — bootstrap despausa as 5 DAGs.
15. PyIceberg em HMS zerada exige `create_namespace_if_not_exists` com location `s3a://` antes de `create_table`.
16. Registro Iceberg idempotente via propriedade `kolmeia_sources` (re-run não duplica).
