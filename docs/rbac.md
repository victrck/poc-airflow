# RBAC Kolmeia (FabAuthManager, Airflow 3.0.2)

## Perfis

| Perfil | UI | DAGs (5) | Trigger/pause | Secrets (Variables/Connections) |
|---|---|---|---|---|
| `kolmeia_admin` (= Admin) | total | total | total | total |
| `kolmeia_read` | ver | `can_read` em `DAG:<id>` ×5 | não | não |
| `kolmeia_write` | ver | `can_read` + `can_edit` ×5 | sim (`can_create` em `DAG Run:<id>`) | não |
| `kolmeia_ops` | ver | = write | = write | `can_read/can_edit/can_create/can_delete` |

Roles built-in (`Admin/Op/User/Viewer/Public`) e usuário `admin` intocados.
FAB não filtra Variables/Connections por item — a separação é ver-vs-editar no endpoint.

## Secrets backend (2026-10-11)

Só o que a execução consome vive no Airflow: Connections `minio_s3`, `trino_default`,
`postgres_organizacao` + Variables `hive_metastore_uri`, `minio_bucket_name`,
`processing_estado?`, `processing_periodo?`, `duckdb_memory_limit`, `duckdb_threads` — via `LocalFilesystemBackend`
(`config/airflow/*.json`, gerados do `.env` pelo bootstrap, mount ro no scheduler e na api-server).
Ordem de leitura: arquivo > `AIRFLOW_VAR_*`/`AIRFLOW_CONN_*` > DB.
A listagem da UI (Browse -> Connections/Variables) mostra **só o DB por design do Airflow**,
por isso o bootstrap espelha os mesmos valores no DB — a UI lista o espelho, a execução
usa o arquivo. Código sem segredo hardcoded (grep-test em `tests/test_secrets_resolution.py`).

## Operação

- Aplicar: `./scripts/rbac_kolmeia.sh` **dentro do scheduler** (`docker compose exec airflow-scheduler bash /tmp/rbac_kolmeia.sh`) — o CLI `airflow roles add-perms` exige sintaxe `-a <action> -r <resource>`, seguido de `airflow sync-perm`.
- Verificar: `./scripts/rbac_verify.sh` → `RBAC-MATRIX-OK` (checa 5 DAGs × Nancy 3 roles + secrets).
- Usuários: `usr_read / usr_write / usr_ops` (`*_@kolmeia.local`). Senhas via env `USR_*_PW` ou default `kolmeia_<perfil>123` — trocar no primeiro login.

## Teste manual (UI http://localhost:8081)

1. `usr_read`: vê as 5 DAGs, abre logs; botão Trigger retorna 403; menu Variables/Connections retorna 403.
2. `usr_write`: dispara `financeiro_dummy` com sucesso; Variables retorna 403.
3. `usr_ops`: `./scripts/login_proof.sh` → `LOGIN-PROOF-OK` (9 checks HTTP reais:
   read vê 5 DAGs + 403 em trigger/variables; write dispara dummy + 403 em variables;
   ops lê/cria/apaga variables).
