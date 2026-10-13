# Roteiro Vídeo 1 — Fundamentos e Arquitetura (2h) — PRONTO P/ GRAVAR
1. DAG/Task/XCom/Sensor/Operator + TaskFlow + Dynamic Mapping AF3 (30min)
2. Arquitetura AF3: api-server (8081, UI+API pública+Execution API) vs scheduler (LocalExecutor) vs dag-processor vs triggerer (30min)
3. Anti-patterns: top-level pesado, Pandas GB em worker, XCom GB (20min)
4. Tour compose: postgres 3DBs, minio organizacao, hive:9083, trino:8080, obs (20min)
5. DoD vídeo: `docker compose ps` tudo healthy + `make gates` verde.

# Roteiro Vídeo 2 — Prática EmpresasBR + RBAC + Comparativo Mage (4h) — PRONTO P/ GRAVAR (smoke)
1. RBAC: `./scripts/login_proof.sh` → LOGIN-PROOF-OK; login `usr_read` (vê 5, 403 em trigger) vs `usr_write` (dispara dummy) vs `usr_ops` (edita Variables) (30min)
2. Híbrido smoke: trigger `silver_zip_to_parquet {modo:smoke}` → `postgres_carga` → `gold_marts` encadeadas; `SELECT count(*) FROM iceberg.silver.meta_cnaes` → **1359**; `organizacao.cnaes` → 1359 (60min)
3. Full mode (roadmap, NÃO gravado ainda): bronze `estabelecimentos/empresas/socios` (multi-GB) → `dados_empresas` 17 cols por UF → `gold_ma` dual PG+MinIO (45min)
4. Comparativo Mage (honesto, só o medido): blocos Mage vs TaskFlow+Mapping; chain via tokens vs TriggerDagRun; idempotência `_SUCCESS` preservada; DuckDB local vs Trino (smoke usa DuckDB; Trino lê o resultado) (45min)
5. Encerramento: prod Celery/K8s + segredos fora do .env (15min)

Queries demo (executadas, 2026-09):
SELECT count(*) FROM iceberg.silver.meta_cnaes;  -- 1359
SELECT periodo, count(*) FROM organizacao.cnaes GROUP BY 1;  -- 2026-09, 1359
