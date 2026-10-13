#!/bin/bash
set -e
# Cria 3 DBs + users na mesma instância PG15 (F1 DoD)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER hive WITH PASSWORD 'hive';
  CREATE DATABASE hive_metastore_db OWNER hive;
  CREATE USER organizacao_user WITH PASSWORD 'organizacao_password';
  CREATE DATABASE organizacaodb OWNER organizacao_user;
  GRANT ALL PRIVILEGES ON DATABASE hive_metastore_db TO hive;
  GRANT ALL PRIVILEGES ON DATABASE organizacaodb TO organizacao_user;
EOSQL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname organizacaodb -c "CREATE SCHEMA IF NOT EXISTS organizacao AUTHORIZATION organizacao_user; CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION organizacao_user;"
