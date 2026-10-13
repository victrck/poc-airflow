.PHONY: lint typecheck test gates up down logs smoke-ma build-ingestao rbac-verify bootstrap login-proof
lint:
	python3 -m ruff check dags include tests docker/ingestao_rf
	python3 -m black --check dags include tests docker/ingestao_rf
	python3 -m flake8 --max-line-length=110 --extend-ignore=E203,W503,E402 dags include tests docker/ingestao_rf
	python3 -m vulture dags include tests docker/ingestao_rf
typecheck:
	python3 -m mypy include
test:
	python3 -m pytest tests -q
gates: lint typecheck test
up:
	docker compose up -d --build postgres minio minio-init
up-full:
	docker compose up -d --build
down:
	docker compose down
logs:
	docker compose logs -f --tail=100
build-ingestao:
	docker build -t poc_airflow-ingestao_rf:latest docker/ingestao_rf
smoke-ma:
	docker build -t poc_airflow-ingestao_rf:latest docker/ingestao_rf
	MINIO_LOCAL_DOMAIN=localhost:9000 python docker/ingestao_rf/script.py --dataset cnaes --estado MA
rbac-verify:
	./scripts/rbac_verify.sh
bootstrap:
	./scripts/bootstrap.sh
login-proof:
	./scripts/login_proof.sh
