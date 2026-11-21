.DEFAULT_GOAL := help
UV ?= uv

.PHONY: help install up down lint fmt test test-integration ingest-gtfs produce-rt consume-rt \
	dbt-build dbt-freshness dbt-docs dagster dashboard

help: ## Affiche cette aide
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-16s %s\n", $$1, $$2}'

install: ## Installe les dépendances et les hooks pre-commit
	$(UV) sync
	$(UV) run pre-commit install

up: ## Démarre l'infra locale (Redpanda, Console, MinIO + bucket)
	docker compose up -d --wait redpanda redpanda-console minio
	docker compose run --rm minio-init

down: ## Arrête l'infra locale (les volumes sont conservés)
	docker compose down

lint: ## ruff check + ruff format --check + mypy
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run mypy

fmt: ## Formate et corrige automatiquement
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

test: ## Lance les tests pytest
	$(UV) run pytest

test-integration: ## Tests d'intégration (nécessite `make up`)
	$(UV) run pytest -m integration

ingest-gtfs: ## Ingère le GTFS théorique STAR dans bronze (MinIO)
	$(UV) run python -m star_pulse.ingestion.gtfs_static

produce-rt: ## Lance le producteur GTFS-RT vers Redpanda (Ctrl+C pour arrêter)
	$(UV) run python -m star_pulse.ingestion.gtfs_rt

consume-rt: ## Consomme Redpanda et écrit le Parquet bronze sur MinIO (Ctrl+C pour arrêter)
	$(UV) run python -m star_pulse.streaming.bronze_writer

dbt-build: ## Construit et teste le silver dbt (bronze MinIO -> silver)
	$(UV) run python -m star_pulse.transform.dbt_runner build

dbt-freshness: ## Vérifie la fraîcheur du bronze temps réel
	$(UV) run python -m star_pulse.transform.dbt_runner source freshness

dbt-docs: ## Génère et sert la documentation dbt (http://localhost:8081)
	$(UV) run python -m star_pulse.transform.dbt_runner docs generate
	$(UV) run python -m star_pulse.transform.dbt_runner docs serve --port 8081

dagster: ## Lance Dagster (UI, schedules, sensor) sur http://localhost:3000
	DAGSTER_HOME="$(CURDIR)/orchestration" $(UV) run dagster dev -m star_pulse.orchestration.definitions -p 3000

dashboard: ## Lance le dashboard Streamlit sur http://localhost:8501
	$(UV) run streamlit run dashboard/app.py --browser.gatherUsageStats false
