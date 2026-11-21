.DEFAULT_GOAL := help
UV ?= uv

.PHONY: help install up down lint fmt test

help: ## Affiche cette aide
	@grep -E '^[a-z]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-10s %s\n", $$1, $$2}'

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
