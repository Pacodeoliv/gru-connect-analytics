# ============================================================================
# GRU Connect Analytics — Makefile
# ============================================================================
# A Makefile gives you simple shortcuts for common commands.
# Instead of typing "docker compose up -d", just type "make up".
#
# Usage:  make <target>
#   make up        Start the full stack
#   make down      Stop all containers
#   make build     Rebuild Docker images
#   make logs      Follow container logs
#   make restart   Stop + start
#   make reset     Stop + remove all data volumes
#   make ps        Show running containers
#   make ingest    Trigger the Bronze ingestion DAG
# ============================================================================

.PHONY: up down build logs ps restart reset ingest

up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build

logs:
	docker compose logs -f

ps:
	docker compose ps

restart: down up

reset:
	docker compose down -v

ingest:
	docker compose exec airflow-webserver airflow dags trigger dag_bronze_ingestion
