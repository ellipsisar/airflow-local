# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Local Apache Airflow 2.9.3 development environment running via Docker Compose with the CeleryExecutor stack: PostgreSQL (metadata DB), Redis (Celery broker), webserver, scheduler, worker, and triggerer.

## Key Commands

### Start / stop

```bash
docker compose up -d            # start all services (detached)
docker compose down             # stop and remove containers (keeps volumes)
docker compose down -v          # stop and delete volumes (wipes DB)
docker compose up --profile flower -d   # also start Flower (Celery monitor on :5555)
```

### View logs

```bash
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker
```

### Run Airflow CLI commands

```bash
docker compose run --rm airflow-cli airflow <command>
# e.g.:
docker compose run --rm airflow-cli airflow dags list
docker compose run --rm airflow-cli airflow tasks test <dag_id> <task_id> <execution_date>
```

### Trigger a DAG manually

```bash
docker compose run --rm airflow-cli airflow dags trigger <dag_id>
```

## Architecture

| Service | Image | Port |
|---|---|---|
| `airflow-webserver` | apache/airflow:2.9.3 | 8080 |
| `airflow-scheduler` | apache/airflow:2.9.3 | — |
| `airflow-worker` | apache/airflow:2.9.3 | — |
| `airflow-triggerer` | apache/airflow:2.9.3 | — |
| `postgres` | postgres:13 | internal |
| `redis` | redis:7.2-bookworm | 6379 (internal) |
| `flower` (optional) | apache/airflow:2.9.3 | 5555 |

**Volume mounts** — changes to these local directories are reflected inside containers immediately without a restart:
- `./dags` → `/opt/airflow/dags`
- `./logs` → `/opt/airflow/logs`
- `./plugins` → `/opt/airflow/plugins`
- `./config` → `/opt/airflow/config`

## Configuration

- `.env` — sets `AIRFLOW_UID` (default `50000`); also used for `_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD`, `AIRFLOW_IMAGE_NAME`, and `_PIP_ADDITIONAL_REQUIREMENTS`.
- Web UI default credentials: `airflow` / `airflow` (set via `_AIRFLOW_WWW_USER_*` env vars).
- To add Python dependencies for quick checks, set `_PIP_ADDITIONAL_REQUIREMENTS` in `.env`. For persistent dependencies, extend the Docker image instead.
- To use a custom `airflow.cfg`, place it in `./config/` and uncomment the `AIRFLOW_CONFIG` line in `docker-compose.yaml`.

## Adding DAGs

Drop Python files into `./dags/`. The scheduler picks them up automatically (no restart needed). DAGs are paused at creation by default (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'`).
