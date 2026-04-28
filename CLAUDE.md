# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Local Apache Airflow 2.9.3 development environment running via Docker Compose with the CeleryExecutor stack: PostgreSQL (metadata DB), Redis (Celery broker), webserver, scheduler, worker, and triggerer.

DAGs are Python-native pipelines that process data from Azure Blob Storage using `delta-rs` + `pandas`, writing Delta tables to ADLS (`az://synapse/transdev/`) following a Medallion architecture (Raw → Intermediate → Gold). The serving layer is Azure Synapse Analytics SQL Pool Dedicated.

## Key Commands

### Start / stop

```bash
docker compose up -d            # start all services (detached)
docker compose down             # stop and remove containers (keeps volumes)
docker compose down -v          # stop and delete volumes (wipes DB)
docker compose up --profile flower -d   # also start Flower (Celery monitor on :5555)
```

### Rebuild image (required after changing Dockerfile or requirements.txt)

```bash
docker compose build
docker compose up -d
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
docker compose run --rm airflow-cli airflow dags list
docker compose run --rm airflow-cli airflow tasks test <dag_id> <task_id> <execution_date>
docker compose run --rm airflow-cli airflow dags trigger <dag_id>
```

## Python Dependencies

Managed in `requirements.txt` and baked into the custom image (`Dockerfile`):

| Package | Purpose |
|---|---|
| `deltalake>=0.17.0` | Read/write Delta tables via `delta-rs` Python binding (no Spark) |
| `azure-storage-blob>=12.19.0` | Download ZIPs from Azure Blob containers |
| `azure-identity>=1.15.0` | Azure authentication support |

To add a dependency: add to `requirements.txt` then `docker compose build`.

## Configuration

- `.env` — sets `AIRFLOW_UID` (default `50000`). Copy from `.env.example`.
- Web UI: `http://localhost:8080` — default credentials `airflow` / `airflow`.
- SMTP is pre-configured for Gmail in `docker-compose.yaml`. Alerts go to `saldabe@ellipsispr.com`.
- `AIRFLOW__CORE__LOAD_EXAMPLES: 'true'` — disable in `docker-compose.yaml` for a cleaner UI once familiar with Airflow.

**Volume mounts** (hot-reloaded, no restart needed):
- `./dags` → `/opt/airflow/dags`
- `./logs` → `/opt/airflow/logs`
- `./plugins` → `/opt/airflow/plugins`
- `./config` → `/opt/airflow/config`

## Airflow Variables (required at runtime)

Set these in the Airflow UI (Admin → Variables) or via CLI before running DAGs:

| Variable | Description |
|---|---|
| `AZURE_STORAGE_ACCOUNT_KEY` | Account key for `aticdwstorage` — used by all DAGs that read/write ADLS |

## DAG Architecture

### Medallion Pattern

Every production DAG follows a three-layer medallion pipeline per entity:

```
Azure Blob (ZIP files)
      │  container: korbato, prefix: <domain>/
      ▼
 PythonOperator
      │
      ├─ Raw     → az://synapse/transdev/raw_<domain>_<entity>/
      │             append, schema_mode="merge", adds _md_filename + _md_processed_at
      │
      ├─ Intermediate → az://synapse/transdev/intermediate_<domain>_<entity>/
      │             overwrite, explicit type casting per column
      │
      └─ Gold    → az://synapse/transdev/<domain>_<entity>/
                    overwrite, deduplicated by natural key, partition_by=["svc_date"]
                         │
                         ▼ (external consumption)
                  Azure Synapse Analytics SQL Pool
```

**Control table**: `az://synapse/transdev/control_raw_file_status` — Delta table that records every processed filename with status (`processed`/`failed`). All Raw ingestion tasks check this table first to skip already-processed files.

### Reference DAG

`dags/dag_sonnell_daily.py` — canonical example of the full medallion pattern. Contains:
- `_storage_options()` — builds `{"account_name": ..., "account_key": Variable.get(...)}` dict
- `_blob_container_client()` — returns Azure Blob container client
- `_read_delta_pandas(path, opts)` — reads any Delta table to pandas DataFrame via PyArrow
- `_get_processed_files(opts)` / `_write_control(records, opts)` — control table helpers
- `_process_raw(container, file_pattern, raw_path, opts, log)` — generic Raw ingestion
- `_cast_<entity>(raw)` — per-entity explicit type casting functions
- Three parallel `PythonOperator` tasks (one per entity)

### DAG Conventions

- **File naming**: `dag_{domain}_{frequency}.py` (e.g., `dag_sonnell_daily.py`)
- **Task naming**: `{domain}_{entity}` in snake_case
- **Schedule**: `"0 13 * * *"` = 8:00 AM UTC-5
- **owner**: `"prita"`, **email**: `"saldabe@ellipsispr.com"`
- **retries**: 2, **retry_delay**: 5 min, **execution_timeout**: 2 hours
- `catchup=False` on all production DAGs
- Always `del df` after last use of large DataFrames to free worker memory
- Never `datetime.now()` in DAG logic — use `{{ ds }}` or `context["execution_date"]`

## Agent Ecosystem

This project has three specialized Claude agents in `.claude/agents/`:

| Agent | When to use |
|---|---|
| `airflow-orchestrator` | Entry point for any multi-step or ambiguous request — routes to the right specialist |
| `airflow-dag-engineer` | Implementing DAGs, adding entities/sources, fixing pipeline bugs, delta-rs patterns |
| `data-platform-architect` | Architecture decisions, schema design, ADRs, DAG dependency planning |

Corresponding skills in `.claude/skills/` provide detailed reference material for each role.
