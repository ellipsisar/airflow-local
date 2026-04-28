---
name: airflow-dag-engineer
description: >
  Use this skill when building, modifying, or debugging Apache Airflow DAGs in this
  project. Triggers include: writing PythonOperator tasks, implementing the Raw→Intermediate→Gold
  medallion pipeline with delta-rs, reading or writing Delta tables on Azure Blob
  (az://synapse/transdev/), processing ZIP/CSV files from Azure Blob containers,
  managing the control_raw_file_status table, working with Airflow Variables or
  Connections for Azure, adding new entities to existing DAG patterns, or debugging
  task failures in this Docker-based Airflow setup.
  Keywords: "DAG", "PythonOperator", "write_deltalake", "DeltaTable", "delta-rs",
  "medallion", "pandas", "pyarrow", "az://synapse", "korbato", "sonnell",
  "control_raw_file_status", "Variable.get", "Azure Blob", "zip ingestion".
---

# Airflow DAG Engineer Skill

You are an expert in building production-grade Airflow 2.9.3 DAGs for this project.
All processing is Python-native using pandas + delta-rs. No ADF, no PySpark, no DBT.

---

## Project Stack

| Layer | Technology |
|---|---|
| Orchestration | Airflow 2.9.3 · Docker Compose · CeleryExecutor |
| Processing | Python 3 · pandas · pyarrow |
| Delta Lake | `deltalake` (delta-rs Python binding) |
| Storage | Azure Blob — account `aticdwstorage` |
| Secrets | `Variable.get("AZURE_STORAGE_ACCOUNT_KEY")` |
| Alerts | SMTP email to `saldabe@ellipsispr.com` |

---

## Path Conventions

```
az://synapse/transdev/                    ← Delta table base
  raw_<domain>_<entity>/                  ← Raw: append, unmodified CSV data
  intermediate_<domain>_<entity>/         ← Intermediate: typed, overwrite
  <domain>_<entity>/                      ← Gold: deduplicated, partitioned
  control_raw_file_status/                ← File processing control table

Source ZIPs:
  container: korbato (or source-specific)
  prefix:    <domain>/   (e.g. "sonnell/")
```

---

## Core Patterns

### Storage Options (always)
```python
def _storage_options() -> dict:
    return {
        "account_name": "aticdwstorage",
        "account_key": Variable.get("AZURE_STORAGE_ACCOUNT_KEY"),
    }
```

### Reading a Delta Table
```python
from deltalake import DeltaTable

def _read_delta_pandas(path: str, opts: dict) -> pd.DataFrame:
    dt = DeltaTable(path, storage_options=opts)
    return dt.to_pyarrow_table().to_pandas()
```

### Writing Raw Layer (append, no type coercion)
```python
from deltalake import write_deltalake

write_deltalake(
    raw_path,
    df_with_metadata,
    mode="append",
    schema_mode="merge",
    storage_options=opts,
)
```

### Writing Intermediate Layer (typed, overwrite)
```python
write_deltalake(
    int_path,
    cast_df,
    mode="overwrite",
    schema_mode="overwrite",
    storage_options=opts,
)
```

### Writing Gold Layer (deduplicated + partitioned)
```python
write_deltalake(
    gold_path,
    gold_df,
    mode="overwrite",
    schema_mode="overwrite",
    partition_by=["svc_date"],
    storage_options=opts,
)
```

### Control Table — Check Processed Files
```python
def _get_processed_files(opts: dict) -> set:
    try:
        df = _read_delta_pandas(CONTROL_PATH, opts)
        return set(df.loc[df["status"] == "processed", "filename"].tolist())
    except Exception:
        return set()
```

### Control Table — Write Results
```python
def _write_control(records: list, opts: dict) -> None:
    """records: list of (filename, status, process_datetime, error_message, record_count)"""
    if not records:
        return
    df = pd.DataFrame(
        records,
        columns=["filename", "status", "process_datetime", "error_message", "record_count"],
    )
    df["process_datetime"] = pd.to_datetime(df["process_datetime"])
    df["record_count"] = df["record_count"].astype("int64")
    write_deltalake(CONTROL_PATH, df, mode="append", schema_mode="merge", storage_options=opts)
```

---

## Entity Cast Function Template

Always define one `_cast_<entity>` function per entity with **explicit types**:

```python
def _cast_<entity>(raw: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        # Date columns
        "<date_col>":     pd.to_datetime(raw["<date_col>"], errors="coerce"),
        # String/ID columns
        "<key_col>":      raw["<key_col>"].astype(str),
        # Integer columns (nullable)
        "<int_col>":      pd.to_numeric(raw["<int_col>"], errors="coerce").astype("Int64"),
        # Float columns
        "<float_col>":    pd.to_numeric(raw["<float_col>"], errors="coerce").astype("float64"),
        # Metadata (always include)
        "_md_filename":   raw["_md_filename"].astype(str),
        "_md_processed_at": pd.to_datetime(raw["_md_processed_at"]),
    })
```

---

## Full Medallion Task Function Template

```python
def process_<domain>_<entity>(**context):
    log = logging.getLogger(__name__)
    opts = _storage_options()
    container = _blob_container_client()

    raw_path  = f"{BASE_PATH}/raw_<domain>_<entity>"
    int_path  = f"{BASE_PATH}/intermediate_<domain>_<entity>"
    gold_path = f"{BASE_PATH}/<domain>_<entity>"

    # ── Raw ───────────────────────────────────────────────────────────────────
    _process_raw(container, "<entity_file_pattern>", raw_path, opts, log)

    # ── Intermediate ──────────────────────────────────────────────────────────
    try:
        raw = _read_delta_pandas(raw_path, opts)
    except Exception as e:
        log.warning(f"Raw table unavailable, skipping intermediate/gold: {e}")
        return

    interm = _cast_<entity>(raw)
    del raw  # free memory immediately
    write_deltalake(int_path, interm, mode="overwrite", schema_mode="overwrite", storage_options=opts)
    log.info(f"Intermediate: {len(interm)} records written to {int_path}")

    # ── Gold ──────────────────────────────────────────────────────────────────
    gold = (
        interm
        .sort_values("_md_processed_at", ascending=False)
        .drop_duplicates(subset=["<natural_key>", "<date_col>"])
    )
    del interm  # free memory
    write_deltalake(
        gold_path, gold,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=["<date_col>"],
        storage_options=opts,
    )
    log.info(f"Gold: {len(gold)} unique records written to {gold_path}")
```

---

## DAG Definition Template

```python
"""
DAG: dag_<domain>_<frequency>

<Brief description of what this DAG does.>
Entities: <entity_1>, <entity_2>, ...
Schedule: <human-readable time>
"""

import datetime
import logging
from datetime import timedelta

import pandas as pd
import pyarrow.dataset as ds
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient
from deltalake import DeltaTable, write_deltalake


ACCOUNT_NAME = "aticdwstorage"
SOURCE_CONTAINER = "<source_container>"
SOURCE_PREFIX = "<domain>/"
BASE_PATH = "az://synapse/transdev"
CONTROL_PATH = f"{BASE_PATH}/control_raw_file_status"
ALERT_EMAIL = "saldabe@ellipsispr.com"


default_args = {
    "owner": "prita",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="dag_<domain>_<frequency>",
    description="<Description>",
    schedule_interval="0 13 * * *",   # 8:00 AM UTC-5
    start_date=datetime.datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["<domain>", "medallion", "<frequency>"],
    doc_md=__doc__,
) as dag:

    t_entity1 = PythonOperator(
        task_id="<domain>_<entity1>",
        python_callable=process_<domain>_<entity1>,
    )

    t_entity2 = PythonOperator(
        task_id="<domain>_<entity2>",
        python_callable=process_<domain>_<entity2>,
    )

    # Independent entities run in parallel
    [t_entity1, t_entity2]
```

---

## Code Standards

- **File naming**: `dag_{domain}_{frequency}.py`
- **Task naming**: `{domain}_{entity}` in snake_case
- **Logging**: `log = logging.getLogger(__name__)` at top of every task function
- **Memory management**: `del df` immediately after last use of large DataFrames
- **No `datetime.now()`**: use `{{ ds }}` or `context["execution_date"]` in Airflow context
- **No hardcoded credentials**: always `Variable.get()`
- **Sensors**: always use `mode="reschedule"` to avoid blocking worker slots
- **Idempotency**: every task must be safely re-runnable (control table for raw, overwrite for int/gold)

---

## Testing a Task Locally

```bash
# Test a specific task without running the full DAG
docker compose run --rm airflow-cli airflow tasks test <dag_id> <task_id> 2025-01-01

# List DAGs
docker compose run --rm airflow-cli airflow dags list

# Trigger a DAG manually
docker compose run --rm airflow-cli airflow dags trigger <dag_id>

# View logs
docker compose logs -f airflow-worker
```

---

## Required Airflow Variables

Every DAG in this project requires:
- `AZURE_STORAGE_ACCOUNT_KEY` — Azure Storage account key for `aticdwstorage`

Additional variables per DAG should be documented in the DAG's `doc_md`.

---

## References
- [delta-rs Python docs](https://delta-io.github.io/delta-rs/python/)
- [Airflow 2.9 PythonOperator](https://airflow.apache.org/docs/apache-airflow/2.9.3/howto/operator/python.html)
- [azure-storage-blob Python SDK](https://learn.microsoft.com/en-us/python/api/azure-storage-blob/)
