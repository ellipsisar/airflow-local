"""
DAG: dag_sonnell_daily

Migración de notebooks PySpark Sonnell a Python nativo en Airflow.
Arquitectura medallion: Raw → Intermediate → Gold (Delta) en ADLS.

Entidades (3 tasks en paralelo):
  - sonnell_checkpoin
  - sonnell_subsystem
  - sonnell_trip

Schedule: 8:00 AM UTC-5 (13:00 UTC) todos los días.
"""

import io
import zipfile
import datetime
import logging
from datetime import timedelta

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient
from deltalake import DeltaTable, write_deltalake


# ── Configuración ──────────────────────────────────────────────────────────────

ACCOUNT_NAME = "aticdwstorage"
SOURCE_CONTAINER = "korbato"
SOURCE_PREFIX = "sonnell/"
BASE_PATH = "az://synapse/transdev"
CONTROL_PATH = f"{BASE_PATH}/control_raw_file_status"
ALERT_EMAIL = "saldabe@ellipsispr.com"
MAX_LOOKBACK_DAYS = 10


# ── Helpers compartidos ────────────────────────────────────────────────────────

def _storage_options() -> dict:
    return {
        "account_name": ACCOUNT_NAME,
        "account_key": Variable.get("AZURE_STORAGE_ACCOUNT_KEY"),
    }


def _blob_container_client():
    key = Variable.get("AZURE_STORAGE_ACCOUNT_KEY")
    return (
        BlobServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
            credential=key,
        )
        .get_container_client(SOURCE_CONTAINER)
    )


def _read_delta_pandas(path: str, opts: dict) -> pd.DataFrame:
    """Lee una tabla Delta usando PyArrow para menor footprint de memoria."""
    dt = DeltaTable(path, storage_options=opts)
    return dt.to_pyarrow_table().to_pandas()


def _get_processed_files(opts: dict) -> set:
    try:
        df = _read_delta_pandas(CONTROL_PATH, opts)
        return set(df.loc[df["status"] == "processed", "filename"].tolist())
    except Exception:
        return set()


def _write_control(records: list, opts: dict) -> None:
    if not records:
        return
    df = pd.DataFrame(
        records,
        columns=["filename", "status", "process_datetime", "error_message", "record_count"],
    )
    df["process_datetime"] = pd.to_datetime(df["process_datetime"])
    df["record_count"] = df["record_count"].astype("int64")
    write_deltalake(CONTROL_PATH, df, mode="append", schema_mode="merge", storage_options=opts)


def _process_raw(container, file_pattern: str, raw_path: str, opts: dict, log, execution_date=None) -> None:
    """Descarga ZIPs del storage, descomprime CSVs y escribe la capa Raw en Delta."""
    processed = _get_processed_files(opts)
    blobs = [
        b for b in container.list_blobs(name_starts_with=SOURCE_PREFIX)
        if file_pattern in b.name.split("/")[-1] and b.name.endswith(".zip")
    ]

    cutoff_date = None
    if execution_date is not None:
        exec_date = execution_date.date() if hasattr(execution_date, "date") else execution_date
        cutoff_date = exec_date - datetime.timedelta(days=MAX_LOOKBACK_DAYS)

    records, raw_dfs = [], []
    for blob in blobs:
        file_name = blob.name.split("/")[-1]
        if file_name in processed:
            log.info(f"Skipping (ya procesado): {file_name}")
            continue

        # Filtrar blobs fuera de la ventana de lookback usando la fecha del nombre de archivo
        if cutoff_date is not None:
            try:
                file_date = datetime.date(
                    int(file_name[:4]), int(file_name[4:6]), int(file_name[6:8])
                )
                if file_date < cutoff_date:
                    log.info(f"Skipping (fuera de ventana {MAX_LOOKBACK_DAYS}d): {file_name}")
                    continue
            except (ValueError, IndexError):
                pass  # si el nombre no empieza con YYYYMMDD, procesar igual

        log.info(f"Raw: procesando {file_name}")
        try:
            content = container.get_blob_client(blob.name).download_blob().readall()
            blob_dfs = []
            with zipfile.ZipFile(io.BytesIO(content), "r") as z:
                for csv_name in z.namelist():
                    if not csv_name.endswith(".csv"):
                        continue
                    with z.open(csv_name) as f:
                        df = pd.read_csv(f)
                    if df.empty:
                        log.warning(f"CSV sin datos (esperado): {csv_name} en {file_name}")
                        continue
                    df["_md_filename"] = file_name
                    df["_md_processed_at"] = datetime.datetime.utcnow()
                    blob_dfs.append(df)

            if blob_dfs:
                combined = pd.concat(blob_dfs, ignore_index=True)
                raw_dfs.append(combined)
                records.append((file_name, "processed", datetime.datetime.utcnow(), None, len(combined)))
            else:
                # ZIP sin datos (CSVs vacíos) — marcar como procesado para no reintentar
                records.append((file_name, "processed", datetime.datetime.utcnow(), None, 0))

        except Exception as e:
            log.error(f"Error procesando {file_name}: {e}")
            records.append((file_name, "failed", datetime.datetime.utcnow(), str(e), 0))

    if raw_dfs:
        write_deltalake(
            raw_path,
            pd.concat(raw_dfs, ignore_index=True),
            mode="append",
            schema_mode="merge",
            storage_options=opts,
        )
        log.info(f"Raw: {sum(len(d) for d in raw_dfs)} registros escritos en {raw_path}")
    else:
        log.info("Raw: no hay archivos nuevos para procesar.")

    _write_control(records, opts)


# ── Intermediate casting por entidad ──────────────────────────────────────────

def _cast_checkpoin(raw: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "svc_date":        pd.to_datetime(raw["svc_date"], errors="coerce"),
        "trip_key":        raw["trip_key"].astype(str),
        "sched_trip":      raw["sched_trip"].astype(str),
        "vehicle_id":      raw["vehicle_id"].astype(str),
        "subsystem":       raw["subsystem"].astype(str),
        "visit_key":       raw["visit_key"].astype(str),
        "seq_in_day":      pd.to_numeric(raw["seq_in_day"], errors="coerce").astype("Int64"),
        "arrival":         pd.to_datetime(raw["arrival"], errors="coerce"),
        "departure":       pd.to_datetime(raw["departure"], errors="coerce"),
        "sched_arrival":   pd.to_datetime(raw["sched_arrival"], errors="coerce"),
        "sched_departure": pd.to_datetime(raw["sched_departure"], errors="coerce"),
        "seq_in_trip":     pd.to_numeric(raw["seq_in_trip"], errors="coerce").astype("Int64"),
        "stop_id":         raw["stop_id"].astype(str),
        "compliant":       raw["compliant"].astype(str),
        "insert_dt":       pd.to_datetime(raw["insert_dt"], errors="coerce"),
        "_md_filename":    raw["_md_filename"].astype(str),
        "_md_processed_at": pd.to_datetime(raw["_md_processed_at"]),
    })


def _cast_subsystem(raw: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "svc_date":        pd.to_datetime(raw["svc_date"], errors="coerce"),
        "subsystem":       raw["subsystem"].astype(str),
        "route_id":        raw["route_id"].astype(str),
        "num_trips":       pd.to_numeric(raw["num_trips"], errors="coerce").astype("Int64"),
        "revenue_meters":  pd.to_numeric(raw["revenue_meters"], errors="coerce").astype("float64"),
        "revenue_seconds": pd.to_numeric(raw["revenue_seconds"], errors="coerce").astype("float64"),
        "insert_dt":       pd.to_datetime(raw["insert_dt"], errors="coerce"),
        "_md_filename":    raw["_md_filename"].astype(str),
        "_md_processed_at": pd.to_datetime(raw["_md_processed_at"]),
    })


def _cast_trip(raw: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "svc_date":        pd.to_datetime(raw["svc_date"], errors="coerce"),
        "trip_key":        raw["trip_key"].astype(str),
        "sched_trip":      raw["sched_trip"].astype(str),
        "vehicle_id":      raw["vehicle_id"].astype(str),
        "route_id":        raw["route_id"].astype(str),
        "pattern_id":      raw["pattern_id"].astype(str),
        "trip_start":      pd.to_datetime(raw["trip_start"], errors="coerce"),
        "trip_end":        pd.to_datetime(raw["trip_end"], errors="coerce"),
        "start_place":     raw["start_place"].astype(str),
        "end_place":       raw["end_place"].astype(str),
        "revenue_seconds": pd.to_numeric(raw["revenue_seconds"], errors="coerce").astype("Int64"),
        "revenue_meters":  pd.to_numeric(raw["revenue_meters"], errors="coerce").astype("Int64"),
        "insert_dt":       pd.to_datetime(raw["insert_dt"], errors="coerce"),
        "_md_filename":    raw["_md_filename"].astype(str),
        "_md_processed_at": pd.to_datetime(raw["_md_processed_at"]),
    })


# ── Task functions ─────────────────────────────────────────────────────────────

def process_sonnell_checkpoin(**context):
    log = logging.getLogger(__name__)
    opts = _storage_options()
    container = _blob_container_client()
    execution_date = context.get("data_interval_start")

    raw_path  = f"{BASE_PATH}/raw_sonnell_checkpoin"
    int_path  = f"{BASE_PATH}/intermediate_sonnell_checkpoin"
    gold_path = f"{BASE_PATH}/sonnell_checkpoin"

    # ── Raw ───────────────────────────────────────────────────────────────────
    _process_raw(container, "sonnell_checkpoin", raw_path, opts, log, execution_date)

    # ── Intermediate ──────────────────────────────────────────────────────────
    try:
        raw = _read_delta_pandas(raw_path, opts)
    except Exception as e:
        log.warning(f"Raw table no disponible, saltando intermediate/gold: {e}")
        return

    interm = _cast_checkpoin(raw)
    del raw
    write_deltalake(int_path, interm, mode="overwrite", schema_mode="overwrite", storage_options=opts)
    log.info(f"Intermediate: {len(interm)} registros escritos en {int_path}")

    # ── Gold ──────────────────────────────────────────────────────────────────
    gold = (
        interm
        .sort_values("_md_processed_at", ascending=False)
        .drop_duplicates(subset=["trip_key", "svc_date"])
    )
    del interm
    write_deltalake(
        gold_path, gold,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=["svc_date"],
        storage_options=opts,
    )
    log.info(f"Gold: {len(gold)} registros únicos escritos en {gold_path}")


def process_sonnell_subsystem(**context):
    log = logging.getLogger(__name__)
    opts = _storage_options()
    container = _blob_container_client()
    execution_date = context.get("data_interval_start")

    raw_path  = f"{BASE_PATH}/raw_sonnell_subsystem"
    int_path  = f"{BASE_PATH}/intermediate_sonnell_subsystem"
    gold_path = f"{BASE_PATH}/sonnell_subsystem"

    # ── Raw ───────────────────────────────────────────────────────────────────
    _process_raw(container, "sonnell_subsystem", raw_path, opts, log, execution_date)

    # ── Intermediate ──────────────────────────────────────────────────────────
    try:
        raw = _read_delta_pandas(raw_path, opts)
    except Exception as e:
        log.warning(f"Raw table no disponible, saltando intermediate/gold: {e}")
        return

    interm = _cast_subsystem(raw)
    del raw
    write_deltalake(int_path, interm, mode="overwrite", schema_mode="overwrite", storage_options=opts)
    log.info(f"Intermediate: {len(interm)} registros escritos en {int_path}")

    # ── Gold ──────────────────────────────────────────────────────────────────
    gold = (
        interm
        .sort_values("_md_processed_at", ascending=False)
        .drop_duplicates(subset=["subsystem", "svc_date"])
    )
    del interm
    write_deltalake(
        gold_path, gold,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=["svc_date"],
        storage_options=opts,
    )
    log.info(f"Gold: {len(gold)} registros únicos escritos en {gold_path}")


def process_sonnell_trip(**context):
    log = logging.getLogger(__name__)
    opts = _storage_options()
    container = _blob_container_client()
    execution_date = context.get("data_interval_start")

    raw_path  = f"{BASE_PATH}/raw_sonnell_trip"
    int_path  = f"{BASE_PATH}/intermediate_sonnell_trip"
    gold_path = f"{BASE_PATH}/sonnell_trip"

    # ── Raw ───────────────────────────────────────────────────────────────────
    _process_raw(container, "sonnell_trip", raw_path, opts, log, execution_date)

    # ── Intermediate ──────────────────────────────────────────────────────────
    try:
        raw = _read_delta_pandas(raw_path, opts)
    except Exception as e:
        log.warning(f"Raw table no disponible, saltando intermediate/gold: {e}")
        return

    interm = _cast_trip(raw)
    del raw
    write_deltalake(int_path, interm, mode="overwrite", schema_mode="overwrite", storage_options=opts)
    log.info(f"Intermediate: {len(interm)} registros escritos en {int_path}")

    # ── Gold ──────────────────────────────────────────────────────────────────
    gold = (
        interm
        .sort_values("_md_processed_at", ascending=False)
        .drop_duplicates(subset=["trip_key", "svc_date"])
    )
    del interm
    write_deltalake(
        gold_path, gold,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=["svc_date"],
        storage_options=opts,
    )
    log.info(f"Gold: {len(gold)} registros únicos escritos en {gold_path}")


# ── DAG ────────────────────────────────────────────────────────────────────────

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
    dag_id="dag_sonnell_daily",
    description="Sonnell checkpoint / subsystem / trip → Delta (Raw→Intermediate→Gold)",
    schedule_interval="0 13 * * *",  # 8:00 AM UTC-5
    start_date=datetime.datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["sonnell", "medallion", "daily"],
    doc_md=__doc__,
) as dag:

    t_checkpoin = PythonOperator(
        task_id="sonnell_checkpoin",
        python_callable=process_sonnell_checkpoin,
    )

    t_subsystem = PythonOperator(
        task_id="sonnell_subsystem",
        python_callable=process_sonnell_subsystem,
    )

    t_trip = PythonOperator(
        task_id="sonnell_trip",
        python_callable=process_sonnell_trip,
    )

    # Secuencial para controlar el pico de memoria en Docker local
    t_checkpoin >> t_subsystem >> t_trip
