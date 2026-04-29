"""
Delta Lake and Azure Blob connectivity helpers.

Named delta_session.py (analogous to spark_session.py) because this project uses delta-rs
(Python-native Delta protocol binding) instead of PySpark. delta-rs provides identical Delta
guarantees — ACID transactions, schema evolution, time travel — without a JVM, which is
critical in a Docker Airflow environment that has no Spark cluster.

Notebook equivalences:
  spark_session.py SparkSession.builder → this module's storage_options() + DeltaTable()
  spark.read.format("delta").load(path)  → read_delta(path, opts)
  spark.table("ati_lakehouse.table")     → read_delta(DELTA_PATH, opts)  [avoids metastore]
"""

import logging

import pandas as pd
from airflow.models import Variable
from azure.storage.blob import BlobServiceClient
from deltalake import DeltaTable

from dag_sonnell_trip.config.settings import ACCOUNT_NAME

log = logging.getLogger(__name__)


def storage_options() -> dict:
    """Builds ADLS Gen2 credentials from Airflow Variable (never hardcoded)."""
    return {
        "account_name": ACCOUNT_NAME,
        "account_key": Variable.get("AZURE_STORAGE_ACCOUNT_KEY"),
    }


def blob_container_client(container: str):
    """Returns a thread-safe Azure Blob ContainerClient."""
    key = Variable.get("AZURE_STORAGE_ACCOUNT_KEY")
    return (
        BlobServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
            credential=key,
        )
        .get_container_client(container)
    )


def read_delta(path: str, opts: dict) -> pd.DataFrame:
    """
    Reads a Delta table into pandas via PyArrow.

    Performance note: DeltaTable → PyArrow → pandas uses columnar memory layout throughout,
    which is more efficient than loading into a pandas DataFrame directly. PyArrow also allows
    column/row pruning before materialising into pandas, reducing peak memory.

    Notebook equivalent: spark.read.format("delta").load(path) or spark.table(...)
    """
    dt = DeltaTable(path, storage_options=opts)
    return dt.to_pyarrow_table().to_pandas()
