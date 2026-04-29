"""
Helpers for the control_raw_file_status Delta table.

Schema (do NOT change — consumed by other DAGs and external processes):
  filename (str), status (str), process_datetime (timestamp),
  error_message (str), record_count (int64)

Idempotency contract:
  Files with status 'processed' OR 'failed' are both excluded from reprocessing.
  'failed' files require manual intervention (investigate + delete the control record)
  before the DAG will retry them. This mirrors the notebook's original behavior.
"""

import logging
import datetime

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from dag_sonnell_trip.config.settings import CONTROL_PATH

log = logging.getLogger(__name__)

_COLUMNS = ["filename", "status", "process_datetime", "error_message", "record_count"]


def get_processed_files(opts: dict) -> set:
    """
    Returns filenames already registered as 'processed' OR 'failed'.
    On the very first run the table doesn't exist yet; we return an empty set
    so all files are treated as candidates.
    """
    try:
        dt = DeltaTable(CONTROL_PATH, storage_options=opts)
        # Only read the two columns we need — avoids pulling the full table into memory
        df = dt.to_pyarrow_table(columns=["filename", "status"]).to_pandas()
        return set(df.loc[df["status"].isin(["processed", "failed"]), "filename"].tolist())
    except Exception as exc:
        log.info("Control table not found (likely first run): %s", exc)
        return set()


def write_control(records: list, opts: dict) -> None:
    """
    Appends one control record per processed ZIP to the shared control table.
    Uses schema_mode='merge' so new columns added by other DAGs are preserved.
    """
    if not records:
        return
    df = pd.DataFrame(records, columns=_COLUMNS)
    df["process_datetime"] = pd.to_datetime(df["process_datetime"])
    df["record_count"] = df["record_count"].astype("int64")
    df["error_message"] = df["error_message"].fillna("").astype(str)
    write_deltalake(
        CONTROL_PATH, df,
        mode="append",
        schema_mode="merge",
        storage_options=opts,
    )
    log.info("Control: wrote %d records.", len(records))
