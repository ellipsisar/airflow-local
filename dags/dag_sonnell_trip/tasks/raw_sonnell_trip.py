"""
RAW layer — sonnell_trip  (notebook cell 1 in Python-native delta-rs).

Notebook cell 1 replicated with three performance improvements over the original:

1. No .collect() equivalent:
   The notebook did df_blobs.collect() to materialise the full blob list in driver memory
   before iterating. Here we filter blob metadata lazily via list_blobs() iterator — no
   full materialisation.

2. Parallel ZIP download+parse (ThreadPoolExecutor):
   The notebook processed ZIPs sequentially. Network I/O (Azure download) dominates per-file
   time. ThreadPoolExecutor with MAX_ZIP_WORKERS=4 runs downloads concurrently, cutting
   wall-clock time roughly proportional to the number of parallel workers.

3. Single batched Delta write:
   All parsed DataFrames are concatenated and written in ONE write_deltalake call (one Delta
   transaction) instead of one per ZIP. Fewer transactions = less metadata overhead + no
   risk of partial writes leaving the table in a mixed state.
"""

import io
import zipfile
import datetime
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from deltalake import write_deltalake

from dag_sonnell_trip.config.settings import (
    SOURCE_CONTAINER, SOURCE_PREFIX, FILE_PATTERN,
    RAW_PATH, MAX_LOOKBACK_DAYS, MAX_ZIP_WORKERS,
)
from dag_sonnell_trip.utils.delta_session import storage_options, blob_container_client
from dag_sonnell_trip.utils.control_table import get_processed_files, write_control

log = logging.getLogger(__name__)


def _parse_zip(container_client, blob_name: str, file_name: str) -> tuple:
    """
    Downloads and parses a single ZIP file. Returns (df|None, record_count, error|None).

    Isolated into its own function so ThreadPoolExecutor can submit it independently
    for each blob without sharing mutable state between workers.

    ContainerClient from azure-storage-blob is thread-safe (stateless HTTP under the hood),
    so sharing the same client across threads is safe.
    """
    try:
        content = container_client.get_blob_client(blob_name).download_blob().readall()
        dfs = []
        with zipfile.ZipFile(io.BytesIO(content), "r") as z:
            for csv_name in z.namelist():
                if not csv_name.endswith(".csv"):
                    continue
                with z.open(csv_name) as f:
                    df = pd.read_csv(f)
                if df.empty:
                    # Empty CSVs are expected for some service dates; log but don't fail
                    log.warning("Empty CSV (expected for some svc_dates): %s / %s", file_name, csv_name)
                    continue
                # Metadata columns mirror notebook: _md_filename + _md_processed_at
                df["_md_filename"] = file_name
                df["_md_processed_at"] = datetime.datetime.utcnow()
                dfs.append(df)

        if not dfs:
            # ZIP contained only empty CSVs — mark processed with 0 rows, don't retry
            return None, 0, None

        combined = pd.concat(dfs, ignore_index=True)
        return combined, len(combined), None

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
        log.error("Error processing %s: %s", file_name, error_msg)
        return None, 0, error_msg


def run_raw(**context) -> dict:
    """
    Incremental RAW ingestion for sonnell_trip.

    Returns a stats dict that is pushed to XCom and consumed by the INTERMEDIATE task
    to decide whether a full overwrite is warranted on this run.
    """
    opts = storage_options()
    container = blob_container_client(SOURCE_CONTAINER)
    execution_date = context.get("data_interval_start")

    # ── Lookback cutoff from execution context ─────────────────────────────────
    # Derived from Airflow's data_interval_start (never datetime.now()) so the DAG
    # is deterministic when backfilling or retrying.
    cutoff = None
    if execution_date:
        exec_date = execution_date.date() if hasattr(execution_date, "date") else execution_date
        cutoff = exec_date - datetime.timedelta(days=MAX_LOOKBACK_DAYS)

    # ── Exclusion set from control table ──────────────────────────────────────
    already_done = get_processed_files(opts)
    log.info("Control table: %d files already processed/failed.", len(already_done))

    # ── Filter candidate blobs (lazy iterator — no .collect() equivalent) ─────
    candidates = []
    for blob in container.list_blobs(name_starts_with=SOURCE_PREFIX):
        fname = blob.name.split("/")[-1]
        if FILE_PATTERN not in fname or not fname.endswith(".zip"):
            continue
        if fname in already_done:
            log.info("Skipping (already in control table): %s", fname)
            continue
        if cutoff:
            try:
                fdate = datetime.date(int(fname[:4]), int(fname[4:6]), int(fname[6:8]))
                if fdate < cutoff:
                    log.info("Skipping (outside %d-day lookback window): %s", MAX_LOOKBACK_DAYS, fname)
                    continue
            except (ValueError, IndexError):
                pass  # filename doesn't start with YYYYMMDD — process it anyway
        candidates.append(blob)

    if not candidates:
        log.info("RAW: no new files to process.")
        return {"files_processed": 0, "records_written": 0, "new_files": []}

    log.info("RAW: %d new ZIP(s) to process.", len(candidates))

    # ── Parallel ZIP download + parse ─────────────────────────────────────────
    raw_dfs: list = []
    control_records: list = []

    def _submit(blob):
        fname = blob.name.split("/")[-1]
        df, count, error = _parse_zip(container, blob.name, fname)
        return fname, df, count, error

    with ThreadPoolExecutor(max_workers=MAX_ZIP_WORKERS) as pool:
        futures = {pool.submit(_submit, b): b for b in candidates}
        for future in as_completed(futures):
            try:
                fname, df, count, error = future.result()
            except Exception as exc:
                log.error("Unexpected error in worker thread: %s", traceback.format_exc())
                raise
            if error:
                control_records.append({
                    "filename": fname,
                    "status": "failed",
                    "process_datetime": datetime.datetime.utcnow(),
                    "error_message": error[:4000],  # truncate to prevent schema issues
                    "record_count": 0,
                })
            else:
                if df is not None:
                    raw_dfs.append(df)
                control_records.append({
                    "filename": fname,
                    "status": "processed",
                    "process_datetime": datetime.datetime.utcnow(),
                    "error_message": None,
                    "record_count": count,
                })

    # ── Single batched Delta write (1 transaction vs. N) ──────────────────────
    total_records = 0
    new_files = [r["filename"] for r in control_records if r["status"] == "processed"]

    if raw_dfs:
        batch = pd.concat(raw_dfs, ignore_index=True)
        total_records = len(batch)
        log.info(
            "RAW: writing batch — rows=%d, cols=%d, dtypes:\n%s",
            len(batch), len(batch.columns), batch.dtypes.to_string(),
        )
        try:
            write_deltalake(
                RAW_PATH, batch,
                mode="append",
                schema_mode="merge",
                storage_options=opts,
            )
        except Exception as exc:
            log.error("write_deltalake FAILED for %s: %s", RAW_PATH, traceback.format_exc())
            raise
        log.info("RAW: %d records written from %d file(s).", total_records, len(raw_dfs))
        del batch

    write_control(control_records, opts)

    return {
        "files_processed": len(control_records),
        "records_written": total_records,
        "new_files": new_files,
    }
