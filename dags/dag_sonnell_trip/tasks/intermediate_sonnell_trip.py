"""
INTERMEDIATE layer — sonnell_trip  (notebook cell 2 in Python-native delta-rs).

Notebook cell 2 replicated with two optimizations:

1. Skip-if-no-new-data:
   If the upstream RAW task reported 0 new records, we skip the full overwrite of
   INTERMEDIATE. On days without new ZIPs this saves a complete read+write cycle
   (~the largest table read in the pipeline). When there IS new data, a full overwrite
   is still required because INTERMEDIATE is always rebuilt from all of RAW.

2. del raw before write:
   Frees the raw DataFrame (~same size as intermediate) before allocating the write
   buffer, keeping peak memory at ~1x table size instead of ~2x.

Notebook equivalence:
  DROP TABLE IF EXISTS ati_lakehouse.intermediate_sonnell_trip  →  schema_mode="overwrite"
  df.write.format("delta").mode("overwrite").save(...)          →  write_deltalake(..., mode="overwrite")
  cast(col(...).as("date"))                                      →  pd.to_datetime(...).dt.date
  cast(col(...).as("int"))                                       →  pd.to_numeric(...).astype("Int64")
"""

import logging

import pandas as pd
from deltalake import write_deltalake

from dag_sonnell_trip.config.settings import RAW_PATH, INTERMEDIATE_PATH
from dag_sonnell_trip.utils.delta_session import storage_options, read_delta

log = logging.getLogger(__name__)


def _cast(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Explicit column-by-column type casting — mirrors notebook cell 2 exactly.

    errors='coerce' converts unparseable values to NaT/NaN (Spark's permissive cast
    equivalent) so bad rows don't abort the entire batch.
    """
    return pd.DataFrame({
        "svc_date":         pd.to_datetime(raw["svc_date"], errors="coerce").dt.date,
        "trip_key":         raw["trip_key"].astype(str),
        "sched_trip":       raw["sched_trip"].astype(str),
        "vehicle_id":       raw["vehicle_id"].astype(str),
        "route_id":         raw["route_id"].astype(str),
        "pattern_id":       raw["pattern_id"].astype(str),
        "trip_start":       pd.to_datetime(raw["trip_start"], errors="coerce"),
        "trip_end":         pd.to_datetime(raw["trip_end"], errors="coerce"),
        "start_place":      raw["start_place"].astype(str),
        "end_place":        raw["end_place"].astype(str),
        "revenue_seconds":  pd.to_numeric(raw["revenue_seconds"], errors="coerce").astype("Int64"),
        "revenue_meters":   pd.to_numeric(raw["revenue_meters"], errors="coerce").astype("Int64"),
        "insert_dt":        pd.to_datetime(raw["insert_dt"], errors="coerce"),
        "_md_filename":     raw["_md_filename"].astype(str),
        "_md_processed_at": pd.to_datetime(raw["_md_processed_at"]),
    })


def run_intermediate(raw_stats: dict) -> dict:
    """
    Full overwrite of intermediate_sonnell_trip with explicitly typed columns.

    raw_stats is passed from the RAW task via XCom and controls the skip-optimization.
    Returns a stats dict consumed by the FINAL task's data quality check.
    """
    opts = storage_options()

    # ── Optimization: skip full overwrite if RAW had no new records ───────────
    if raw_stats.get("records_written", 0) == 0:
        log.info("INTERMEDIATE: RAW had no new records — skipping overwrite.")
        try:
            existing = read_delta(INTERMEDIATE_PATH, opts)
            return {"records_written": len(existing)}
        except Exception:
            return {"records_written": 0}

    # ── Read all of RAW (required for overwrite; can't partially update) ───────
    raw = read_delta(RAW_PATH, opts)

    # ── Data quality check: fail loudly before touching INTERMEDIATE ──────────
    if raw.empty:
        raise ValueError(
            f"DQ CHECK FAILED: {RAW_PATH} is empty. "
            "Aborting INTERMEDIATE write to protect the Gold layer from data loss."
        )
    log.info("INTERMEDIATE: read %d rows from RAW.", len(raw))

    intermediate = _cast(raw)
    del raw  # free ~1x table size before the write allocates its buffer

    write_deltalake(
        INTERMEDIATE_PATH, intermediate,
        mode="overwrite",
        schema_mode="overwrite",  # mirrors DROP TABLE + recreate from notebook cell 2
        storage_options=opts,
    )
    count = len(intermediate)
    log.info("INTERMEDIATE: %d records written.", count)
    del intermediate

    return {"records_written": count}
