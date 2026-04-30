"""
FINAL (Gold) layer — sonnell_trip  (notebook cell 3 in Python-native delta-rs).

Notebook cell 3 replicated with an exact logical equivalence for the Window deduplication.

Notebook (PySpark):
    w = Window.partitionBy("trip_key", "svc_date").orderBy(col("_md_processed_at").desc())
    df.withColumn("rank", row_number().over(w)).filter(col("rank") == 1).drop("rank")

pandas equivalent:
    df.sort_values("_md_processed_at", ascending=False)
      .drop_duplicates(subset=["trip_key", "svc_date"], keep="first")

Both produce the same result: one row per (trip_key, svc_date) pair — the row with the
largest _md_processed_at value. The pandas version has zero JVM/Spark overhead and runs
entirely in-process on the Celery worker.

Additional notebook equivalences:
  DROP TABLE IF EXISTS ati_lakehouse.sonnell_trip  →  schema_mode="overwrite"
  .write.partitionBy("svc_date")                  →  partition_by=PARTITION_BY
  .option("overwriteSchema", "true")              →  schema_mode="overwrite"
"""

import logging

import pandas as pd
from deltalake import write_deltalake

from dag_sonnell_trip.config.settings import INTERMEDIATE_PATH, FINAL_PATH, NATURAL_KEY, PARTITION_BY
from dag_sonnell_trip.utils.delta_session import storage_options, read_delta

log = logging.getLogger(__name__)


def run_final(intermediate_stats: dict) -> dict:
    """
    Deduplication + full overwrite into sonnell_trip (Gold / Final layer).

    intermediate_stats is passed from the INTERMEDIATE task via XCom.
    Returns a stats dict for the summary task.
    """
    opts = storage_options()

    # ── Skip if INTERMEDIATE has nothing (avoids writing an empty Gold table) ─
    if intermediate_stats.get("records_written", 0) == 0:
        log.info("FINAL: INTERMEDIATE had 0 records — skipping.")
        return {"records_written": 0}

    # ── Read INTERMEDIATE ─────────────────────────────────────────────────────
    intermediate = read_delta(INTERMEDIATE_PATH, opts)

    # ── Data quality check: fail loudly before touching Gold ──────────────────
    if intermediate.empty:
        raise ValueError(
            f"DQ CHECK FAILED: {INTERMEDIATE_PATH} is empty. "
            "Aborting FINAL write to protect the Gold layer from data loss."
        )
    log.info("FINAL: read %d rows from INTERMEDIATE.", len(intermediate))

    # ── Deduplication — mirrors Window.partitionBy + ROW_NUMBER == 1 ──────────
    # sort DESC on _md_processed_at, then drop_duplicates(keep="first") keeps
    # the most-recent record per natural key — identical semantics to the notebook.
    gold = (
        intermediate
        .sort_values("_md_processed_at", ascending=False)
        .drop_duplicates(subset=NATURAL_KEY, keep="first")
    )
    del intermediate  # free memory before the write buffer is allocated

    # ── Overwrite Gold, partitioned by svc_date ───────────────────────────────
    write_deltalake(
        FINAL_PATH, gold,
        mode="overwrite",
        schema_mode="overwrite",   # mirrors .option("overwriteSchema", "true")
        partition_by=PARTITION_BY,
        storage_options=opts,
    )
    count = len(gold)
    log.info(
        "FINAL: %d deduplicated records written, partitioned by %s.",
        count, PARTITION_BY,
    )
    del gold

    return {"records_written": count}
