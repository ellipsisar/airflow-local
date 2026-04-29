"""
DAG: dag_sonnell_trip

Replicates the Synapse Analytics PySpark notebook as a pure-Python Airflow DAG.
Processing stack: delta-rs (Python-native, no JVM) + pandas + pyarrow.

Pipeline (strictly sequential — each layer is a gate for the next):
  1. raw_sonnell_trip        — incremental append of new ZIPs → raw_sonnell_trip
  2. intermediate_sonnell_trip — full typed overwrite → intermediate_sonnell_trip
  3. final_sonnell_trip      — deduplication + overwrite → sonnell_trip (partitioned by svc_date)
  4. pipeline_summary        — logs completion stats; hook here for EmailOperator if needed

Idempotency:
  - Control table prevents double-processing ZIPs (raw layer).
  - Overwrite semantics on INTERMEDIATE and FINAL mean re-running is safe.
  - Skipping INTERMEDIATE/FINAL on no-op days (no new ZIPs) is also idempotent.

Schedule: 08:00 America/Puerto_Rico (= 13:00 UTC) daily.
Start date: set to the actual deployment date before activating; 2025-01-01 is a safe default.

Required Airflow Variables:
  AZURE_STORAGE_ACCOUNT_KEY — account key for aticdwstorage
"""

import logging
import traceback
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.email import send_email

from dag_sonnell_trip.config.settings import DEFAULT_ALERT_EMAILS
from dag_sonnell_trip.tasks.raw_sonnell_trip import run_raw
from dag_sonnell_trip.tasks.intermediate_sonnell_trip import run_intermediate
from dag_sonnell_trip.tasks.final_sonnell_trip import run_final

log = logging.getLogger(__name__)


def _on_failure_callback(context: dict) -> None:
    """
    Logs the full stack trace on task failure and sends a custom-subject alert email.

    email_on_failure=True in default_args sends Airflow's built-in alert; this callback
    adds: (a) structured logging with stack trace, (b) a richer email subject line that
    includes DAG, task, and execution date — easier to triage from a shared inbox.
    """
    ti = context["task_instance"]
    exc = context.get("exception")
    exec_date = context.get("execution_date")
    exec_date_str = exec_date.strftime("%Y-%m-%d") if exec_date else "unknown"

    log.error(
        "TASK FAILED | dag=%s | task=%s | run=%s | exec_date=%s",
        ti.dag_id, ti.task_id, ti.run_id, exec_date_str,
    )
    if exc:
        log.error("Exception details:\n%s", traceback.format_exc())

    subject = f"[AIRFLOW FAILURE] {ti.dag_id} | {ti.task_id} | {exec_date_str}"
    body = (
        f"<b>DAG:</b> {ti.dag_id}<br>"
        f"<b>Task:</b> {ti.task_id}<br>"
        f"<b>Execution date:</b> {exec_date_str}<br>"
        f"<b>Run ID:</b> {ti.run_id}<br><br>"
        f"<pre>{traceback.format_exc()}</pre>"
    )
    try:
        send_email(to=DEFAULT_ALERT_EMAILS, subject=subject, html_content=body)
    except Exception as mail_exc:
        log.warning("Could not send failure email: %s", mail_exc)


default_args = {
    "owner": "prita",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": DEFAULT_ALERT_EMAILS,
    "email_on_failure": True,
    "email_on_retry": False,
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": _on_failure_callback,
}


@dag(
    dag_id="dag_sonnell_trip",
    description="sonnell_trip · Raw → Intermediate → Final (Delta, Python-native delta-rs)",
    schedule_interval="0 13 * * *",  # 08:00 America/Puerto_Rico = 13:00 UTC
    start_date=pendulum.datetime(2026, 4, 29, tz="America/Puerto_Rico"),
    catchup=False,
    max_active_runs=1,  # prevents concurrent runs from overlapping Delta writes
    default_args=default_args,
    tags=["sonnell", "trip", "medallion", "daily"],
    doc_md=__doc__,
)
def dag_sonnell_trip():

    @task(task_id="raw_sonnell_trip")
    def raw_task() -> dict:
        ctx = get_current_context()
        return run_raw(**ctx)

    @task(task_id="intermediate_sonnell_trip")
    def intermediate_task(raw_stats: dict) -> dict:
        ctx = get_current_context()
        return run_intermediate(raw_stats, **ctx)

    @task(task_id="final_sonnell_trip")
    def final_task(intermediate_stats: dict) -> dict:
        ctx = get_current_context()
        return run_final(intermediate_stats, **ctx)

    @task(task_id="pipeline_summary")
    def summary_task(raw_stats: dict, intermediate_stats: dict, final_stats: dict) -> None:
        """
        Logs a human-readable pipeline completion summary.
        To send a success email, replace the log.info with an EmailOperator call
        or call send_email() directly with the body constructed from these stats.
        """
        log.info(
            "PIPELINE COMPLETE | files_processed=%d | raw_records=%d"
            " | intermediate_records=%d | final_records=%d",
            raw_stats.get("files_processed", 0),
            raw_stats.get("records_written", 0),
            intermediate_stats.get("records_written", 0),
            final_stats.get("records_written", 0),
        )

    # ── Task wiring ────────────────────────────────────────────────────────────
    # TaskFlow API resolves XCom dependencies automatically from the argument names.
    # The strict chain (raw → intermediate → final) enforces the medallion contract:
    # each layer only runs after the previous one completes successfully.
    raw_stats = raw_task()
    intermediate_stats = intermediate_task(raw_stats)
    final_stats = final_task(intermediate_stats)
    summary_task(raw_stats, intermediate_stats, final_stats)


dag_sonnell_trip()
