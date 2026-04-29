"""
Central configuration for dag_sonnell_trip.
All paths, table names, and tunable parameters live here.
Change a value once → propagates to all layers.
"""

# Azure Blob source
ACCOUNT_NAME = "aticdwstorage"
SOURCE_CONTAINER = "korbato"
SOURCE_PREFIX = "sonnell/"

# Delta table base path (ADLS Gen2 via delta-rs abfs driver)
BASE_PATH = "az://synapse/transdev"

# Table paths — names are consumed by external processes; do NOT rename.
CONTROL_PATH = f"{BASE_PATH}/control_raw_file_status"
RAW_PATH = f"{BASE_PATH}/raw_sonnell_trip"
INTERMEDIATE_PATH = f"{BASE_PATH}/intermediate_sonnell_trip"
FINAL_PATH = f"{BASE_PATH}/sonnell_trip"

# Blob filter: only ZIPs whose filename contains this pattern are candidates
FILE_PATTERN = "sonnell_trip"

# Deduplication key for Gold layer (mirrors notebook Window.partitionBy args)
NATURAL_KEY = ["trip_key", "svc_date"]

# Delta partition column for Gold (mirrors notebook partitionBy("svc_date"))
PARTITION_BY = ["svc_date"]

# Alert recipients — override via Airflow Variable "SONNELL_TRIP_ALERT_EMAILS" (JSON list)
DEFAULT_ALERT_EMAILS = ["saldabe@ellipsispr.com"]

# Only process ZIPs whose date (parsed from filename prefix YYYYMMDD) is within this window.
# Prevents inadvertently reprocessing months of history after a clean-slate restart.
MAX_LOOKBACK_DAYS = 10

# ThreadPoolExecutor workers for parallel ZIP download+parse in the RAW layer.
# 2 workers balances download parallelism with Docker memory limits.
# The sonnell_trip CSVs are large; running 4 concurrent pandas loads caused OOM
# in the same way dag_sonnell_daily did before switching to sequential tasks.
MAX_ZIP_WORKERS = 2
