# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Validation Pipeline — V3 Multi-Server
# MAGIC
# MAGIC **Execution**: Databricks Job with dedicated clusters
# MAGIC - **Source**: Per-server CSVs + schema JSON from Azure Blob Storage (`wasbs://`)
# MAGIC - **Target**: Snowflake via native Spark-Snowflake connector
# MAGIC - **Output**: `diff_report.csv` written back to Azure Blob Storage
# MAGIC - **Secrets**: Azure Key Vault via Databricks secret scope `etl-secrets`
# MAGIC - **Flow**: Load per-server → Transform → Union → Verify Target → Compare

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 1: CONFIGURATION — Job Parameters
# ═══════════════════════════════════════════════════════════════

import logging, time as _time, os, sys

logging.basicConfig(
    format="[%(asctime)s] %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
_log = logging.getLogger("etl_pipeline")

# ── Job Parameters (passed via Databricks Job config) ──────────
dbutils.widgets.text("TABLE_NAME", "public_dim_vehicle_master")
dbutils.widgets.text("SUB_PATH", "xl")
dbutils.widgets.text("STORAGE_ACCOUNT", "etlstorage0907")
dbutils.widgets.text("CONTAINER", "etl-source-data")
dbutils.widgets.text("PK_FILTER_MODE", "full")
dbutils.widgets.text("DATE_WATERMARK_MODE", "full")
dbutils.widgets.text("PK_RANGE_LOWER", "")
dbutils.widgets.text("PK_RANGE_UPPER", "")
dbutils.widgets.text("DATE_FROM", "")
dbutils.widgets.text("DATE_FROM_COL", "")
dbutils.widgets.text("DATE_TO", "")
dbutils.widgets.text("DATE_TO_COL", "")

# ── Snowflake database override (widget vs secrets toggle) ─────
dbutils.widgets.text("SF_DATABASE", "ANALYTICS_DW")
SF_DATABASE = dbutils.widgets.get("SF_DATABASE") or None

TABLE_NAME = dbutils.widgets.get("TABLE_NAME")
SUB_PATH = dbutils.widgets.get("SUB_PATH")
STORAGE_ACCOUNT = dbutils.widgets.get("STORAGE_ACCOUNT")
CONTAINER = dbutils.widgets.get("CONTAINER")
PK_FILTER_MODE = dbutils.widgets.get("PK_FILTER_MODE")
DATE_WATERMARK_MODE = dbutils.widgets.get("DATE_WATERMARK_MODE")

# PK range (optional)
_pk_lower = dbutils.widgets.get("PK_RANGE_LOWER")
_pk_upper = dbutils.widgets.get("PK_RANGE_UPPER")
PK_RANGE = {
    "lower": int(_pk_lower) if _pk_lower else None,
    "upper": int(_pk_upper) if _pk_upper else None,
}
PK_SET = set()

# Date watermark (optional)
DATE_FROM = dbutils.widgets.get("DATE_FROM") or None
DATE_FROM_COL = dbutils.widgets.get("DATE_FROM_COL") or None
DATE_TO = dbutils.widgets.get("DATE_TO") or None
DATE_TO_COL = dbutils.widgets.get("DATE_TO_COL") or None

EXCLUDE_COLS = ["load_ts", "batch_id"]

_log.info(f"Table        : {TABLE_NAME}")
_log.info(f"Sub-path     : {SUB_PATH}")
_log.info(f"Storage      : {STORAGE_ACCOUNT}/{CONTAINER}")
_log.info(f"PK filter    : {PK_FILTER_MODE}")
_log.info(f"Date filter  : {DATE_WATERMARK_MODE}")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 2: AZURE BLOB STORAGE — Configure wasbs:// access
# ═══════════════════════════════════════════════════════════════

BLOB_BASE = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"
# Per-server paths: BLOB_TABLE_BASE/<server>/source_raw.csv
BLOB_TABLE_BASE = f"{BLOB_BASE}/{TABLE_NAME}/{SUB_PATH}"
OUTPUT_DIR = f"{BLOB_BASE}/output/{TABLE_NAME}/{SUB_PATH}"
REPORT_CSV = f"{OUTPUT_DIR}/diff_report.csv"

_log.info(f"Blob base    : {BLOB_TABLE_BASE}")
_log.info(f"Output dir   : {OUTPUT_DIR}")
_log.info(f"Report CSV   : {REPORT_CSV}")

_log.info("✅ Azure Blob Storage configured")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 3: ADD PROJECT TO PYTHON PATH
# ═══════════════════════════════════════════════════════════════

REPO_PATH = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()

if not os.path.isdir(REPO_PATH):
    _log.error(f"Repo not found at {REPO_PATH}")
    raise FileNotFoundError(f"Repo not found: {REPO_PATH}")

if REPO_PATH not in sys.path:
    sys.path.insert(0, REPO_PATH)

_log.info(f"✅ Repo found: {REPO_PATH}")
_log.info(f"   Contents: {os.listdir(REPO_PATH)}")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 4: SPARK CONFIG
# ═══════════════════════════════════════════════════════════════

try:
    spark.conf.set("spark.sql.debug.maxToStringFields", 500)
except Exception:
    pass
try:
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
except Exception:
    pass

_log.info("✅ Spark config set")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 5: BUILD PIPELINE CONTEXT
# ═══════════════════════════════════════════════════════════════

from utils.auto_config import get_table_config
from utils.logger import get_logger
from utils.custom_execution_utils import build_load_filters

logger = get_logger("etl_pipeline")

config = get_table_config(TABLE_NAME, target_mode="snowflake")

_log.info(f"✅ Auto-config loaded for: {TABLE_NAME}")
_log.info(f"   Target table : {config.get('target_table')}")
_log.info(f"   Primary Keys : {config['primary_keys']}")
_log.info(f"   Servers      : {[s['server_name'] for s in config['servers']]}")

SOURCE_FILTER, TARGET_FILTER = build_load_filters(
    config=config,
    pk_filter_mode=PK_FILTER_MODE,
    pk_range=PK_RANGE,
    pk_set=PK_SET,
    date_mode=DATE_WATERMARK_MODE,
    date_from=DATE_FROM,
    date_from_col=DATE_FROM_COL,
    date_to=DATE_TO,
    date_to_col=DATE_TO_COL,
)

_log.info(f"   Source filter: {SOURCE_FILTER['description']}")
_log.info(f"   Target filter: {TARGET_FILTER['description']}")

pipeline_ctx = dict(
    config=config,
    target_mode="snowflake",
    target_query=None,
    target_query_file=config["target_query_file"],
    primary_keys=config["primary_keys"],
    exclude_cols=EXCLUDE_COLS,
    compare_cols=None,
    target_ddl=config["target_ddl"],
    report_csv=REPORT_CSV,
    source_filter=SOURCE_FILTER,
    target_filter=TARGET_FILTER,
    sf_database_override=SF_DATABASE,
)

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 6: STEP 1 — LOAD + TRANSFORM + UNION (per server)
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_1_load_transform_union

_t0 = _time.time()
transformed_df, _row_count = step_1_load_transform_union(spark, pipeline_ctx, BLOB_TABLE_BASE)
_log.info(f"✅ Load+Transform+Union: {_row_count} rows, {len(transformed_df.columns)} columns ({_time.time()-_t0:.1f}s)")
_log.info(f"   Columns: {transformed_df.columns}")
display(transformed_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 7: STEP 2 — VERIFY TARGET SCHEMA (Snowflake live)
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_2_verify_target_schema

_t0 = _time.time()
passed = step_2_verify_target_schema(spark, pipeline_ctx)
if passed:
    _log.info(f"✅ Target schema verification PASSED ({_time.time()-_t0:.1f}s)")
else:
    _log.warning(f"❌ Target schema verification FAILED ({_time.time()-_t0:.1f}s) — check logs above")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 8: STEP 3 — EXTRACT TARGET FROM SNOWFLAKE
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_3_extract_target

_t0 = _time.time()
target_df, _tgt_row_count = step_3_extract_target(spark, pipeline_ctx)
_log.info(f"✅ Target loaded: {_tgt_row_count} rows, {len(target_df.columns)} columns ({_time.time()-_t0:.1f}s)")
display(target_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 9: STEP 4 — COMPARE & GENERATE DIFF REPORT
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_4_compare

_t0 = _time.time()
exit_code = step_4_compare(spark, transformed_df, target_df, pipeline_ctx)
elapsed = _time.time() - _t0

if exit_code == 0:
    _log.info(f"🟢 PASS — No differences found! ({elapsed:.1f}s)")
else:
    _log.warning(f"🔴 FAIL — Differences found. ({elapsed:.1f}s)")
    _log.warning(f"   Report: {REPORT_CSV}")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 10: VIEW RESULTS
# ═══════════════════════════════════════════════════════════════

try:
    report_df = spark.read.option("header", True).csv(REPORT_CSV)
    _log.info(f"Diff report: {report_df.count()} rows")
    display(report_df)
except Exception:
    _log.info("No diff report file — either PASS or report path issue")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 11: CACHE CLEANUP
# ═══════════════════════════════════════════════════════════════

_log.info("Clearing any remaining cached DataFrames...")
spark.catalog.clearCache()
_log.info("✅ Cache cleanup complete")
