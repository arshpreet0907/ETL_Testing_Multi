# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Validation Pipeline — Azure Databricks
# MAGIC
# MAGIC **Execution**: Databricks Job with dedicated clusters
# MAGIC - **Source**: CSV + schema JSON from Azure Blob Storage (`wasbs://`)
# MAGIC - **Target**: Snowflake via native Spark-Snowflake connector
# MAGIC - **Output**: `diff_report.csv` written back to Azure Blob Storage
# MAGIC - **Secrets**: Azure Key Vault via Databricks secret scope `etl-secrets`
# MAGIC - **Caching**: Selective (transformed + target only; raw source uncached)

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
dbutils.widgets.text("TABLE_NAME", "warranty_claims")
dbutils.widgets.text("SUB_PATH", "xl")
dbutils.widgets.text("STORAGE_ACCOUNT", "etlstorage0907")
dbutils.widgets.text("CONTAINER", "etl-source-data")
dbutils.widgets.text("VERIFY_SCHEMA", "true")
dbutils.widgets.text("PK_FILTER_MODE", "full")
dbutils.widgets.text("DATE_WATERMARK_MODE", "full")
dbutils.widgets.text("PK_RANGE_LOWER", "")
dbutils.widgets.text("PK_RANGE_UPPER", "")
dbutils.widgets.text("DATE_FROM", "")
dbutils.widgets.text("DATE_FROM_COL", "")
dbutils.widgets.text("DATE_TO", "")
dbutils.widgets.text("DATE_TO_COL", "")

# ── Snowflake database override (widget vs secrets toggle) ─────
# Uncomment the next line to pick database from a widget instead of Key Vault:
dbutils.widgets.text("SF_DATABASE", "ETL_OUTPUT_SNOWFLAKE_TARGET_JOINS")
SF_DATABASE = dbutils.widgets.get("SF_DATABASE") or None  # None = use secret
# To always use Key Vault secret, comment out the two lines above and set:
# SF_DATABASE = None

TABLE_NAME = dbutils.widgets.get("TABLE_NAME")
SUB_PATH = dbutils.widgets.get("SUB_PATH")
STORAGE_ACCOUNT = dbutils.widgets.get("STORAGE_ACCOUNT")
CONTAINER = dbutils.widgets.get("CONTAINER")
VERIFY_SCHEMA = dbutils.widgets.get("VERIFY_SCHEMA").lower() == "true"
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
SOURCE_CSV_PATH = f"{BLOB_BASE}/{TABLE_NAME}/{SUB_PATH}/source_raw.csv"
SOURCE_SCHEMA_JSON = f"{BLOB_BASE}/{TABLE_NAME}/{SUB_PATH}/source_raw.schema.json"
SOURCE_DB_SCHEMA_JSON = f"{BLOB_BASE}/{TABLE_NAME}/{SUB_PATH}/source_db_schema.json"

OUTPUT_DIR = f"{BLOB_BASE}/output/{TABLE_NAME}/{SUB_PATH}"
REPORT_CSV = f"{OUTPUT_DIR}/diff_report.csv"

_log.info(f"Source CSV   : {SOURCE_CSV_PATH}")
_log.info(f"Schema JSON  : {SOURCE_SCHEMA_JSON}")
_log.info(f"Db Schema    : {SOURCE_DB_SCHEMA_JSON}")
_log.info(f"Output dir   : {OUTPUT_DIR}")
_log.info(f"Report CSV   : {REPORT_CSV}")

_log.info("✅ Azure Blob Storage configured")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 3: ADD PROJECT TO PYTHON PATH
# ═══════════════════════════════════════════════════════════════

REPO_PATH = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()

# Verify repo/workspace path exists
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
_log.info(f"   Source table : {config.get('source_table')}")
_log.info(f"   Target table : {config.get('target_table')}")
_log.info(f"   Primary Keys : {config['primary_keys']}")
_log.info(f"   Transform    : {os.path.basename(config['transform_file'])}")

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
    verify_schema=VERIFY_SCHEMA,
    source_query=None,
    source_query_file=None,
    target_query=None,
    target_query_file=config["target_query_file"],
    transform_file=config["transform_file"],
    primary_keys=config["primary_keys"],
    exclude_cols=EXCLUDE_COLS,
    compare_cols=None,
    source_ddl=config["source_ddl"],
    target_ddl=config["target_ddl"],
    report_csv=REPORT_CSV,
    source_filter=SOURCE_FILTER,
    target_filter=TARGET_FILTER,
    source_csv_path=SOURCE_CSV_PATH,
    source_schema_json=SOURCE_SCHEMA_JSON,
    source_db_schema_json=SOURCE_DB_SCHEMA_JSON,
    sf_database_override=SF_DATABASE,
)

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 6: STEP 0 — VERIFY SOURCE SCHEMA (.schema.json vs DDL)
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_0_verify_source_schema

_t0 = _time.time()
passed = step_0_verify_source_schema(spark, pipeline_ctx)
if passed:
    _log.info(f"✅ Source schema verification PASSED ({_time.time()-_t0:.1f}s)")
else:
    _log.warning(f"❌ Source schema verification FAILED ({_time.time()-_t0:.1f}s) — check logs above")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 7: STEP 1 — LOAD SOURCE CSV FROM AZURE BLOB STORAGE
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_1_extract_source

_t0 = _time.time()
source_df = step_1_extract_source(spark, pipeline_ctx)
_log.info(f"✅ Source loaded (lazy): {len(source_df.columns)} columns ({_time.time()-_t0:.1f}s)")
display(source_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 8: STEP 2 — TRANSFORM (cached for reuse in comparison)
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_2_transform

_t0 = _time.time()
transformed_df, _src_row_count = step_2_transform(source_df, pipeline_ctx, "snowflake")
_log.info(f"✅ Transformed: {_src_row_count} rows, {len(transformed_df.columns)} columns ({_time.time()-_t0:.1f}s)")
_log.info(f"   Columns: {transformed_df.columns}")
display(transformed_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 9: STEP 3.5 — VERIFY TARGET SCHEMA (Snowflake live)
# ═══════════════════════════════════════════════════════════════
# Requires Snowflake connector library installed on cluster:
#   Maven: net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5
#   Maven: net.snowflake:snowflake-jdbc:3.18.0

from utils.custom_execution_utils import step_3_5_verify_target_schema

_t0 = _time.time()
passed = step_3_5_verify_target_schema(spark, pipeline_ctx)
if passed:
    _log.info(f"✅ Target schema verification PASSED ({_time.time()-_t0:.1f}s)")
else:
    _log.warning(f"❌ Target schema verification FAILED ({_time.time()-_t0:.1f}s) — check logs above")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 10: STEP 4 — EXTRACT TARGET FROM SNOWFLAKE
# ═══════════════════════════════════════════════════════════════
# Snowflake credentials loaded from Key Vault via secret scope "etl-secrets"

from utils.custom_execution_utils import step_4_extract_target

_t0 = _time.time()
target_df, _tgt_row_count = step_4_extract_target(spark, pipeline_ctx)
_log.info(f"✅ Target loaded: {_tgt_row_count} rows, {len(target_df.columns)} columns ({_time.time()-_t0:.1f}s)")
display(target_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 11: STEP 5 — COMPARE & GENERATE DIFF REPORT
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_5_compare

_t0 = _time.time()
exit_code = step_5_compare(spark, transformed_df, target_df, pipeline_ctx)
elapsed = _time.time() - _t0

if exit_code == 0:
    _log.info(f"🟢 PASS — No differences found! ({elapsed:.1f}s)")
else:
    _log.warning(f"🔴 FAIL — Differences found. ({elapsed:.1f}s)")
    _log.warning(f"   Report: {REPORT_CSV}")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 12: VIEW RESULTS
# ═══════════════════════════════════════════════════════════════

try:
    report_df = spark.read.option("header", True).csv(REPORT_CSV)
    _log.info(f"Diff report: {report_df.count()} rows")
    display(report_df)
except Exception:
    _log.info("No diff report file — either PASS or report path issue")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 13: CACHE CLEANUP
# ═══════════════════════════════════════════════════════════════

# All intermediate caches (transformed_df, target_df, source_norm, target_norm)
# are already unpersisted inside compare_dataframes(). This is a safety net.
_log.info("Clearing any remaining cached DataFrames...")
spark.catalog.clearCache()
_log.info("✅ Cache cleanup complete")
