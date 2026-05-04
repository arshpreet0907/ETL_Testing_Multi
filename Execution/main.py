# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Validation Pipeline — V3 Multi-Server
# MAGIC
# MAGIC **Execution**: Databricks Job with dedicated clusters
# MAGIC - **Source**: Per-server CSVs + schema JSON from Azure Blob Storage (`wasbs://`)
# MAGIC - **Target**: Snowflake via native Spark-Snowflake connector
# MAGIC - **Output**: `diff_report.csv` written back to Azure Blob Storage
# MAGIC - **Secrets**: Azure Key Vault via Databricks secret scope `etl-secrets`
# MAGIC - **Flow**: Generate Runbook → Load parameters.json → Resolve vars → Load per-server → Transform → Union → Verify Target → Compare

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 1: BOOTSTRAP — Generate Runbook, Load Parameters, Resolve All Vars
# ═══════════════════════════════════════════════════════════════
import logging, time as _time, os, sys, json

logging.basicConfig(
    format="[%(asctime)s] %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
_log = logging.getLogger("etl_pipeline")

# ── Widget definitions (Databricks Job config / manual overrides) ──
# These serve as fallback when a parameter is absent from the Excel sheet.
dbutils.widgets.text("TABLE_NAME",          "")  # optional override; derived from EXCEL_FILE if blank
dbutils.widgets.text("SUB_PATH",            "xl")
dbutils.widgets.text("STORAGE_ACCOUNT",     "etlstorage0907")
dbutils.widgets.text("CONTAINER",           "etl-source-data")
dbutils.widgets.text("VERIFY_SCHEMA",       "true")
dbutils.widgets.text("DATE_WATERMARK_MODE", "full")
dbutils.widgets.text("DATE_FROM",           "")
dbutils.widgets.text("DATE_FROM_COL",       "")
dbutils.widgets.text("DATE_TO",             "")
dbutils.widgets.text("DATE_TO_COL",         "")
dbutils.widgets.text("PARTIAL_COLS",        "")
dbutils.widgets.text("RUN_SYNTAX_CHECK",    "true")
dbutils.widgets.text("EXCEL_FILE",          "analytics_dw.public.dim_vehicle_master.xlsx")
dbutils.widgets.text("SF_DATABASE",         "ANALYTICS_DW")
dbutils.widgets.text("CLEAN_OUTPUT",         "true")


# ── Repo path + sys.path (needed to import generate_runbook below) ──
REPO_PATH = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
if not os.path.isdir(REPO_PATH):
    raise FileNotFoundError(f"Repo not found: {REPO_PATH}")
if REPO_PATH not in sys.path:
    sys.path.insert(0, REPO_PATH)
_log.info(f"Repo         : {REPO_PATH}")

# ── Helper: resolve parameter — parameters.json → widget → default/error ──
def _get_param(name: str, params_json: dict, *, required: bool = True,
               default: str = "") -> str:
    """
    Priority:
      1. parameters.json  (from Excel _parameters sheet, written by generate_runbook)
      2. Databricks widget
      3. Provided default
      4. raise ValueError  (if required and nothing found)
    """
    val = params_json.get(name.upper(), "").strip()
    if val:
        return val
    try:
        val = dbutils.widgets.get(name).strip()
        if val:
            return val
    except Exception:
        pass
    if default:
        return default
    if required:
        raise ValueError(
            f"Parameter '{name}' not found in parameters.json or widgets. "
            f"Add it to the Excel _parameters sheet or set it as a widget."
        )
    return ""

# ──────────────────────────────────────────────────────────────────
# Step A — Widgets-only bootstrap (minimum needed to run the runbook
#           generator, which will write parameters.json)
# ──────────────────────────────────────────────────────────────────
EXCEL_FILE       = dbutils.widgets.get("EXCEL_FILE")
RUN_SYNTAX_CHECK = dbutils.widgets.get("RUN_SYNTAX_CHECK").lower() == "true"
CLEAN_OUTPUT = dbutils.widgets.get("CLEAN_OUTPUT").lower() == "true"


# Derive TABLE_NAME from excel filename (e.g. analytics_dw.public.dim_vehicle_master.xlsx → public_dim_vehicle_master)
_excel_stem      = os.path.splitext(EXCEL_FILE)[0]           # analytics_dw.public.dim_vehicle_master
_derived_table   = "_".join(_excel_stem.split(".")[1:])      # public_dim_vehicle_master
TABLE_NAME       = _derived_table or dbutils.widgets.get("TABLE_NAME").strip()

# PARTIAL_COLS for runbook generation: widget is the bootstrap source.
# After parameters.json is written it will also hold the Excel value
# (if defined in the _parameters sheet) — resolved below in Step C.
_partial_bootstrap = dbutils.widgets.get("PARTIAL_COLS").strip()
_partial_for_gen   = [c.strip().upper() for c in _partial_bootstrap.split(",") if c.strip()] or None

_log.info(f"EXCEL_FILE   : {EXCEL_FILE}")
_log.info(f"TABLE_NAME   : {TABLE_NAME}  (bootstrap widget)")

# ──────────────────────────────────────────────────────────────────
# Step B — Generate runbook + write parameters.json
# ──────────────────────────────────────────────────────────────────
from excel_files.generate_etl_runbook import generate_runbook

_excel_path = os.path.join(REPO_PATH, "excel_files", EXCEL_FILE)
_output_dir = os.path.join(REPO_PATH, "excel_files", "etl_output")

_log.info(f"Generating runbook from : {EXCEL_FILE}")
_log.info(f"  Partial cols (widget) : {_partial_for_gen}")
_log.info(f"  Syntax check          : {RUN_SYNTAX_CHECK}")
_log.info(f"  Clean output          : {CLEAN_OUTPUT}")

_t_rb = _time.time()
generate_runbook(
    _excel_path,
    _output_dir,
    partial_cols=_partial_for_gen,
    run_syntax_check=RUN_SYNTAX_CHECK,
    clean_output=CLEAN_OUTPUT
)
_log.info(f"✅ Runbook generated ({_time.time()-_t_rb:.1f}s)")

# ──────────────────────────────────────────────────────────────────
# Step C — Load parameters.json → resolve ALL pipeline variables
# ──────────────────────────────────────────────────────────────────
_table_folder     = TABLE_NAME.replace(".", "_")
_params_json_path = os.path.join(_output_dir, _table_folder, "parameters.json")

_PARAMS_JSON: dict = {}
if os.path.isfile(_params_json_path):
    with open(_params_json_path, "r", encoding="utf-8") as _pf:
        _PARAMS_JSON = json.load(_pf)
    _log.info(f"✅ parameters.json loaded: {len(_PARAMS_JSON)} parameter(s)")
    _log.info(f"   Keys: {list(_PARAMS_JSON.keys())}")
else:
    _log.info("ℹ  No parameters.json written (no _parameters sheet) — using widgets only")

# All variables resolved here; widgets act as fallback for anything not
# present in the Excel _parameters sheet.
TABLE_NAME          = _get_param("TABLE_NAME", _PARAMS_JSON, required=False) or _derived_table
SUB_PATH            = _get_param("SUB_PATH",            _PARAMS_JSON)
STORAGE_ACCOUNT     = _get_param("STORAGE_ACCOUNT",     _PARAMS_JSON)
CONTAINER           = _get_param("CONTAINER",           _PARAMS_JSON)
VERIFY_SCHEMA       = _get_param("VERIFY_SCHEMA",       _PARAMS_JSON, default="true").lower() == "true"
DATE_WATERMARK_MODE = _get_param("DATE_WATERMARK_MODE", _PARAMS_JSON, default="full")
SF_DATABASE         = _get_param("SF_DATABASE",         _PARAMS_JSON, required=False) or None

_partial_raw        = _get_param("PARTIAL_COLS",        _PARAMS_JSON, required=False)
PARTIAL_COLS        = [c.strip().upper() for c in _partial_raw.split(",") if c.strip()] or None

DATE_FROM           = _get_param("DATE_FROM",           _PARAMS_JSON, required=False) or None
DATE_FROM_COL       = _get_param("DATE_FROM_COL",       _PARAMS_JSON, required=False) or None
DATE_TO             = _get_param("DATE_TO",             _PARAMS_JSON, required=False) or None
DATE_TO_COL         = _get_param("DATE_TO_COL",         _PARAMS_JSON, required=False) or None

EXCLUDE_COLS = ["load_ts", "batch_id"]

_log.info("─── Resolved pipeline parameters ───────────────────────────")
_log.info(f"  TABLE_NAME          : {TABLE_NAME}")
_log.info(f"  SUB_PATH            : {SUB_PATH}")
_log.info(f"  STORAGE_ACCOUNT     : {STORAGE_ACCOUNT}")
_log.info(f"  CONTAINER           : {CONTAINER}")
_log.info(f"  VERIFY_SCHEMA       : {VERIFY_SCHEMA}")
_log.info(f"  DATE_WATERMARK_MODE : {DATE_WATERMARK_MODE}")
_log.info(f"  PARTIAL_COLS        : {PARTIAL_COLS}")
_log.info(f"  SF_DATABASE         : {SF_DATABASE}")
_log.info(f"  Source              : {'parameters.json + widgets' if _PARAMS_JSON else 'widgets only'}")
_log.info("────────────────────────────────────────────────────────────")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 2: AZURE BLOB STORAGE — Configure wasbs:// access
# ═══════════════════════════════════════════════════════════════

BLOB_BASE       = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"
BLOB_TABLE_BASE = f"{BLOB_BASE}/{TABLE_NAME}/{SUB_PATH}"
OUTPUT_DIR      = f"{BLOB_BASE}/output/{TABLE_NAME}/{SUB_PATH}"
REPORT_CSV      = f"{OUTPUT_DIR}/diff_report.csv"

_log.info(f"Blob base    : {BLOB_TABLE_BASE}")
_log.info(f"Output dir   : {OUTPUT_DIR}")
_log.info(f"Report CSV   : {REPORT_CSV}")
_log.info("✅ Azure Blob Storage configured")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 3: SPARK CONFIG
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
# CELL 4: BUILD PIPELINE CONTEXT
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
# CELL 5: STEP 1 — LOAD SOURCE CSVs (per server)
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_1_load_source

_t0 = _time.time()
server_dfs = step_1_load_source(spark, pipeline_ctx, BLOB_TABLE_BASE)
_log.info(f"✅ Loaded {len(server_dfs)} server(s) ({_time.time()-_t0:.1f}s)")
for entry in server_dfs:
    _log.info(f"   {entry['server_name']}: {len(entry['df'].columns)} columns")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 6: STEP 2 — TRANSFORM + UNION (per server)
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_2_transform_union

_t0 = _time.time()
transformed_df, _row_count = step_2_transform_union(spark, server_dfs, pipeline_ctx)
_log.info(f"✅ Transform+Union: {_row_count} rows, {len(transformed_df.columns)} columns ({_time.time()-_t0:.1f}s)")
_log.info(f"   Columns: {transformed_df.columns}")
display(transformed_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 7: STEP 3 — VERIFY TARGET SCHEMA (Snowflake live)
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_3_verify_target_schema

_t0 = _time.time()
passed = step_3_verify_target_schema(spark, pipeline_ctx)
if passed:
    _log.info(f"✅ Target schema verification PASSED ({_time.time()-_t0:.1f}s)")
else:
    _log.warning(f"❌ Target schema verification FAILED ({_time.time()-_t0:.1f}s) — check logs above")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 8: STEP 4 — EXTRACT TARGET FROM SNOWFLAKE
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_4_extract_target

_t0 = _time.time()
target_df, _tgt_row_count = step_4_extract_target(spark, pipeline_ctx)
_log.info(f"✅ Target loaded: {_tgt_row_count} rows, {len(target_df.columns)} columns ({_time.time()-_t0:.1f}s)")
display(target_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 9: STEP 5 — COMPARE & GENERATE DIFF REPORT
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
