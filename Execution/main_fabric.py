# Fabric notebook source
# MAGIC %md
# MAGIC # ETL Validation Pipeline — V3 Multi-Server (Fabric)
# MAGIC
# MAGIC **Execution**: Microsoft Fabric Data Pipeline with Notebook activity
# MAGIC - **Source**: Per-server CSVs + schema JSON from Azure Blob Storage (`abfss://`)
# MAGIC - **Target**: Snowflake via native Spark-Snowflake connector
# MAGIC - **Output**: `diff_report.csv` written back to Azure Blob Storage (OneLake or ADLS)
# MAGIC - **Secrets**: Azure Key Vault via notebookutils (mssparkutils)
# MAGIC - **Flow**: Generate Runbook → Load parameters.json → Resolve vars → Load per-server → Transform → Union → Verify Target → Compare

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# PARAMETER CELL
# Right-click this cell → "Toggle Parameter Cell" in Fabric UI
# These are the defaults used when running the notebook interactively.
# When triggered from a Fabric Pipeline, the pipeline's Base Parameters
# override these values at runtime — no widgets needed.
# ═══════════════════════════════════════════════════════════════

TABLE_NAME          = ""                                              # derived from EXCEL_FILE if blank
SUB_PATH            = "xl"
STORAGE_ACCOUNT     = "etlstorage0907"
CONTAINER           = "etl-source-data"
VERIFY_SCHEMA       = "true"
DATE_WATERMARK_MODE = "full"
DATE_FROM           = ""
DATE_FROM_COL       = ""
DATE_TO             = ""
DATE_TO_COL         = ""
PARTIAL_COLS        = ""
RUN_SYNTAX_CHECK    = "true"
EXCEL_FILE          = "analytics_dw.public.dim_vehicle_master.xlsx"
SF_DATABASE         = "ANALYTICS_DW"
CLEAN_OUTPUT        = "true"

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 1: BOOTSTRAP — Generate Runbook, Load Parameters, Resolve All Vars
# ═══════════════════════════════════════════════════════════════
import logging, time as _time, os, sys, json
from notebookutils import mssparkutils                                # ← Fabric: replaces dbutils

logging.basicConfig(
    format="[%(asctime)s] %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
_log = logging.getLogger("etl_pipeline")

# ── Helper: resolve parameter — parameters.json → pipeline-injected var → default/error ──
def _get_param(name: str, params_json: dict, *, required: bool = True,
               default: str = "") -> str:
    """
    Priority:
      1. parameters.json  (from Excel _parameters sheet, written by generate_runbook)
      2. Pipeline-injected variable (already in scope as a plain Python variable)
      3. Provided default
      4. raise ValueError  (if required and nothing found)
    """
    val = params_json.get(name.upper(), "").strip()
    if val:
        return val
    # ← Fabric: instead of dbutils.widgets.get(), read the plain variable
    #   injected by the Fabric pipeline into this notebook's scope.
    try:
        injected = globals().get(name, "")
        if isinstance(injected, str) and injected.strip():
            return injected.strip()
    except Exception:
        pass
    if default:
        return default
    if required:
        raise ValueError(
            f"Parameter '{name}' not found in parameters.json or pipeline parameters. "
            f"Add it to the Excel _parameters sheet or set it as a pipeline Base Parameter."
        )
    return ""

# ──────────────────────────────────────────────────────────────────
# Step A — Bootstrap (minimum needed to run the runbook generator)
# ──────────────────────────────────────────────────────────────────

# ── Repo path + sys.path (needed to import generate_runbook below) ──
REPO_PATH = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
if not os.path.isdir(REPO_PATH):
    raise FileNotFoundError(f"Repo not found: {REPO_PATH}")
if REPO_PATH not in sys.path:
    sys.path.insert(0, REPO_PATH)
_log.info(f"Repo         : {REPO_PATH}")

# ← Fabric: EXCEL_FILE and other vars are already plain Python variables
#   (either from the parameter cell above, or overridden by the pipeline).
#   Cast booleans explicitly since pipeline injects everything as strings.
_EXCEL_FILE       = str(EXCEL_FILE).strip()
_RUN_SYNTAX_CHECK = str(RUN_SYNTAX_CHECK).strip().lower() == "true"
_CLEAN_OUTPUT     = str(CLEAN_OUTPUT).strip().lower() == "true"

# Derive TABLE_NAME from excel filename
# e.g. analytics_dw.public.dim_vehicle_master.xlsx → public_dim_vehicle_master
_excel_stem    = os.path.splitext(_EXCEL_FILE)[0]         # analytics_dw.public.dim_vehicle_master
_derived_table = "_".join(_excel_stem.split(".")[1:])     # public_dim_vehicle_master
_TABLE_NAME    = _derived_table or str(TABLE_NAME).strip()

_partial_bootstrap = str(PARTIAL_COLS).strip()
_partial_for_gen   = [c.strip().upper() for c in _partial_bootstrap.split(",") if c.strip()] or None

_log.info(f"EXCEL_FILE   : {_EXCEL_FILE}")
_log.info(f"TABLE_NAME   : {_TABLE_NAME}  (bootstrap)")

# ──────────────────────────────────────────────────────────────────
# Step B — Generate runbook + write parameters.json
# ──────────────────────────────────────────────────────────────────
from excel_files.generate_etl_runbook import generate_runbook

_excel_path = os.path.join(REPO_PATH, "excel_files", _EXCEL_FILE)
_output_dir = os.path.join(REPO_PATH, "excel_files", "etl_output")

_log.info(f"Generating runbook from : {_EXCEL_FILE}")
_log.info(f"  Partial cols (param)  : {_partial_for_gen}")
_log.info(f"  Syntax check          : {_RUN_SYNTAX_CHECK}")
_log.info(f"  Clean output          : {_CLEAN_OUTPUT}")

_t_rb = _time.time()
generate_runbook(
    _excel_path,
    _output_dir,
    partial_cols=_partial_for_gen,
    run_syntax_check=_RUN_SYNTAX_CHECK,
    clean_output=_CLEAN_OUTPUT
)
_log.info(f"✅ Runbook generated ({_time.time()-_t_rb:.1f}s)")

# ──────────────────────────────────────────────────────────────────
# Step C — Load parameters.json → resolve ALL pipeline variables
# ──────────────────────────────────────────────────────────────────
_table_folder     = _TABLE_NAME.replace(".", "_")
_params_json_path = os.path.join(_output_dir, _table_folder, "parameters.json")

_PARAMS_JSON: dict = {}
if os.path.isfile(_params_json_path):
    with open(_params_json_path, "r", encoding="utf-8") as _pf:
        _PARAMS_JSON = json.load(_pf)
    _log.info(f"✅ parameters.json loaded: {len(_PARAMS_JSON)} parameter(s)")
    _log.info(f"   Keys: {list(_PARAMS_JSON.keys())}")
else:
    _log.info("ℹ  No parameters.json written (no _parameters sheet) — using pipeline params only")

# All variables resolved here; pipeline-injected vars act as fallback
# for anything not present in the Excel _parameters sheet.
TABLE_NAME          = _get_param("TABLE_NAME",          _PARAMS_JSON, required=False) or _derived_table
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
_log.info(f"  Source              : {'parameters.json + pipeline params' if _PARAMS_JSON else 'pipeline params only'}")
_log.info("────────────────────────────────────────────────────────────")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 2: AZURE BLOB STORAGE — Configure abfss:// access
# ═══════════════════════════════════════════════════════════════

# ← Fabric: retrieve storage account key from Key Vault via mssparkutils.
#   Replace the vault URL with your actual Key Vault URL.
#   The secret name "storage-account-key" should match what's in your Key Vault.
_KV_URL              = "https://<your-keyvault-name>.vault.azure.net/"   # ← update this
_STORAGE_ACCOUNT_KEY = mssparkutils.credentials.getSecret(_KV_URL, "storage-account-key")

# ← Fabric: set ABFS credential on the Spark session so all reads/writes
#   to this storage account are authenticated automatically.
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    _STORAGE_ACCOUNT_KEY
)

# ← Fabric: use abfss:// (ADLS Gen2 / Azure Blob with hierarchical namespace)
#   instead of wasbs:// (legacy Azure Blob Storage protocol).
#   Structure mirrors your container layout exactly:
#   container/TABLE_NAME/SUB_PATH/<server_name>/raw.csv
#   container/output/TABLE_NAME/SUB_PATH/diff_report.csv
BLOB_BASE       = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BLOB_TABLE_BASE = f"{BLOB_BASE}/{TABLE_NAME}/{SUB_PATH}"
OUTPUT_DIR      = f"{BLOB_BASE}/output/{TABLE_NAME}/{SUB_PATH}"
REPORT_CSV      = f"{OUTPUT_DIR}/diff_report.csv"

_log.info(f"Blob base    : {BLOB_TABLE_BASE}")
_log.info(f"Output dir   : {OUTPUT_DIR}")
_log.info(f"Report CSV   : {REPORT_CSV}")
_log.info("✅ Azure Blob Storage configured (abfss://)")

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

from Extract.utils.auto_config import get_table_config
from Extract.utils.logger import get_logger
from Extract.utils.custom_execution_utils import build_load_filters

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

from Extract.utils.custom_execution_utils import step_1_load_source

_t0 = _time.time()
server_dfs = step_1_load_source(spark, pipeline_ctx, BLOB_TABLE_BASE)
_log.info(f"✅ Loaded {len(server_dfs)} server(s) ({_time.time()-_t0:.1f}s)")
for entry in server_dfs:
    _log.info(f"   {entry['server_name']}: {len(entry['df'].columns)} columns")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 6: STEP 2 — TRANSFORM + UNION (per server)
# ═══════════════════════════════════════════════════════════════

from Extract.utils.custom_execution_utils import step_2_transform_union

_t0 = _time.time()
transformed_df, _row_count = step_2_transform_union(spark, server_dfs, pipeline_ctx)
_log.info(f"✅ Transform+Union: {_row_count} rows, {len(transformed_df.columns)} columns ({_time.time()-_t0:.1f}s)")
_log.info(f"   Columns: {transformed_df.columns}")
display(transformed_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 7: STEP 3 — VERIFY TARGET SCHEMA (Snowflake live)
# ═══════════════════════════════════════════════════════════════

from Extract.utils.custom_execution_utils import step_3_verify_target_schema

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

from Extract.utils.custom_execution_utils import step_4_extract_target

_t0 = _time.time()
target_df, _tgt_row_count = step_4_extract_target(spark, pipeline_ctx)
_log.info(f"✅ Target loaded: {_tgt_row_count} rows, {len(target_df.columns)} columns ({_time.time()-_t0:.1f}s)")
display(target_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 9: STEP 5 — COMPARE & GENERATE DIFF REPORT
# ═══════════════════════════════════════════════════════════════

from Extract.utils.custom_execution_utils import step_5_compare

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
