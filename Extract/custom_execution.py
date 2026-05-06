"""
custom_execution.py
--------------------
Source Data Extraction Pipeline (Multi-Server).

Connects to source databases (one per server), extracts raw source data
using the combined aliased SQL query, and saves the result as CSV.

Flow:
  1. Set TABLE_NAME below
  2. Runbook is auto-generated from Excel (parse + verify syntax)
  3. parameters.json is loaded to fill all config variables
  4. Override any variable in the USER OVERRIDES section
  5. Run extraction

FILTER QUICK REFERENCE
======================

DATE_WATERMARK_MODE  options
-----------------------------
  "full"   → no date filter applied
  "range"  → apply date bounds (see DATE_FROM / DATE_TO below)
"""

import json
import os
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Literal, Optional, Set

from Extract.utils.connections.spark_session import get_spark_session
from Extract.utils.auto_config import get_table_config, list_available_tables
from Extract.utils.logger import get_logger
from Extract.utils.custom_execution_utils import (
    step_0_verify_partial_schemas,
    step_1_extract_source_v3,
    step_1_5_save_raw_source_csv_v3,
    build_source_filter,
)

logger = get_logger(__name__)


# ============================================================================
# SECTION 1 — TABLE CONFIGURATION (only TABLE_NAME is required)
# ============================================================================

# TABLE_NAME = "public_dim_vehicle_master"
# TABLE_NAME = "public_fact_commercial"
TABLE_NAME = "public_fact_production"
# PARTIAL_TARGET_COLS =['MODEL_NAME','PRIMARY_PART_NO','SUPPLIER_NAME','PLANT_CONTACT_NAME']#["id", "name"]
PARTIAL_TARGET_COLS =None#["id", "name"]
CLEAN_OUTPUT = True  # this makes sure previous table folder is deleted then new files are made, make it false to stop

# ============================================================================
# SECTION 2 — RUNBOOK GENERATION (auto-runs on every execution)
# ============================================================================

EXCEL_FILES_DIR: str = "excel_files"

logger.info("=" * 60)
logger.info("Step A: Discovering Excel file for table: %s", TABLE_NAME)

_excel_files_dir = Path(EXCEL_FILES_DIR)
_excel_path: Optional[Path] = None

for _xlsx in sorted(_excel_files_dir.glob("*.xlsx")):
    _parts = _xlsx.stem.split(".")
    if len(_parts) >= 3:
        _folder = ".".join(_parts[1:]).replace(".", "_")
    elif len(_parts) == 2:
        _folder = _parts[1]
    else:
        _folder = _xlsx.stem
    if _folder == TABLE_NAME:
        _excel_path = _xlsx
        break

if _excel_path is None:
    logger.error(
        "No Excel mapping file found for table '%s' in '%s'. "
        "Expected a file like '<db>.%s.xlsx'.",
        TABLE_NAME, EXCEL_FILES_DIR,
        TABLE_NAME.replace("_", "."),
    )
    sys.exit(1)

logger.info("  Excel file: %s", _excel_path.name)

# ── Generate runbook artifacts ──
logger.info("Step B: Generating runbook (parse + verify syntax) ...")
try:
    from excel_files.generate_etl_runbook import generate_runbook as _generate_runbook

    _runbook_output_dir = Path("excel_files") / "etl_output"
    _generate_runbook(
        _excel_path,
        _runbook_output_dir,
        partial_cols=PARTIAL_TARGET_COLS,
        run_syntax_check=True,
        clean_output=CLEAN_OUTPUT,
    )
    logger.info("  Runbook ready → %s", _runbook_output_dir / TABLE_NAME)
except Exception as _rb_exc:
    logger.error("Runbook generation failed: %s", _rb_exc)
    sys.exit(1)


# ============================================================================
# SECTION 3 — LOAD parameters.json → fill all variables automatically
# ============================================================================

_params_json_path = _runbook_output_dir / TABLE_NAME / "parameters.json"
_PARAMS: dict = {}

if _params_json_path.is_file():
    with open(_params_json_path, "r", encoding="utf-8") as _pf:
        _PARAMS = json.load(_pf)
    logger.info("Step C: Loaded parameters.json (%d keys)", len(_PARAMS))
else:
    logger.info("Step C: No parameters.json found — using defaults")


def _p(key: str, default: str = "") -> str:
    """Get a parameter from parameters.json, or return default."""
    return _PARAMS.get(key, "").strip() or default


# ── Populate all config variables from parameters.json ──
SUBPATH            = _p("SUB_PATH", "xl")
VERIFY_SCHEMA      = _p("VERIFY_SCHEMA", "true").lower() == "true"
DATE_WATERMARK_MODE = _p("DATE_WATERMARK_MODE", "full")

DATE_FROM           = _p("DATE_FROM") or None
DATE_FROM_COL       = _p("DATE_FROM_COL") or None
DATE_TO             = _p("DATE_TO") or None
DATE_TO_COL         = _p("DATE_TO_COL") or None

ENABLE_PARTITIONING: bool = False
PARTITION_COL:       Optional[str] = None
PARTITION_LOWER_BOUND: Optional[int] = None
PARTITION_UPPER_BOUND: Optional[int] = None
NUM_PARTITIONS:      int = 4


# ============================================================================
# SECTION 4 — USER OVERRIDES
#   Uncomment any line below to override the value loaded from parameters.json.
#   This section is intentionally AFTER the JSON loader so your values take
#   precedence, and you can still see what the JSON provided.
# ============================================================================

# SUBPATH            = "xxl"
# VERIFY_SCHEMA      = False
# DATE_WATERMARK_MODE = "range"
# DATE_FROM          = "2024-01-01"
# DATE_FROM_COL      = "created_date"
# DATE_TO            = "2024-12-31"
# DATE_TO_COL        = "created_date"
# ENABLE_PARTITIONING = True
# PARTITION_COL      = "id"
# PARTITION_LOWER_BOUND = 1
# PARTITION_UPPER_BOUND = 1000000
# NUM_PARTITIONS     = 8

logger.info("  SUBPATH            : %s", SUBPATH)
logger.info("  VERIFY_SCHEMA      : %s", VERIFY_SCHEMA)
logger.info("  DATE_WATERMARK_MODE: %s", DATE_WATERMARK_MODE)
logger.info("=" * 60)


# ============================================================================
# AUTO-CONFIGURATION LOADER
# ============================================================================

logger.info("Loading auto-configuration for table: %s", TABLE_NAME)

try:
    _config = get_table_config(TABLE_NAME)
except Exception as e:
    logger.error("Auto-configuration failed: %s", e)
    logger.info("Available tables: %s", ", ".join(list_available_tables()))
    sys.exit(1)

if "servers" not in _config:
    logger.error(
        "Table '%s' does not have a multi-server layout. "
        "Expected subfolders with 03_extract_source.sql under etl_output/%s/",
        TABLE_NAME, TABLE_NAME,
    )
    sys.exit(1)

logger.info("multi-server layout detected")
logger.info("  Target table : %s", _config["table_name"])
logger.info("  Primary Keys : %s", _config["primary_keys"])
logger.info("  Servers      : %s", [s["server_name"] for s in _config["servers"]])
for sc in _config["servers"]:
    logger.info("    [%s] DDLs: %d, Query: %s",
                sc["server_name"], len(sc["source_ddls"]),
                os.path.basename(sc["source_query_file"]))


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:
    """
    Run V3 source extraction pipeline.
    For each server: Step 0 (verify schema) → Step 1 (extract) → Step 1.5 (save CSV).
    """
    start_time = time.time()

    try:
        logger.info("=" * 60)
        logger.info("Source Extraction Pipeline  [Multi-Server]")
        logger.info("  Table : %s", TABLE_NAME)
        logger.info("=" * 60)

        spark = get_spark_session(app_name="ETL_SourceExtract")
        spark.conf.set("spark.sql.debug.maxToStringFields", 500)

        failed_servers = []

        for server_cfg in _config["servers"]:
            server_name = server_cfg["server_name"]
            output_dir  = os.path.join("output", TABLE_NAME, SUBPATH, server_name)
            os.makedirs(output_dir, exist_ok=True)

            logger.info("")
            logger.info("-" * 60)
            logger.info("SERVER: %s", server_name)
            logger.info("-" * 60)

            server_ctx = dict(
                server_name          = server_name,
                source_ddls          = server_cfg["source_ddls"],
                source_query         = None,
                source_query_file    = server_cfg["source_query_file"],
                output_dir           = output_dir,
                raw_source_csv       = os.path.join(output_dir, "source_raw.csv"),
                source_filter        = {"where_clause": "", "description": "full load (no filters)"},
                enable_partitioning  = ENABLE_PARTITIONING,
                needs_prequery_bounds= False,
                source_partition_col = PARTITION_COL,
                partition_lower_bound= PARTITION_LOWER_BOUND,
                partition_upper_bound= PARTITION_UPPER_BOUND,
                num_partitions       = NUM_PARTITIONS,
            )

            if DATE_WATERMARK_MODE != "full":
                try:
                    server_ctx["source_filter"] = build_source_filter(
                        config         = _config,
                        date_mode      = DATE_WATERMARK_MODE,
                        date_from      = DATE_FROM,
                        date_from_col  = DATE_FROM_COL,
                        date_to        = DATE_TO,
                        date_to_col    = DATE_TO_COL,
                    )
                except (ValueError, FileNotFoundError) as exc:
                    logger.error("[%s] Filter error: %s", server_name, exc)
                    failed_servers.append(server_name)
                    continue

            try:
                if VERIFY_SCHEMA:
                    if not step_0_verify_partial_schemas(spark, server_ctx):
                        logger.error("[%s] Skipping extraction due to schema failure", server_name)
                        failed_servers.append(server_name)
                        continue

                source_df = step_1_extract_source_v3(spark, server_ctx)
                step_1_5_save_raw_source_csv_v3(source_df, server_ctx)

                logger.info("[%s] Extraction complete. CSV saved to: %s", server_name, server_ctx["raw_source_csv"])

            except Exception as exc:
                logger.exception("[%s] Extraction FAILED: %s", server_name, exc)
                failed_servers.append(server_name)

        total   = len(_config["servers"])
        success = total - len(failed_servers)
        logger.info("")
        logger.info("=" * 60)
        logger.info("EXTRACTION SUMMARY: %d/%d servers succeeded", success, total)
        if failed_servers:
            logger.error("  Failed servers: %s", failed_servers)
        logger.info("=" * 60)

        return 0 if not failed_servers else 2

    except (FileNotFoundError, ValueError) as exc:
        logger.error("Configuration error: %s", exc)
        return 1
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        return 1
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        return 99
    finally:
        elapsed = timedelta(seconds=int(time.time() - start_time))
        logger.info("Total time: %d min %d sec", elapsed.seconds // 60, elapsed.seconds % 60)


if __name__ == "__main__":
    sys.exit(main())
