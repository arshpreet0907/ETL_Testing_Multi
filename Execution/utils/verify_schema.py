"""
utils/verify_schema.py
-----------------------
Databricks version — two validation modes:

  1. JSON schema validation (source): .schema.json vs DDL file
     Used when source data comes from CSV (no live MySQL connection).

  2. Live INFORMATION_SCHEMA validation (Snowflake target):
     Queries Snowflake directly — same as original.
"""

import json
import logging
import os
import re
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from utils.logger import get_logger

logger = get_logger(__name__)


class SchemaVerificationError(Exception):
    def __init__(self, table: str, mismatches: List[str]) -> None:
        self.table = table
        self.mismatches = mismatches
        detail = "\n  ".join(mismatches)
        super().__init__(f"Schema verification FAILED for '{table}':\n  {detail}")


# --------------------------------------------------------------------------- #
# Public API                                                                   #
# --------------------------------------------------------------------------- #

def verify_schema_from_json_file(
    spark,
    schema_json_path: str,
    ddl_file: str,
    dialect: str = "mysql",
) -> bool:
    """
    Verify a .schema.json file's columns match a DDL file.
    Used when source data is loaded from CSV (no live DB connection).

    Parameters
    ----------
    schema_json_path : str
        Path to .schema.json file
    ddl_file : str
        Path to the CREATE TABLE DDL file
    dialect : str
        "mysql" or "snowflake"

    Returns
    -------
    bool
        True if schema matches, False if missing columns found
    """
    logger.info("Verifying schema JSON %s against DDL %s", schema_json_path, ddl_file)

    # Read schema JSON — supports both wasbs:// (via dbutils) and local paths
    if schema_json_path.startswith("wasbs://") or schema_json_path.startswith("dbfs:"):
        # from pyspark.sql import SparkSession
        # spark = SparkSession.getActiveSession()
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns.get("dbutils")
        schema_content = dbutils.fs.head(schema_json_path, 1048576)
    else:
        with open(schema_json_path, "r", encoding="utf-8") as f:
            schema_content = f.read()

    spark_schema = StructType.fromJson(json.loads(schema_content))

    json_columns = {field.name.upper() for field in spark_schema.fields}
    ddl_columns = _parse_ddl_columns(ddl_file)

    mismatches = []
    for col_name in ddl_columns:
        if col_name not in json_columns:
            mismatches.append(f"Column '{col_name}' in DDL but NOT in schema JSON")

    for col_name in json_columns:
        if col_name not in ddl_columns:
            mismatches.append(f"Column '{col_name}' in schema JSON but NOT in DDL (may be intentional)")

    missing_cols = [m for m in mismatches if "NOT in schema JSON" in m]

    if mismatches:
        for m in mismatches:
            level = logger.error if "NOT in schema JSON" in m else logger.warning
            level("[SCHEMA CHECK] %s", m)

    if missing_cols:
        logger.error("Schema JSON verification FAILED — %d missing columns", len(missing_cols))
        return False

    logger.info("Schema JSON verification PASSED (%d columns)", len(ddl_columns))
    return True


def verify_schema_from_ddl(
    spark: SparkSession,
    jdbc_opts: dict,
    ddl_file: str,
    database: str,
    table: str,
    dialect: str = "snowflake",
    schema: str = None,
) -> bool:
    """
    Verify live database schema against a DDL file.
    On Databricks, only used for Snowflake targets.
    """
    logger.info("Verifying schema for %s.%s against DDL: %s", database, table, ddl_file)

    ddl_columns = _parse_ddl_columns(ddl_file)

    if dialect == "snowflake":
        if not schema:
            schema = jdbc_opts.get("sfSchema", "PUBLIC")
        live_columns = _fetch_snowflake_schema(spark, jdbc_opts, database, schema, table)
    else:
        raise ValueError("Only Snowflake target is supported on Databricks")

    mismatches = _compare_columns(ddl_columns, live_columns)

    if mismatches:
        for m in mismatches:
            logger.error("[SCHEMA MISMATCH] %s", m)
        return False

    logger.info("Schema verification PASSED for %s.%s.%s", database, schema, table)
    return True


# --------------------------------------------------------------------------- #
# DDL parser                                                                   #
# --------------------------------------------------------------------------- #

def _parse_ddl_columns(ddl_path: str) -> Dict[str, str]:
    """Extract {column_name: data_type} from a CREATE TABLE DDL file."""
    if not os.path.isfile(ddl_path):
        raise FileNotFoundError(f"DDL file not found: {ddl_path}")

    columns: Dict[str, str] = {}
    with open(ddl_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip().rstrip(",")
            if not line or line.startswith("--") or line.startswith("CREATE"):
                continue
            if line.upper().startswith("PRIMARY KEY"):
                continue
            if line in ("(", ")"):
                continue

            match = re.match(
                r'^[`"]?(\w+)[`"]?\s+([A-Z][A-Z0-9_(),.]+)',
                line,
                re.IGNORECASE,
            )
            if match:
                col_name = match.group(1).upper()
                col_type = match.group(2).upper()
                col_type = re.sub(r"\s+(NOT\s+NULL|NULL)$", "", col_type).strip()
                columns[col_name] = col_type

    return columns


# --------------------------------------------------------------------------- #
# Live schema fetcher (Snowflake only)                                         #
# --------------------------------------------------------------------------- #

def _fetch_snowflake_schema(
    spark: SparkSession,
    sf_opts: dict,
    database_name: str,
    schema_name: str,
    table_name: str,
) -> Dict[str, str]:
    """Query INFORMATION_SCHEMA.COLUMNS on Snowflake via native connector."""
    info_schema = f"{database_name.upper()}.INFORMATION_SCHEMA"

    query = (
        f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
        f"NUMERIC_PRECISION, NUMERIC_SCALE "
        f"FROM {info_schema}.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{schema_name.upper()}' "
        f"AND TABLE_NAME = '{table_name.upper()}' "
        f"ORDER BY ORDINAL_POSITION"
    )

    logger.info("Snowflake schema query: %s", query)

    # [SERVERLESS] Use 'snowflake' format instead of 'jdbc' (jdbc not supported on serverless)
    rows = (
        spark.read.format("snowflake")
        .options(**sf_opts)
        .option("query", query)
        .load()
        .collect()
    )

    logger.info("Found %d columns in %s.%s.%s", len(rows), database_name, schema_name, table_name)

    if len(rows) == 0:
        logger.error("No columns found — table may not exist")
        return {}

    result: Dict[str, str] = {}
    for row in rows:
        col = row["COLUMN_NAME"].upper()
        dtype = row["DATA_TYPE"].upper()
        if dtype in ("TEXT", "VARCHAR") and row["CHARACTER_MAXIMUM_LENGTH"]:
            dtype = f"VARCHAR({row['CHARACTER_MAXIMUM_LENGTH']})"
        elif dtype == "NUMBER" and row["NUMERIC_PRECISION"]:
            if row["NUMERIC_SCALE"] is not None and row["NUMERIC_SCALE"] > 0:
                dtype = f"NUMBER({row['NUMERIC_PRECISION']},{row['NUMERIC_SCALE']})"
            else:
                dtype = f"NUMBER({row['NUMERIC_PRECISION']})"
        result[col] = dtype

    return result


# --------------------------------------------------------------------------- #
# Column comparator                                                            #
# --------------------------------------------------------------------------- #

_TYPE_ALIASES: Dict[str, List[str]] = {
    "INT":        ["INT", "INT(11)", "INTEGER", "INT(10)", "INT(4)"],
    "BIGINT":     ["BIGINT", "BIGINT(20)"],
    "TINYINT(1)": ["TINYINT(1)", "BOOLEAN", "BOOL"],
    "SMALLINT":   ["SMALLINT", "SMALLINT(6)"],
    "DATE":       ["DATE"],
    "DATETIME":   ["DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"],
    "TEXT":       ["TEXT", "LONGTEXT", "MEDIUMTEXT"],
}


def _types_compatible(ddl_type: str, live_type: str) -> bool:
    ddl_norm = ddl_type.upper().strip()
    live_norm = live_type.upper().strip()

    if ddl_norm == live_norm:
        return True

    if ddl_norm.startswith("NUMBER(") and live_norm.startswith("NUMBER("):
        ddl_match = re.match(r"NUMBER\((\d+)(?:,(\d+))?\)", ddl_norm)
        live_match = re.match(r"NUMBER\((\d+)(?:,(\d+))?\)", live_norm)
        if ddl_match and live_match:
            if (ddl_match.group(1) == live_match.group(1) and
                    (ddl_match.group(2) or "0") == (live_match.group(2) or "0")):
                return True

    for canonical, aliases in _TYPE_ALIASES.items():
        ddl_in = any(ddl_norm == a for a in aliases)
        live_in = any(live_norm == a for a in aliases)
        if ddl_in and live_in:
            return True

    return False


def _compare_columns(
    ddl_cols: Dict[str, str],
    live_cols: Dict[str, str],
) -> List[str]:
    mismatches: List[str] = []

    for col_name, ddl_type in ddl_cols.items():
        if col_name not in live_cols:
            mismatches.append(f"Column '{col_name}' in DDL but NOT FOUND in live DB.")
            continue
        live_type = live_cols[col_name]
        if not _types_compatible(ddl_type, live_type):
            mismatches.append(
                f"Column '{col_name}': DDL='{ddl_type}', live='{live_type}' — not compatible."
            )

    for col_name in live_cols:
        if col_name not in ddl_cols:
            mismatches.append(f"Column '{col_name}' in live DB but NOT in DDL (may be intentional).")

    return mismatches

