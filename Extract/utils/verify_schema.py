"""
verify_schema.py
-----------------
Purpose : Compare generated DDL files against the live MySQL source database schema
          before data extraction begins.

          Queries INFORMATION_SCHEMA.COLUMNS via the active Spark JDBC connection
          so no additional DB driver (pymysql etc.) is required.

Public API
----------
    verify_schema_from_ddl(spark, jdbc_opts, ddl_file, database, table, ...) -> bool
"""

import json
import os
import re
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession

from Extract.utils.logger import get_logger

logger = get_logger(__name__)

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))


# --------------------------------------------------------------------------- #
# Custom exception                                                             #
# --------------------------------------------------------------------------- #

class SchemaVerificationError(Exception):
    """
    Raised by pipeline scripts when schema verification fails.

    Attributes
    ----------
    table      : str            — fully qualified table name
    mismatches : list of str    — human-readable mismatch descriptions
    """

    def __init__(self, table: str, mismatches: List[str]) -> None:
        self.table = table
        self.mismatches = mismatches
        detail = "\n  ".join(mismatches)
        super().__init__(
            f"Schema verification FAILED for table '{table}':\n  {detail}"
        )


# --------------------------------------------------------------------------- #
# Public API                                                                   #
# --------------------------------------------------------------------------- #

def verify_partial_ddls(
    spark: SparkSession,
    jdbc_opts: dict,
    ddl_files: list,
    dialect: str = "mysql",
    schema: str = None,
    output_dir: str = None,
) -> bool:
    """
    Verify multiple partial source DDLs against the live database.

    For each DDL file:
    1. Parse DDL → get db.table name + mapped columns
    2. Fetch live schema via INFORMATION_SCHEMA
    3. Check: every DDL column must exist in live DB (extras in live DB are OK)

    Parameters
    ----------
    spark : SparkSession
    jdbc_opts : dict
        JDBC connection options
    ddl_files : list[str]
        Paths to partial source DDL files (01_src_*_ddl.sql)
    dialect : str
        "mysql" or "sqlserver"
    schema : str, optional
        Schema name for SQL Server (e.g., "dbo"). None for MySQL.
    output_dir : str, optional
        Directory to write live DB schema JSON files

    Returns
    -------
    bool
        True if all DDLs pass verification, False if any mismatch found
    """
    from Extract.utils.auto_config import parse_source_ddl_table_info

    all_passed = True

    for ddl_file in ddl_files:
        try:
            database, table = parse_source_ddl_table_info(ddl_file)
        except ValueError as exc:
            logger.error("Could not parse DDL file %s: %s", ddl_file, exc)
            all_passed = False
            continue

        full_name = f"{database}.{schema}" if schema else database
        logger.info("Verifying partial DDL: %s.%s from %s", full_name, table, os.path.basename(ddl_file))

        ddl_columns = _parse_ddl_columns(ddl_file)
        live_columns = _fetch_schema(spark, jdbc_opts, database, table, dialect, schema)

        if not live_columns:
            logger.error("Could not fetch live schema for %s.%s", full_name, table)
            all_passed = False
            continue

        # Partial check: only DDL columns must exist in live DB
        mismatches = _compare_columns_partial(ddl_columns, live_columns)

        if mismatches:
            for m in mismatches:
                logger.error("[SCHEMA MISMATCH] %s.%s: %s", full_name, table, m)
            all_passed = False
        else:
            logger.info("  OK: Partial schema verified for %s.%s (%d/%d columns verified)",
                        full_name, table, len(ddl_columns), len(live_columns))


    return all_passed


def verify_schema_from_ddl(
    spark: SparkSession,
    jdbc_opts: dict,
    ddl_file: str,
    database: str,
    table: str,
    dialect: str = "mysql",
    schema: str = None,
    output_dir: str = None,
    schema_side: str = "source",
    schema_subpath: str = "xl",
) -> bool:
    """
    Verify database schema against a DDL file.

    Parameters
    ----------
    spark : SparkSession
    jdbc_opts : dict
        JDBC connection options
    ddl_file : str
        Path to DDL file (CREATE TABLE statement)
    database : str
        Database/schema name
    table : str
        Table name
    dialect : str
        "mysql" or "sqlserver"
    schema : str, optional
        Schema name for SQL Server (e.g., "dbo"). None for MySQL.
    output_dir : str, optional
        Directory to write the live DB schema JSON file.
    schema_side : str
        "source" — determines the JSON filename.
    schema_subpath : str, optional
        Subdirectory within output_dir to save schema file (default: "xl")

    Returns
    -------
    bool
        True if schema matches, False if mismatches found
    """
    full_name = f"{database}.{schema}" if schema else database
    logger.info(
        "Verifying schema for %s.%s against DDL: %s",
        full_name, table, ddl_file
    )

    ddl_columns = _parse_ddl_columns(ddl_file)
    live_columns = _fetch_schema(spark, jdbc_opts, database, table, dialect, schema)

    if output_dir and live_columns:
        _write_schema_json(spark, jdbc_opts, output_dir, schema_side, database, table, schema_subpath)

    mismatches = _compare_columns(ddl_columns, live_columns)

    if mismatches:
        for m in mismatches:
            logger.error("[SCHEMA MISMATCH] %s", m)
        return False

    logger.info("Schema verification PASSED for %s.%s", full_name, table)
    return True


# --------------------------------------------------------------------------- #
# DDL parser                                                                   #
# --------------------------------------------------------------------------- #

def _parse_ddl_columns(ddl_path: str) -> Dict[str, str]:
    """
    Extract {column_name: data_type} from a generated CREATE TABLE DDL file.

    Handles lines like:
        order_id   INT NOT NULL,
        status     VARCHAR(20),
        `col_name` CHAR(10) NOT NULL,
    Ignores PRIMARY KEY lines and comment lines.
    """
    if not os.path.isfile(ddl_path):
        raise FileNotFoundError(
            f"Generated DDL file not found: {ddl_path}\n"
            f"Run generate_rulebook.py first."
        )

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

            # Extract column name (may be backtick-quoted or double-quoted)
            match = re.match(
                r'^[`"]?(\w+)[`"]?\s+([A-Z][A-Z0-9_(),.]+)',
                line,
                re.IGNORECASE,
            )
            if match:
                col_name = match.group(1).upper()
                col_type = match.group(2).upper()
                # Strip NOT NULL / NULL suffix from type
                col_type = re.sub(r"\s+(NOT\s+NULL|NULL)$", "", col_type).strip()
                columns[col_name] = col_type

    return columns


# --------------------------------------------------------------------------- #
# Live schema fetcher                                                          #
# --------------------------------------------------------------------------- #

def _fetch_schema(
    spark: SparkSession,
    jdbc_opts: dict,
    database_name: str,
    table_name: str,
    dialect: str = "mysql",
    schema_name: str = None,
) -> Dict[str, str]:
    """Query INFORMATION_SCHEMA.COLUMNS via JDBC (supports MySQL and SQL Server)."""
    if dialect == "sqlserver":
        # SQL Server: build full type with length/precision from multiple columns
        subquery = (
            f"(SELECT "
            f"  COLUMN_NAME, "
            f"  DATA_TYPE, "
            f"  CHARACTER_MAXIMUM_LENGTH, "
            f"  NUMERIC_PRECISION, "
            f"  NUMERIC_SCALE "
            f"FROM [{database_name}].INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_CATALOG = '{database_name}' "
            f"AND TABLE_SCHEMA = '{schema_name or 'dbo'}' "
            f"AND TABLE_NAME = '{table_name}' "
            f") AS schema_query"
        )
        
        rows = (
            spark.read.format("jdbc")
            .options(**jdbc_opts)
            .option("dbtable", subquery)
            .load()
            .orderBy("COLUMN_NAME")
            .collect()
        )
        
        # Build full type string for SQL Server
        result = {}
        for row in rows:
            col_name = row["COLUMN_NAME"].upper()
            data_type = row["DATA_TYPE"].upper()
            char_len = row["CHARACTER_MAXIMUM_LENGTH"]
            num_prec = row["NUMERIC_PRECISION"]
            num_scale = row["NUMERIC_SCALE"]
            
            # Build full type string
            if data_type in ("VARCHAR", "CHAR", "NVARCHAR", "NCHAR"):
                if char_len and char_len > 0:
                    full_type = f"{data_type}({char_len})"
                else:
                    full_type = f"{data_type}(MAX)"
            elif data_type in ("DECIMAL", "NUMERIC"):
                if num_prec and num_scale is not None:
                    full_type = f"{data_type}({num_prec},{num_scale})"
                else:
                    full_type = data_type
            else:
                full_type = data_type
            
            result[col_name] = full_type
        
        return result
        
    else:
        # MySQL: COLUMN_TYPE already includes length/precision
        query = (
            f"SELECT COLUMN_NAME, COLUMN_TYPE "
            f"FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{database_name}' "
            f"AND TABLE_NAME = '{table_name}' "
            f"ORDER BY ORDINAL_POSITION"
        )
        rows = (
            spark.read.format("jdbc")
            .options(**jdbc_opts)
            .option("query", query)
            .load()
            .collect()
        )
        
        return {
            row["COLUMN_NAME"].upper(): row["COLUMN_TYPE"].upper()
            for row in rows
        }


# --------------------------------------------------------------------------- #
# Column comparator                                                            #
# --------------------------------------------------------------------------- #

_TYPE_ALIASES: Dict[str, List[str]] = {
    "INT":         ["INT", "INT(11)", "INTEGER", "INT(10)", "INT(4)"],
    "BIGINT":      ["BIGINT", "BIGINT(20)"],
    "TINYINT(1)":  ["TINYINT(1)", "BOOLEAN", "BOOL"],
    "SMALLINT":    ["SMALLINT", "SMALLINT(6)"],
    "DATE":        ["DATE"],
    "DATETIME":    ["DATETIME", "TIMESTAMP", "DATETIME2"],
    "TEXT":        ["TEXT", "LONGTEXT", "MEDIUMTEXT", "VARCHAR(MAX)", "NVARCHAR(MAX)"],
}


def _normalize_type(type_str: str) -> str:
    """Normalize a data type string for comparison."""
    type_upper = type_str.upper().strip()
    
    # Handle VARCHAR/CHAR with different lengths as compatible
    # e.g., VARCHAR(50) and VARCHAR(100) are considered compatible
    if type_upper.startswith(("VARCHAR", "CHAR", "NVARCHAR", "NCHAR")):
        # Extract base type without length
        base_match = re.match(r"(N?(?:VAR)?CHAR)", type_upper)
        if base_match:
            return base_match.group(1)
    
    # Handle DECIMAL/NUMERIC with different precision as compatible
    # e.g., DECIMAL(10,2) and DECIMAL(12,2) are compatible if scale matches
    if type_upper.startswith(("DECIMAL", "NUMERIC")):
        # Extract scale (second number)
        scale_match = re.match(r"(DECIMAL|NUMERIC)\((\d+),(\d+)\)", type_upper)
        if scale_match:
            return f"{scale_match.group(1)}_SCALE_{scale_match.group(3)}"
        return type_upper.split("(")[0]  # Just base type if no precision
    
    return type_upper


def _types_compatible(ddl_type: str, live_type: str) -> bool:
    """Return True if ddl_type and live_type are considered compatible."""
    ddl_norm = _normalize_type(ddl_type)
    live_norm = _normalize_type(live_type)

    if ddl_norm == live_norm:
        return True

    # Check type aliases
    for canonical, aliases in _TYPE_ALIASES.items():
        ddl_in = any(ddl_norm == _normalize_type(a) for a in aliases)
        live_in = any(live_norm == _normalize_type(a) for a in aliases)
        if ddl_in and live_in:
            return True

    return False


def _compare_columns_partial(
    ddl_cols: Dict[str, str],
    live_cols: Dict[str, str],
) -> List[str]:
    """
    Partial DDL verification: every DDL column must exist in live DB.
    Extra columns in live DB are silently ignored (not flagged).
    """
    mismatches: List[str] = []

    for col_name, ddl_type in ddl_cols.items():
        if col_name not in live_cols:
            mismatches.append(
                f"Column '{col_name}' present in DDL but NOT FOUND in live DB."
            )
            continue

        live_type = live_cols[col_name]
        if not _types_compatible(ddl_type, live_type):
            mismatches.append(
                f"Column '{col_name}': DDL says '{ddl_type}', "
                f"live DB says '{live_type}' - types not compatible."
            )

    return mismatches


def _compare_columns(
    ddl_cols: Dict[str, str],
    live_cols: Dict[str, str],
) -> List[str]:
    """
    Compare DDL columns to live DB columns.
    Returns a list of human-readable mismatch descriptions (empty = all OK).
    """
    mismatches: List[str] = []

    for col_name, ddl_type in ddl_cols.items():
        if col_name not in live_cols:
            mismatches.append(
                f"Column '{col_name}' present in DDL but NOT FOUND in live DB."
            )
            continue

        live_type = live_cols[col_name]
        if not _types_compatible(ddl_type, live_type):
            mismatches.append(
                f"Column '{col_name}': DDL says '{ddl_type}', "
                f"live DB says '{live_type}' - types not compatible."
            )

    for col_name in live_cols:
        if col_name not in ddl_cols:
            mismatches.append(
                f"Column '{col_name}' present in live DB but NOT in DDL "
                f"(extra column - may be intentional)."
            )

    return mismatches


# --------------------------------------------------------------------------- #
# Schema JSON writer                                                           #
# --------------------------------------------------------------------------- #

def _write_schema_json(
    spark: "SparkSession",
    jdbc_opts: dict,
    output_dir: str,
    schema_side: str,
    database: str,
    table: str,
    schema_subpath: str = "xl",
) -> None:
    """Write live DB schema to a JSON file in PySpark StructType format."""
    schema_dir = os.path.join(output_dir, schema_subpath)
    os.makedirs(schema_dir, exist_ok=True)
    filename = "source_db_schema.json"
    filepath = os.path.join(schema_dir, filename)

    full_table = f"{database}.{table}"
    df = (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option("dbtable", f"(SELECT * FROM {full_table} WHERE 1=0) _schema_probe")
        .load()
    )

    with open(filepath, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(json.loads(df.schema.json()), indent=2))

    logger.info("Live DB schema (Spark StructType) written to: %s", filepath)


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #

def _split_table(full_table: str) -> Tuple[str, str]:
    """Split "schema.table" into (schema, table)."""
    parts = full_table.split(".")
    if len(parts) != 2 or not all(parts):
        raise ValueError(
            f"Expected 'schema.table' format, got: '{full_table}'"
        )
    return parts[0], parts[1]
