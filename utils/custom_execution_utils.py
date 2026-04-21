"""
utils/custom_execution_utils.py
--------------------------------
Databricks version — pipeline step functions.

No CSV saves (transformed/target). Only diff_report.csv is produced.
Source data comes from storage CSV, target from Snowflake connector.
"""

import os
from typing import Literal, Optional, Set

from utils.auto_config import build_filter_for_query
from utils.connections.target_connection import get_target_connection
from utils.compare import compare_and_report
from utils.get_data import get_data_from_storage, get_data_from_snowflake
from utils.logger import get_logger
from utils.perform_transform import perform_transform
from utils.query_filter import apply_filter_to_sql
from utils.verify_schema import verify_schema_from_ddl, verify_schema_from_json_file

logger = get_logger(__name__)


# ============================================================================
# STANDALONE HELPERS
# ============================================================================

def resolve_query(query: str, query_file: str, query_type: str) -> str:
    """Return SQL string from either inline query or file."""
    if query:
        sql = query.strip()
        return sql[:-1].strip() if sql.endswith(";") else sql
    if query_file:
        if not os.path.isfile(query_file):
            raise FileNotFoundError(
                f"{query_type.upper()} query file not found: {query_file}"
            )
        with open(query_file, "r", encoding="utf-8") as fh:
            content = fh.read().strip()
        return content[:-1].strip() if content.endswith(";") else content
    raise ValueError(
        f"Either {query_type.upper()}_QUERY or {query_type.upper()}_QUERY_FILE must be provided"
    )


# ============================================================================
# PIPELINE STEPS
# ============================================================================

def step_0_verify_source_schema(spark, ctx: dict) -> bool:
    """
    Step 0: Verify source schema — compare source_raw.schema.json (from DDL)
    against the source DDL file.

    source_raw.schema.json is generated from the source DDL and represents
    the actual live source table structure.
    """
    if not ctx["verify_schema"] or not ctx["source_ddl"]:
        return True

    logger.info("=" * 60)
    logger.info("STEP 0: Verify Source Schema (DDL-based JSON vs DDL)")
    logger.info("=" * 60)

    schema_json_path = ctx.get("source_db_schema_json")
    if not schema_json_path:
        logger.warning("No source_db_schema_json path in context — error in source schema check")
        return False

    # Read schema JSON via dbutils for blob storage compatibility
    passed = verify_schema_from_json_file(
        spark=spark,
        schema_json_path=schema_json_path,
        ddl_file=ctx["source_ddl"],
        dialect="mysql",
    )

    if not passed:
        logger.error("Source schema verification FAILED")
        return False

    logger.info("Source schema verification PASSED")
    return True


def _apply_filter_to_df(df, filter_dict: dict, label: str = "source"):
    """
    Apply a SQL-style where_clause from a filter dict to a Spark DataFrame.
    Uses DataFrame.filter() with F.expr() to avoid temp views (serverless-safe).
    Returns the filtered DataFrame, or the original if no filter applies.
    """
    from pyspark.sql import functions as F

    where_clause = (filter_dict or {}).get("where_clause", "")
    if not where_clause:
        return df

    filtered_df = df.filter(F.expr(where_clause))
    logger.info("Applied %s filter: %s", label, where_clause)
    return filtered_df


def step_1_extract_source(spark, ctx: dict):
    """
    Step 1: Load source data from storage CSV (DBFS/Volumes).
    Uses source_extracted_raw.schema.json (schema of the extracted CSV,
    which includes join columns and excludes dropped columns).
    Applies PK and date filters if configured.
    """
    logger.info("=" * 60)
    logger.info("STEP 1: Load Source Data from Storage")
    logger.info("=" * 60)

    source_csv_path = ctx["source_csv_path"]
    # Use the extracted CSV schema (not the DDL-based one) for type inference
    schema_json_path = ctx.get("source_schema_json")

    source_df = get_data_from_storage(spark, source_csv_path, schema_json_path)

    # Apply PK / date filters to source DataFrame (mirrors target SQL filtering)
    source_filter = ctx.get("source_filter")
    source_df = _apply_filter_to_df(source_df, source_filter, label="source")

    logger.info("Step 1 complete.")
    return source_df


def step_2_transform(source_df, ctx: dict, target_mode: str = "snowflake"):
    """
    Step 2: Apply transformations to source data.
    Caches transformed_df for reuse (display + comparison).
    Returns (transformed_df, row_count) so caller can log without extra .count().
    """
    logger.info("=" * 60)
    logger.info("STEP 2: Apply Transformations")
    logger.info("=" * 60)

    transformed_df = perform_transform(
        df=source_df,
        transform_file=ctx["transform_file"],
        target_mode=target_mode,
    )

    # Cache — transformed_df is reused: display in main.py + comparison in step 5.
    # Single .count() materializes the cache and gives us the row count.
    transformed_df.cache()
    row_count = transformed_df.count()
    logger.info("Transformed DataFrame: %d rows", row_count)

    logger.info("Step 2 complete.")
    return transformed_df, row_count


def step_3_5_verify_target_schema(spark, ctx: dict) -> bool:
    """Step 3.5: Verify target schema (Snowflake live INFORMATION_SCHEMA)."""
    if not ctx["verify_schema"] or not ctx["target_ddl"]:
        return True

    logger.info("=" * 60)
    logger.info("STEP 3.5: Verify Target Schema (Snowflake)")
    logger.info("=" * 60)

    config = ctx["config"]
    sf_db_override = ctx.get("sf_database_override")
    sf_opts = get_target_connection(mode="snowflake", database_override=sf_db_override)

    # Fall back to Snowflake connection database if DDL didn't specify one
    target_database = config.get("target_database") or sf_opts.get("sfDatabase")

    passed = verify_schema_from_ddl(
        spark=spark,
        jdbc_opts=sf_opts,
        ddl_file=ctx["target_ddl"],
        database=target_database,
        table=config.get("target_table"),
        dialect="snowflake",
        schema=sf_opts.get("sfSchema", "PUBLIC"),
    )

    if not passed:
        logger.error("Target schema verification FAILED")
        return False

    logger.info("Target schema verification PASSED")
    return True


def step_4_extract_target(spark, ctx: dict):
    """Step 4: Extract target data from Snowflake via native connector."""
    target_filter = ctx["target_filter"]

    logger.info("=" * 60)
    logger.info("STEP 4: Extract Target from Snowflake  [filter: %s]", target_filter["description"])
    logger.info("=" * 60)

    base_sql = resolve_query(ctx["target_query"], ctx["target_query_file"], "target")
    final_sql = apply_filter_to_sql(base_sql, target_filter["where_clause"])

    if target_filter["where_clause"]:
        logger.info("Applied WHERE clause: %s", target_filter["where_clause"])

    sf_db_override = ctx.get("sf_database_override")
    sf_opts = get_target_connection(mode="snowflake", database_override=sf_db_override)
    target_df, row_count = get_data_from_snowflake(spark, final_sql, sf_opts)

    logger.info("Step 4 complete.")
    return target_df, row_count


def step_5_compare(spark, transformed_df, target_df, ctx: dict) -> int:
    """Step 5: Compare source and target DataFrames, produce diff_report.csv only."""
    logger.info("=" * 60)
    logger.info("STEP 5: Compare Data & Generate Report")
    logger.info("=" * 60)

    compare_cols = ctx["compare_cols"]
    if compare_cols is None:
        # Case-insensitive column matching (Snowflake returns UPPERCASE)
        src_cols_lower = {c.lower() for c in transformed_df.columns}
        tgt_cols_lower = {c.lower() for c in target_df.columns}
        common_lower = src_cols_lower & tgt_cols_lower
        pk_lower = {pk.lower() for pk in ctx["primary_keys"]}
        exc_lower = {e.lower() for e in (ctx["exclude_cols"] or [])}
        compare_cols = sorted(common_lower - pk_lower - exc_lower)
        logger.info("Auto-detected compare columns: %s", compare_cols)

    exit_code = compare_and_report(
        spark=spark,
        source_df=transformed_df,
        target_df=target_df,
        primary_key_cols=ctx["primary_keys"],
        compare_cols=compare_cols,
        output_path=ctx["report_csv"],
    )

    logger.info("Step 5 complete.")
    return exit_code


# ============================================================================
# FILTER BUILDER
# ============================================================================

def build_load_filters(
    config: dict,
    pk_filter_mode: str,
    pk_range: dict,
    pk_set: Set,
    date_mode: str,
    date_from: Optional[str] = None,
    date_from_col: Optional[str] = None,
    date_to: Optional[str] = None,
    date_to_col: Optional[str] = None,
) -> tuple:
    """Build separate WHERE clause filters for source and target queries."""
    if not config:
        return (
            {"where_clause": "", "description": "no config"},
            {"where_clause": "", "description": "no config"},
        )

    source_filter = build_filter_for_query(
        query_type="source", config=config,
        pk_filter_mode=pk_filter_mode, pk_range=pk_range, pk_set=pk_set,
        date_mode=date_mode, date_from=date_from, date_from_col=date_from_col,
        date_to=date_to, date_to_col=date_to_col,
    )

    target_filter = build_filter_for_query(
        query_type="target", config=config,
        pk_filter_mode=pk_filter_mode, pk_range=pk_range, pk_set=pk_set,
        date_mode=date_mode, date_from=date_from, date_from_col=date_from_col,
        date_to=date_to, date_to_col=date_to_col,
    )

    return source_filter, target_filter

