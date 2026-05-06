"""
utils/custom_execution_utils.py
--------------------------------
Databricks version — V3 multi-server pipeline step functions.

No CSV saves (transformed/target). Only diff_report.csv is produced.
Source data comes from per-server CSVs in Azure Blob Storage.
Target data comes from Snowflake via native Spark-Snowflake connector.

Steps:
  1. Load per-server CSVs → Transform each → Union all
  2. Verify target schema (Snowflake live)
  3. Extract target from Snowflake
  4. Compare & generate diff report
"""

import os
from typing import Optional, Set

from Execution.utils.auto_config import build_filter_for_query
from Execution.utils.connections.target_connection import get_target_connection
from Execution.utils.compare import compare_and_report
from Execution.utils.get_data import get_data_from_storage, get_data_from_snowflake
from Execution.utils.logger import get_logger
from Execution.utils.perform_transform import perform_transform
from Execution.utils.query_filter import apply_filter_to_sql
from Execution.utils.verify_schema import verify_schema_from_ddl

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


# ============================================================================
# PIPELINE STEPS
# ============================================================================

def step_1_load_source(spark, ctx: dict, blob_base: str) -> list:
    """
    Step 1: Load per-server CSVs from Azure Blob Storage.

    For each server in config["servers"]:
      - Load CSV from blob_base/<server_name>/source_raw.csv
      - Load schema from source_raw.schema.json
      - Apply source filter (PK/date)

    Returns a list of dicts: [{"server_name": str, "df": DataFrame, "server_cfg": dict}, ...]
    """
    logger.info("=" * 60)
    logger.info("STEP 1: Load Source CSVs (per server)")
    logger.info("=" * 60)

    servers = ctx["config"]["servers"]
    source_filter = ctx.get("source_filter")
    server_dfs = []

    for server_cfg in servers:
        server_name = server_cfg["server_name"]
        csv_path = f"{blob_base}/{server_name}/source_raw.csv"
        schema_path = f"{blob_base}/{server_name}/source_raw.schema.json"

        logger.info("─── Server: %s ───", server_name)
        logger.info("  CSV   : %s", csv_path)
        logger.info("  Schema: %s", schema_path)

        # Load raw CSV with schema
        source_df = get_data_from_storage(spark, csv_path, schema_path)
        logger.info("  Loaded: %d columns", len(source_df.columns))

        # Apply PK / date filters
        source_df = _apply_filter_to_df(source_df, source_filter, label=f"source({server_name})")

        server_dfs.append({
            "server_name": server_name,
            "df": source_df,
            "server_cfg": server_cfg,
        })

    logger.info("Step 1 complete: %d server(s) loaded", len(server_dfs))
    return server_dfs


def step_2_transform_union(spark, server_dfs: list, ctx: dict):
    """
    Step 2: Transform each per-server DataFrame, then union all.

    For each entry in server_dfs (produced by step_1_load_source):
      - Transform with server-specific 04_transform.py
      - Add _source_server column for traceability

    Union all transformed DFs via unionByName(allowMissingColumns=True).
    Cache result and return (df, row_count).
    """
    from pyspark.sql import functions as F
    
    logger.info("=" * 60)
    logger.info("STEP 2: Transform + Union (per server)")
    logger.info("=" * 60)

    transformed_dfs = []

    for entry in server_dfs:
        server_name = entry["server_name"]
        source_df = entry["df"]
        server_cfg = entry["server_cfg"]

        logger.info("─── Transforming server: %s ───", server_name)

        transformed_df = perform_transform(
            df=source_df,
            transform_file=server_cfg["transform_file"],
        )
        # Add source server identifier for traceability in diff reports
        transformed_df = transformed_df.withColumn("_source_server", F.lit(server_name))
        logger.info("  Transformed: %d columns (+ _source_server)", len(transformed_df.columns) - 1)
        transformed_dfs.append(transformed_df)

    if not transformed_dfs:
        raise ValueError("No server DataFrames to union — step_1_load_source returned empty list")

    # Union all server DataFrames
    if len(transformed_dfs) == 1:
        result_df = transformed_dfs[0]
        logger.info("Single server — no union needed")
    else:
        result_df = transformed_dfs[0]
        for sdf in transformed_dfs[1:]:
            result_df = result_df.unionByName(sdf, allowMissingColumns=True)
        logger.info("Unioned %d server DataFrames via unionByName", len(transformed_dfs))

    # Cache — reused for display + comparison
    result_df.cache()
    row_count = result_df.count()
    logger.info("Step 2 complete: %d rows, %d columns", row_count, len(result_df.columns))

    return result_df, row_count


def step_1_load_transform_union(spark, ctx: dict, blob_base: str):
    """Legacy wrapper — calls step_1_load_source + step_2_transform_union."""
    server_dfs = step_1_load_source(spark, ctx, blob_base)
    return step_2_transform_union(spark, server_dfs, ctx)


def step_3_verify_target_schema(spark, ctx: dict) -> bool:
    """Step 3: Verify target schema (Snowflake live INFORMATION_SCHEMA)."""
    if not ctx["verify_schema"] or not ctx["target_ddl"]:
        return True

    logger.info("=" * 60)
    logger.info("STEP 2: Verify Target Schema (Snowflake)")
    logger.info("=" * 60)

    config = ctx["config"]
    sf_db_override = ctx.get("sf_database_override")
    sf_opts = get_target_connection(mode="snowflake", database_override=sf_db_override)

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

    logger.info("Step 3 complete.")
    return target_df, row_count


def step_5_compare(spark, transformed_df, target_df, ctx: dict) -> int:
    """Step 4: Compare source and target DataFrames, produce diff_report.csv only."""
    logger.info("=" * 60)
    logger.info("STEP 4: Compare Data & Generate Report")
    logger.info("=" * 60)

    compare_cols = ctx["compare_cols"]
    if compare_cols is None:
        # Case-insensitive column matching (Snowflake returns UPPERCASE)
        src_cols_lower = {c.lower() for c in transformed_df.columns}
        tgt_cols_lower = {c.lower() for c in target_df.columns}
        common_lower = src_cols_lower & tgt_cols_lower
        pk_lower = {pk.lower() for pk in ctx["primary_keys"]}
        exc_lower = {e.lower() for e in (ctx["exclude_cols"] or [])}
        # Exclude _source_server from comparison columns (it's metadata)
        compare_cols = sorted(common_lower - pk_lower - exc_lower - {"_source_server"})
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

    # V3: use target DDL for both source and target filters
    # (PKs are aliased to target names in transform files)
    source_filter = build_filter_for_query(
        config=config,
        date_mode=date_mode, date_from=date_from, date_from_col=date_from_col,
        date_to=date_to, date_to_col=date_to_col,
    )

    target_filter = build_filter_for_query(
        config=config,
        date_mode=date_mode, date_from=date_from, date_from_col=date_from_col,
        date_to=date_to, date_to_col=date_to_col,
    )

    return source_filter, target_filter
