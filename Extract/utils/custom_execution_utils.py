"""
utils/custom_execution_utils.py
--------------------------------
Pipeline step functions for source data extraction.
Supports both V3 multi-server and legacy single-table modes.

V3 Context keys (per-server)
-----------------------------
    server_name          : str
    source_ddls          : list[str]   — partial DDL file paths
    source_query_file    : str         — path to 03_extract_source.sql
    output_dir           : str         — e.g. "output/<table>/<server>"
    raw_source_csv       : str         — e.g. "output/<table>/<server>/source_raw.csv"
    source_filter        : dict
    enable_partitioning  : bool
    needs_prequery_bounds: bool
    source_partition_col : str|None
    partition_lower_bound: int|None
    partition_upper_bound: int|None
    num_partitions       : int

Legacy Context keys
--------------------
    config               : dict
    verify_schema        : bool
    source_query         : str|None
    source_query_file    : str|None
    primary_keys         : list[str]
    source_ddl           : str|None
    raw_source_csv       : str
    source_filter        : dict
    enable_partitioning  : bool
"""

import os
from typing import Optional, Set

from utils.auto_config import build_filter_for_query
from utils.connections.source_connection import get_source_connection
from utils.csv_writer import save_dataframe_as_csv
from utils.get_data import get_data
from utils.logger import get_logger
from utils.query_filter import apply_filter_to_sql
from utils.verify_schema import verify_schema_from_ddl, verify_partial_ddls

logger = get_logger(__name__)


# ============================================================================
# STANDALONE HELPERS
# ============================================================================

def resolve_query(query: str, query_file: str, query_type: str) -> str:
    """Return SQL string from either inline query or file (no trailing semicolon)."""
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
        # Remove USE statements (not needed for JDBC)
        # Handles both MySQL (use database;) and SQL Server (use database.schema;)
        # Use re.MULTILINE so ^ matches start of any line (handles comments before USE)
        import re
        content = re.sub(r'(?im)^\s*use\s+[^;]+;\s*\n?', '', content)
        
        # Remove SQL comments (-- style) - important for SQL Server CTEs
        # SQL Server doesn't like comments before WITH when wrapped in subquery by Spark
        content = re.sub(r'--[^\n]*\n?', '', content)
        
        content = content.strip()
        return content[:-1].strip() if content.endswith(";") else content
    raise ValueError(
        f"Either {query_type.upper()}_QUERY or {query_type.upper()}_QUERY_FILE "
        "must be provided"
    )


def detect_partition_bounds(spark, partition_col: str, config: dict,
                            server_name: str = None) -> tuple:
    """
    Run a lightweight SELECT MIN/MAX pre-query to detect partition bounds.

    Returns
    -------
    tuple[int, int]
        (lower_bound, upper_bound)
    """
    jdbc_opts = get_source_connection(server_name=server_name)
    table = config.get("source_table", "unknown")
    database = config.get("source_database", "")

    full_table = f"{database}.{table}" if database else table
    bounds_query = (
        f"SELECT MIN({partition_col}) AS min_val, "
        f"MAX({partition_col}) AS max_val FROM {full_table}"
    )

    bounds_df = (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option("query", bounds_query)
        .load()
    )
    row = bounds_df.first()
    lb = int(row["min_val"])
    ub = int(row["max_val"])
    return lb, ub


# ============================================================================
# V3 PIPELINE STEPS (per-server)
# ============================================================================

def step_0_verify_partial_schemas(spark, ctx: dict) -> bool:
    """
    Step 0 (V3): Verify all partial source DDLs for one server against live DB.
    """
    source_ddls = ctx.get("source_ddls", [])
    server_name = ctx.get("server_name")

    if not source_ddls:
        logger.info("[%s] No source DDLs to verify - skipping Step 0", server_name)
        return True

    logger.info("=" * 60)
    logger.info("STEP 0: Verify Source Schemas [server: %s]", server_name)
    logger.info("  DDL files: %d", len(source_ddls))
    logger.info("=" * 60)

    jdbc_opts = get_source_connection(server_name=server_name)
    db_type = jdbc_opts.get("db_type", "mysql")
    schema = jdbc_opts.get("schema")

    passed = verify_partial_ddls(
        spark=spark,
        jdbc_opts=jdbc_opts,
        ddl_files=source_ddls,
        dialect=db_type,
        schema=schema,
        output_dir=ctx.get("output_dir"),
    )

    if not passed:
        logger.error("[%s] Source schema verification FAILED", server_name)
        return False

    logger.info("[%s] Source schema verification PASSED", server_name)
    return True


def step_1_extract_source_v3(spark, ctx: dict):
    """
    Step 1 (V3): Extract source data for one server using combined SQL.
    """
    server_name = ctx.get("server_name")
    source_filter = ctx.get("source_filter", {"where_clause": "", "description": "full load"})

    logger.info("=" * 60)
    logger.info("STEP 1: Extract Source Data [server: %s, filter: %s]",
                server_name, source_filter.get("description", ""))
    logger.info("=" * 60)

    base_sql = resolve_query(
        ctx.get("source_query"),
        ctx.get("source_query_file"),
        "source",
    )
    final_sql = apply_filter_to_sql(base_sql, source_filter.get("where_clause", ""))

    if source_filter.get("where_clause"):
        logger.info("Applied WHERE clause: %s", source_filter["where_clause"])

    # Build partition kwargs (empty dict = single-partition)
    partition_kwargs = {}
    if ctx.get("enable_partitioning"):
        if ctx.get("needs_prequery_bounds"):
            logger.info("Running pre-query to detect partition bounds...")
            _lb, _ub = detect_partition_bounds(
                spark, ctx["source_partition_col"], ctx.get("config", {}),
                server_name=server_name,
            )
            ctx["partition_lower_bound"] = _lb
            ctx["partition_upper_bound"] = _ub
            logger.info("Detected bounds: [%d, %d]", _lb, _ub)

        partition_kwargs = dict(
            partition_col=ctx["source_partition_col"],
            lower_bound=ctx["partition_lower_bound"],
            upper_bound=ctx["partition_upper_bound"],
            num_partitions=ctx["num_partitions"],
        )

    # print("final_query: '",final_sql,"'")
    source_df = get_data(
        spark=spark,
        db_type="source",
        query=final_sql,
        server_name=server_name,
        **partition_kwargs,
    )

    logger.info("[%s] Step 1 complete.", server_name)
    return source_df


def step_1_5_save_raw_source_csv_v3(source_df, ctx: dict):
    """Step 1.5 (V3): Save raw source data to CSV for one server."""
    server_name = ctx.get("server_name")

    logger.info("=" * 60)
    logger.info("STEP 1.5: Save RAW Source CSV [server: %s]", server_name)
    logger.info("=" * 60)

    csv_path = ctx["raw_source_csv"]
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    save_dataframe_as_csv(source_df, csv_path)
    logger.info("[%s] Raw Source data saved to: %s", server_name, csv_path)
    logger.info("[%s] Step 1.5 complete.", server_name)


# ============================================================================
# LEGACY PIPELINE STEPS (backward compatible)
# ============================================================================

def step_0_verify_source_schema(spark, ctx: dict, path_sub=None) -> bool:
    """Step 0 (Legacy): Verify source schema (optional)."""
    if not ctx.get("verify_schema") or not ctx.get("source_ddl"):
        return True

    logger.info("=" * 60)
    logger.info("STEP 0: Verify Source Schema")
    logger.info("=" * 60)

    config = ctx["config"]
    passed = verify_schema_from_ddl(
        spark=spark,
        jdbc_opts=get_source_connection(),
        ddl_file=ctx["source_ddl"],
        database=config.get("source_database"),
        table=config.get("source_table"),
        dialect="mysql",
        output_dir=ctx.get("output_dir"),
        schema_side="source",
        schema_subpath=path_sub or "xl",
    )

    if not passed:
        logger.error("Source schema verification FAILED")
        return False

    logger.info("Source schema verification PASSED")
    return True


def step_1_extract_source(spark, ctx: dict):
    """Step 1 (Legacy): Extract source data, applying SOURCE_FILTER WHERE clause."""
    source_filter = ctx["source_filter"]

    logger.info("=" * 60)
    logger.info("STEP 1: Extract Source Data  [filter: %s]", source_filter["description"])
    logger.info("=" * 60)

    base_sql = resolve_query(ctx["source_query"], ctx["source_query_file"], "source")
    final_sql = apply_filter_to_sql(base_sql, source_filter["where_clause"])

    if source_filter["where_clause"]:
        logger.info("Applied WHERE clause: %s", source_filter["where_clause"])

    # Build partition kwargs (empty dict = single-partition)
    partition_kwargs = {}
    if ctx.get("enable_partitioning"):
        if ctx.get("needs_prequery_bounds"):
            logger.info("Running pre-query to detect partition bounds...")
            _lb, _ub = detect_partition_bounds(
                spark, ctx["source_partition_col"], ctx["config"],
            )
            ctx["partition_lower_bound"] = _lb
            ctx["partition_upper_bound"] = _ub
            logger.info("Detected bounds: [%d, %d]", _lb, _ub)

        partition_kwargs = dict(
            partition_col=ctx["source_partition_col"],
            lower_bound=ctx["partition_lower_bound"],
            upper_bound=ctx["partition_upper_bound"],
            num_partitions=ctx["num_partitions"],
        )

    source_df = get_data(
        spark=spark, db_type="source", query=final_sql, **partition_kwargs
    )

    logger.info("Step 1 complete.")
    return source_df


def step_1_5_save_raw_source_csv(source_df, ctx: dict):
    """Step 1.5 (Legacy): Save raw source data to CSV."""
    logger.info("=" * 60)
    logger.info("STEP 1.5: Save RAW Source CSV")
    logger.info("=" * 60)

    save_dataframe_as_csv(source_df, ctx["raw_source_csv"])
    logger.info("Raw Source data saved to: %s", ctx["raw_source_csv"])
    logger.info("Step 1.5 complete.")


# ============================================================================
# FILTER BUILDER
# ============================================================================

def build_source_filter(
    config: dict,
    date_mode: str,
    date_from: Optional[str] = None,
    date_from_col: Optional[str] = None,
    date_to: Optional[str] = None,
    date_to_col: Optional[str] = None,
) -> dict:
    """
    Build WHERE clause filter for source query.

    Returns
    -------
    dict
        {"where_clause": str, "date_mode": str, "description": str}
    """
    if not config:
        return {"where_clause": "", "description": "no config"}

    return build_filter_for_query(
        query_type="source",
        config=config,
        date_mode=date_mode,
        date_from=date_from,
        date_from_col=date_from_col,
        date_to=date_to,
        date_to_col=date_to_col,
    )
