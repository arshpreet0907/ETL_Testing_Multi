"""
get_data.py
-----------
MySQL source data extraction module.
Returns PySpark DataFrames for further processing.

Two read paths
--------------
1. Single-partition (default):
   Uses spark.read ... .option("query", sql).load()

2. Partitioned read:
   Uses spark.read ... .option("dbtable", subquery) with numPartitions,
   partitionColumn, lowerBound, upperBound.
"""
import time
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from utils.connections.source_connection import get_source_connection
from utils.logger import get_logger

logger = get_logger(__name__)


def get_data(
    spark: SparkSession,
    db_type: str = "source",
    query: str = "",
    server_name: Optional[str] = None,
    partition_col: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
    num_partitions: Optional[int] = None,
    **kwargs,
) -> DataFrame:
    """
    Extract data from source database using SQL query.

    Parameters
    ----------
    spark : SparkSession
    db_type : str
        Must be "source".
    query : str
        SQL SELECT query to execute.
    server_name : str, optional
        Server key from source_config.yaml (multi-server support).
    partition_col, lower_bound, upper_bound, num_partitions :
        Optional JDBC partitioning parameters.

    Returns
    -------
    DataFrame
    """
    # Validate that partition params are all-or-nothing
    partition_params = [partition_col, lower_bound, upper_bound, num_partitions]
    provided = [p for p in partition_params if p is not None]
    if 0 < len(provided) < 4:
        raise ValueError(
            "Partitioned read requires ALL FOUR parameters: "
            "partition_col, lower_bound, upper_bound, num_partitions. "
            f"Only {len(provided)} of 4 were provided."
        )
    use_partitioning = len(provided) == 4

    jdbc_opts = get_source_connection(server_name=server_name)
    label = f"[{server_name}] " if server_name else ""
    logger.info("%sExtracting data from SOURCE database", label)
    logger.debug("Query (%d chars): %s", len(query), query[:200])

    try:
        if use_partitioning:
            df = _read_partitioned(spark, jdbc_opts, query,
                                   partition_col, lower_bound,
                                   upper_bound, num_partitions)
        else:
            df = _read_single(spark, jdbc_opts, query)

        row_count = df.count()
        logger.info("Extraction complete: %d rows, %d columns", row_count, len(df.columns))
        logger.info("Columns: %s", df.columns)

        return df

    except Exception as exc:
        safe_url = jdbc_opts.get("url", "unknown")
        user = jdbc_opts.get("user", "unknown")
        logger.error(
            "JDBC extraction failed: %s\n  URL: %s\n  User: %s",
            exc, safe_url, user,
        )
        raise


def _read_single(spark: SparkSession, jdbc_opts: dict, query: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option("query", query)
        .load()
    )


def _read_partitioned(
    spark: SparkSession, jdbc_opts: dict, query: str,
    partition_col: str, lower_bound: int, upper_bound: int, num_partitions: int,
) -> DataFrame:
    logger.info(
        "Partitioned JDBC read: col=%s  bounds=[%d, %d]  partitions=%d",
        partition_col, lower_bound, upper_bound, num_partitions,
    )
    dbtable_expr = f"({query}) AS _jdbc_subquery"
    return (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option("dbtable", dbtable_expr)
        .option("partitionColumn", partition_col)
        .option("lowerBound", str(lower_bound))
        .option("upperBound", str(upper_bound))
        .option("numPartitions", str(num_partitions))
        .load()
    )
