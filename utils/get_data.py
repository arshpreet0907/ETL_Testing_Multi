"""
utils/get_data.py
-----------------
Azure Databricks version — two data sources:
  1. Raw source CSV from Azure Blob Storage (wasbs://) — no cache (single-use)
  2. Snowflake target via native Spark-Snowflake connector — cached
     (without cache, comparator's normalisation would re-query Snowflake)
"""

import json
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from utils.logger import get_logger

logger = get_logger(__name__)


def _get_dbutils():
    """Get dbutils reference on Databricks."""
    spark = SparkSession.getActiveSession()
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        import IPython
        return IPython.get_ipython().user_ns.get("dbutils")


def get_data_from_storage(
    spark: SparkSession,
    csv_path: str,
    schema_json_path: str = None,
) -> DataFrame:
    """
    Read data from Azure Blob Storage (wasbs://) with schema from .schema.json.

    Parameters
    ----------
    spark : SparkSession
    csv_path : str
        wasbs:// path to CSV file.
    schema_json_path : str, optional
        wasbs:// path to .schema.json file.
    """
    logger.info("Loading data from storage: %s", csv_path)

    # Load schema from JSON via dbutils (works with wasbs:// paths)
    schema = None
    if schema_json_path:
        try:
            dbutils = _get_dbutils()
            schema_str = dbutils.fs.head(schema_json_path, 1048576)  # 1MB max
            schema = StructType.fromJson(json.loads(schema_str))
            logger.info("Schema loaded from: %s (%d fields)", schema_json_path, len(schema.fields))
        except Exception as e:
            logger.warning("Could not load schema from %s: %s — using inferSchema", schema_json_path, e)

    # Read CSV
    reader = spark.read.option("header", True).option("nullValue", "")
    if schema:
        df = reader.option("inferSchema", True).csv(csv_path)
        from pyspark.sql.functions import col
        cast_exprs = []
        for field in schema.fields:
            if field.name in df.columns:
                cast_exprs.append(col(field.name).cast(field.dataType).alias(field.name))
        df = df.select(cast_exprs)
    else:
        df = reader.option("inferSchema", True).csv(csv_path)

    # No cache — source_df is used once (flows into transform) then discarded.
    # Row count is deferred to step_2_transform where it's needed anyway.
    logger.info(
        "Loaded from storage (lazy): %d columns",
        len(df.columns),
    )
    logger.info("Columns: %s", df.columns)

    return df


def get_data_from_snowflake(
    spark: SparkSession,
    query: str,
    sf_options: dict,
) -> DataFrame:
    """
    Read from Snowflake via native Spark-Snowflake connector.

    Parameters
    ----------
    spark : SparkSession
    query : str
        SQL query to execute on Snowflake.
    sf_options : dict
        Snowflake connector options from get_target_connection().
    """
    logger.info("Extracting data from Snowflake (native Spark connector)")
    logger.debug("Query (%d chars): %s", len(query), query[:200])

    df = (
        spark.read
        .format("snowflake")
        .options(**sf_options)
        .option("query", query)
        .load()
    )

    # Cache target_df — without this, comparator's _normalise_df(target_df)
    # would re-execute the Snowflake query over the network.
    # Unlike source CSV (which flows into transform creating a new DF),
    # target_df is passed directly to the comparator which reads it to
    # build target_norm. The cache is unpersisted by comparator after
    # target_norm is materialized (comparator.py line ~285).
    df.cache()
    start_time = time.time()
    row_count = df.count()
    extract_time = time.time() - start_time

    logger.info("Snowflake extract: %.2fs | Row count: %d", extract_time, row_count)
    logger.info("Extraction complete: %d rows, %d columns", row_count, len(df.columns))
    logger.info("Columns: %s", df.columns)
    return df, row_count

