"""
utils/csv_writer.py
--------------------
Azure Databricks version — saves a PySpark DataFrame as a single named CSV file.
Supports wasbs:// (Azure Blob Storage) and local paths.
Uses dbutils.fs for blob storage file operations.
"""

import json
import logging
import os

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def _get_dbutils():
    """Get dbutils reference on Databricks."""
    spark = SparkSession.getActiveSession()
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        import IPython
        return IPython.get_ipython().user_ns.get("dbutils")


def save_dataframe_as_csv(df: DataFrame, file_path: str) -> None:
    """
    Write a PySpark DataFrame to a single CSV file at `file_path`.

    Supports wasbs:// paths (Azure Blob Storage) and local/DBFS paths.
    Uses coalesce(1) to produce one part file, then renames it.
    """
    is_cloud_path = file_path.startswith("wasbs://") or file_path.startswith("dbfs:")

    tmp_dir = file_path + "_tmp_spark"
    logger.info("Writing DataFrame to temporary Spark directory: %s", tmp_dir)

    col_count = len(df.columns)
    row_count = 0

    # Write with caching support (dedicated cluster)
    is_cached = df.is_cached
    if is_cached:
        df.coalesce(1).write.mode("overwrite").option("header", "true").option("nullValue", "").csv(tmp_dir)
    else:
        df_cached = df.coalesce(1).cache()
        row_count = df_cached.count()
        df_cached.write.mode("overwrite").option("header", "true").option("nullValue", "").csv(tmp_dir)
        df_cached.unpersist()

    if is_cloud_path:
        # Use dbutils.fs for cloud storage paths
        dbutils = _get_dbutils()

        # Find the part file
        files = dbutils.fs.ls(tmp_dir)
        part_files = [f.path for f in files if f.name.startswith("part-")]

        if not part_files:
            raise FileNotFoundError(f"Spark produced no part file in {tmp_dir}")
        if len(part_files) > 1:
            raise RuntimeError(f"Expected 1 part file, found {len(part_files)}")

        dbutils.fs.mv(part_files[0], file_path)

        # Count rows if we didn't cache
        if row_count == 0:
            content = dbutils.fs.head(file_path, 10485760)  # 10MB
            row_count = max(content.count("\n") - 1, 0)

        logger.info("CSV saved: %s (%d rows, %d columns)", file_path, row_count, col_count)

        # Save schema alongside CSV
        schema_path = file_path.rsplit(".", 1)[0] + ".schema.json"
        schema_json = json.dumps(json.loads(df.schema.json()), indent=2)
        dbutils.fs.put(schema_path, schema_json, overwrite=True)
        logger.info("Schema saved: %s", schema_path)

        # Cleanup temp dir
        dbutils.fs.rm(tmp_dir, recurse=True)
    else:
        # Local filesystem path
        import glob
        import shutil

        file_path = os.path.normpath(file_path)
        parent_dir = os.path.dirname(file_path) or "."
        os.makedirs(parent_dir, exist_ok=True)

        part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
        if not part_files:
            part_files = glob.glob(os.path.join(tmp_dir, "part-*"))

        if not part_files:
            raise FileNotFoundError(f"Spark produced no part file in {tmp_dir}")
        if len(part_files) > 1:
            raise RuntimeError(f"Expected 1 part file, found {len(part_files)}")

        shutil.move(part_files[0], file_path)

        if row_count == 0:
            with open(file_path, "r", encoding="utf-8") as f:
                row_count = max(sum(1 for _ in f) - 1, 0)

        logger.info("CSV saved: %s (%d rows, %d columns)", file_path, row_count, col_count)

        schema_path = os.path.splitext(file_path)[0] + ".schema.json"
        with open(schema_path, "w", encoding="utf-8") as sf:
            sf.write(json.dumps(json.loads(df.schema.json()), indent=2))
        logger.info("Schema saved: %s", schema_path)

        shutil.rmtree(tmp_dir, ignore_errors=True)
