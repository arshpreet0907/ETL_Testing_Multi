"""
connections/spark_session.py
-----------------------------
Azure Databricks only — returns the active SparkSession.
"""

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "ETLValidator") -> SparkSession:
    """Return the active Databricks SparkSession."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
    logger.info("SparkSession ready (app=%s)", app_name)
    return spark

