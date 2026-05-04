"""
connections/target_connection.py
---------------------------------
Azure Databricks — Snowflake credentials from Key Vault-backed secret scope.

Uses Databricks Secrets API (scope: "etl-secrets") backed by Azure Key Vault.
"""

import logging

logger = logging.getLogger(__name__)

_SECRET_SCOPE = "etl-secrets"


def _get_dbutils():
    """Get dbutils reference on Databricks."""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        import IPython
        return IPython.get_ipython().user_ns.get("dbutils")


def get_target_connection(mode: str = "snowflake", database_override: str = None) -> dict:
    """
    Return Snowflake connection options from Databricks Secrets.

    Parameters
    ----------
    mode : str
        Only "snowflake" is supported.
    database_override : str, optional
        If provided, overrides the sf-database secret from Key Vault.
        Passed from the SF_DATABASE widget in main.py.

    Returns
    -------
    dict
        Spark-Snowflake connector options dict.
    """
    if mode != "snowflake":
        raise ValueError(
            f"Only 'snowflake' target mode is supported, got: {mode!r}"
        )
    return _build_snowflake_connector_opts(database_override=database_override)


def get_snowflake_jdbc_opts() -> dict:
    """
    Return JDBC options for Snowflake schema verification.
    Used by verify_schema.py to query INFORMATION_SCHEMA.COLUMNS.
    """
    dbutils = _get_dbutils()

    account = dbutils.secrets.get(_SECRET_SCOPE, "sf-account")
    database = dbutils.secrets.get(_SECRET_SCOPE, "sf-database")
    schema = dbutils.secrets.get(_SECRET_SCOPE, "sf-schema") if _secret_exists(dbutils, "sf-schema") else "PUBLIC"

    jdbc_url = f"jdbc:snowflake://{account}.snowflakecomputing.com/?db={database}&schema={schema}"
    logger.info("Snowflake JDBC URL: %s", jdbc_url)

    return {
        "url": jdbc_url,
        "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
        "user": dbutils.secrets.get(_SECRET_SCOPE, "sf-user"),
        "password": dbutils.secrets.get(_SECRET_SCOPE, "sf-password"),
        "sfWarehouse": dbutils.secrets.get(_SECRET_SCOPE, "sf-warehouse"),
        "sfDatabase": database,
        "sfSchema": schema,
        "sfRole": dbutils.secrets.get(_SECRET_SCOPE, "sf-role"),
    }


def _build_snowflake_connector_opts(database_override: str = None) -> dict:
    """Build Spark-Snowflake connector options from Databricks Secrets."""
    dbutils = _get_dbutils()

    account = dbutils.secrets.get(_SECRET_SCOPE, "sf-account")
    # Use widget override if provided, otherwise fall back to Key Vault secret
    database = database_override or dbutils.secrets.get(_SECRET_SCOPE, "sf-database")
    schema = dbutils.secrets.get(_SECRET_SCOPE, "sf-schema") if _secret_exists(dbutils, "sf-schema") else "PUBLIC"

    opts = {
        "sfURL": f"{account}.snowflakecomputing.com",
        "sfUser": dbutils.secrets.get(_SECRET_SCOPE, "sf-user"),
        "sfPassword": dbutils.secrets.get(_SECRET_SCOPE, "sf-password"),
        "sfDatabase": database,
        "sfSchema": schema,
        "sfWarehouse": dbutils.secrets.get(_SECRET_SCOPE, "sf-warehouse"),
        "sfRole": dbutils.secrets.get(_SECRET_SCOPE, "sf-role"),
        "sfTimezone": "UTC",
        "preActions": "ALTER SESSION SET TIMEZONE = 'UTC'",
    }

    logger.info(
        "Snowflake connector opts built (account=%s, db=%s)",
        account, database,
    )
    return opts


def _secret_exists(dbutils, key: str) -> bool:
    """Check if a secret key exists in the scope (safe fallback)."""
    try:
        dbutils.secrets.get(_SECRET_SCOPE, key)
        return True
    except Exception:
        return False
