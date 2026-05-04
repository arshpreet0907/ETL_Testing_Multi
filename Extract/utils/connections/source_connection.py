"""
connections/source_connection.py
---------------------------------
Purpose : Build and return JDBC options dict for the source database.
          Supports both MySQL and Microsoft SQL Server (on-prem / SSMS).
          Multi-server: pass server_name matching a key under servers: in
          source_config.yaml (e.g. "mysql_server", "ms_server").
Inputs  : config/source_config.yaml
Outputs : dict of JDBC options ready to pass to spark.read.format("jdbc").options(**opts)
Usage   : from connections.source_connection import get_source_connection
          jdbc_opts = get_source_connection("mysql_server")
          jdbc_opts = get_source_connection("ms_server")

source_config.yaml (multi-server format):
  servers:
    mysql_server:
      db_type  : "mysql"
      host     : localhost
      port     : 3306
      database : schema / database name
      user     : DB username
      password : DB password
      fetchsize: rows per JDBC batch (optional, default 10000)

    ms_server:
      db_type  : "sqlserver"
      host     : localhost          # or localhost\\SQLEXPRESS
      port     : 1433
      database : database name
      user     : SQL Server login
      password : SQL Server password
      fetchsize: rows per JDBC batch
      encrypt              : false
      trustServerCertificate: true
"""

import logging

from utils.config_loader import load_config

logger = logging.getLogger(__name__)

_SOURCE_CONFIG_PATH = "config/source_config.yaml"


def get_source_connection(server_name: str = None) -> dict:
    """
    Read config/source_config.yaml and return a JDBC options dict for the source database.

    Parameters
    ----------
    server_name : str, optional
        Key under ``servers:`` in source_config.yaml.  When provided, reads
        connection details from ``servers.<server_name>``.  When None, falls
        back to top-level flat keys (legacy format).

    Returns
    -------
    dict
        Keys: url, driver, user, password, fetchsize, db_type, schema

    Raises
    ------
    KeyError
        If required config keys are missing.
    ValueError
        If db_type is not supported or server_name not found.
    """
    cfg = load_config(_SOURCE_CONFIG_PATH)

    # Resolve server block
    if server_name and "servers" in cfg:
        servers = cfg["servers"]
        if server_name not in servers:
            raise ValueError(
                f"Server '{server_name}' not found in source_config.yaml. "
                f"Available servers: {list(servers.keys())}"
            )
        server_cfg = servers[server_name]
    elif "servers" in cfg and not server_name:
        # No server_name given but multi-server config — use first server
        first_key = next(iter(cfg["servers"]))
        logger.warning(
            "No server_name specified; defaulting to first server: '%s'", first_key
        )
        server_cfg = cfg["servers"][first_key]
    else:
        # Legacy flat format
        server_cfg = cfg

    required_keys = ("host", "port", "database", "user", "password")
    missing = [k for k in required_keys if k not in server_cfg]
    if missing:
        raise KeyError(
            f"source_config.yaml server '{server_name or 'root'}' is missing "
            f"required key(s): {missing}"
        )

    return _build_jdbc_opts(server_cfg, server_name)


def _build_jdbc_opts(server_cfg: dict, server_name: str = None) -> dict:
    """Build JDBC options dict from a server config block."""
    db_type: str = server_cfg.get("db_type", "mysql").lower().strip()
    host: str = server_cfg["host"]
    port: int = int(server_cfg["port"])
    database: str = server_cfg["database"]
    user: str = server_cfg["user"]
    password: str = server_cfg["password"]
    fetchsize: int = int(server_cfg.get("fetchsize", 10_000))
    schema: str = server_cfg.get("schema")
    if schema and schema.lower() == "none":
        schema = None

    if db_type == "mysql":
        jdbc_url = (
            f"jdbc:mysql://{host}:{port}/{database}"
            "?useSSL=false&allowPublicKeyRetrieval=true"
        )
        driver = "com.mysql.cj.jdbc.Driver"

    elif db_type == "sqlserver":
        encrypt = str(server_cfg.get("encrypt", "false")).lower()
        trust_cert = str(server_cfg.get("trustServerCertificate", "true")).lower()
        jdbc_url = (
            f"jdbc:sqlserver://{host}:{port};"
            f"databaseName={database};"
            f"encrypt={encrypt};"
            f"trustServerCertificate={trust_cert};"
        )
        driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    else:
        raise ValueError(
            f"Unsupported db_type '{db_type}' in source_config.yaml "
            f"(server: {server_name or 'root'}). Valid values: 'mysql', 'sqlserver'"
        )

    label = f"[{server_name}] " if server_name else ""
    logger.info("%sSource JDBC URL: %s (user=%s)", label, jdbc_url, user)


    jdbc_opts ={
        "url": jdbc_url,
        "driver": driver,
        "user": user,
        "password": password,
        "fetchsize": str(fetchsize),
        "db_type": db_type,
    }
    if schema:
        jdbc_opts["schema"]=schema
    return jdbc_opts
