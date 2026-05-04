"""
connections/spark_session.py
-----------------------------
Purpose : Build and return a PySpark SparkSession configured for local ETL runs.
          Handles Windows-specific HADOOP_HOME / JAVA_HOME setup, auto-discovers
          all JARs in the project's jars/ directory, and sets sensible defaults
          for driver memory and logging.

Usage   :
    from connections.spark_session import get_spark_session
    spark = get_spark_session()
    # or with a custom app name:
    spark = get_spark_session(app_name="orders_pipeline")
"""

import glob
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# Path to the project root — go up 3 levels from utils/connections/spark_session.py
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_spark_session(app_name: str = "ETLValidator"):
    """
    Build a local[*] SparkSession ready for JDBC-based ETL validation.

    Environment setup (Windows)
    ---------------------------
    1. JAVA_HOME  — set from pipeline_config.yaml if not already in the environment.
    2. HADOOP_HOME — set from pipeline_config.yaml if not already in the environment.
       Without this, Spark on Windows raises:
       "Could not locate executable null\\bin\\winutils.exe"
    3. PYSPARK_PYTHON — set to current Python executable to avoid "Python worker failed" errors.

    JAR discovery
    -------------
    All *.jar files found in the project's jars/ directory are added to
    spark.jars automatically.  Drop any new JDBC driver into jars/ and it
    will be picked up without changing this file.

    Parameters
    ----------
    app_name : str
        The Spark application name shown in the Spark UI / logs.

    Returns
    -------
    pyspark.sql.SparkSession
        A running local SparkSession.
    """
    _set_env_from_config()
    _set_python_executable()
    jars_csv = _discover_jars()

    from pyspark.sql import SparkSession  # imported here so env vars are set first

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")

        # Driver memory — increased from 2g -> 4g
        # At 1M rows with 20+ columns, the full-outer-join shuffle + cached
        # source/target DataFrames comfortably exceed 2 GB of driver heap.
        # 4 GB keeps the GC overhead low and avoids spill-to-disk during joins.
        # If your machine has < 8 GB RAM available, drop to 3g.
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")  # low for single-machine ETL
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

        # Kryo serializer — faster than Java default
        # Kryo is ~10× faster to serialize/deserialize than Java's default
        # ObjectOutputStream. For local mode this matters during shuffle and
        # cache operations on large DataFrames.
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")

        # JDBC network timeout — prevents silent hangs
        # MySQL defaults can stall the JDBC connection on large reads without
        # raising an exception. These settings ensure Spark fails fast and
        # raises a visible error instead of hanging indefinitely.
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")

        # Python worker hardening — prevents "Python worker failed to connect
        # back" SocketTimeoutException under memory pressure.
        # Reuse workers instead of spawning new ones for each task.
        .config("spark.python.worker.reuse", "true")
        # Increase the connection timeout (default 15s is tight on loaded machines).
        .config("spark.python.worker.timeout", "120")
    )

    if jars_csv:
        builder = builder.config("spark.jars", jars_csv)
        logger.info("Spark JARs loaded: %s", jars_csv)
    else:
        logger.warning(
            "No JARs found in %s — JDBC reads will fail unless JARs are on the classpath.",
            os.path.join(_PROJECT_ROOT, "jars"),
        )

    spark = builder.getOrCreate()

    # Suppress verbose Spark / Hadoop INFO chatter; keep WARN and above
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession started (app=%s, master=local[*])", app_name)

    return spark


# --------------------------------------------------------------------------- #
# Private helpers                                                              #
# --------------------------------------------------------------------------- #

def _set_env_from_config() -> None:
    """
    Read JAVA_HOME and HADOOP_HOME from pipeline_config.yaml and inject them
    into the process environment if they are not already set.

    This must happen BEFORE PySpark is imported so the JVM picks up the values.
    """
    try:
        from utils.config_loader import load_config
        config_path = os.path.join(_PROJECT_ROOT, "config", "pipeline_config.yaml")
        cfg = load_config(config_path)
    except FileNotFoundError:
        logger.debug("pipeline_config.yaml not found — skipping env injection.")
        return
    except Exception as exc:
        logger.warning("Could not load pipeline_config.yaml: %s", exc)
        return

    java_home: Optional[str] = cfg.get("java_home")
    hadoop_home: Optional[str] = cfg.get("hadoop_home")

    if java_home and not os.environ.get("JAVA_HOME"):
        os.environ["JAVA_HOME"] = java_home
        logger.debug("JAVA_HOME set from config: %s", java_home)

    if hadoop_home and not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = hadoop_home
        logger.debug("HADOOP_HOME set from config: %s", hadoop_home)


def _set_python_executable() -> None:
    """
    Set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to the current Python executable.
    This prevents "Python worker failed to connect back" errors on Windows.
    """
    import sys
    python_exe = sys.executable
    
    if not os.environ.get("PYSPARK_PYTHON"):
        os.environ["PYSPARK_PYTHON"] = python_exe
        logger.debug("PYSPARK_PYTHON set to: %s", python_exe)
    
    if not os.environ.get("PYSPARK_DRIVER_PYTHON"):
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
        logger.debug("PYSPARK_DRIVER_PYTHON set to: %s", python_exe)


def _discover_jars() -> str:
    """
    Find all *.jar files in the project's jars/ directory and return them
    as a comma-separated string suitable for spark.jars.

    Returns an empty string if the directory does not exist or contains no JARs.
    """
    jars_dir = os.path.join(_PROJECT_ROOT, "jars")
    if not os.path.isdir(jars_dir):
        return ""

    jar_paths = []
    for p in glob.glob(os.path.join(jars_dir, "*.jar")):
        # Convert to absolute path and then to file:// URI for Windows compatibility
        abs_path = os.path.abspath(p)
        # On Windows, convert C:\path to file:///C:/path
        if os.name == 'nt':
            # Replace backslashes with forward slashes and add file:/// prefix
            file_uri = "file:///" + abs_path.replace("\\", "/")
        else:
            # On Unix, just add file:// prefix
            file_uri = "file://" + abs_path
        jar_paths.append(file_uri)

    return ",".join(jar_paths)
