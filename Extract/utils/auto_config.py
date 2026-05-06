"""
auto_config.py
--------------
Auto-configuration utility that extracts source pipeline settings from
the etl_output folder structure based on table name.

Supports two folder layouts:
  V3 (multi-server):  <table>/<server>/03_extract_source.sql  +  source_tables/
  Legacy (single):    <table>/01_create_source_table.sql  +  03_extract_source.sql

Usage:
    from Extract.utils.auto_config import get_table_config, list_available_tables

    config = get_table_config("public_dim_vehicle_master")
"""

import glob
import os
import re
from typing import Dict, List, Optional

import yaml


# ============================================================================
# PUBLIC API
# ============================================================================

def get_table_config(
    table_name: str,
    base_path: str = None,
    **kwargs,
) -> Dict:
    """
    Auto-configure source pipeline settings based on table name.

    Auto-detects V3 (multi-server) vs legacy (flat) folder structure.

    Parameters
    ----------
    table_name : str
        Table folder name (e.g., "public_dim_vehicle_master")
    base_path : str, optional
        Base path to etl_output folder. If None, auto-detects from project root.

    Returns
    -------
    dict
        V3 layout returns::

            {
                "table_name": str,
                "target_ddl": str,
                "target_query_file": str,
                "primary_keys": list[str],
                "servers": [
                    {
                        "server_name": str,
                        "source_query_file": str,
                        "transform_file": str,
                        "source_ddls": list[str],
                        "output_dir": str,
                    },
                ],
            }
    """
    if base_path is None:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        base_path = os.path.join(project_root, "excel_files", "etl_output")

    table_folder = os.path.join(base_path, table_name)

    if not os.path.isdir(table_folder):
        raise ValueError(
            f"Table folder not found: {table_folder}\n"
            f"Available tables: {', '.join(_list_available_tables(base_path))}"
        )

    # Detect layout: V3 has server subdirectories with 03_extract_source.sql
    server_dirs = _detect_server_dirs(table_folder)

    if server_dirs:
        return _build_v3_config(table_name, table_folder, server_dirs)
    else:
        return _build_legacy_config(table_name, table_folder)


def list_available_tables(base_path: str = None) -> List[str]:
    """List all available table names in etl_output folder."""
    if base_path is None:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        base_path = os.path.join(project_root, "excel_files", "etl_output")
    return _list_available_tables(base_path)


# ============================================================================
# V3 MULTI-SERVER CONFIG
# ============================================================================

def _detect_server_dirs(table_folder: str) -> List[str]:
    """
    Find subdirectories that contain 03_extract_source.sql (V3 server folders).
    Returns list of server directory names, or empty list if none found.
    """
    servers = []
    for entry in sorted(os.listdir(table_folder)):
        entry_path = os.path.join(table_folder, entry)
        if not os.path.isdir(entry_path):
            continue
        extract_sql = os.path.join(entry_path, "03_extract_source.sql")
        if os.path.isfile(extract_sql):
            servers.append(entry)
    return servers


def _build_v3_config(table_name: str, table_folder: str, server_dirs: List[str]) -> Dict:
    """Build configuration dict for V3 multi-server layout."""

    target_ddl = os.path.join(table_folder, "02_create_target_sf.sql")
    target_query = os.path.join(table_folder, "05_extract_target_sf.sql")

    if not os.path.isfile(target_ddl):
        raise FileNotFoundError(f"Target DDL not found: {target_ddl}")
    if not os.path.isfile(target_query):
        raise FileNotFoundError(f"Target query not found: {target_query}")

    primary_keys = _parse_primary_keys_from_target_ddl(target_ddl)

    servers = []
    for server_name in server_dirs:
        server_path = os.path.join(table_folder, server_name)
        source_query_file = os.path.join(server_path, "03_extract_source.sql")
        transform_file = os.path.join(server_path, "04_transform.py")

        source_tables_dir = os.path.join(server_path, "source_tables")
        source_ddls = sorted(glob.glob(
            os.path.join(source_tables_dir, "01_src_*_ddl.sql")
        )) if os.path.isdir(source_tables_dir) else []

        output_dir = os.path.join("output", table_name, server_name)

        servers.append({
            "server_name": server_name,
            "source_query_file": source_query_file,
            "transform_file": transform_file,
            "source_ddls": source_ddls,
            "output_dir": output_dir,
        })

    return {
        "table_name": table_name,
        "target_ddl": target_ddl,
        "target_query_file": target_query,
        "primary_keys": primary_keys,
        "exclude_cols": ["load_ts", "batch_id"],
        "servers": servers,
    }


# ============================================================================
# LEGACY SINGLE-TABLE CONFIG (backward compatible)
# ============================================================================

def _build_legacy_config(table_name: str, table_folder: str) -> Dict:
    """Build configuration dict for legacy flat layout (no server subdirs)."""

    source_ddl = os.path.join(table_folder, "01_create_source_table.sql")
    source_query_file = os.path.join(table_folder, "03_extract_source.sql")

    for label, path in [("Source DDL", source_ddl), ("Source query", source_query_file)]:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"{label} file not found: {path}")

    source_db, source_tbl = _parse_ddl_table_name(source_ddl)
    if source_db is None:
        source_db = _get_database_from_config()

    source_primary_keys = _parse_primary_keys(source_ddl)
    output_dir = os.path.join("output", table_name)

    return {
        "table_name": table_name,
        "source_ddl": source_ddl,
        "source_query_file": source_query_file,
        "source_database": source_db,
        "source_table": source_tbl,
        "source_primary_keys": source_primary_keys,
        "primary_keys": source_primary_keys,
        "exclude_cols": ["load_ts"],
        "output_dir": output_dir,
    }


# ============================================================================
# DDL PARSERS
# ============================================================================

def _parse_primary_keys_from_target_ddl(ddl_file: str) -> List[str]:
    """Extract primary key column names from a Snowflake target DDL file."""
    with open(ddl_file, "r", encoding="utf-8") as fh:
        content = fh.read()

    match = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)", content, re.IGNORECASE)
    if match:
        pk_str = match.group(1)
        return [col.strip().strip('`"').upper() for col in pk_str.split(",")]

    pks = []
    for line in content.split("\n"):
        if "-- PK" in line or "--PK" in line:
            m = re.match(r"\s*([A-Za-z0-9_]+)\s+", line)
            if m:
                pks.append(m.group(1).upper())
    return pks


def _parse_ddl_table_name(ddl_file: str) -> tuple:
    """Extract database and table name from DDL file."""
    with open(ddl_file, "r", encoding="utf-8") as fh:
        content = fh.read()

    # Try three-part name first (database.schema.table for SQL Server)
    match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)",
        content, re.IGNORECASE
    )
    if match:
        # Return (database, table) - skip schema in the middle
        return match.group(1), match.group(3)

    # Try two-part name (database.table for MySQL)
    match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)",
        content, re.IGNORECASE
    )
    if match:
        return match.group(1), match.group(2)

    # Try single table name
    match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)",
        content, re.IGNORECASE
    )
    if match:
        return None, match.group(1)

    raise ValueError(f"Could not parse table name from DDL file: {ddl_file}")


def parse_source_ddl_table_info(ddl_file: str) -> tuple:
    """
    Parse database and table name from a partial source DDL file.

    Returns (database, table_name).
    For SQL Server three-part names (db.schema.table), returns (db, table).
    """
    with open(ddl_file, "r", encoding="utf-8") as fh:
        content = fh.read()

    # Try three-part name first (database.schema.table for SQL Server)
    match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)",
        content, re.IGNORECASE
    )
    if match:
        # Return (database, table) - skip schema in the middle
        return match.group(1), match.group(3)

    # Try two-part name (database.table for MySQL)
    match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)",
        content, re.IGNORECASE
    )
    if match:
        return match.group(1), match.group(2)

    # Try comment header with three-part name
    match = re.search(
        r"--\s*Partial\s+source\s+DDL:\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)",
        content, re.IGNORECASE
    )
    if match:
        return match.group(1), match.group(3)

    # Try comment header with two-part name
    match = re.search(
        r"--\s*Partial\s+source\s+DDL:\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)",
        content, re.IGNORECASE
    )
    if match:
        return match.group(1), match.group(2)

    raise ValueError(f"Could not parse table info from source DDL: {ddl_file}")


def _parse_primary_keys(ddl_file: str) -> List[str]:
    """Extract primary key column names from DDL file."""
    with open(ddl_file, "r", encoding="utf-8") as fh:
        content = fh.read()

    match = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)", content, re.IGNORECASE)
    if match:
        pk_str = match.group(1)
        return [col.strip().strip("`\"") for col in pk_str.split(",")]

    pks = []
    for line in content.split("\n"):
        if "-- PK" in line or "--PK" in line:
            m = re.match(r"\s*([a-zA-Z0-9_]+)\s+", line)
            if m:
                pks.append(m.group(1))
    if pks:
        return pks

    raise ValueError(f"Could not parse primary keys from DDL file: {ddl_file}")


# ============================================================================
# HELPERS
# ============================================================================

def _list_available_tables(base_path: str) -> List[str]:
    """List subdirectories in base_path."""
    if not os.path.isdir(base_path):
        return []
    return [
        d for d in sorted(os.listdir(base_path))
        if os.path.isdir(os.path.join(base_path, d)) and not d.startswith(".")
    ]


def _get_database_from_config() -> Optional[str]:
    """Read source database name from config YAML."""
    try:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_file = os.path.join(project_root, "config", "source_config.yaml")
        if os.path.isfile(config_file):
            with open(config_file, "r", encoding="utf-8") as fh:
                config = yaml.safe_load(fh)
                return config.get("database")
    except Exception:
        pass
    return None


def build_filter_for_query(
    query_type: str,
    config: dict,
    date_mode: str,
    date_from: str,
    date_from_col: str,
    date_to: str,
    date_to_col: str,
) -> dict:
    """Build WHERE clause filter for date filtering only."""
    from Extract.utils.query_filter import build_where_clause, get_columns_from_ddl

    available_cols = []
    ddl_file = config.get("source_ddl") or config.get("target_ddl")
    if ddl_file:
        available_cols = get_columns_from_ddl(ddl_file)

    where_clause = build_where_clause(
        date_mode=date_mode,
        date_from=date_from if date_mode == "range" else None,
        date_from_col=date_from_col if date_mode == "range" else None,
        date_to=date_to if date_mode == "range" else None,
        date_to_col=date_to_col if date_mode == "range" else None,
        available_cols=available_cols,
    )

    description = f"DATE={date_mode}" if date_mode != "full" else "full load (no filters)"

    return {
        "where_clause": where_clause,
        "date_mode": date_mode,
        "description": description,
    }
