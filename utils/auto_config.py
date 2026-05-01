"""
utils/auto_config.py
--------------------
Databricks version — auto-configuration from excel_files/etl_output/ folder.
V3 multi-server layout: detects server subdirectories containing 04_transform.py.

Returns a config dict with a 'servers' list — one entry per server.
"""

import os
import re
from typing import Dict, List


# ============================================================================
# PUBLIC API
# ============================================================================

def get_table_config(
    table_name: str,
    base_path: str = None,
    target_mode: str = "snowflake",
) -> Dict:
    """
    Auto-configure pipeline settings for a V3 multi-server table.

    Parameters
    ----------
    table_name : str
        Table folder name (e.g., "public_dim_vehicle_master")
    base_path : str, optional
        Base path to etl_output folder. Auto-detects from project root if None.
    target_mode : str
        Only "snowflake" supported on Databricks.

    Returns
    -------
    dict
        {
            "table_name": str,
            "target_ddl": str,
            "target_query_file": str,
            "target_database": str,
            "target_table": str,
            "primary_keys": list[str],
            "exclude_cols": list[str],
            "servers": [
                {
                    "server_name": str,
                    "transform_file": str,
                },
                ...
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

    # Detect server subdirectories
    server_dirs = _detect_server_dirs(table_folder)
    if not server_dirs:
        raise ValueError(
            f"No V3 server directories found in: {table_folder}\n"
            f"Expected subdirectories containing 03_extract_source.sql"
        )

    # Shared target files
    target_ddl = os.path.join(table_folder, "02_create_target_sf.sql")
    target_query_file = os.path.join(table_folder, "05_extract_target_sf.sql")

    for label, path in [
        ("Target DDL", target_ddl),
        ("Target query", target_query_file),
    ]:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"{label} file not found: {path}")

    # Parse PKs from target DDL
    primary_keys = _parse_primary_keys_from_target_ddl(target_ddl)

    # Parse target table name from DDL
    target_db, target_schema, target_tbl = _parse_ddl_table_name(target_ddl)

    # Build per-server configs
    servers = []
    for server_name in server_dirs:
        server_path = os.path.join(table_folder, server_name)
        transform_file = os.path.join(server_path, "04_transform.py")

        if not os.path.isfile(transform_file):
            raise FileNotFoundError(
                f"Transform file not found for server '{server_name}': {transform_file}"
            )

        servers.append({
            "server_name": server_name,
            "transform_file": transform_file,
        })

    # Build qualified table name: db.schema.table (skip schema if None)
    qualified_parts = [p for p in [target_db, target_schema, target_tbl] if p]
    qualified_table_name = ".".join(qualified_parts) if qualified_parts else target_tbl

    return {
        "table_name": table_name,
        "target_ddl": target_ddl,
        "target_query_file": target_query_file,
        "target_database": target_db,
        "target_schema": target_schema,
        "target_table": target_tbl,
        "qualified_table_name": qualified_table_name,
        "primary_keys": primary_keys,
        "exclude_cols": ["load_ts", "batch_id"],
        "servers": servers,
    }


def list_available_tables(target_mode: str = "snowflake") -> List[str]:
    """List all available table names."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    base_path = os.path.join(project_root, "excel_files", "etl_output")
    return _list_available_tables(base_path)


# ============================================================================
# V3 DETECTION
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

    # Fallback: look for -- PK comments
    pks = []
    for line in content.split("\n"):
        if "-- PK" in line or "--PK" in line:
            m = re.match(r"\s*([A-Za-z0-9_]+)\s+", line)
            if m:
                pks.append(m.group(1).upper())
    return pks


def _parse_ddl_table_name(ddl_file: str) -> tuple:
    """Extract database, schema, and table name from DDL file.
    
    Returns
    -------
    tuple: (database, schema, table) where any component can be None
    """
    with open(ddl_file, "r", encoding="utf-8") as fh:
        content = fh.read()

    # db.schema.table
    match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\.(\w+)\.(\w+)",
        content, re.IGNORECASE,
    )
    if match:
        return match.group(1), match.group(2), match.group(3)

    # db.table (no schema)
    match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\.(\w+)",
        content, re.IGNORECASE,
    )
    if match:
        return match.group(1), None, match.group(2)

    # just table
    match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
        content, re.IGNORECASE,
    )
    if match:
        return None, None, match.group(1)

    raise ValueError(f"Could not parse table name from DDL file: {ddl_file}")


# ============================================================================
# FILTER BUILDER
# ============================================================================

def build_filter_for_query(
    config: dict,
    date_mode: str,
    date_from: str,
    date_from_col: str,
    date_to: str,
    date_to_col: str,
) -> dict:
    """Build WHERE clause filter for date filtering only."""
    from utils.query_filter import build_where_clause, get_columns_from_ddl

    available_cols = []
    ddl_file = config.get("target_ddl")
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


# ============================================================================
# HELPERS
# ============================================================================

def _list_available_tables(base_path: str) -> List[str]:
    if not os.path.isdir(base_path):
        return []
    return [
        d for d in sorted(os.listdir(base_path))
        if os.path.isdir(os.path.join(base_path, d)) and not d.startswith(".")
    ]
