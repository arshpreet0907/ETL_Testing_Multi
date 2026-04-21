"""
utils/auto_config.py
--------------------
Databricks version — auto-configuration from excel_files/etl_output/ folder.
Flattened path (no dummy/ subfolder). Snowflake target only.
"""

import os
import re
import yaml
from typing import Dict, List, Optional


def get_table_config(
    table_name: str,
    base_path: str = None,
    target_mode: str = "snowflake",
) -> Dict:
    """
    Auto-configure pipeline settings based on table name.

    Parameters
    ----------
    table_name : str
        Table folder name (e.g., "cost_ledger", "employee_master")
    base_path : str, optional
        Base path to etl_output folder. Auto-detects from project root if None.
    target_mode : str
        Only "snowflake" supported on Databricks.
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

    # Build file paths
    source_ddl = os.path.join(table_folder, "01_create_source_table.sql")
    source_query_file = os.path.join(table_folder, "03_extract_source.sql")
    transform_file = os.path.join(table_folder, "04_transform.py")

    # Snowflake target files only
    target_ddl = os.path.join(table_folder, "02_create_target_sf.sql")
    target_query_file = os.path.join(table_folder, "05_extract_target_sf.sql")

    # Validate required files exist
    for label, path in [
        ("Source DDL", source_ddl),
        ("Target DDL", target_ddl),
        ("Source query", source_query_file),
        ("Transform", transform_file),
        ("Target query", target_query_file),
    ]:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"{label} file not found: {path}")

    # Extract database and table names from DDL
    source_db, source_tbl = _parse_ddl_table_name(source_ddl)
    target_db, target_tbl = _parse_ddl_table_name(target_ddl)

    # Extract primary keys
    source_primary_keys = _parse_primary_keys(source_ddl)
    target_primary_keys = _parse_primary_keys(target_ddl)

    exclude_cols = ["load_ts"]
    output_dir = os.path.join("output", table_name)

    return {
        "table_name": table_name,
        "source_ddl": source_ddl,
        "target_ddl": target_ddl,
        "source_query_file": source_query_file,
        "target_query_file": target_query_file,
        "transform_file": transform_file,
        "source_database": source_db,
        "source_table": source_tbl,
        "target_database": target_db,
        "target_table": target_tbl,
        "source_primary_keys": source_primary_keys,
        "target_primary_keys": target_primary_keys,
        "primary_keys": target_primary_keys,
        "exclude_cols": exclude_cols,
        "output_dir": output_dir,
    }


def list_available_tables(target_mode: str = "snowflake") -> List[str]:
    """List all available table names."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    base_path = os.path.join(project_root, "excel_files", "etl_output")
    return _list_available_tables(base_path)


def _list_available_tables(base_path: str) -> List[str]:
    if not os.path.isdir(base_path):
        return []
    return [
        d for d in sorted(os.listdir(base_path))
        if os.path.isdir(os.path.join(base_path, d)) and not d.startswith(".")
    ]


def _parse_ddl_table_name(ddl_file: str) -> tuple:
    with open(ddl_file, "r", encoding="utf-8") as fh:
        content = fh.read()

    match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)",
        content, re.IGNORECASE,
    )
    if match:
        return match.group(1), match.group(2)

    match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)",
        content, re.IGNORECASE,
    )
    if match:
        table_name = match.group(1)
        db_match = re.search(r"--\s*(?:Source|Target)\s+table\s*:\s*([a-zA-Z0-9_]+)", content)
        if db_match:
            return None, db_match.group(1)
        return None, table_name

    raise ValueError(f"Could not parse table name from DDL file: {ddl_file}")


def _parse_primary_keys(ddl_file: str) -> List[str]:
    with open(ddl_file, "r", encoding="utf-8") as fh:
        content = fh.read()

    match = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)", content, re.IGNORECASE)
    if match:
        return [col.strip().strip('`"') for col in match.group(1).split(",")]

    pks = []
    for line in content.split("\n"):
        if "-- PK" in line or "--PK" in line:
            m = re.match(r"\s*([a-zA-Z0-9_]+)\s+", line)
            if m:
                pks.append(m.group(1))
    if pks:
        return pks

    raise ValueError(f"Could not parse primary keys from DDL file: {ddl_file}")


def build_filter_for_query(
    query_type: str,
    config: dict,
    pk_filter_mode: str,
    pk_range: dict,
    pk_set: set,
    date_mode: str,
    date_from: str,
    date_from_col: str,
    date_to: str,
    date_to_col: str,
) -> dict:
    """Build WHERE clause filter using correct PK column for source or target query."""
    from utils.query_filter import build_where_clause, get_columns_from_ddl

    if query_type == "source":
        pk_col = config["source_primary_keys"][0] if config.get("source_primary_keys") else None
        ddl_file = config["source_ddl"]
    else:
        pk_col = config["target_primary_keys"][0] if config.get("target_primary_keys") else None
        ddl_file = config["target_ddl"]

    available_cols = get_columns_from_ddl(ddl_file) if ddl_file else []

    where_clause = build_where_clause(
        pk_filter_mode=pk_filter_mode,
        pk_col=pk_col,
        pk_range=pk_range,
        pk_set=pk_set if pk_filter_mode == "pk_set" else None,
        date_mode=date_mode,
        date_from=date_from if date_mode == "range" else None,
        date_from_col=date_from_col if date_mode == "range" else None,
        date_to=date_to if date_mode == "range" else None,
        date_to_col=date_to_col if date_mode == "range" else None,
        available_cols=available_cols,
    )

    parts = []
    if pk_filter_mode != "full":
        parts.append(f"PK={pk_filter_mode}")
    if date_mode != "full":
        parts.append(f"DATE={date_mode}")
    description = ", ".join(parts) if parts else "full load (no filters)"

    return {
        "where_clause": where_clause,
        "pk_mode": pk_filter_mode,
        "date_mode": date_mode,
        "description": description,
    }

