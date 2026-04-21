"""
utils/query_filter.py
---------------------
Builds and injects WHERE clause filters into SQL queries at runtime,
based on PK filter mode and date watermark mode settings from
custom_execution.py.

Two independent filter axes — both may apply simultaneously:

    PK Filter
    ---------
    PK_FILTER_MODE = "full"      → no PK filter
    PK_FILTER_MODE = "pk_range"  → WHERE pk >= LOWER  AND/OR pk <= UPPER
                                   (either bound may be None)
    PK_FILTER_MODE = "pk_set"    → WHERE pk IN (...)

    Date Watermark
    --------------
    DATE_WATERMARK_MODE = "full"  → no date filter
    DATE_WATERMARK_MODE = "range" → WHERE DATE_FROM_COL >= 'DATE_FROM'
                                        AND/OR DATE_TO_COL <= 'DATE_TO'
                                    (either bound / col may be None)

Public API
----------
    from utils.query_filter import build_where_clause, apply_filter_to_sql

    where = build_where_clause(
        pk_filter_mode   = "pk_range",
        pk_col           = "ledger_id",
        pk_range         = {"lower": 1, "upper": 500000},
        pk_set           = None,
        date_mode        = "range",
        date_from        = "2024-01-01",
        date_from_col    = "created_at",
        date_to          = None,
        date_to_col      = None,
        available_cols   = ["ledger_id", "created_at", "updated_at", ...],
    )

    filtered_sql = apply_filter_to_sql(base_sql, where)
"""

from __future__ import annotations

from typing import Optional, Set
from utils.logger import get_logger

logger = get_logger(__name__)

# Valid mode constants
_PK_MODES   = {"full", "pk_range", "pk_set"}
_DATE_MODES = {"full", "range"}


# ── Validation helpers ─────────────────────────────────────────────────────

def _validate_pk_mode(mode: str) -> None:
    if mode not in _PK_MODES:
        raise ValueError(
            f"PK_FILTER_MODE must be one of {sorted(_PK_MODES)}, got: {mode!r}"
        )


def _validate_date_mode(mode: str) -> None:
    if mode not in _DATE_MODES:
        raise ValueError(
            f"DATE_WATERMARK_MODE must be one of {sorted(_DATE_MODES)}, got: {mode!r}"
        )


def _check_col_exists(col: str, available_cols: list[str], context: str) -> None:
    """Raise ValueError if col is not in available_cols (case-insensitive)."""
    lower_available = [c.lower() for c in available_cols]
    if col.lower() not in lower_available:
        raise ValueError(
            f"{context}: column '{col}' not found in table.\n"
            f"  Available columns: {available_cols}\n"
            f"  Check your DATE_FROM_COL / DATE_TO_COL settings in custom_execution.py."
        )


# ── Core builder ───────────────────────────────────────────────────────────

def build_where_clause(
    pk_filter_mode:   str,
    pk_col:           Optional[str],
    pk_range:         Optional[dict],          # {"lower": int|None, "upper": int|None}
    pk_set:           Optional[Set],
    date_mode:        str,
    date_from:        Optional[str],
    date_from_col:    Optional[str],
    date_to:          Optional[str],
    date_to_col:      Optional[str],
    available_cols:   list[str],
) -> str:
    """
    Build a SQL WHERE clause string (without the 'WHERE' keyword) combining
    the PK filter and date watermark conditions.

    Returns an empty string if both modes are 'full' or produce no conditions.

    Parameters
    ----------
    pk_filter_mode : str
        "full" | "pk_range" | "pk_set"
    pk_col : str or None
        Primary key column name. Required for pk_range / pk_set modes.
    pk_range : dict or None
        {"lower": value_or_None, "upper": value_or_None}
        Used when pk_filter_mode == "pk_range".
    pk_set : set or None
        Set of PK values. Used when pk_filter_mode == "pk_set".
    date_mode : str
        "full" | "range"
    date_from : str or None
        Lower date bound. Used when date_mode == "range".
    date_from_col : str or None
        Column for the lower date bound. Required when date_from is set.
    date_to : str or None
        Upper date bound. Used when date_mode == "range".
    date_to_col : str or None
        Column for the upper date bound. Required when date_to is set.
    available_cols : list[str]
        Column names present in the table (for validation).

    Returns
    -------
    str
        WHERE conditions joined by AND, or "" if no conditions apply.
    """
    _validate_pk_mode(pk_filter_mode)
    _validate_date_mode(date_mode)

    conditions: list[str] = []

    # ── PK filter ──────────────────────────────────────────────────────────
    if pk_filter_mode == "pk_range":
        if not pk_col:
            raise ValueError(
                "PK_FILTER_MODE is 'pk_range' but no PK column could be determined. "
                "Ensure PRIMARY_KEYS is set."
            )
        lower = (pk_range or {}).get("lower")
        upper = (pk_range or {}).get("upper")

        if lower is None and upper is None:
            raise ValueError(
                "PK_FILTER_MODE is 'pk_range' but PK_RANGE has both lower and upper as None. "
                "Provide at least one bound."
            )

        if lower is not None and upper is not None:
            conditions.append(f"{pk_col} >= {lower} AND {pk_col} <= {upper}")
            logger.info("PK filter: %s BETWEEN %s AND %s", pk_col, lower, upper)
        elif lower is not None:
            conditions.append(f"{pk_col} >= {lower}")
            logger.info("PK filter: %s >= %s (lower bound only)", pk_col, lower)
        else:
            conditions.append(f"{pk_col} <= {upper}")
            logger.info("PK filter: %s <= %s (upper bound only)", pk_col, upper)

    elif pk_filter_mode == "pk_set":
        if not pk_col:
            raise ValueError(
                "PK_FILTER_MODE is 'pk_set' but no PK column could be determined. "
                "Ensure PRIMARY_KEYS is set."
            )
        if not pk_set:
            raise ValueError(
                "PK_FILTER_MODE is 'pk_set' but PK_SET is empty or None. "
                "Provide a non-empty set of PK values."
            )
        values_csv = ", ".join(str(v) for v in sorted(pk_set))
        conditions.append(f"{pk_col} IN ({values_csv})")
        logger.info("PK filter: %s IN (%d values)", pk_col, len(pk_set))

    # ── Date watermark filter ──────────────────────────────────────────────
    if date_mode == "range":
        if date_from is None and date_to is None:
            raise ValueError(
                "DATE_WATERMARK_MODE is 'range' but both DATE_FROM and DATE_TO are None. "
                "Provide at least one date bound."
            )

        if date_from is not None:
            if not date_from_col:
                raise ValueError(
                    "DATE_FROM is set but DATE_FROM_COL is None. "
                    "Provide the column name to filter on."
                )
            _check_col_exists(date_from_col, available_cols, "DATE_FROM_COL")
            conditions.append(f"{date_from_col} >= '{date_from}'")
            logger.info("Date filter: %s >= '%s'", date_from_col, date_from)

        if date_to is not None:
            if not date_to_col:
                raise ValueError(
                    "DATE_TO is set but DATE_TO_COL is None. "
                    "Provide the column name to filter on."
                )
            _check_col_exists(date_to_col, available_cols, "DATE_TO_COL")
            conditions.append(f"{date_to_col} <= '{date_to}'")
            logger.info("Date filter: %s <= '%s'", date_to_col, date_to)

    return " AND ".join(conditions)


# ── SQL injector ───────────────────────────────────────────────────────────

def apply_filter_to_sql(base_sql: str, where_clause: str) -> str:
    """
    Inject a WHERE clause into a SQL SELECT statement.

    The SQL is expected to have an ORDER BY clause at the end (as generated
    by build_extract_sql / build_extract_target). The WHERE clause is inserted
    between the FROM/JOIN block and the ORDER BY clause.

    If where_clause is empty, the original SQL is returned unchanged.

    Parameters
    ----------
    base_sql : str
        The SQL string read from the .sql file (semicolon may or may not be present).
    where_clause : str
        Conditions string (no WHERE keyword). Empty string = no filter.

    Returns
    -------
    str
        SQL with WHERE clause injected, ready to pass to get_data().
    """
    if not where_clause:
        return base_sql

    sql = base_sql.strip()
    # Strip trailing semicolon — Spark JDBC does not want it
    if sql.endswith(";"):
        sql = sql[:-1].strip()

    import re

    # Insert WHERE before ORDER BY if present, otherwise append before end
    order_match = re.search(r"\bORDER\s+BY\b", sql, re.IGNORECASE)
    if order_match:
        insert_pos = order_match.start()
        injected = sql[:insert_pos].rstrip() + f"\nWHERE {where_clause}\n" + sql[insert_pos:]
    else:
        injected = sql + f"\nWHERE {where_clause}"

    logger.debug("Filter injected into SQL:\n%s", injected)
    return injected


# ── Convenience: extract available columns from a DDL file ─────────────────

def get_columns_from_ddl(ddl_file: str) -> list[str]:
    """
    Parse column names from a generated CREATE TABLE DDL file.
    Used to validate DATE_FROM_COL / DATE_TO_COL before running.

    Parameters
    ----------
    ddl_file : str
        Path to a 01_create_source_table.sql or 02_create_target_*.sql file.

    Returns
    -------
    list[str]
        Column names found in the DDL, in order.
    """
    import re
    from pathlib import Path

    content = Path(ddl_file).read_text(encoding="utf-8")

    # Extract the body between CREATE TABLE ( ... )
    body_match = re.search(r"CREATE\s+TABLE[^(]*\((.+)\)", content, re.DOTALL | re.IGNORECASE)
    if not body_match:
        logger.warning("Could not parse columns from DDL: %s", ddl_file)
        return []

    body = body_match.group(1)
    cols = []
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        # Skip constraints and empty lines
        if not line or line.upper().startswith("PRIMARY") or line.startswith("--"):
            continue
        col_match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s+", line)
        if col_match:
            cols.append(col_match.group(1))

    return cols
