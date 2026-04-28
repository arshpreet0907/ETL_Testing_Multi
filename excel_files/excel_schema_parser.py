"""
excel_schema_parser_v2.py
-------------------------
Multi-source ETL parser (v2) — reads Excel mapping specs following the
standard syntax defined in docs/excel_syntax_guide.md v2.

Produces a single JSON output with:
  - target_db / target_table (from filename)
  - unified target schema
  - per-server: source_tables, column_mappings, joins, global_transforms
  - cross-sheet consistency_errors

NOT backward compatible with v1.

Usage:
    python excel_schema_parser_v2.py <excel_file> [--output out.json] [--pretty]
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field, asdict
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

import openpyxl

# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

FILENAME_RE = re.compile(
    r'^(?P<target_db>[^.]+)\.(?P<target_schema>[^.]+)\.(?P<target_table>.+)\.xlsx$',
    re.IGNORECASE,
)

JOINS_SHEET = "_joins"

# Sheets to always skip (case-insensitive prefix/glob)
SKIP_SHEETS = {
    "_joins", "_parameters", "versionhistory", "introduction", "loading steps",
    "sql-script", "load-ref_category_type",
}
SKIP_PREFIXES = ("retired", "data prof")

# Required target headers (normalised lowercase)
REQUIRED_TGT_HEADERS = {"schema", "table", "column", "data type", "allow nulls", "transformation"}
# Required source headers (normalised lowercase)
REQUIRED_SRC_HEADERS = {"database", "data type"}
# Source headers with aliases
SRC_TABLE_ALIASES = {"table name", "table"}
SRC_FIELD_ALIASES = {"field name", "field"}

# Global transforms marker
GT_MARKER = "_global_transforms"
GT_HEADERS_EXPECTED = {"order", "operation", "parameters", "condition", "notes"}

# NULL-handling patterns in Notes column
NULL_PATTERNS = [
    (re.compile(r'DEFAULT\s+(\S+)\s+IF\s+NULL', re.I), r'fill:\1'),
    (re.compile(r'UNSPECIFIED\s+IF\s+NULL', re.I), 'fill:UNSPECIFIED'),
    (re.compile(r"DEFAULT\s+'([^']+)'", re.I), r'fill:\1'),
    (re.compile(r'DEFAULT\s+(\S+)', re.I), r'fill:\1'),
]

# Chained replace pattern
REPLACE_PATTERN = re.compile(
    r'replace\s*\(\s*"([^"]*)"\s*,\s*"([^"]*)"\s*\)', re.IGNORECASE,
)

# Joins required headers
JOINS_REQUIRED = {
    "sheet_name", "left_table", "join_alias", "join_table",
    "join_type", "left_key", "right_key", "fetch_columns",
}

# D13: Validate that Field Desc matches target Column (case-insensitive).
# Set to False to suppress these warnings.
VALIDATE_FIELD_DESC = False


# ═══════════════════════════════════════════════════════════════════════════
# Data Models
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TargetColumnDef:
    """A single column in the target table."""
    name: str
    dtype: str
    nullable: bool
    is_pk: bool
    pk_marker: str       # e.g. "PK", "SK,PK", "BK", ""
    default_val: str
    null_handling: str   # e.g. "fill:0", "fill:UNSPECIFIED", ""


@dataclass
class SourceTableRef:
    """A distinct source table referenced in a server sheet."""
    database: str
    schema: str
    table: str
    columns: list[dict] = field(default_factory=list)   # [{name, dtype, nullable, is_bk}]


@dataclass
class ColumnMapping:
    """One row mapping: source field → target column with transform."""
    target_column: str
    target_dtype: str
    target_nullable: bool
    target_pk_marker: str
    source_database: str
    source_schema: str
    source_table: str
    source_field: str
    source_dtype: str
    source_desc: str
    transform: str          # raw Transformation cell
    null_handling: str      # parsed from Notes
    notes: str


@dataclass
class JoinDef:
    """A join definition from the _joins sheet."""
    sheet_name: str
    left_table: str
    join_alias: str
    join_table: str
    join_db: str
    join_schema: str
    join_type: str          # LEFT / INNER / RIGHT / FULL
    key_pairs: list[tuple[str, str]] = field(default_factory=list)
    fetch_columns: list[str] = field(default_factory=list)


@dataclass
class GlobalTransform:
    """One global transform rule."""
    order: int
    operation: str
    parameters: str
    condition: str
    notes: str


@dataclass
class ServerData:
    """All parsed data for one server sheet."""
    sheet_name: str
    source_tables: dict[str, SourceTableRef] = field(default_factory=dict)
    column_mappings: list[ColumnMapping] = field(default_factory=list)
    joins: list[JoinDef] = field(default_factory=list)
    global_transforms: list[GlobalTransform] = field(default_factory=list)
    target_columns: list[TargetColumnDef] = field(default_factory=list)


@dataclass
class ParseResult:
    """Complete parser output."""
    file_name: str
    target_db: str
    target_table: str
    target_schema: dict = field(default_factory=dict)
    servers: dict[str, dict] = field(default_factory=dict)
    consistency_errors: list[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════
# Helper Utilities
# ═══════════════════════════════════════════════════════════════════════════

def _clean(val: Any) -> str:
    """Return cleaned string from cell value, or empty string."""
    if val is None:
        return ""
    s = str(val).strip()
    # Remove non-breaking spaces
    s = s.replace('\xa0', ' ').strip()
    return s


def _norm(val: Any) -> str:
    """Normalised lowercase version of a cell value."""
    return _clean(val).lower()


def _bool_nullable(val: str) -> bool:
    """Parse Allow Nulls field → True if nullable."""
    v = val.strip().upper()
    return v in ("YES", "Y", "TRUE", "1")


def _parse_null_handling(notes: str) -> str:
    """Extract null-handling directive from Notes column."""
    if not notes:
        return ""
    for pattern, replacement in NULL_PATTERNS:
        m = pattern.search(notes)
        if m:
            if isinstance(replacement, str) and '\\' in replacement:
                return pattern.sub(replacement, m.group(0))
            return replacement if not m.groups() else f"fill:{m.group(1)}"
    return ""


def _source_table_key(db: str, schema: str, table: str) -> str:
    """Build a unique key for a source table."""
    parts = [p for p in [db, schema, table] if p]
    return ".".join(parts).upper()


# ═══════════════════════════════════════════════════════════════════════════
# Sheet Discovery
# ═══════════════════════════════════════════════════════════════════════════

def _should_skip_sheet(name: str) -> bool:
    """Check if a sheet should be skipped based on name."""
    nl = name.lower().strip()
    if nl in SKIP_SHEETS:
        return True
    for prefix in SKIP_PREFIXES:
        if nl.startswith(prefix):
            return True
    return False


def _is_server_sheet(ws) -> bool:
    """Check if worksheet has a recognisable header row in rows 1–14.
    Supports two formats:
      - v2 format: headers contain 'Schema' + 'Column'
      - project format: headers contain 'src_db' or 'src_col_name'
    """
    for row_idx in range(1, min(15, ws.max_row + 1)):
        cells = []
        for col_idx in range(1, min(ws.max_column + 1, 25)):
            v = _norm(ws.cell(row_idx, col_idx).value)
            cells.append(v)
        # v2 format
        if "schema" in cells and "column" in cells:
            return True
        # project format (src_db, src_col_name, tgt_col_name)
        if "src_db" in cells and "tgt_col_name" in cells:
            return True
    return False


def discover_sheets(wb) -> tuple[list[str], bool]:
    """
    Return (server_sheet_names, has_joins).

    Skips known non-data sheets and validates that remaining sheets
    have a recognisable header structure.
    """
    server_sheets = []
    has_joins = JOINS_SHEET in [s.lower() for s in wb.sheetnames]

    for name in wb.sheetnames:
        if _should_skip_sheet(name):
            continue
        ws = wb[name]
        if _is_server_sheet(ws):
            server_sheets.append(name)

    return server_sheets, has_joins


# ═══════════════════════════════════════════════════════════════════════════
# Header Detection
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class HeaderMap:
    """Maps header names to column indices for one sheet."""
    header_row: int
    separator_col: int
    tgt_headers: dict[str, int]   # normalised name → col index
    src_headers: dict[str, int]   # normalised name → col index


def detect_headers(ws) -> HeaderMap:
    """
    Scan rows 1–14 for the header row.
    Supports two formats:
      - v2 format: 'Schema' + 'Column' with separator column splitting target/source sides
      - project format: flat headers like 'src_db', 'tgt_col_name' (no separator)
    """
    header_row = -1
    is_project_format = False
    max_col = min(ws.max_column or 1, 30)

    # Find header row
    for row_idx in range(1, min(15, (ws.max_row or 1) + 1)):
        row_vals = [_norm(ws.cell(row_idx, c).value) for c in range(1, max_col + 1)]
        if "schema" in row_vals and "column" in row_vals:
            header_row = row_idx
            break
        if "src_db" in row_vals and "tgt_col_name" in row_vals:
            header_row = row_idx
            is_project_format = True
            break

    if header_row < 0:
        raise ValueError(f"Sheet '{ws.title}': cannot find header row")

    if is_project_format:
        # Project format: flat headers, no separator column
        # Map src_* headers as source-side, tgt_* and transform_* as target-side
        tgt_headers: dict[str, int] = {}
        src_headers: dict[str, int] = {}
        for col_idx in range(1, max_col + 1):
            name = _norm(ws.cell(header_row, col_idx).value)
            if not name:
                continue
            # Route headers to appropriate side based on prefix
            if name.startswith("src_"):
                src_headers[name] = col_idx
                # Also map without prefix for compatibility (e.g. "database" → src_db)
                short = name[4:]  # strip "src_"
                if short == "db":
                    src_headers["database"] = col_idx
                elif short == "col_name":
                    src_headers["field name"] = col_idx
                    src_headers["field"] = col_idx
                elif short == "table":
                    src_headers["table name"] = col_idx
                    src_headers["table"] = col_idx
                elif short == "schema":
                    src_headers["schema"] = col_idx
                elif short == "dtype":
                    src_headers["data type"] = col_idx
                elif short == "is_pk":
                    src_headers["business key (y/n)"] = col_idx
                else:
                    src_headers[short] = col_idx
            elif name.startswith("tgt_"):
                tgt_headers[name] = col_idx
                short = name[4:]
                if short == "col_name":
                    tgt_headers["column"] = col_idx
                elif short == "dtype":
                    tgt_headers["data type"] = col_idx
                elif short == "nullable":
                    tgt_headers["allow nulls"] = col_idx
                elif short == "is_pk":
                    tgt_headers["bk/sk/pk"] = col_idx
                elif short == "default_val":
                    tgt_headers["default_val"] = col_idx
                else:
                    tgt_headers[short] = col_idx
            elif name == "transform_type":
                tgt_headers["transformation"] = col_idx
                tgt_headers["transform_type"] = col_idx
            elif name == "transform_rule":
                tgt_headers["transform_rule"] = col_idx
            elif name == "null_handling":
                tgt_headers["null_handling"] = col_idx
            elif name == "notes":
                tgt_headers["notes"] = col_idx
            elif name == "force_dtype":
                tgt_headers["force_dtype"] = col_idx

        return HeaderMap(
            header_row=header_row,
            separator_col=-1,
            tgt_headers=tgt_headers,
            src_headers=src_headers,
        )

    # v2 format: find separator column
    row_vals = [_clean(ws.cell(header_row, c).value) for c in range(1, max_col + 1)]
    separator_col = -1
    for i in range(1, len(row_vals) - 1):
        if not row_vals[i] and row_vals[i - 1] and any(row_vals[j] for j in range(i + 1, len(row_vals))):
            separator_col = i + 1  # 1-based
            break

    if separator_col < 0:
        separator_col = max_col + 1

    tgt_headers = {}
    src_headers = {}
    for col_idx in range(1, max_col + 1):
        name = _norm(ws.cell(header_row, col_idx).value)
        if not name:
            continue
        if col_idx < separator_col:
            tgt_headers[name] = col_idx
        elif col_idx > separator_col:
            src_headers[name] = col_idx

    return HeaderMap(
        header_row=header_row,
        separator_col=separator_col,
        tgt_headers=tgt_headers,
        src_headers=src_headers,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Data Row Parsing
# ═══════════════════════════════════════════════════════════════════════════

def _get_tgt_col(hdr: HeaderMap, ws, row: int, name: str) -> str:
    """Read a target-side cell by header name."""
    col_idx = hdr.tgt_headers.get(name)
    if col_idx is None:
        return ""
    return _clean(ws.cell(row, col_idx).value)


def _get_src_col(hdr: HeaderMap, ws, row: int, name: str) -> str:
    """Read a source-side cell by header name, trying aliases."""
    col_idx = hdr.src_headers.get(name)
    if col_idx is None:
        return ""
    return _clean(ws.cell(row, col_idx).value)


def _resolve_src_header(hdr: HeaderMap, primary: str, aliases: set[str]) -> str:
    """Find the actual header name from a set of aliases."""
    nl = primary.lower()
    if nl in hdr.src_headers:
        return nl
    for alias in aliases:
        if alias.lower() in hdr.src_headers:
            return alias.lower()
    return nl


def parse_data_rows(ws, hdr: HeaderMap) -> tuple[list[ColumnMapping], list[TargetColumnDef], dict[str, SourceTableRef], int, list[str]]:
    """
    Parse all data rows from the header row to the global transforms marker or end.

    Returns:
        (column_mappings, target_columns, source_tables, last_data_row, field_desc_warnings)
    """
    mappings: list[ColumnMapping] = []
    target_cols: list[TargetColumnDef] = []
    source_tables: dict[str, SourceTableRef] = {}
    seen_tgt_cols: set[str] = set()
    field_desc_warnings: list[str] = []

    # Resolve source header aliases
    src_table_key = _resolve_src_header(hdr, "table name", SRC_TABLE_ALIASES)
    src_field_key = _resolve_src_header(hdr, "field name", SRC_FIELD_ALIASES)

    last_data_row = hdr.header_row

    for row_idx in range(hdr.header_row + 1, (ws.max_row or hdr.header_row) + 1):
        # Check for global transforms marker
        cell_a = _clean(ws.cell(row_idx, 1).value)
        if cell_a.lower() == GT_MARKER:
            break

        # Check if entire row is blank
        all_blank = True
        for col_idx in range(1, min((ws.max_column or 1) + 1, 25)):
            if _clean(ws.cell(row_idx, col_idx).value):
                all_blank = False
                break
        if all_blank:
            continue

        last_data_row = row_idx

        # Read target side
        tgt_schema = _get_tgt_col(hdr, ws, row_idx, "schema")
        tgt_table = _get_tgt_col(hdr, ws, row_idx, "table")
        tgt_col = _get_tgt_col(hdr, ws, row_idx, "column")
        tgt_dtype = _get_tgt_col(hdr, ws, row_idx, "data type")
        tgt_pk = _get_tgt_col(hdr, ws, row_idx, "bk/sk/pk")
        tgt_nulls = _get_tgt_col(hdr, ws, row_idx, "allow nulls")
        tgt_transform = _get_tgt_col(hdr, ws, row_idx, "transformation") or _get_tgt_col(hdr, ws, row_idx, "transform_type")
        tgt_notes = _get_tgt_col(hdr, ws, row_idx, "notes")

        if not tgt_col:
            continue

        # Read source side
        src_db = _get_src_col(hdr, ws, row_idx, "database")
        src_schema = _get_src_col(hdr, ws, row_idx, "schema")
        src_table = _get_src_col(hdr, ws, row_idx, src_table_key)
        src_field = _get_src_col(hdr, ws, row_idx, src_field_key)
        src_desc = _get_src_col(hdr, ws, row_idx, "field desc")
        # D13: Field Desc should match target Column (case-insensitive)
        if VALIDATE_FIELD_DESC and src_desc and tgt_col and src_desc.strip().upper() != tgt_col.strip().upper():
            warn = (f"Row {row_idx}: Field Desc '{src_desc}' does not match "
                    f"target Column '{tgt_col}' (case-insensitive)")
            field_desc_warnings.append(warn)
            print(f"  WARNING [D13]: {warn}")
        src_dtype = _get_src_col(hdr, ws, row_idx, "data type")
        src_bk = _get_src_col(hdr, ws, row_idx, "business key (y/n)")

        # Parse null handling from Notes
        null_handling = _parse_null_handling(tgt_notes)

        # Determine PK
        is_pk = bool(tgt_pk and "PK" in tgt_pk.upper())
        nullable = _bool_nullable(tgt_nulls) if tgt_nulls else True

        # Build target column def (deduplicated)
        tgt_col_upper = tgt_col.upper()
        if tgt_col_upper not in seen_tgt_cols:
            seen_tgt_cols.add(tgt_col_upper)
            target_cols.append(TargetColumnDef(
                name=tgt_col_upper,
                dtype=tgt_dtype.upper() if tgt_dtype else "",
                nullable=nullable,
                is_pk=is_pk,
                pk_marker=tgt_pk,
                default_val="",
                null_handling=null_handling,
            ))

        # Build source table ref
        if src_db and src_table:
            st_key = _source_table_key(src_db, src_schema, src_table)
            if st_key not in source_tables:
                source_tables[st_key] = SourceTableRef(
                    database=src_db, schema=src_schema, table=src_table, columns=[],
                )
            # Add column to source table if not already there
            existing_cols = {c["name"].upper() for c in source_tables[st_key].columns}
            if src_field and src_field.upper() not in existing_cols:
                source_tables[st_key].columns.append({
                    "name": src_field,
                    "dtype": src_dtype,
                    "nullable": _bool_nullable(tgt_nulls) if tgt_nulls else True,
                    "is_bk": src_bk.upper() in ("Y", "YES") if src_bk else False,
                })

        # Build mapping
        mappings.append(ColumnMapping(
            target_column=tgt_col_upper,
            target_dtype=tgt_dtype.upper() if tgt_dtype else "",
            target_nullable=nullable,
            target_pk_marker=tgt_pk,
            source_database=src_db,
            source_schema=src_schema,
            source_table=src_table,
            source_field=src_field,
            source_dtype=src_dtype,
            source_desc=src_desc,
            transform=tgt_transform,
            null_handling=null_handling,
            notes=tgt_notes,
        ))

    return mappings, target_cols, source_tables, last_data_row, field_desc_warnings


# ═══════════════════════════════════════════════════════════════════════════
# Global Transforms Parsing
# ═══════════════════════════════════════════════════════════════════════════

def parse_global_transforms(ws, hdr: HeaderMap) -> list[GlobalTransform]:
    """
    Find the _global_transforms marker row and parse the transform entries below it.
    """
    marker_row = -1
    for row_idx in range(hdr.header_row + 1, (ws.max_row or hdr.header_row) + 1):
        cell_a = _clean(ws.cell(row_idx, 1).value)
        if cell_a.lower() == GT_MARKER:
            marker_row = row_idx
            break

    if marker_row < 0:
        return []

    # Find GT headers in the row after marker
    gt_hdr_row = marker_row + 1
    if gt_hdr_row > (ws.max_row or 0):
        return []

    gt_hdr_map: dict[str, int] = {}
    for col_idx in range(1, min((ws.max_column or 1) + 1, 15)):
        val = _norm(ws.cell(gt_hdr_row, col_idx).value)
        if val in GT_HEADERS_EXPECTED:
            gt_hdr_map[val] = col_idx

    if "order" not in gt_hdr_map or "operation" not in gt_hdr_map:
        return []

    transforms: list[GlobalTransform] = []
    for row_idx in range(gt_hdr_row + 1, (ws.max_row or gt_hdr_row) + 1):
        order_val = _clean(ws.cell(row_idx, gt_hdr_map["order"]).value)
        op_val = _clean(ws.cell(row_idx, gt_hdr_map["operation"]).value)

        if not order_val and not op_val:
            break

        try:
            order_int = int(order_val)
        except (ValueError, TypeError):
            order_int = len(transforms) + 1

        params = _clean(ws.cell(row_idx, gt_hdr_map.get("parameters", 0)).value) if "parameters" in gt_hdr_map else ""
        condition = _clean(ws.cell(row_idx, gt_hdr_map.get("condition", 0)).value) if "condition" in gt_hdr_map else ""
        notes = _clean(ws.cell(row_idx, gt_hdr_map.get("notes", 0)).value) if "notes" in gt_hdr_map else ""

        transforms.append(GlobalTransform(
            order=order_int,
            operation=op_val.upper(),
            parameters=params,
            condition=condition,
            notes=notes,
        ))

    return transforms


# ═══════════════════════════════════════════════════════════════════════════
# _joins Sheet Parsing
# ═══════════════════════════════════════════════════════════════════════════

def parse_joins_sheet(ws) -> dict[str, list[JoinDef]]:
    """
    Parse the _joins sheet into JoinDef objects grouped by sheet_name.
    Supports composite keys (multiple rows with same alias are merged).
    """
    # Read header row
    header_map: dict[str, int] = {}
    for col_idx in range(1, (ws.max_column or 1) + 1):
        val = _norm(ws.cell(1, col_idx).value)
        if val:
            header_map[val] = col_idx

    missing = JOINS_REQUIRED - set(header_map.keys())
    if missing:
        print(f"  WARNING: _joins sheet missing columns: {missing}")

    # Temporary storage for composite key grouping
    # key = (sheet_name, join_alias, join_table)
    grouped: dict[tuple, JoinDef] = {}
    order: list[tuple] = []

    for row_idx in range(2, (ws.max_row or 1) + 1):
        def _get(name: str) -> str:
            idx = header_map.get(name, 0)
            return _clean(ws.cell(row_idx, idx).value) if idx else ""

        sheet_name = _get("sheet_name")
        if not sheet_name:
            continue

        left_table = _get("left_table")
        join_alias = _get("join_alias")
        join_table = _get("join_table")
        join_db = _get("join_db")
        join_schema = _get("join_schema")
        join_type = _get("join_type").upper() or "LEFT"
        left_key = _get("left_key")
        right_key = _get("right_key")
        fetch_raw = _get("fetch_columns")

        if not all([join_alias, join_table, left_key, right_key]):
            print(f"  WARNING: _joins row {row_idx} incomplete, skipping.")
            continue

        gkey = (sheet_name, join_alias, join_table)

        if gkey in grouped:
            # Composite key — add key pair
            grouped[gkey].key_pairs.append((left_key, right_key))
        else:
            fetch_cols = [c.strip() for c in fetch_raw.split(",") if c.strip()]
            jd = JoinDef(
                sheet_name=sheet_name,
                left_table=left_table,
                join_alias=join_alias,
                join_table=join_table,
                join_db=join_db,
                join_schema=join_schema,
                join_type=join_type,
                key_pairs=[(left_key, right_key)],
                fetch_columns=fetch_cols,
            )
            grouped[gkey] = jd
            order.append(gkey)

    # Group by sheet_name
    result: dict[str, list[JoinDef]] = {}
    for gkey in order:
        jd = grouped[gkey]
        result.setdefault(jd.sheet_name, []).append(jd)

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Cross-Sheet Validation
# ═══════════════════════════════════════════════════════════════════════════

def validate_cross_sheet(servers: dict[str, ServerData]) -> list[str]:
    """
    Validate that all server sheets produce the same target table structure.

    Checks:
      - Same set of target column names (X1)
      - Same data types per column (X2)
      - Same PK markers per column (X3)
    """
    errors: list[str] = []
    sheet_names = list(servers.keys())
    if len(sheet_names) < 2:
        return errors

    ref_name = sheet_names[0]
    ref = servers[ref_name]
    ref_cols = {tc.name for tc in ref.target_columns}
    ref_types = {tc.name: tc.dtype for tc in ref.target_columns}
    ref_pks = {tc.name: tc.pk_marker for tc in ref.target_columns}

    for other_name in sheet_names[1:]:
        other = servers[other_name]
        other_cols = {tc.name for tc in other.target_columns}
        other_types = {tc.name: tc.dtype for tc in other.target_columns}
        other_pks = {tc.name: tc.pk_marker for tc in other.target_columns}

        # X1: Column names
        missing_in_other = ref_cols - other_cols
        extra_in_other = other_cols - ref_cols
        if missing_in_other:
            errors.append(
                f"[X1] Sheet '{other_name}' missing target columns present in '{ref_name}': "
                f"{sorted(missing_in_other)}"
            )
        if extra_in_other:
            errors.append(
                f"[X1] Sheet '{other_name}' has extra target columns not in '{ref_name}': "
                f"{sorted(extra_in_other)}"
            )

        # X2: Data types
        for col in ref_cols & other_cols:
            if ref_types.get(col, "").upper() != other_types.get(col, "").upper():
                errors.append(
                    f"[X2] Column '{col}' dtype mismatch: "
                    f"'{ref_name}'={ref_types[col]} vs '{other_name}'={other_types[col]}"
                )

        # X3: PK markers
        for col in ref_cols & other_cols:
            r_pk = (ref_pks.get(col) or "").upper()
            o_pk = (other_pks.get(col) or "").upper()
            if r_pk != o_pk:
                errors.append(
                    f"[X3] Column '{col}' PK marker mismatch: "
                    f"'{ref_name}'={r_pk!r} vs '{other_name}'={o_pk!r}"
                )

    return errors


# ═══════════════════════════════════════════════════════════════════════════
# Server Data Serialization
# ═══════════════════════════════════════════════════════════════════════════

def _serialize_server(sd: ServerData) -> dict:
    """Convert a ServerData to a JSON-serializable dict."""
    return {
        "source_tables": {
            key: {
                "database": st.database,
                "schema": st.schema,
                "table": st.table,
                "columns": st.columns,
            }
            for key, st in sd.source_tables.items()
        },
        "column_mappings": [asdict(m) for m in sd.column_mappings],
        "joins": [
            {
                "sheet_name": j.sheet_name,
                "left_table": j.left_table,
                "join_alias": j.join_alias,
                "join_table": j.join_table,
                "join_db": j.join_db,
                "join_schema": j.join_schema,
                "join_type": j.join_type,
                "key_pairs": j.key_pairs,
                "fetch_columns": j.fetch_columns,
            }
            for j in sd.joins
        ],
        "global_transforms": [asdict(gt) for gt in sd.global_transforms],
    }


# ═══════════════════════════════════════════════════════════════════════════
# Top-Level Parser
# ═══════════════════════════════════════════════════════════════════════════

def parse_excel(excel_path: str | Path) -> dict:
    """
    Main entry point. Parse an Excel mapping spec and return a dict.

    Runs verify_excel_syntax.verify() first — raises ValueError if verification fails.
    """
    path = Path(excel_path)
    fname = path.name

    # Run syntax verification before parsing
    from verify_excel_syntax import verify as verify_syntax
    print(f"\n  Verifying syntax: {fname}")
    vresult = verify_syntax(str(path))
    print(f"  {vresult.summary()}")
    if not vresult.passed:
        raise ValueError(
            f"Excel syntax verification FAILED for '{fname}'. "
            f"Fix errors above before parsing."
        )

    # Extract target_db, target_schema, and target_table from filename
    m = FILENAME_RE.match(fname)
    if m:
        target_db = m.group("target_db").upper()
        target_table_raw = f"{m.group('target_schema')}.{m.group('target_table')}"
    else:
        print(f"  WARNING: filename '{fname}' does not match <db>.<schema>.<table>.xlsx pattern")
        target_db = ""
        target_table_raw = path.stem

    wb = openpyxl.load_workbook(str(path), data_only=True)

    # 1. Discover sheets
    server_sheet_names, has_joins = discover_sheets(wb)
    if not server_sheet_names:
        raise ValueError(f"No valid server sheets found in '{fname}'")

    print(f"  Server sheets: {server_sheet_names}")

    # 2. Parse _joins sheet
    joins_by_sheet: dict[str, list[JoinDef]] = {}
    if has_joins:
        joins_ws_name = next(s for s in wb.sheetnames if s.lower() == JOINS_SHEET)
        print(f"  Found '{joins_ws_name}' sheet — parsing joins...")
        joins_by_sheet = parse_joins_sheet(wb[joins_ws_name])
        print(f"  Joins loaded for sheets: {list(joins_by_sheet.keys())}")

    # 3. Parse each server sheet
    servers: dict[str, ServerData] = {}

    for sheet_name in server_sheet_names:
        ws = wb[sheet_name]
        print(f"  Parsing sheet '{sheet_name}'...")

        # Detect headers
        hdr = detect_headers(ws)

        # Parse data rows
        mappings, target_cols, source_tables, last_row, fd_warnings = parse_data_rows(ws, hdr)
        print(f"    {len(mappings)} column mappings, {len(source_tables)} source table(s)")
        if fd_warnings:
            print(f"    ⚠ {len(fd_warnings)} Field Desc mismatch warning(s)")

        # Parse global transforms
        gt = parse_global_transforms(ws, hdr)
        if gt:
            print(f"    {len(gt)} global transform(s)")

        # Get joins for this sheet
        sheet_joins = joins_by_sheet.get(sheet_name, [])

        sd = ServerData(
            sheet_name=sheet_name,
            source_tables=source_tables,
            column_mappings=mappings,
            joins=sheet_joins,
            global_transforms=gt,
            target_columns=target_cols,
        )
        sd._field_desc_warnings = fd_warnings  # stash for later
        servers[sheet_name] = sd

    # 4. Cross-sheet validation
    consistency_errors = validate_cross_sheet(servers)
    # 4b. Collect Field Desc mismatch warnings (D13)
    for srv_name, srv_data in servers.items():
        for w in getattr(srv_data, '_field_desc_warnings', []):
            consistency_errors.append(f"[{srv_name}] {w}")
    if consistency_errors:
        print(f"\n  ⚠ Cross-sheet consistency errors:")
        for err in consistency_errors:
            print(f"    {err}")

    # 5. Build unified target schema from first server
    first_server = servers[server_sheet_names[0]]
    target_schema_name = ""
    target_table_name = ""

    # Try to get schema.table from data rows
    if first_server.column_mappings:
        first_mapping = first_server.column_mappings[0]
        # Look for target schema/table in the headers
        ws0 = wb[server_sheet_names[0]]
        hdr0 = detect_headers(ws0)
        tgt_schema_val = _get_tgt_col(hdr0, ws0, hdr0.header_row + 1, "schema")
        tgt_table_val = _get_tgt_col(hdr0, ws0, hdr0.header_row + 1, "table")
        if tgt_schema_val:
            target_schema_name = tgt_schema_val.upper()
        if tgt_table_val:
            target_table_name = tgt_table_val.upper()

    full_target = f"{target_schema_name}.{target_table_name}" if target_schema_name else target_table_name

    pks = [tc.name for tc in first_server.target_columns if tc.is_pk]

    result = {
        "file_name": fname,
        "target_db": target_db,
        "target_table": full_target or target_table_raw.upper(),
        "target_schema": {
            "schema": target_schema_name,
            "table": full_target or target_table_raw.upper(),
            "primary_keys": pks,
            "columns": [
                {
                    "name": tc.name,
                    "dtype": tc.dtype,
                    "nullable": tc.nullable,
                    "is_pk": tc.is_pk,
                    "pk_marker": tc.pk_marker,
                    "default_val": tc.default_val,
                    "null_handling": tc.null_handling,
                }
                for tc in first_server.target_columns
            ],
        },
        "servers": {
            name: _serialize_server(sd) for name, sd in servers.items()
        },
        "consistency_errors": consistency_errors,
    }

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Configuration — edit these variables before running
# ═══════════════════════════════════════════════════════════════════════════

# Path to the Excel mapping file (must be in same folder or provide full path)
# Examples: "analytics_dw.dimensional.dim_vehicle_master.xlsx"
#           "analytics_dw.reporting.fact_commercial.xlsx"
#           "analytics_dw.reporting.fact_production.xlsx"

# EXCEL_FILE = "analytics_dw.public.dim_vehicle_master.xlsx"
# EXCEL_FILE = "analytics_dw.public.fact_commercial.xlsx"
EXCEL_FILE = "analytics_dw.public.fact_production.xlsx"

# Output JSON path (None = auto-generate as <stem>_parsed.json in same folder)
# Examples: "parsed_output.json", None
OUTPUT_JSON = None

# Pretty-print the JSON output (True = indented, False = compact)
PRETTY_PRINT = True


if __name__ == "__main__":
    excel_path = Path(EXCEL_FILE)
    if not excel_path.exists():
        print(f"ERROR: {excel_path} not found.", file=sys.stderr)
        sys.exit(1)

    print(f"\nParsing: {excel_path.name}")
    try:
        result = parse_excel(excel_path)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    out_path = Path(OUTPUT_JSON) if OUTPUT_JSON else excel_path.with_name(excel_path.stem + "_parsed.json")
    out_path.write_text(
        json.dumps(result, indent=2 if PRETTY_PRINT else None, default=str),
        encoding="utf-8",
    )

    n_servers = len(result["servers"])
    n_errors = len(result["consistency_errors"])
    print(f"\n  OK  {n_servers} server(s) parsed → {out_path}")
    if n_errors:
        print(f"  ⚠  {n_errors} consistency error(s) — see output JSON")


