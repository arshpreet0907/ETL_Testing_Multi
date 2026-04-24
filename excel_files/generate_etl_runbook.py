"""
generate_etl_runbook.py
------------------------
Runbook Generator — Combined Aliased Extract (single CSV per server).

Reads Excel mapping specs (this project's format) and generates:
  Shared:
    02_create_target_sf.sql
    05_extract_target_sf.sql
  Per server:
    source_tables/01_src_*_ddl.sql
    03_extract_source.sql   (combined query, aliased to target names)
    04_transform.py         (single-DF, COPY skipped)
    06_pipeline_log.md

If a parsed JSON file already exists for the Excel file, it is reused.
Otherwise, the parser (which runs syntax verification first) is invoked.

Usage:
    python generate_etl_runbook.py
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import openpyxl

# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

HEADER_NAMES = [
    "src_db", "src_schema", "src_table", "src_col_name", "src_dtype",
    "src_nullable", "src_is_pk", "src_is_unique",
    "transform_type", "transform_rule",
    "tgt_col_name", "tgt_dtype", "tgt_nullable", "tgt_is_pk",
    "tgt_default_val", "force_dtype", "null_handling", "notes",
]

JOINS_HEADERS = [
    "sheet_name", "join_alias", "join_table", "join_db", "join_schema",
    "join_type", "left_key", "right_key", "fetch_columns",
]

SKIP_SHEETS = {"_joins"}

# Global transforms marker and headers
GT_MARKER = "_global_transforms"
GT_HEADERS_EXPECTED = {"order", "operation", "parameters", "condition", "notes"}

# Chained replace pattern
REPLACE_CHAIN_RE = re.compile(
    r'replace\s*\(\s*"([^"]*)"\s*,\s*"([^"]*)"\s*\)', re.IGNORECASE,
)


# ═══════════════════════════════════════════════════════════════════════════
# Data Models
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ColumnMapping:
    src_db: str
    src_schema: str
    src_table: str
    src_col_name: str
    src_dtype: str
    src_nullable: str
    src_is_pk: str
    src_is_unique: str
    transform_type: str
    transform_rule: str
    tgt_col_name: str
    tgt_dtype: str
    tgt_nullable: str
    tgt_is_pk: str
    tgt_default_val: str
    force_dtype: str
    null_handling: str
    notes: str


@dataclass
class JoinDef:
    sheet_name: str
    join_alias: str
    join_table: str
    join_db: str
    join_schema: str
    join_type: str
    left_key: str
    right_key: str
    fetch_columns: list[str] = field(default_factory=list)


@dataclass
class GlobalTransform:
    """One global transform rule applied after all column-level transforms."""
    order: int
    operation: str      # TRIM, UPPER, LOWER, REPLACE, REGEX_REPLACE, STRIP_WHITESPACE, CAST
    parameters: str     # operation-specific params
    condition: str      # dtype:VARCHAR, col:*NAME*, exclude:COL1,COL2, or blank=all
    notes: str


@dataclass
class ServerData:
    name: str
    mappings: list[ColumnMapping] = field(default_factory=list)
    joins: list[JoinDef] = field(default_factory=list)
    global_transforms: list[GlobalTransform] = field(default_factory=list)
    source_tables: dict[str, list[ColumnMapping]] = field(default_factory=dict)
    # main_table is the table with the most mapped columns
    main_table: str = ""
    main_db: str = ""
    main_schema: str = ""


@dataclass
class ParsedExcel:
    file_name: str
    target_db: str
    target_schema: str
    target_table: str
    servers: dict[str, ServerData] = field(default_factory=dict)
    target_columns: list[dict] = field(default_factory=list)
    target_pks: list[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _clean(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip().replace('\xa0', ' ').strip()


def _find_header_row(ws) -> int:
    """Find the row containing column headers (src_db, src_schema, etc.)."""
    for r in range(1, min(6, (ws.max_row or 1) + 1)):
        for c in range(1, min((ws.max_column or 1) + 1, 5)):
            if _clean(ws.cell(r, c).value).lower() == "src_db":
                return r
    return -1


def _build_header_map(ws, header_row: int) -> dict[str, int]:
    """Map header name → column index."""
    hmap = {}
    for c in range(1, (ws.max_column or 1) + 1):
        name = _clean(ws.cell(header_row, c).value).lower()
        if name:
            hmap[name] = c
    return hmap


# ═══════════════════════════════════════════════════════════════════════════
# Parser
# ═══════════════════════════════════════════════════════════════════════════

def parse_excel(excel_path: str | Path) -> ParsedExcel:
    path = Path(excel_path)
    fname = path.name
    stem = path.stem  # e.g. "analytics_dw.dimensional.dim_vehicle_master"

    # Run syntax verification before parsing
    from verify_excel_syntax import verify as verify_syntax
    print(f"  Verifying syntax: {fname}")
    vresult = verify_syntax(str(path))
    if not vresult.passed:
        print(f"  {vresult.summary()}")
        raise ValueError(
            f"Excel syntax verification FAILED for '{fname}'. "
            f"Fix errors above before generating runbook."
        )
    errors = [i for i in vresult.issues if i.severity == "ERROR"]
    warnings = [i for i in vresult.issues if i.severity == "WARNING"]
    print(f"  Syntax OK ({len(errors)} errors, {len(warnings)} warnings)")

    # Parse filename: <target_db>.<target_schema>.<target_table>
    parts = stem.split(".")
    if len(parts) >= 3:
        target_db = parts[0]
        target_schema = parts[1]
        target_table = ".".join(parts[1:])  # schema.table
    elif len(parts) == 2:
        target_db = parts[0]
        target_schema = ""
        target_table = parts[1]
    else:
        target_db = ""
        target_schema = ""
        target_table = stem

    wb = openpyxl.load_workbook(str(path), data_only=True)

    # Discover server sheets
    server_sheets = [s for s in wb.sheetnames if s.lower() not in SKIP_SHEETS]

    # Parse _joins
    joins_by_sheet: dict[str, list[JoinDef]] = {}
    if "_joins" in [s.lower() for s in wb.sheetnames]:
        jws_name = next(s for s in wb.sheetnames if s.lower() == "_joins")
        jws = wb[jws_name]
        jhmap = _build_header_map(jws, 1)
        for r in range(2, (jws.max_row or 1) + 1):
            def _jget(name):
                idx = jhmap.get(name, 0)
                return _clean(jws.cell(r, idx).value) if idx else ""
            sn = _jget("sheet_name")
            if not sn:
                continue
            fetch_raw = _jget("fetch_columns")
            fetch_cols = [c.strip() for c in fetch_raw.split(",") if c.strip()]
            jd = JoinDef(
                sheet_name=sn,
                join_alias=_jget("join_alias"),
                join_table=_jget("join_table"),
                join_db=_jget("join_db"),
                join_schema=_jget("join_schema"),
                join_type=_jget("join_type").upper() or "LEFT",
                left_key=_jget("left_key"),
                right_key=_jget("right_key"),
                fetch_columns=fetch_cols,
            )
            joins_by_sheet.setdefault(sn, []).append(jd)

    # Parse each server sheet
    result = ParsedExcel(
        file_name=fname,
        target_db=target_db,
        target_schema=target_schema,
        target_table=target_table,
    )

    for sheet_name in server_sheets:
        ws = wb[sheet_name]
        header_row = _find_header_row(ws)
        if header_row < 0:
            print(f"  WARNING: sheet '{sheet_name}' has no recognized headers, skipping.")
            continue

        hmap = _build_header_map(ws, header_row)
        mappings: list[ColumnMapping] = []
        seen_tgt: set[str] = set()

        for r in range(header_row + 1, (ws.max_row or header_row) + 1):
            def _get(name):
                idx = hmap.get(name, 0)
                return _clean(ws.cell(r, idx).value) if idx else ""

            tgt_col = _get("tgt_col_name")
            transform_type = _get("transform_type")
            if not tgt_col and not transform_type:
                # Check if completely blank row
                if not any(_clean(ws.cell(r, c).value) for c in range(1, min((ws.max_column or 1) + 1, 20))):
                    continue
                # Might be a legend row — stop
                first_cell = _clean(ws.cell(r, 1).value).lower()
                if first_cell in ("transform type legend", GT_MARKER, ""):
                    break
                continue

            if not tgt_col:
                continue

            cm = ColumnMapping(
                src_db=_get("src_db"),
                src_schema=_get("src_schema"),
                src_table=_get("src_table"),
                src_col_name=_get("src_col_name"),
                src_dtype=_get("src_dtype"),
                src_nullable=_get("src_nullable"),
                src_is_pk=_get("src_is_pk"),
                src_is_unique=_get("src_is_unique"),
                transform_type=transform_type.lower() if transform_type else "direct",
                transform_rule=_get("transform_rule"),
                tgt_col_name=tgt_col.upper(),
                tgt_dtype=_get("tgt_dtype"),
                tgt_nullable=_get("tgt_nullable"),
                tgt_is_pk=_get("tgt_is_pk"),
                tgt_default_val=_get("tgt_default_val"),
                force_dtype=_get("force_dtype"),
                null_handling=_get("null_handling"),
                notes=_get("notes"),
            )
            mappings.append(cm)

            # Build target column list (deduplicated)
            if cm.tgt_col_name not in seen_tgt:
                seen_tgt.add(cm.tgt_col_name)
                is_pk = cm.tgt_is_pk and cm.tgt_is_pk.upper() in ("Y", "YES")
                result.target_columns.append({
                    "name": cm.tgt_col_name,
                    "dtype": cm.tgt_dtype,
                    "nullable": cm.tgt_nullable.upper() not in ("N", "NO") if cm.tgt_nullable else True,
                    "is_pk": is_pk,
                    "default_val": cm.tgt_default_val,
                    "force_dtype": cm.force_dtype,
                })
                if is_pk:
                    result.target_pks.append(cm.tgt_col_name)

        # Group mappings by source table
        source_tables: dict[str, list[ColumnMapping]] = {}
        for cm in mappings:
            if cm.src_table:
                source_tables.setdefault(cm.src_table, []).append(cm)

        # Parse global transforms section
        global_transforms: list[GlobalTransform] = []
        for r in range(header_row + 1, (ws.max_row or header_row) + 1):
            cell_a = _clean(ws.cell(r, 1).value).lower()
            if cell_a == GT_MARKER:
                # Next row has sub-headers: order, operation, parameters, condition, notes
                gt_hdr_row = r + 1
                if gt_hdr_row > (ws.max_row or 0):
                    break
                gt_hdr_map: dict[str, int] = {}
                for c in range(1, min((ws.max_column or 1) + 1, 15)):
                    v = _clean(ws.cell(gt_hdr_row, c).value).lower()
                    if v in GT_HEADERS_EXPECTED:
                        gt_hdr_map[v] = c
                if "order" not in gt_hdr_map or "operation" not in gt_hdr_map:
                    break
                for gr in range(gt_hdr_row + 1, (ws.max_row or gt_hdr_row) + 1):
                    order_val = _clean(ws.cell(gr, gt_hdr_map["order"]).value)
                    op_val = _clean(ws.cell(gr, gt_hdr_map["operation"]).value)
                    if not order_val and not op_val:
                        break
                    try:
                        order_int = int(order_val)
                    except (ValueError, TypeError):
                        break  # Non-numeric order means end of global transforms
                    params = _clean(ws.cell(gr, gt_hdr_map.get("parameters", 0)).value) if "parameters" in gt_hdr_map else ""
                    condition = _clean(ws.cell(gr, gt_hdr_map.get("condition", 0)).value) if "condition" in gt_hdr_map else ""
                    notes = _clean(ws.cell(gr, gt_hdr_map.get("notes", 0)).value) if "notes" in gt_hdr_map else ""
                    global_transforms.append(GlobalTransform(
                        order=order_int,
                        operation=op_val.upper(),
                        parameters=params,
                        condition=condition,
                        notes=notes,
                    ))
                if global_transforms:
                    print(f"    {len(global_transforms)} global transform(s) parsed")
                break

        # Determine main table (most columns)
        main_table = ""
        main_db = ""
        main_schema = ""
        if source_tables:
            main_table = max(source_tables, key=lambda k: len(source_tables[k]))
            first_of_main = source_tables[main_table][0]
            main_db = first_of_main.src_db
            main_schema = first_of_main.src_schema

        sd = ServerData(
            name=sheet_name,
            mappings=mappings,
            joins=joins_by_sheet.get(sheet_name, []),
            global_transforms=global_transforms,
            source_tables=source_tables,
            main_table=main_table,
            main_db=main_db,
            main_schema=main_schema,
        )
        result.servers[sheet_name] = sd

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Source DDL Generation
# ═══════════════════════════════════════════════════════════════════════════

def _source_table_fqn(db: str, schema: str, table: str) -> str:
    parts = [p for p in [db, schema, table] if p]
    return ".".join(parts)


def _src_key(src_table: str, src_col_name: str) -> tuple[str, str]:
    """Case-insensitive key for a source column scoped to its table."""
    return (src_table or "").strip().lower(), (src_col_name or "").strip().lower()


def _get_pk_col(cms: list[ColumnMapping]) -> str:
    """Return the source PK column name for ROW_NUMBER ORDER BY.
    Falls back to '(SELECT NULL)' if no PK is declared in the mapping spec."""
    for cm in cms:
        if cm.src_is_pk and cm.src_is_pk.strip().upper() in ("Y", "YES"):
            return cm.src_col_name
    return "(SELECT NULL)"


def _null_cast_type(cm: ColumnMapping) -> str:
    """Prefer target dtype for SQL NULL placeholders, then source dtype."""
    return (cm.tgt_dtype or cm.src_dtype or "VARCHAR(255)").strip()


def build_source_ddl(db: str, schema: str, table: str, columns: list[dict]) -> str:
    fqn = _source_table_fqn(db, schema, table)
    lines = [
        f"-- ============================================================",
        f"-- Partial source DDL: {fqn}",
        f"-- Only columns referenced in mapping spec",
        f"-- Generated: {datetime.now():%Y-%m-%d %H:%M}",
        f"-- ============================================================",
        f"",
        f"CREATE TABLE IF NOT EXISTS {fqn} (",
    ]
    for i, col in enumerate(columns):
        nn = " NOT NULL" if col.get("nullable", "Y").upper() in ("N", "NO") else ""
        pk = "  ,-- PK" if col.get("is_pk", "").upper() in ("Y", "YES") else ""
        comma = "," if i < len(columns) - 1 else ""
        lines.append(f"    {col['name']:<34} {col['dtype']:<26}{nn}{comma}{pk}")
    lines.append(");")
    return "\n".join(lines)


def generate_source_ddls(server: ServerData, out_dir: Path):
    src_dir = out_dir / "source_tables"
    src_dir.mkdir(parents=True, exist_ok=True)

    # DDLs for directly mapped tables
    for table_name, cms in server.source_tables.items():
        db = cms[0].src_db
        schema = cms[0].src_schema
        cols = []
        seen = set()
        for cm in cms:
            if cm.src_col_name and cm.src_col_name not in seen:
                seen.add(cm.src_col_name)
                cols.append({
                    "name": cm.src_col_name,
                    "dtype": cm.src_dtype,
                    "nullable": cm.src_nullable,
                    "is_pk": cm.src_is_pk,
                })
        safe_name = f"01_src_{db}_{table_name}_ddl.sql".replace(" ", "_").lower()
        ddl = build_source_ddl(db, schema, table_name, cols)
        (src_dir / safe_name).write_text(ddl, encoding="utf-8")

    # DDLs for join-only tables
    for jd in server.joins:
        safe_name = f"01_src_{jd.join_db or server.main_db}_{jd.join_table}_ddl.sql".replace(" ", "_").lower()
        if (src_dir / safe_name).exists():
            continue
        # Minimal DDL with fetch columns + join key
        cols = [{"name": jd.right_key, "dtype": "INT", "nullable": "N", "is_pk": "Y"}]
        for fc in jd.fetch_columns:
            cols.append({"name": fc, "dtype": "VARCHAR(255)", "nullable": "Y", "is_pk": ""})
        ddl = build_source_ddl(jd.join_db or server.main_db, jd.join_schema, jd.join_table, cols)
        (src_dir / safe_name).write_text(ddl, encoding="utf-8")


# ═══════════════════════════════════════════════════════════════════════════
# Target DDL Generation
# ═══════════════════════════════════════════════════════════════════════════

_SF_MAP = {
    "int": "NUMBER(10,0)", "integer": "NUMBER(10,0)", "tinyint": "NUMBER(3,0)",
    "smallint": "NUMBER(5,0)", "bigint": "NUMBER(19,0)",
    "float": "FLOAT", "double": "FLOAT", "real": "FLOAT",
    "varchar": "VARCHAR", "char": "CHAR", "text": "VARCHAR",
    "date": "DATE", "datetime": "TIMESTAMP_NTZ", "timestamp": "TIMESTAMP_NTZ",
    "timestamp_ntz": "TIMESTAMP_NTZ", "boolean": "BOOLEAN", "bool": "BOOLEAN",
    "decimal": "NUMBER", "numeric": "NUMBER",
}

def _to_sf_type(raw: str) -> str:
    if not raw:
        return "VARCHAR"
    d = raw.strip()
    base = re.match(r'^([a-z_]+)', d.lower())
    bk = base.group(1) if base else d.lower()
    if bk in _SF_MAP:
        prec = re.search(r'\([\d,\s]+\)', d)
        mapped = _SF_MAP[bk]
        if prec and '(' not in mapped:
            return mapped + prec.group(0)
        return mapped
    return d


def build_target_ddl(parsed: ParsedExcel) -> str:
    fqn = f"{parsed.target_db}.{parsed.target_table}"
    cols = parsed.target_columns
    pks = parsed.target_pks

    lines = [
        f"-- ============================================================",
        f"-- Target table: {fqn}",
        f"-- Dialect: Snowflake",
        f"-- Generated: {datetime.now():%Y-%m-%d %H:%M}",
        f"-- ============================================================",
        f"",
        f"CREATE TABLE IF NOT EXISTS {fqn} (",
    ]

    col_lines = []
    for c in cols:
        dtype = _to_sf_type(c["dtype"])
        nn = "" if c["nullable"] else " NOT NULL"
        default = ""
        if c["default_val"]:
            dv = c["default_val"].strip().upper()
            if dv in ("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()"):
                default = " DEFAULT CURRENT_TIMESTAMP()"
            else:
                default = f" DEFAULT {c['default_val']}"
        pk_cmt = "  ,-- PK" if c["name"] in pks else ""
        col_lines.append(f"    {c['name']:<36} {dtype:<26}{nn}{default}{pk_cmt}")

    for i, cl in enumerate(col_lines):
        comma = "," if i < len(col_lines) - 1 or pks else ""
        lines.append(f"{cl}{comma}")

    if pks:
        lines.append(f"    PRIMARY KEY ({', '.join(pks)})")
    lines.append(");")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# Extract Source SQL — V3 Combined Aliased
# ═══════════════════════════════════════════════════════════════════════════

def build_extract_sql_v3(server: ServerData, parsed: ParsedExcel) -> str:
    """
    Build a single combined extract SQL for a server.

    All source tables get aliases upfront:
      - Main table → 'm'
      - Tables with _joins entry → their join_alias (e.g. 'parts', 'sup')
      - Other mapped tables → auto-generated short alias (t1, t2, ...)

    These aliases are used consistently in SELECT and FROM/JOIN clauses.
    Tables with explicit _joins use those join conditions.
    Tables without joins use CTE + ROW_NUMBER alignment.
    """
    mappings = server.mappings
    joins = server.joins

    # Identify all source tables
    src_tables = list(server.source_tables.keys())

    # ── Step 1: Assign aliases to ALL source tables upfront ───────────
    table_alias: dict[str, str] = {}    # table_name → alias
    table_fqn: dict[str, str] = {}      # table_name → fully qualified name

    # Main table always gets 'm'
    table_alias[server.main_table] = "m"
    table_fqn[server.main_table] = _source_table_fqn(server.main_db, server.main_schema, server.main_table)

    # Tables with _joins entries get their join_alias
    join_by_table: dict[str, JoinDef] = {}
    for jd in joins:
        join_by_table[jd.join_table] = jd
        if jd.join_table not in table_alias:
            table_alias[jd.join_table] = jd.join_alias
            jdb = jd.join_db or server.main_db
            table_fqn[jd.join_table] = _source_table_fqn(jdb, jd.join_schema, jd.join_table)

    # Remaining mapped tables get auto-aliases (t1, t2, ...)
    auto_idx = 1
    for tbl in src_tables:
        if tbl not in table_alias:
            table_alias[tbl] = f"t{auto_idx}"
            auto_idx += 1
            cms = server.source_tables[tbl]
            table_fqn[tbl] = _source_table_fqn(cms[0].src_db, cms[0].src_schema, tbl)

    main_alias = table_alias[server.main_table]
    main_fqn = table_fqn[server.main_table]

    # Classify tables
    joined_tables = {jd.join_table for jd in joins}
    non_main_tables = [t for t in src_tables if t != server.main_table]
    extra_tables = [t for t in non_main_tables if t not in joined_tables]

    # ── Step 2: Build SELECT columns ──────────────────────────────────
    select_parts = []
    extra_src_cols_needed: dict[str, set[str]] = {}  # alias → {col_name}

    # Track projected source columns by table+column (avoid cross-table collisions)
    already_projected_src: dict[tuple[str, str], str] = {}

    for cm in mappings:
        if cm.transform_type == "constant":
            continue
        if cm.transform_type == "drop":
            continue

        src = cm.src_col_name
        tgt = cm.tgt_col_name
        table = cm.src_table

        if not src or not table:
            continue

        alias = table_alias.get(table, "m")

        src_key = _src_key(table, src)

        if cm.transform_type in ("direct", "rename"):
            if src == tgt:
                select_parts.append(f"    {alias}.{src}")
            else:
                select_parts.append(f"    {alias}.{src} AS {tgt}")
            already_projected_src[src_key] = tgt
        elif cm.transform_type == "cast" and cm.transform_rule:
            select_parts.append(f"    {alias}.{src} AS {tgt}")
            already_projected_src[src_key] = tgt
        elif cm.transform_type == "derived" and cm.transform_rule:
            # If this source (table+col) is already projected under a different target name,
            # emit NULL as a placeholder — the real value is computed in 04_transform.py
            if src_key in already_projected_src:
                prior_tgt = already_projected_src[src_key]
                null_dtype = _null_cast_type(cm)
                select_parts.append(
                    f"    CAST(NULL AS {null_dtype}) AS {tgt}"
                    f"  -- derived in 04_transform.py from {prior_tgt}"
                )
            else:
                select_parts.append(f"    {alias}.{src} AS {tgt}")
                already_projected_src[src_key] = tgt
            rule = cm.transform_rule
            refs = re.findall(r'\b([a-z_][a-z0-9_]*)\b', rule, re.I)
            reserved = {'val', 'if', 'else', 'and', 'or', 'not', 'None', 'True', 'False',
                         'UPPER', 'LOWER', 'TRIM', 'ROUND', 'CAST', 'CONCAT', 'COALESCE',
                         'AS', 'upper', 'lower', 'str', 'int', 'float', 'round',
                         'Y', 'N', 'T', 'F', 'YES', 'NO'}
            for ref in refs:
                if ref in reserved or ref == src:
                    continue
                found = False
                for tbl_name, tbl_cms in server.source_tables.items():
                    for tcm in tbl_cms:
                        if tcm.src_col_name == ref:
                            a = table_alias.get(tbl_name, "m")
                            extra_src_cols_needed.setdefault(a, set()).add(ref)
                            found = True
                for jd in joins:
                    if ref in jd.fetch_columns:
                        extra_src_cols_needed.setdefault(table_alias.get(jd.join_table, jd.join_alias), set()).add(ref)
                        found = True
                # If ref not found in any mapped column or join, assume it belongs
                # to the same source table as this mapping (unmapped source column)
                if not found:
                    extra_src_cols_needed.setdefault(alias, set()).add(ref)
        else:
            select_parts.append(f"    {alias}.{src} AS {tgt}")

    # Add extra source columns needed for derived transforms
    already_selected = set()
    for sp in select_parts:
        m = re.search(r'\.(\w+)(?:\s+AS\s+(\w+))?', sp, re.I)
        if m:
            already_selected.add(m.group(1).lower())
            if m.group(2):
                already_selected.add(m.group(2).lower())

    for alias, cols in extra_src_cols_needed.items():
        for col in cols:
            if col.lower() not in already_selected:
                select_parts.append(f"    {alias}.{col}")
                already_selected.add(col.lower())

    # ── Step 3: Build FROM + JOIN clauses ─────────────────────────────
    from_clause = f"FROM {main_fqn} {main_alias}"
    join_clauses = []

    # Explicit joins from _joins sheet
    for jd in joins:
        a = table_alias[jd.join_table]
        fqn = table_fqn[jd.join_table]
        join_clauses.append(
            f"{jd.join_type} JOIN {fqn} {a}\n"
            f"    ON {main_alias}.{jd.left_key} = {a}.{jd.right_key}"
        )

    # Handle extra tables (no join defined) with CTE + ROW_NUMBER
    cte_parts = []
    # Build reverse alias→table map for injecting extra cols into CTEs
    alias_to_table = {v: k for k, v in table_alias.items()}
    if extra_tables:
        # Wrap main table in CTE
        main_cols_in_select = [cm.src_col_name for cm in server.source_tables.get(server.main_table, []) if cm.src_col_name]
        # Also include join keys from _joins so the JOINs still work inside the CTE
        for jd in joins:
            if jd.left_key not in main_cols_in_select:
                main_cols_in_select.append(jd.left_key)
        # Include extra cols needed by derived transforms for the main table
        main_old_alias = table_alias[server.main_table]
        for ecol in extra_src_cols_needed.get(main_old_alias, set()):
            if ecol not in main_cols_in_select:
                main_cols_in_select.append(ecol)
        main_pk = _get_pk_col(server.source_tables.get(server.main_table, []))
        cte_parts.append(
            f"cte_main AS (\n"
            f"    SELECT\n"
            f"        {', '.join(main_cols_in_select)},\n"
            f"        ROW_NUMBER() OVER (ORDER BY {main_pk}) AS rn\n"
            f"    FROM {main_fqn}\n"
            f")"
        )
        # Update main alias to point at CTE
        old_main_alias = main_alias
        main_alias = "cte_main"
        table_alias[server.main_table] = main_alias
        from_clause = f"FROM cte_main"

        # Re-build explicit join clauses to use new main alias
        join_clauses = []
        for jd in joins:
            a = table_alias[jd.join_table]
            fqn = table_fqn[jd.join_table]
            join_clauses.append(
                f"{jd.join_type} JOIN {fqn} {a}\n"
                f"    ON {main_alias}.{jd.left_key} = {a}.{jd.right_key}"
            )

        # Add CTE + JOIN for each extra table
        for et in extra_tables:
            et_alias = table_alias[et]
            et_fqn = table_fqn[et]
            et_cols = [cm.src_col_name for cm in server.source_tables[et] if cm.src_col_name]
            # Include extra cols needed by derived transforms for this table
            et_old_alias = table_alias[et]
            for ecol in extra_src_cols_needed.get(et_old_alias, set()):
                if ecol not in et_cols:
                    et_cols.append(ecol)
            et_pk = _get_pk_col(server.source_tables.get(et, []))
            cte_parts.append(
                f"cte_{et} AS (\n"
                f"    SELECT\n"
                f"        {', '.join(et_cols)},\n"
                f"        ROW_NUMBER() OVER (ORDER BY {et_pk}) AS rn\n"
                f"    FROM {et_fqn}\n"
                f")"
            )
            # Override alias to CTE name
            table_alias[et] = f"cte_{et}"
            join_clauses.append(
                f"JOIN cte_{et} ON {main_alias}.rn = cte_{et}.rn"
            )

        # Rebuild SELECT parts with updated aliases
        # Build a map of old alias → new alias for all changed tables
        alias_remap = {old_main_alias: main_alias}
        for et in extra_tables:
            # The old alias was assigned earlier (t1, t2, ...)
            # Find it by checking what index it was
            old_a = None
            auto_i = 1
            for tbl in src_tables:
                if tbl != server.main_table and tbl not in joined_tables:
                    if tbl == et:
                        old_a = f"t{auto_i}"
                        break
                    auto_i += 1
            if old_a:
                alias_remap[old_a] = f"cte_{et}"

        select_parts_new = []
        for sp in select_parts:
            updated = sp
            for old_a, new_a in alias_remap.items():
                updated = updated.replace(f"{old_a}.", f"{new_a}.")
            select_parts_new.append(updated)
        select_parts = select_parts_new

    # ── Step 4: Build full SQL ────────────────────────────────────────
    comment = [
        "-- ============================================================",
        f"-- EXTRACT SOURCE | server: {server.name}",
        f"-- Target: {parsed.target_db}.{parsed.target_table}",
        f"-- Generated: {datetime.now():%Y-%m-%d %H:%M}",
        "-- ============================================================",
        f"-- Main table: {table_fqn[server.main_table]} AS {main_alias}",
    ]
    # List all table aliases
    for tbl in src_tables:
        if tbl != server.main_table:
            a = table_alias[tbl]
            fqn = table_fqn.get(tbl, tbl)
            jd = join_by_table.get(tbl)
            if jd:
                comment.append(f"-- {jd.join_type} JOIN {fqn} AS {a} ON {main_alias}.{jd.left_key} = {a}.{jd.right_key}")
            else:
                comment.append(f"-- ROW_NUMBER JOIN {fqn} AS {a}")
    if extra_tables:
        comment.append(f"-- Extra tables (ROW_NUMBER aligned): {', '.join(extra_tables)}")
    comment.append("--")
    comment.append("-- All source tables aliased upfront.")
    comment.append("-- Columns aliased to target names where possible.")
    comment.append("-- Non-trivial transforms applied in 04_transform.py.")
    comment.append("")

    sql_parts = []
    # Add USE statement for the main database
    main_db_schema = _source_table_fqn(server.main_db, server.main_schema, "").rstrip(".")
    if main_db_schema:
        sql_parts.append(f"use {main_db_schema};")
    if cte_parts:
        sql_parts.append("WITH")
        sql_parts.append(",\n".join(cte_parts))

    sql_parts.append("SELECT")
    sql_parts.append(",\n".join(select_parts))
    sql_parts.append(from_clause)
    for jc in join_clauses:
        sql_parts.append(jc)
    sql_parts.append(";")

    return "\n".join(comment) + "\n".join(sql_parts)


# ═══════════════════════════════════════════════════════════════════════════
# Transform Python — V3 (single-DF, COPY skipped)
# ═══════════════════════════════════════════════════════════════════════════

def _lit_or_col(token: str, src: str) -> str:
    t = token.strip()
    if (t.startswith("'") and t.endswith("'")) or (t.startswith('"') and t.endswith('"')):
        return f'F.lit("{t[1:-1]}")'
    try:
        float(t)
        return f"F.lit({t})"
    except ValueError:
        pass
    if t.lower() in ("true",):
        return "F.lit(True)"
    if t.lower() in ("false",):
        return "F.lit(False)"
    if t == "val":
        return f"F.col('{src}')"
    if re.match(r'^\w+$', t):
        return f"F.col('{t}')"
    safe = t.replace("\\", "\\\\").replace('"', '\\"')
    return f'F.expr("{safe}")'


def _const_spark(rule: str) -> str:
    r = rule.strip()
    up = r.upper()
    if up in ("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()"):
        return "F.current_timestamp()"
    if up in ("CURRENT_DATE", "CURRENT_DATE()"):
        return "F.current_date()"
    if up in ("FALSE",):
        return "F.lit(False)"
    if up in ("TRUE",):
        return "F.lit(True)"
    if "IDENTITY" in up:
        return "F.lit(None).cast(IntegerType())"
    if up in ("NULL", "NONE"):
        return "F.lit(None)"
    if up == "SYS_BATCH_ID":
        return 'F.lit("ETL_VALIDATION")'
    if r.startswith("'") and r.endswith("'"):
        return f'F.lit("{r[1:-1]}")'
    try:
        float(r)
        return f"F.lit({r})"
    except ValueError:
        pass
    safe = r.replace("\\", "\\\\").replace('"', '\\"')
    return f'F.lit("{safe}")'


def _spark_expr(rule: str, src_col: str, tgt_col: str) -> str:
    """Convert transform rule to PySpark expression.
    In V3, the input DF columns are already target-named (via SQL aliasing).
    So for single-source transforms, the column to read from is tgt_col.
    For multi-source derived transforms, other columns use their target names too.
    """
    if not rule:
        return f"F.col('{tgt_col}')"
    r = rule.strip()
    first = r.split("\n")[0].strip()

    # Python ternary with is None / is not None
    m = re.match(r'^(.+?)\s+if\s+(\w+)\s+is\s+(not\s+)?None\s+else\s+(.+)$', first, re.I)
    if m:
        tv, col, is_not, fv = m.group(1).strip(), m.group(2).strip(), bool(m.group(3)), m.group(4).strip()
        # 'val' refers to the target column (since SQL aliased it)
        col = tgt_col if col == "val" else col
        cond = f"F.col('{col}').isNotNull()" if is_not else f"F.col('{col}').isNull()"
        return f"F.when({cond}, {_lit_or_col(tv, tgt_col)}).otherwise({_lit_or_col(fv, tgt_col)})"

    # Python ternary with compound condition: tv if col and col op cmp else fv
    # e.g. "1 if actual_arrival_dt and actual_arrival_dt > estimated_arrival_dt else 0"
    m = re.match(r'^(.+?)\s+if\s+(\w+)\s+and\s+(\w+)\s*(==|!=|<=|>=|<|>)\s*(.+?)\s+else\s+(.+)$', first, re.I)
    if m:
        tv, null_col, cmp_col, op, cmp_val, fv = (
            m.group(1).strip(), m.group(2).strip(), m.group(3).strip(),
            m.group(4), m.group(5).strip(), m.group(6).strip()
        )
        null_col = tgt_col if null_col == "val" else null_col
        cmp_col  = tgt_col if cmp_col  == "val" else cmp_col
        cond = f"F.col('{null_col}').isNotNull() & (F.col('{cmp_col}') {op} {_lit_or_col(cmp_val, tgt_col)})"
        return f"F.when({cond}, {_lit_or_col(tv, tgt_col)}).otherwise({_lit_or_col(fv, tgt_col)})"

    # Python ternary with comparison
    m = re.match(r'^(.+?)\s+if\s+(\w+)\s*(==|!=|<=|>=|<|>)\s*(.+?)\s+else\s+(.+)$', first, re.I)
    if m:
        tv, col, op, cmp, fv = m.group(1).strip(), m.group(2).strip(), m.group(3), m.group(4).strip(), m.group(5).strip()
        col = tgt_col if col == "val" else col
        return f"F.when(F.col('{col}') {op} {_lit_or_col(cmp, tgt_col)}, {_lit_or_col(tv, tgt_col)}).otherwise({_lit_or_col(fv, tgt_col)})"

    # UPPER(TRIM(col))
    m = re.match(r'UPPER\(\s*TRIM\(\s*(\w+)\s*\)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.upper(F.trim(F.col('{col}')))"

    # UPPER(col)
    m = re.match(r'UPPER\(\s*(\w+)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.upper(F.col('{col}'))"

    # TRIM(col)
    m = re.match(r'TRIM\(\s*(\w+)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.trim(F.col('{col}'))"

    # LOWER(col)
    m = re.match(r'LOWER\(\s*(\w+)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.lower(F.col('{col}'))"

    # ROUND(col, n)
    m = re.match(r'ROUND\(\s*(\w+)\s*,\s*(\d+)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.round(F.col('{col}'), {m.group(2)})"

    # COALESCE(col, default)
    m = re.match(r'COALESCE\(\s*(\w+)\s*,\s*(.+?)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.coalesce(F.col('{col}'), {_lit_or_col(m.group(2).strip(), tgt_col)})"

    # ISNULL(col, default) → same as COALESCE
    m = re.match(r'ISNULL\(\s*(\w+)\s*,\s*(.+?)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.coalesce(F.col('{col}'), {_lit_or_col(m.group(2).strip(), tgt_col)})"

    # IFNULL(col, default) → same as COALESCE
    m = re.match(r'IFNULL\(\s*(\w+)\s*,\s*(.+?)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.coalesce(F.col('{col}'), {_lit_or_col(m.group(2).strip(), tgt_col)})"

    # CAST(col AS type)
    m = re.match(r'CAST\(\s*(\w+)\s+AS\s+(\S+?)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        spark_type = m.group(2).strip().lower()
        type_map = {"int": "IntegerType()", "integer": "IntegerType()", "bigint": "LongType()",
                     "float": "DoubleType()", "double": "DoubleType()", "string": "StringType()",
                     "boolean": "BooleanType()", "timestamp": "TimestampType()", "date": "DateType()"}
        cast_type = type_map.get(spark_type)
        if cast_type:
            return f"F.col('{col}').cast({cast_type})"
        return f"F.col('{col}').cast('{spark_type}')"

    # MySQL IF(cond, true_val, false_val)
    m = re.match(r'IF\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)$', first, re.I)
    if m:
        cond_raw, tv_raw, fv_raw = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
        cm = re.match(r'(\w+)\s*(==|!=|<=|>=|<|>|=)\s*(.+)', cond_raw)
        if cm:
            col_name = tgt_col if cm.group(1) == "val" else cm.group(1)
            op = "==" if cm.group(2) == "=" else cm.group(2)
            return f"F.when(F.col('{col_name}') {op} {_lit_or_col(cm.group(3).strip(), tgt_col)}, {_lit_or_col(tv_raw, tgt_col)}).otherwise({_lit_or_col(fv_raw, tgt_col)})"
        cond_safe = cond_raw.replace('"', '\\"')
        tv_safe = tv_raw.replace('"', '\\"')
        fv_safe = fv_raw.replace('"', '\\"')
        return f'F.expr("CASE WHEN {cond_safe} THEN {tv_safe} ELSE {fv_safe} END")'

    # SUBSTRING(col, start, len)
    m = re.match(r'SUBSTRING\(\s*(\w+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.substring(F.col('{col}'), {m.group(2)}, {m.group(3)})"

    # LEFT(col, n)
    m = re.match(r'LEFT\(\s*(\w+)\s*,\s*(\d+)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.substring(F.col('{col}'), 1, {m.group(2)})"

    # RIGHT(col, n)
    m = re.match(r'RIGHT\(\s*(\w+)\s*,\s*(\d+)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.expr(\"RIGHT({col}, {m.group(2)})\")"

    # LPAD(col, n, char)
    m = re.match(r'LPAD\(\s*(\w+)\s*,\s*(\d+)\s*,\s*[\'"](.+?)[\'"]\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.lpad(F.col('{col}'), {m.group(2)}, '{m.group(3)}')"

    # RPAD(col, n, char)
    m = re.match(r'RPAD\(\s*(\w+)\s*,\s*(\d+)\s*,\s*[\'"](.+?)[\'"]\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.rpad(F.col('{col}'), {m.group(2)}, '{m.group(3)}')"

    # LTRIM(col)
    m = re.match(r'LTRIM\(\s*(\w+)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.ltrim(F.col('{col}'))"

    # RTRIM(col)
    m = re.match(r'RTRIM\(\s*(\w+)\s*\)', first, re.I)
    if m:
        col = tgt_col if m.group(1) == "val" else m.group(1)
        return f"F.rtrim(F.col('{col}'))"

    # Chained replace: replace("old","new");replace("old2","new2")
    replace_matches = REPLACE_CHAIN_RE.findall(first)
    if replace_matches:
        lines_parts = [f"F.col('{tgt_col}')"]
        for old_val, new_val in replace_matches:
            prev = lines_parts[-1]
            lines_parts[-1] = f"F.regexp_replace({prev}, '{old_val}', '{new_val}')"
        return lines_parts[-1]

    # Aggregate: SUM/COUNT/AVG/MIN/MAX
    m = re.match(r'(SUM|COUNT|AVG|MIN|MAX)\(\s*(\w+)\s*\)', first, re.I)
    if m:
        func = m.group(1).lower()
        col = tgt_col if m.group(2) == "val" else m.group(2)
        return f"F.{func}(F.col('{col}'))"

    # Inline Python UDF: (value): ... return ...
    if first.startswith("(value):"):
        udf_body = first[8:].strip()
        return f'F.expr("{tgt_col}")  # TODO: implement UDF: {udf_body[:50]}'

    # CONCAT(a, b, ...)
    m = re.match(r'CONCAT\((.+)\)', first, re.I)
    if m:
        args = [a.strip() for a in re.split(r',(?![^()]*\))', m.group(1))]
        py_args = [_lit_or_col(a, tgt_col) for a in args]
        return f"F.concat({', '.join(py_args)})"

    # String concatenation: col1 + ' ' + col2
    if '+' in first and ("'" in first or '"' in first):
        tokens = re.split(r'\s*\+\s*', first)
        parts = []
        for tok in tokens:
            tok = tok.strip()
            if tok == "val":
                parts.append(f"F.col('{tgt_col}')")
            elif (tok.startswith("'") and tok.endswith("'")) or (tok.startswith('"') and tok.endswith('"')):
                parts.append(f'F.lit("{tok[1:-1]}")')
            else:
                parts.append(f"F.col('{tok}')")
        return f"F.concat({', '.join(parts)})"

    # Python builtins: str(col), str(col).zfill(n)
    if 'str(' in first:
        m = re.match(r'^str\(\s*(\w+)\s*\)\.zfill\(\s*(\d+)\s*\)$', first)
        if m:
            col = tgt_col if m.group(1) == "val" else m.group(1)
            return f"F.lpad(F.col('{col}').cast('string'), {m.group(2)}, '0')"
        m = re.match(r'^str\(\s*(\w+)\s*\)$', first)
        if m:
            col = tgt_col if m.group(1) == "val" else m.group(1)
            return f"F.col('{col}').cast('string')"

    # Bare column name
    if re.match(r'^\w+$', first):
        col = tgt_col if first == "val" else first
        return f"F.col('{col}')"

    # Fallback: F.expr
    resolved = re.sub(r'\bval\b', tgt_col, first)
    safe = resolved.replace("\\", "\\\\").replace('"', '\\"')
    safe = safe.replace("IFNULL(", "COALESCE(").replace("ifnull(", "coalesce(")
    return f'F.expr("{safe}")'


def build_transform_py(server: ServerData, parsed: ParsedExcel) -> str:
    """Generate 04_transform.py — V3 style (single DF, COPY skipped)."""
    tgt_table = f"{parsed.target_db}.{parsed.target_table}"
    tgt_cols = [c["name"] for c in parsed.target_columns]
    tgt_col_types = {c["name"]: c["dtype"] for c in parsed.target_columns}

    L: list[str] = []
    a = L.append

    # Header
    a('"""')
    a(f"04_transform.py  —  {server.name} -> {tgt_table}")
    a(f"Generated: {datetime.now():%Y-%m-%d %H:%M}")
    a(f"Approach: V3 (Combined Aliased Extract)")
    a("")
    a("Input DataFrame columns are already aliased to target names by the")
    a("extract SQL. COPY/RENAME transforms are skipped.")
    a('"""')
    a("from __future__ import annotations")
    a("import logging")
    a("from pyspark.sql import DataFrame")
    a("import pyspark.sql.functions as F")
    a("from pyspark.sql.types import (")
    a("    BooleanType, DecimalType, DoubleType,")
    a("    IntegerType, LongType, StringType, TimestampType,")
    a(")")
    a("")
    a("logger = logging.getLogger(__name__)")
    a("")
    a("")
    a("def apply_transforms(df: DataFrame) -> DataFrame:")
    a('    """')
    a(f"    Apply transforms for server '{server.name}'.")
    a(f"    Target: {tgt_table}")
    a("")
    a("    Input DF columns are target-named (SQL aliased).")
    a("    COPY/RENAME transforms are no-ops.")
    a('    """')
    a("    logger.info('=' * 70)")
    a(f"    logger.info('START TRANSFORM | {server.name} -> {tgt_table}')")
    a("    logger.info('  Input cols: %s', df.columns)")
    a("")

    # Per-mapping transforms
    nn_cols: list[str] = []

    # Build source->target lookup for resolving raw source col names in derived rules.
    # Keep both table-qualified and name-only views; name-only is only used when unambiguous.
    # Include direct, rename, cast, AND derived mappings where the src col was
    # aliased to target in SQL (i.e. src wasn't a NULL placeholder)
    src_to_tgt_by_key: dict[tuple[str, str], str] = {}
    src_to_tgt_by_name: dict[str, set[str]] = {}
    _seen_src_derived: set[tuple[str, str]] = set()
    for cm in server.mappings:
        src_key = _src_key(cm.src_table, cm.src_col_name)
        src_name = (cm.src_col_name or "").strip().lower()
        if cm.transform_type in ("direct", "rename", "cast") and cm.src_col_name:
            src_to_tgt_by_key[src_key] = cm.tgt_col_name
            src_to_tgt_by_name.setdefault(src_name, set()).add(cm.tgt_col_name)
        elif cm.transform_type == "derived" and cm.src_col_name:
            # First derived mapping for a table+source col gets aliased in SQL
            if src_key not in src_to_tgt_by_key and src_key not in _seen_src_derived:
                src_to_tgt_by_key[src_key] = cm.tgt_col_name
                src_to_tgt_by_name.setdefault(src_name, set()).add(cm.tgt_col_name)
            _seen_src_derived.add(src_key)

    reserved_rule_tokens = {
        "if", "else", "and", "or", "not", "none", "true", "false",
        "upper", "lower", "trim", "round", "cast", "concat", "coalesce",
        "ifnull", "isnull", "substring", "left", "right", "lpad", "rpad",
        "ltrim", "rtrim", "sum", "count", "avg", "min", "max", "case",
        "when", "then", "end", "as", "current_timestamp", "current_date", "val",
    }

    for cm in server.mappings:
        tt = cm.transform_type
        tgt = cm.tgt_col_name
        src = cm.src_col_name
        rule = cm.transform_rule
        nh = cm.null_handling
        note = cm.notes
        cmt = f"  # {note}" if note else ""

        if tt == "drop":
            a(f"    # DROP: {src} — excluded from target{cmt}")
            a("")
            continue

        if tt == "constant":
            a(f"    # CONSTANT: → {tgt}{cmt}")
            a(f"    df = df.withColumn('{tgt}', {_const_spark(rule)})")
            a("")
            continue

        if tt in ("direct", "rename"):
            # Already aliased in SQL — SKIP
            a(f"    # {tt.upper()}: {src} → {tgt} — already aliased in SQL{cmt}")
            # Just log, no transform needed
            a("")

            # Handle null_handling
            if nh and nh.lower() not in ("error", ""):
                if nh.lower() == "fill_zero":
                    a(f"    df = df.fillna({{'{tgt}': 0}})")
                elif nh.lower() == "fill_empty":
                    a(f"    # NULL values in {tgt} remain as NULL")
                elif nh.lower().startswith("fill:"):
                    fill_val = nh[5:]
                    try:
                        fill_val = int(fill_val)
                        a(f"    df = df.fillna({{'{tgt}': {fill_val}}})")
                    except ValueError:
                        a(f"    df = df.fillna({{'{tgt}': '{fill_val}'}})")
                a("")
            elif nh and nh.lower() == "error":
                nn_cols.append(tgt)
            continue

        # Non-trivial transform
        a(f"    # {tt.upper()}: {src} → {tgt}{cmt}")
        # Resolve any raw source column names in the rule to their target aliases
        # (V3: DF is target-named, raw src names won't exist as columns)
        resolved_rule = rule
        if rule:
            current_key = _src_key(cm.src_table, src)

            def _replace_src_token(match: re.Match) -> str:
                token = match.group(0)
                token_l = token.lower()
                if token_l in reserved_rule_tokens:
                    return token
                # Prefer table-qualified resolution for this mapping row
                if current_key in src_to_tgt_by_key and token_l == current_key[1]:
                    return src_to_tgt_by_key[current_key]
                # Only replace by name when the mapping is unambiguous across all tables
                candidates = src_to_tgt_by_name.get(token_l, set())
                if len(candidates) == 1:
                    return next(iter(candidates))
                return token

            resolved_rule = re.sub(r'\b[a-z_][a-z0-9_]*\b', _replace_src_token, resolved_rule, flags=re.I)
        # src_col used for 'val' substitution — resolve to its target name
        src_as_tgt = src_to_tgt_by_key.get(_src_key(cm.src_table, src), tgt)
        a(f"    df = df.withColumn('{tgt}', {_spark_expr(resolved_rule, src_as_tgt, tgt)})")

        # Null handling
        if nh and nh.lower() not in ("error", ""):
            if nh.lower() == "fill_zero":
                a(f"    df = df.fillna({{'{tgt}': 0}})")
            elif nh.lower() == "fill_empty":
                a(f"    # NULL values in {tgt} remain as NULL")
            elif nh.lower().startswith("fill:"):
                fill_val = nh[5:]
                try:
                    fill_val_num = int(fill_val)
                    a(f"    df = df.fillna({{'{tgt}': {fill_val_num}}})")
                except ValueError:
                    a(f"    df = df.fillna({{'{tgt}': '{fill_val}'}})")
        elif nh and nh.lower() == "error":
            nn_cols.append(tgt)
        a("")

    # ── Step 3: Global transforms (applied after all column-level transforms) ──
    if server.global_transforms:
        a("    # ═══════════════════════════════════════════════════════════════")
        a("    # GLOBAL TRANSFORMS — applied to filtered column sets in order")
        a("    # ═══════════════════════════════════════════════════════════════")
        a("")

        for gt in sorted(server.global_transforms, key=lambda g: g.order):
            op = gt.operation
            params = gt.parameters
            cond = gt.condition
            gt_note = gt.notes
            cmt = f"  # {gt_note}" if gt_note else ""

            a(f"    # Global #{gt.order}: {op} | condition={cond or 'all'}{cmt}")

            # Resolve condition → list of target columns
            if not cond:
                col_list_expr = f"{tgt_cols!r}"
            elif cond.lower().startswith("dtype:"):
                dtype_filter = cond[6:].strip().upper()
                dtype_families = {
                    "VARCHAR": {"VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "TEXT", "STRING"},
                    "INT": {"INT", "INTEGER", "TINYINT", "SMALLINT", "BIGINT", "NUMBER"},
                    "DECIMAL": {"DECIMAL", "NUMERIC", "NUMBER", "FLOAT", "DOUBLE"},
                    "DATE": {"DATE", "DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ"},
                }
                family = dtype_families.get(dtype_filter, {dtype_filter})
                matching = [c["name"] for c in parsed.target_columns
                           if c["dtype"].upper().split("(")[0].strip() in family]
                col_list_expr = f"{matching!r}"
            elif cond.lower().startswith("col:"):
                pattern = cond[4:].strip()
                from fnmatch import fnmatch
                matching = [c["name"] for c in parsed.target_columns if fnmatch(c["name"], pattern)]
                col_list_expr = f"{matching!r}"
            elif cond.lower().startswith("exclude:"):
                excludes = {x.strip().upper() for x in cond[8:].split(",")}
                matching = [c["name"] for c in parsed.target_columns if c["name"] not in excludes]
                col_list_expr = f"{matching!r}"
            else:
                col_list_expr = f"{tgt_cols!r}"

            a(f"    _gt_cols = {col_list_expr}")
            a(f"    for _c in _gt_cols:")

            if op == "TRIM":
                a(f"        if _c in df.columns:")
                a(f"            df = df.withColumn(_c, F.trim(F.col(_c)))")
            elif op == "UPPER":
                a(f"        if _c in df.columns:")
                a(f"            df = df.withColumn(_c, F.upper(F.col(_c)))")
            elif op == "LOWER":
                a(f"        if _c in df.columns:")
                a(f"            df = df.withColumn(_c, F.lower(F.col(_c)))")
            elif op == "STRIP_WHITESPACE":
                a(f"        if _c in df.columns:")
                a(f"            df = df.withColumn(_c, F.regexp_replace(F.col(_c), r'\\s+', ' '))")
                a(f"            df = df.withColumn(_c, F.trim(F.col(_c)))")
            elif op == "REPLACE":
                rm = re.match(r'"([^"]*)"\s*,\s*"([^"]*)"', params)
                if rm:
                    old_v, new_v = rm.group(1), rm.group(2)
                    a(f"        if _c in df.columns:")
                    a(f"            df = df.withColumn(_c, F.regexp_replace(F.col(_c), F.lit('{old_v}'), F.lit('{new_v}')))")
                else:
                    a(f"        pass  # TODO: parse REPLACE params: {params}")
            elif op == "REGEX_REPLACE":
                rm = re.match(r'"([^"]*)"\s*,\s*"([^"]*)"', params)
                if rm:
                    pattern_v, repl_v = rm.group(1), rm.group(2)
                    a(f"        if _c in df.columns:")
                    a(f"            df = df.withColumn(_c, F.regexp_replace(F.col(_c), '{pattern_v}', '{repl_v}'))")
                else:
                    a(f"        pass  # TODO: parse REGEX_REPLACE params: {params}")
            elif op == "CAST":
                a(f"        if _c in df.columns:")
                a(f"            df = df.withColumn(_c, F.col(_c).cast('{params}'))")
            else:
                a(f"        pass  # TODO: unrecognized global op: {op}")

            a(f"    logger.debug('  [global #{gt.order}] {op} applied to %d cols', len(_gt_cols))")
            a("")

    # Reorder to target schema
    a(f"    # Reorder to target schema")
    a(f"    _exp = {tgt_cols!r}")
    a(f"    _pres = [c for c in _exp if c in df.columns]")
    a(f"    _miss = [c for c in _exp if c not in df.columns]")
    a(f"    if _miss:")
    a(f"        logger.warning('  Missing target cols: %s', _miss)")
    a(f"    df = df.select(*_pres)")

    # Null validation
    if nn_cols:
        a("")
        a("    # Batch null validation")
        a(f"    _nn_cols = {nn_cols!r}")
        a("    _null_exprs = [F.count(F.when(F.col(c).isNull(), 1)).alias(f'_null_{c}') for c in _nn_cols if c in df.columns]")
        a("    _null_exprs.append(F.count('*').alias('_total_rows'))")
        a("    _null_result = df.select(*_null_exprs).first()")
        a("    _null_violations = {c: _null_result[f'_null_{c}'] for c in _nn_cols if c in df.columns and _null_result[f'_null_{c}'] > 0}")
        a("    if _null_violations:")
        a("        for _col, _cnt in _null_violations.items():")
        a("            logger.error(\"  NULL in non-nullable '%s': %d rows\", _col, _cnt)")
        a("        raise ValueError(f'NULL values in non-nullable columns: {_null_violations}')")
        a("    logger.info('  Null check passed for %d cols', len(_nn_cols))")
        a("    logger.info('  Output rows: %d', _null_result['_total_rows'])")
    else:
        a("")
        a("    _row_count = df.count()")
        a("    logger.info('  Output rows: %d', _row_count)")

    a(f"    logger.info('  Output cols: %s', df.columns)")
    a(f"    logger.info('END TRANSFORM')")
    a(f"    logger.info('=' * 70)")
    a(f"    return df")

    return "\n".join(L) + "\n"


# ═══════════════════════════════════════════════════════════════════════════
# Extract Target SQL
# ═══════════════════════════════════════════════════════════════════════════

def build_extract_target(parsed: ParsedExcel) -> str:
    fqn = f"{parsed.target_db}.{parsed.target_table}"
    cols = [c["name"] for c in parsed.target_columns]
    col_list = ",\n".join(f"    {c}" for c in cols)
    return "\n".join([
        "-- ============================================================",
        f"-- EXTRACT TARGET | {fqn}",
        f"-- Dialect: Snowflake",
        f"-- Generated: {datetime.now():%Y-%m-%d %H:%M}",
        "-- ============================================================",
        "",
        "SELECT",
        col_list,
        f"FROM {fqn};",
    ])


# ═══════════════════════════════════════════════════════════════════════════
# Pipeline Log
# ═══════════════════════════════════════════════════════════════════════════

def build_pipeline_log(server: ServerData, parsed: ParsedExcel) -> str:
    tgt = f"{parsed.target_db}.{parsed.target_table}"
    lines = [
        f"# Pipeline Log — {server.name}",
        "",
        f"> Generated: {datetime.now():%Y-%m-%d %H:%M}",
        f"> Server: `{server.name}`",
        f"> Target: `{tgt}`",
        f"> Approach: V3 (Combined Aliased Extract)",
        f"> Source tables: {len(server.source_tables)}",
        f"> Joins: {len(server.joins)}",
        "",
        "---",
        "",
        "## Source Tables",
        "",
    ]

    for table_name, cms in server.source_tables.items():
        db = cms[0].src_db
        lines.append(f"### `{_source_table_fqn(db, cms[0].src_schema, table_name)}`")
        lines.append("")
        lines.append("| Source Column | Type | → Target | Transform | SQL Note |")
        lines.append("|---|---|---|---|---|")
        # Track src cols already projected to detect shared-source derived cols
        _proj: dict[str, str] = {}
        for cm in cms:
            if cm.transform_type in ("direct", "rename", "cast") and cm.src_col_name:
                _proj[cm.src_col_name] = cm.tgt_col_name
        for cm in cms:
            sql_note = ""
            if cm.transform_type == "derived" and cm.src_col_name in _proj:
                sql_note = f"NULL placeholder in SQL; computed from `{_proj[cm.src_col_name]}`"
            lines.append(f"| `{cm.src_col_name}` | `{cm.src_dtype}` | `{cm.tgt_col_name}` | {cm.transform_type} | {sql_note} |")
        lines.append("")

    if server.joins:
        lines.append("## Joins")
        lines.append("")
        lines.append("| Alias | Table | Type | Left Key | Right Key | Fetch |")
        lines.append("|---|---|---|---|---|---|")
        for jd in server.joins:
            fc = ", ".join(jd.fetch_columns[:5])
            if len(jd.fetch_columns) > 5:
                fc += "..."
            lines.append(f"| `{jd.join_alias}` | `{jd.join_table}` | {jd.join_type} | `{jd.left_key}` | `{jd.right_key}` | {fc} |")
        lines.append("")

    lines.append("## Column Mappings")
    lines.append("")
    lines.append("| # | Source | Transform | Target | Null Handling |")
    lines.append("|---|---|---|---|---|")
    for i, cm in enumerate(server.mappings, 1):
        src = f"`{cm.src_col_name}`" if cm.src_col_name else "(constant)"
        rule_info = f"{cm.transform_type}"
        if cm.transform_rule:
            rule_info += f": `{cm.transform_rule[:40]}`"
        lines.append(f"| {i} | {src} | {rule_info} | `{cm.tgt_col_name}` | {cm.null_handling or '-'} |")

    lines.append("")
    lines.append("---")

    if server.global_transforms:
        lines.append("")
        lines.append("## Global Transforms")
        lines.append("")
        lines.append("| # | Operation | Parameters | Condition | Notes |")
        lines.append("|---|---|---|---|---|")
        for gt in sorted(server.global_transforms, key=lambda g: g.order):
            lines.append(f"| {gt.order} | **{gt.operation}** | `{gt.parameters or '-'}` | `{gt.condition or 'all'}` | {gt.notes or '-'} |")
        lines.append("")
        lines.append("---")

    lines.append("")
    lines.append("## Verification Checklist")
    lines.append("")
    lines.append("- [ ] Row count matches source extract")
    lines.append("- [ ] No NULLs in NOT NULL columns")
    lines.append("- [ ] Derived columns spot-checked")
    lines.append("- [ ] PK uniqueness confirmed")
    lines.append("- [ ] Join fan-out checked")
    lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# Main Generator
# ═══════════════════════════════════════════════════════════════════════════

def generate_runbook(excel_path: str | Path, output_dir: str | Path = "etl_output") -> None:
    path = Path(excel_path)
    out_base = Path(output_dir)

    # Check if parsed JSON already exists
    parsed_json_path = path.with_name(path.stem + "_parsed.json")
    if parsed_json_path.exists():
        print(f"\n  Found existing parsed JSON: {parsed_json_path.name}")
        print(f"  Delete it to force re-parse from Excel.")

    print(f"\n  Parsing: {path.name}")
    parsed = parse_excel(path)

    # Output folder = target_table name (sanitized)
    table_folder = parsed.target_table.replace(".", "_")
    table_dir = out_base / table_folder
    table_dir.mkdir(parents=True, exist_ok=True)

    # Shared files
    (table_dir / "02_create_target_sf.sql").write_text(
        build_target_ddl(parsed), encoding="utf-8"
    )
    print(f"  OK  02_create_target_sf.sql")

    (table_dir / "05_extract_target_sf.sql").write_text(
        build_extract_target(parsed), encoding="utf-8"
    )
    print(f"  OK  05_extract_target_sf.sql")

    # Per-server files
    for srv_name, server in parsed.servers.items():
        srv_dir = table_dir / srv_name
        srv_dir.mkdir(parents=True, exist_ok=True)

        # Source DDLs
        generate_source_ddls(server, srv_dir)
        n_ddls = len(list((srv_dir / "source_tables").glob("*.sql")))
        print(f"  OK  {srv_name}/source_tables/ ({n_ddls} DDL files)")

        # Extract SQL
        extract_sql = build_extract_sql_v3(server, parsed)
        (srv_dir / "03_extract_source.sql").write_text(extract_sql, encoding="utf-8")
        print(f"  OK  {srv_name}/03_extract_source.sql")

        # Transform
        transform_py = build_transform_py(server, parsed)
        (srv_dir / "04_transform.py").write_text(transform_py, encoding="utf-8")
        print(f"  OK  {srv_name}/04_transform.py")

        # Pipeline log
        log_md = build_pipeline_log(server, parsed)
        (srv_dir / "06_pipeline_log.md").write_text(log_md, encoding="utf-8")
        print(f"  OK  {srv_name}/06_pipeline_log.md")

    print(f"\n  Done. Output -> {table_dir}/")


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

# Edit these variables before running:
EXCEL_FILES = [
    "analytics_dw.public.dim_vehicle_master.xlsx",
    "analytics_dw.public.fact_commercial.xlsx",
    "analytics_dw.public.fact_production.xlsx",
]
OUTPUT_DIR = "etl_output"


if __name__ == "__main__":
    out = Path(OUTPUT_DIR)
    out.mkdir(parents=True, exist_ok=True)

    for excel_file in EXCEL_FILES:
        p = Path(excel_file)
        if not p.exists():
            print(f"  SKIP: {p.name} not found")
            continue
        try:
            generate_runbook(p, out)
        except Exception as e:
            print(f"  ERROR: {p.name}: {e}")
            import traceback
            traceback.print_exc()





