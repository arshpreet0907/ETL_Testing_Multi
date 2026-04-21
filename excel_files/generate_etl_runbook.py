"""
generate_etl_runbook.py
-----------------------
Reads JSON from excel_schema_parser.py and generates per-table:
  01_create_source_table.sql   -- MySQL
  02_create_target_sf.sql      -- Snowflake
  03_extract_source.sql        -- MySQL
  04_transform.py              -- PySpark
  05_extract_target_sf.sql     -- Snowflake
  06_pipeline_log.md
Plus 00_summary.md at the top level.

Note: MySQL target files (create_target_ms, extract_target_ms) have been removed.

Usage:
    python generate_etl_runbook.py
"""

from __future__ import annotations
import argparse, json, re, sys
from pathlib import Path
from datetime import datetime

# ── Dialect config ─────────────────────────────────────────────────────────

DIALECT_NOTES = {
    "mysql":     "MySQL 8+",
    "snowflake": "Snowflake",
    "postgres":  "PostgreSQL 14+",
}

# ── Dtype normalisation ────────────────────────────────────────────────────

_MYSQL_MAP = {
    "int8":    "TINYINT",
    "int16":   "SMALLINT",
    "int32":   "INT",
    "int64":   "BIGINT",
    "float32": "FLOAT",
    "float64": "DOUBLE",
    "string":  "TEXT",
    "str":     "TEXT",
    "bool":    "TINYINT(1)",
    "boolean": "TINYINT(1)",
    "integer": "INT",
    "number":  "DOUBLE",
    "datetime":"DATETIME",
}

_SNOWFLAKE_MAP = {
    "int8":    "NUMBER(3,0)",
    "int16":   "NUMBER(5,0)",
    "int32":   "NUMBER(10,0)",
    "int64":   "NUMBER(19,0)",
    "float32": "FLOAT",
    "float64": "FLOAT",
    "string":  "VARCHAR",
    "str":     "VARCHAR",
    "bool":    "BOOLEAN",
    "boolean": "BOOLEAN",
    "integer": "NUMBER(10,0)",
    "int":     "NUMBER(10,0)",
    "datetime":"TIMESTAMP_NTZ",
    "datetime2":"TIMESTAMP_NTZ",
    "timestamp":"TIMESTAMP_NTZ",
    "timestamp_ntz":"TIMESTAMP_NTZ",
    "timestampntz":"TIMESTAMP_NTZ",
}

_POSTGRES_MAP = {
    "int8":    "SMALLINT",
    "int16":   "SMALLINT",
    "int32":   "INTEGER",
    "int64":   "BIGINT",
    "float32": "REAL",
    "float64": "DOUBLE PRECISION",
    "string":  "TEXT",
    "str":     "TEXT",
    "bool":    "BOOLEAN",
    "boolean": "BOOLEAN",
    "integer": "INTEGER",
    "datetime":"TIMESTAMP",
}

_DTYPE_MAP = {
    "mysql":     _MYSQL_MAP,
    "snowflake": _SNOWFLAKE_MAP,
    "postgres":  _POSTGRES_MAP,
}

def _normalise_dtype(raw: str, dialect: str) -> str:
    if not raw:
        return "VARCHAR(255)" if dialect == "mysql" else "VARCHAR"
    d = raw.strip()
    key = d.lower()
    dmap = _DTYPE_MAP[dialect]

    if key in dmap:
        return dmap[key]

    base = re.match(r'^([a-z_]+)', key)
    base_key = base.group(1) if base else key
    if base_key in dmap:
        precision = re.search(r'\([\d,\s]+\)', d)
        mapped = dmap[base_key]
        if precision and '(' not in mapped:
            return mapped + precision.group(0)
        return mapped

    if dialect == "mysql":
        d = re.sub(r'TIMESTAMP_NTZ', 'DATETIME', d, flags=re.IGNORECASE)
        d = re.sub(r'\bINTEGER\b', 'INT', d, flags=re.IGNORECASE)
        return d

    if dialect == "snowflake":
        d = re.sub(r'\bINTEGER\b', 'NUMBER(10,0)', d, flags=re.IGNORECASE)
        d = re.sub(r'\bINT\b(?!\()', 'NUMBER(10,0)', d, flags=re.IGNORECASE)
        d = re.sub(r'\bDECIMAL\b', 'NUMBER', d, flags=re.IGNORECASE)
        d = re.sub(r'\bDATETIME\b', 'TIMESTAMP_NTZ', d, flags=re.IGNORECASE)
        d = re.sub(r'\bTIMESTAMP\b(?!_NTZ)', 'TIMESTAMP_NTZ', d, flags=re.IGNORECASE)
        d = re.sub(r'\bVARCHAR\b(?!\()', 'VARCHAR(256)', d, flags=re.IGNORECASE)
        return d

    if dialect == "postgres":
        d = re.sub(r'\bINT\b(?!\()', 'INTEGER', d, flags=re.IGNORECASE)
        d = re.sub(r'\bDECIMAL\b', 'NUMERIC', d, flags=re.IGNORECASE)
        d = re.sub(r'\bDATETIME\b', 'TIMESTAMP', d, flags=re.IGNORECASE)
        d = re.sub(r'\bVARCHAR\b(?!\()', 'VARCHAR(255)', d, flags=re.IGNORECASE)
        return d

    return d


def _ts_default(dialect: str) -> str:
    return {
        "mysql":     "CURRENT_TIMESTAMP",
        "snowflake": "CURRENT_TIMESTAMP()",
        "postgres":  "NOW()",
    }.get(dialect, "CURRENT_TIMESTAMP")

def _pk_constraint(pk_cols: list[str]) -> str:
    return f"    PRIMARY KEY ({', '.join(pk_cols)})" if pk_cols else ""

def _not_null(nullable: bool) -> str:
    return "" if nullable else " NOT NULL"


# ── CREATE SOURCE TABLE ────────────────────────────────────────────────────

def build_create_source(spec: dict) -> str:
    dialect = "mysql"
    tname   = spec["source_schema"]["table"]
    pks     = spec["source_schema"]["primary_keys"]
    columns = spec["source_schema"]["columns"]

    col_lines = []
    for c in columns:
        dtype   = _normalise_dtype(c["dtype"], dialect)
        nn      = _not_null(c["nullable"])
        pk_flag = "  -- PK" if c["is_pk"] else ""
        col_lines.append((f"    {c['name']:<34} {dtype:<26}{nn}", pk_flag))

    lines_out = []
    for i, (body, flag) in enumerate(col_lines):
        comma = "," if i < len(col_lines) - 1 or pks else ""
        lines_out.append(f"{body}{comma}{flag}")

    block = "\n".join(lines_out)
    if pks:
        block += f"\n{_pk_constraint(pks)}"

    return "\n".join([
        f"-- ============================================================",
        f"-- Source table : {tname}",
        f"-- Dialect      : {DIALECT_NOTES[dialect]}",
        f"-- Generated    : {datetime.now():%Y-%m-%d %H:%M}",
        f"-- ============================================================",
        f"",
        f"CREATE TABLE IF NOT EXISTS {tname} (",
        block,
        f");",
    ])


# ── CREATE TARGET TABLE ────────────────────────────────────────────────────

def build_create_target(spec: dict, dialect: str) -> str:
    tname   = spec["target_schema"]["table"]
    pks     = spec["target_schema"]["primary_keys"]
    columns = spec["target_schema"]["columns"]
    m_idx   = {m["target_column"]: m for m in spec["column_mappings"] if m["target_column"]}

    col_lines = []
    for c in columns:
        raw_type = c.get("effective_dtype") or c["dtype"]
        dtype    = _normalise_dtype(raw_type, dialect)
        nn       = _not_null(c["nullable"])
        m        = m_idx.get(c["name"], {})

        default = ""
        if c.get("default_val"):
            dv = str(c["default_val"]).strip().upper()
            if dv in ("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()"):
                default = f" DEFAULT {_ts_default(dialect)}"
            else:
                default = f" DEFAULT {c['default_val']}"

        comment_parts = []
        if c["name"] in pks:
            comment_parts.append("PK")
        if m.get("notes"):
            comment_parts.append(m["notes"])
        elif m.get("transform_type") == "derived":
            comment_parts.append(f"derived: {m.get('transform_rule','')}")
        elif m.get("transform_type") == "constant":
            comment_parts.append(f"constant: {m.get('transform_rule','')}")
        elif m.get("join_columns"):
            comment_parts.append(f"from join: {', '.join(m['join_columns'])}")
        comment = f"  -- {'; '.join(comment_parts)}" if comment_parts else ""

        col_lines.append((f"    {c['name']:<36} {dtype:<26}{nn}{default}", comment))

    lines_out = []
    for i, (body, comment) in enumerate(col_lines):
        comma = "," if i < len(col_lines) - 1 or pks else ""
        lines_out.append(f"{body}{comma}{comment}")

    block = "\n".join(lines_out)
    if pks:
        block += f"\n{_pk_constraint(pks)}"

    return "\n".join([
        f"-- ============================================================",
        f"-- Target table : {tname}",
        f"-- Dialect      : {DIALECT_NOTES[dialect]}",
        f"-- Generated    : {datetime.now():%Y-%m-%d %H:%M}",
        f"-- ============================================================",
        f"",
        f"CREATE TABLE IF NOT EXISTS {tname} (",
        block,
        f");",
    ])


# ── EXTRACT SQL ────────────────────────────────────────────────────────────

def build_extract_sql(spec: dict) -> str:
    dialect = "mysql"
    tname         = spec["source_schema"]["table"]
    src_col_names = [c["name"] for c in spec["source_schema"]["columns"]]
    joins         = spec.get("joins", [])
    pks           = spec["source_schema"]["primary_keys"]

    needed: set[str] = set()
    # Always include join keys first (even if marked as "drop")
    for jd in joins:
        needed.add(jd["left_key"])
    
    for m in spec["column_mappings"]:
        if m["transform_type"] == "drop":
            continue
        for sc in m["source_columns"]:
            needed.add(sc)
        if m.get("transform_rule"):
            for word in re.findall(r'\b[a-z_][a-z0-9_]*\b', m["transform_rule"]):
                if word in src_col_names:
                    needed.add(word)
    if not needed:
        needed = set(src_col_names)

    dropped = [m["source_columns"][0] for m in spec["column_mappings"]
               if m["transform_type"] == "drop" and m["source_columns"]]

    use_alias = bool(joins)
    ma = "m" if use_alias else ""
    ap = f"{ma}." if use_alias else ""

    select_parts = []
    for col in src_col_names:
        if col in needed:
            select_parts.append(f"    {ap}{col}")
    for jd in joins:
        for col in jd.get("fetch_columns", []):
            aliased = f"{jd['join_alias']}_{col}"
            select_parts.append(f"    {jd['join_alias']}.{col} AS {aliased}")

    # removing order by in extract ORDER BY omitted — Spark join handles ordering internally.
    # order_cols = [f"{ap}{p}" for p in pks] if pks else []
    # order_clause = f"\nORDER BY {', '.join(order_cols)}" if order_cols else ""

    from_line = f"FROM {tname}{(' ' + ma) if ma else ''}"
    join_lines = []
    for jd in joins:
        db_prefix = f"{jd['join_db']}." if jd.get("join_db") else ""
        join_lines.append(
            f"{jd['join_type']} JOIN {db_prefix}{jd['join_table']} {jd['join_alias']}"
            f"\n    ON {ma}.{jd['left_key']} = {jd['join_alias']}.{jd['right_key']}"
        )

    comment_lines = [
        "-- ============================================================",
        f"-- STEP 3 : EXTRACT  |  source: {tname}",
        "-- ============================================================",
        f"-- Dialect    : {DIALECT_NOTES[dialect]}",
        f"-- Generated  : {datetime.now():%Y-%m-%d %H:%M}",
        "--",
        "-- Columns fetched from main source table (dropped cols excluded).",
    ]
    if dropped:
        comment_lines.append(f"-- Excluded (drop): {', '.join(dropped)}")
    if joins:
        comment_lines.append("--")
        comment_lines.append("-- Enrichment joins:")
        for jd in joins:
            db_prefix = f"{jd['join_db']}." if jd.get("join_db") else ""
            comment_lines.append(
                f"--   {jd['join_type']:<6} JOIN {db_prefix}{jd['join_table']}"
                f"  AS {jd['join_alias']}"
                f"  ON {ma}.{jd['left_key']} = {jd['join_alias']}.{jd['right_key']}"
            )
            comment_lines.append(
                f"--          fetches: {', '.join(jd['fetch_columns'])}"
            )

    # ── Detect date columns present in source schema ───────────────────────
    src_col_names_lower = [c["name"].lower() for c in spec["source_schema"]["columns"]]
    _has_created = any(c["name"].lower() == "created_at" for c in spec["source_schema"]["columns"])
    _has_updated = any(c["name"].lower() == "updated_at" for c in spec["source_schema"]["columns"])

    # ── Build WHERE placeholder block ──────────────────────────────────────
    pk_col_ref = f"{ap}{pks[0]}" if pks else None
    where_placeholder_lines = [
        "--",
        "-- [[FILTER_PLACEHOLDER]]",
        "-- The following WHERE clause is injected at runtime by custom_execution.py.",
        "-- Do NOT edit this block manually — it is overwritten on each run.",
        "--",
        "-- PK filter modes (PK_FILTER_MODE in custom_execution.py):",
        f"--   full    : no PK WHERE clause",
    ]
    if pk_col_ref:
        where_placeholder_lines += [
            f"--   pk_range: WHERE {pk_col_ref} >= {{LOWER}} AND/OR {pk_col_ref} <= {{UPPER}}",
            f"--             (either bound may be None → single-sided limit)",
            f"--   pk_set  : WHERE {pk_col_ref} IN ({{PK_SET}})",
        ]
    where_placeholder_lines += [
        "--",
        "-- Date watermark modes (DATE_WATERMARK_MODE in custom_execution.py):",
        "--   full    : no date WHERE clause",
        "--   range   : WHERE {{DATE_FROM_COL}} >= '{{DATE_FROM}}'",
        "--             AND/OR {{DATE_TO_COL}} <= '{{DATE_TO}}'",
        "--             (either bound may be None → single-sided limit)",
        "--",
        "-- [[/FILTER_PLACEHOLDER]]",
    ]
    if _has_created or _has_updated:
        date_col_info = []
        if _has_created:
            date_col_info.append("created_at")
        if _has_updated:
            date_col_info.append("updated_at")
        where_placeholder_lines.append(
            f"-- Date cols available in this table: {', '.join(date_col_info)}"
        )

    comment_lines += ["--"] + where_placeholder_lines

    sql_parts = [from_line] + join_lines
    # ORDER BY omitted — Spark join handles ordering internally.
    # full_sql = "\n".join(sql_parts) + order_clause + ";"
    full_sql = "\n".join(sql_parts) + ";"

    return "\n".join(comment_lines) + "\n\nSELECT\n" + ",\n".join(select_parts) + "\n" + full_sql


# ── TRANSFORM Python ───────────────────────────────────────────────────────

def _resolve_val(expr: str, src: str) -> str:
    """Replace the 'val' placeholder with the actual source column reference."""
    # 'val' is a legacy placeholder meaning "the value of the source column"
    return re.sub(r'\bval\b', f"F.col('{src}')", expr)


def _lit_or_col(token: str, src: str) -> str:
    """
    Turn a bare token from a rule into a PySpark expression fragment.
    Strings like 'Y' → F.lit('Y'), numbers → F.lit(n), bare words → F.col('word').
    """
    t = token.strip()
    # String literal  'Y'  or  "Y"
    if (t.startswith("'") and t.endswith("'")) or (t.startswith('"') and t.endswith('"')):
        inner = t[1:-1]
        return f'F.lit("{inner}")'
    # Numeric literal
    try:
        float(t)
        return f"F.lit({t})"
    except ValueError:
        pass
    # Boolean literals
    if t.lower() == "true":
        return "F.lit(True)"
    if t.lower() == "false":
        return "F.lit(False)"
    # Otherwise treat as a column name
    return f"F.col('{t}')"


def _split_concat_chain(expr: str) -> list[str] | None:
    """
    Split a Python-style string concatenation expression on top-level ' + ' operators,
    respecting parentheses and quoted strings.
    Returns a list of token strings, or None if the structure cannot be parsed.
    """
    tokens: list[str] = []
    depth   = 0
    in_sq   = False
    in_dq   = False
    current: list[str] = []
    i = 0
    while i < len(expr):
        ch = expr[i]
        if ch == "'" and not in_dq:
            in_sq = not in_sq
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
        elif not in_sq and not in_dq:
            if ch in ('(', '['):
                depth += 1
            elif ch in (')', ']'):
                depth -= 1
            elif ch == '+' and depth == 0:
                # A concat operator must be surrounded by spaces
                if i > 0 and expr[i - 1] == ' ' and i + 1 < len(expr) and expr[i + 1] == ' ':
                    tokens.append(''.join(current).strip())
                    current = []
                    i += 2  # skip the trailing space
                    continue
        current.append(ch)
        i += 1
    if current:
        tokens.append(''.join(current).strip())
    return tokens if tokens else None


def _rewrite_py_builtins_to_spark(rule: str, src: str) -> str | None:
    """
    Detect and rewrite Python built-in patterns that are invalid in Spark SQL/F.expr.
    Returns a valid PySpark expression string, or None if the rule doesn't match.

    Handled patterns (may be chained with ' + '):
      str(col)          ->  F.col('col').cast('string')
      str(col).zfill(n) ->  F.lpad(F.col('col').cast('string'), n, '0')
      'literal'         ->  F.lit("literal")
      bare_col          ->  F.col('col')

    Example:
      str(fiscal_yr) + '-' + str(fiscal_period).zfill(2)
      -> F.concat(F.col('fiscal_yr').cast('string'),
                  F.lit("-"),
                  F.lpad(F.col('fiscal_period').cast('string'), 2, '0'))
    """
    r = rule.strip()
    # Only engage when the rule contains Python-style str() casting
    if not re.search(r'\bstr\s*\(', r):
        return None

    tokens = _split_concat_chain(r)
    if not tokens:
        return None

    spark_parts: list[str] = []
    for tok in tokens:
        tok = tok.strip()

        # str(col).zfill(n)
        m = re.match(r'^str\(\s*(\w+)\s*\)\.zfill\(\s*(\d+)\s*\)$', tok)
        if m:
            col = src if m.group(1) == "val" else m.group(1)
            spark_parts.append(f"F.lpad(F.col('{col}').cast('string'), {m.group(2)}, '0')")
            continue

        # str(col)
        m = re.match(r'^str\(\s*(\w+)\s*\)$', tok)
        if m:
            col = src if m.group(1) == "val" else m.group(1)
            spark_parts.append(f"F.col('{col}').cast('string')")
            continue

        # String literal  'x'  or  "x"
        if (tok.startswith("'") and tok.endswith("'")) or \
           (tok.startswith('"') and tok.endswith('"')):
            spark_parts.append(f'F.lit("{tok[1:-1]}")')
            continue

        # Numeric literal
        try:
            float(tok)
            spark_parts.append(f"F.lit({tok})")
            continue
        except ValueError:
            pass

        # Bare column name
        if re.match(r'^\w+$', tok):
            col = src if tok == "val" else tok
            spark_parts.append(f"F.col('{col}')")
            continue

        # Unrecognised token — bail out and let the caller fall through
        return None

    if len(spark_parts) == 1:
        return spark_parts[0]
    return f"F.concat({', '.join(spark_parts)})"


def _spark_expr(rule: str, src: str) -> str:
    """Convert a transform rule string to a PySpark F.expr / F.col call."""
    if not rule:
        return f"F.col('{src}')" if src else "F.lit(None)"
    r = rule.strip()
    first = r.split("\n")[0].strip()

    # ── Python ternary with `is None` / `is not None`:
    # e.g. "0 if defect_type_cd is None else 1"
    # e.g. "1 if defect_type_cd is not None else 0"
    m_none = re.match(
        r'^(.+?)\s+if\s+(\w+)\s+is\s+(not\s+)?None\s+else\s+(.+)$',
        first, re.I,
    )
    if m_none:
        true_val  = m_none.group(1).strip()
        col_name  = m_none.group(2).strip()
        is_not    = bool(m_none.group(3))
        false_val = m_none.group(4).strip()
        if col_name == "val":
            col_name = src
        col_expr  = f"F.col('{col_name}')"
        true_lit  = _lit_or_col(true_val,  src)
        false_lit = _lit_or_col(false_val, src)
        if is_not:
            return f"F.when({col_expr}.isNotNull(), {true_lit}).otherwise({false_lit})"
        else:
            return f"F.when({col_expr}.isNull(), {true_lit}).otherwise({false_lit})"

    # ── Python ternary:  <true_val> if <col/val> == <cmp_val> else <false_val>
    # Handles both  "1 if val == 'Y' else 0"  and  "1 if status == 'Y' else 0"
    m = re.match(
        r'^(.+?)\s+if\s+(\w+)\s*(==|!=|<=|>=|<|>)\s*(.+?)\s+else\s+(.+)$',
        first, re.I,
    )
    if m:
        true_val  = m.group(1).strip()
        col_name  = m.group(2).strip()
        op        = m.group(3).strip()
        cmp_val   = m.group(4).strip()
        false_val = m.group(5).strip()
        # Resolve 'val' placeholder → actual source column
        if col_name == "val":
            col_name = src
        col_expr  = f"F.col('{col_name}')"
        true_lit  = _lit_or_col(true_val,  src)
        false_lit = _lit_or_col(false_val, src)
        cmp_lit   = _lit_or_col(cmp_val,   src)
        op_map = {"==": "equalTo", "!=": "notEqual", ">": "gt", "<": "lt", ">=": "geq", "<=": "leq"}
        if op == "==":
            return f"F.when({col_expr} == {cmp_lit}, {true_lit}).otherwise({false_lit})"
        elif op == "!=":
            return f"F.when({col_expr} != {cmp_lit}, {true_lit}).otherwise({false_lit})"
        else:
            return f"F.when({col_expr} {op} {cmp_lit}, {true_lit}).otherwise({false_lit})"

    # ── UPPER(TRIM(col))
    m = re.match(r'UPPER\(\s*TRIM\(\s*(\w+)\s*\)\s*\)', first, re.I)
    if m:
        col = src if m.group(1) == "val" else m.group(1)
        return f"F.upper(F.trim(F.col('{col}')))"
    # ── UPPER(col / val)
    m = re.match(r'UPPER\(\s*(\w+)\s*\)', first, re.I)
    if m:
        col = src if m.group(1) == "val" else m.group(1)
        return f"F.upper(F.col('{col}'))"
    # ── TRIM(col / val)
    m = re.match(r'TRIM\(\s*(\w+)\s*\)', first, re.I)
    if m:
        col = src if m.group(1) == "val" else m.group(1)
        return f"F.trim(F.col('{col}'))"
    # ── LOWER(col / val)
    m = re.match(r'LOWER\(\s*(\w+)\s*\)', first, re.I)
    if m:
        col = src if m.group(1) == "val" else m.group(1)
        return f"F.lower(F.col('{col}'))"
    # ── COALESCE(col, default)
    m = re.match(r'COALESCE\(\s*(\w+)\s*,\s*(.+?)\s*\)', first, re.I)
    if m:
        col  = src if m.group(1) == "val" else m.group(1)
        dflt = m.group(2).strip()
        lit  = _lit_or_col(dflt, src)
        return f"F.coalesce(F.col('{col}'), {lit})"
    # ── CAST(col AS type) → column only; dialect coercion block handles actual casting
    m = re.match(r'CAST\(\s*(\w+)\s+AS\s+(\S+?)\s*\)', first, re.I)
    if m:
        col = src if m.group(1) == "val" else m.group(1)
        return f"F.col('{col}')  # TODO: add explicit .cast() for type {m.group(2)} if needed"
    # ── MySQL IF(cond, true_val, false_val) → F.when().otherwise()
    m = re.match(r'IF\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(.+?)\s*\)$', first, re.I)
    if m:
        cond_raw, tv_raw, fv_raw = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
        # Try to build a structured F.when if the condition is a simple comparison
        cm = re.match(r'(\w+)\s*(==|!=|<=|>=|<|>|=)\s*(.+)', cond_raw)
        if cm:
            col_name = src if cm.group(1) == "val" else cm.group(1)
            op       = "==" if cm.group(2) == "=" else cm.group(2)
            cmp_lit  = _lit_or_col(cm.group(3).strip(), src)
            true_lit = _lit_or_col(tv_raw, src)
            false_lit= _lit_or_col(fv_raw, src)
            return f"F.when(F.col('{col_name}') {op} {cmp_lit}, {true_lit}).otherwise({false_lit})"
        # Fallback to CASE WHEN in F.expr
        # Use double-quote delimiter so single-quotes inside SQL are passed through cleanly.
        # Escape any literal double-quotes in the values (rare but safe).
        cond_safe = cond_raw.replace("\\", "\\\\").replace('"', '\\"')
        tv_safe   = tv_raw.replace("\\", "\\\\").replace('"', '\\"')
        fv_safe   = fv_raw.replace("\\", "\\\\").replace('"', '\\"')
        return f'F.expr("CASE WHEN {cond_safe} THEN {tv_safe} ELSE {fv_safe} END")'
    # ── CONCAT(col1, col2, ...) → F.concat(...)
    m = re.match(r'CONCAT\((.+)\)', first, re.I)
    if m:
        raw_args = [a.strip() for a in re.split(r',(?![^()]*\))', m.group(1))]
        py_args  = [_lit_or_col(a, src) for a in raw_args]
        return f"F.concat({', '.join(py_args)})"
    # ── Bare column name (or 'val' placeholder)
    if re.match(r'^\w+$', first):
        col = src if first == "val" else first
        return f"F.col('{col}')"
    # ── Python built-ins: str(col), str(col).zfill(n), and their + concatenations
    # These are valid Python but not valid Spark SQL — rewrite to PySpark API calls.
    py_builtin = _rewrite_py_builtins_to_spark(first, src)
    if py_builtin is not None:
        return py_builtin

    # ── Fallback: F.expr() with double-quote delimiter + MySQL→Spark normalisations
    # Double-quote delimiter means single-quotes inside SQL pass through untouched.
    # We only need to escape backslashes and any literal double-quotes in the rule.
    sfx  = "  # TODO: review multi-line rule" if "\n" in r else ""
    # Resolve 'val' placeholder before emitting
    resolved = re.sub(r'\bval\b', src, first)
    safe = (
        resolved
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("IFNULL(", "COALESCE(")
        .replace("ifnull(", "coalesce(")
    )
    safe = re.sub(r'\bIF\s*\(', 'CASE WHEN ', safe, flags=re.IGNORECASE)
    # In Spark SQL, + is numeric-only; || is string concatenation.
    # Detect MySQL/Python-style string concat (col + ' ' + col) and convert + to ||.
    if "'" in safe and '+' in safe:
        safe = safe.replace(' + ', ' || ')
    return f'F.expr("{safe}"){sfx}'


def _const_spark(rule: str) -> str:
    """Convert a constant rule to a PySpark literal expression."""
    r = rule.strip()
    up = r.upper()
    if up in ("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()"):
        return "F.current_timestamp()"
    if up in ("CURRENT_DATE", "CURRENT_DATE()"):
        return "F.current_date()"
    if up == "FALSE":
        return "F.lit(False)"
    if up == "TRUE":
        return "F.lit(True)"
    if "IDENTITY" in up or "AUTO_INCREMENT" in up:
        return "F.lit(None).cast(IntegerType())  # identity — DB assigns"
    if up in ("NULL", "NONE"):
        return "F.lit(None)"
    if up == "SYS_BATCH_ID":
        # SYS_BATCH_ID is an ETL engine system variable injected at load time.
        # It does not exist as a source column, so use a placeholder literal.
        # batch_id should be added to EXCLUDE_COLS so it is not compared.
        return 'F.lit("ETL_VALIDATION")  # placeholder — real batch ID set by ETL engine'
    if r.startswith("'") and r.endswith("'"):
        return f'F.lit("{r[1:-1]}")'
    try:
        float(r)
        return f"F.lit({r})"
    except ValueError:
        pass
    # Use double-quote delimited F.expr so single-quotes inside SQL pass through cleanly.
    # Only backslashes and literal double-quotes need escaping.
    safe = r.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")
    # Replace MySQL-specific functions that Spark SQL doesn't support
    safe = re.sub(r'\bNOW\(\)', 'CURRENT_TIMESTAMP()', safe, flags=re.IGNORECASE)
    safe = re.sub(r'\bUUID\(\)', 'UUID()', safe, flags=re.IGNORECASE)  # Spark 3.x supports UUID()
    return f'F.expr("{safe}")  # TODO: verify this expression is valid Spark SQL'


def build_transform_py(spec: dict, sheet_name: str, dialect: str = "mysql") -> str:
    src_tname = spec["source_schema"]["table"]
    tgt_tname = spec["target_schema"]["table"]
    mappings  = spec["column_mappings"]
    joins     = spec.get("joins", [])
    tcols     = [c["name"] for c in spec["target_schema"]["columns"]]

    # Collect target column types for the dialect coercion block
    tgt_col_types = {
        c["name"]: (c.get("effective_dtype") or c.get("dtype") or "")
        for c in spec["target_schema"]["columns"]
    }

    L = []
    a = L.append

    # ── Docstring / header ──────────────────────────────────────────────
    a('"""')
    a(f"04_transform.py  —  {src_tname}  ->  {tgt_tname}")
    a(f"Generated : {datetime.now():%Y-%m-%d %H:%M}")
    a("")
    a("Public API")
    a("----------")
    a("    from 04_transform import apply_transforms")
    a(f"    df_out = apply_transforms(df_source, dialect='mysql')")
    a(f"    df_out = apply_transforms(df_source, dialect='snowflake')")
    a("")
    a("Parameters")
    a("----------")
    a("    df_source : pyspark.sql.DataFrame")
    a("        Raw source extract DataFrame. Column names must match")
    a("        the src_col_name values in the mapping spec.")
    a("        The input DataFrame is always assumed to be MySQL dialect")
    a("        (e.g. TINYINT(1) as 0/1, CHAR columns space-padded,")
    a("        zero-dates as '0000-00-00', TIME as timedelta, etc.).")
    if joins:
        a("        Must also include join-fetched columns (aliased as")
        a("        <join_alias>_<column>) from the extract SQL.")
    a("")
    a("    dialect : Literal['mysql', 'snowflake']")
    a("        Target dialect for the returned DataFrame.")
    a("        - 'mysql'     : values left in MySQL-native form (default).")
    a("        - 'snowflake' : applies dialect coercions so the frame is")
    a("                        ready for write_pandas / Snowflake ingestion.")
    a("")
    a("Returns")
    a("-------")
    a("    pyspark.sql.DataFrame")
    a(f"        Transformed frame shaped to {tgt_tname} target schema,")
    a("        with values coerced to the requested dialect.")
    a('"""')

    # ── Imports ─────────────────────────────────────────────────────────
    a("from __future__ import annotations")
    a("import logging")
    a("from typing import Literal")
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

    # ── Function signature ───────────────────────────────────────────────
    a("def apply_transforms(")
    a("    df: DataFrame,")
    a("    dialect: Literal['mysql', 'snowflake'] = 'mysql',")
    a(") -> DataFrame:")
    a('    """')
    a(f"    Apply all column-level transforms for {sheet_name}.")
    a("")
    a("    The input DataFrame must contain every source field referenced in")
    a("    the mapping spec. Extra columns in the input are silently ignored.")
    a("    Input values are always assumed to be MySQL-dialect.")
    a("    Pass dialect='snowflake' to coerce values for Snowflake ingestion.")
    if joins:
        a("")
        a("    Join columns expected in input DataFrame:")
        for jd in joins:
            a(f"        [{jd['join_alias']}] {jd['join_table']}: "
              f"{', '.join(jd['fetch_columns'])}")
    a('    """')
    a("    if dialect not in ('mysql', 'snowflake'):")
    a("        raise ValueError(f\"dialect must be 'mysql' or 'snowflake', got {dialect!r}\")")
    a('    logger.info("=" * 70)')
    a(f"    logger.info('START TRANSFORM | {src_tname} -> {tgt_tname} | dialect=%s', dialect)")
    a("    logger.info('  Input  cols : %s', df.columns)")
    a("")

    # ── Per-mapping transform lines ──────────────────────────────────────
    # Build join alias map: "alias.col" -> "alias_col" (the aliased column
    # name that the extract SQL exposes)
    join_alias_map: dict[str, str] = {}
    for jd in joins:
        for fc in jd.get("fetch_columns", []):
            aliased = f"{jd['join_alias']}_{fc}"
            join_alias_map[f"{jd['join_alias']}.{fc}".lower()] = aliased

    # Collect non-nullable column names for batched null check after all transforms
    _nn_cols_gen: list[str] = []

    for m in mappings:
        tt   = m["transform_type"]
        tgt  = m["target_column"]
        srcs = m["source_columns"]
        rule = (m.get("transform_rule") or "").strip()
        nh   = (m.get("null_handling") or "").strip()
        note = (m.get("notes") or "").strip()
        src  = srcs[0] if srcs else ""
        cmt  = f"  # {note}" if note else ""

        if tt == "drop":
            a(f"    # DROP: {src} — excluded from target{cmt}")
            a(f"    logger.debug('  [drop]      {src}')")
            a("")
            continue

        a(f"    # {tt.upper()}: {src or '(no src)'} -> {tgt}{cmt}")
        a(f"    logger.debug('  [{tt:<9}] %s -> {tgt}', '{src}')")

        if tt == "constant":
            a(f"    df = df.withColumn('{tgt}', {_const_spark(rule)})")

        elif tt in ("direct", "rename"):
            # Resolve join alias references in the source column name
            resolved = join_alias_map.get(src.lower(), src)
            expr = f"F.col('{resolved}')" if resolved else "F.lit(None).cast(StringType())"
            a(f"    df = df.withColumn('{tgt}', {expr})")

        elif tt in ("cast", "derived"):
            # Substitute any "alias.col" references with their aliased names
            resolved_rule = rule
            for ref, aliased in join_alias_map.items():
                alias, col = ref.split(".", 1)
                resolved_rule = re.sub(
                    rf'\b{re.escape(alias)}\.{re.escape(col)}\b',
                    aliased, resolved_rule, flags=re.IGNORECASE,
                )
            a(f"    df = df.withColumn('{tgt}', {_spark_expr(resolved_rule, src)})")

        else:
            expr = f"F.col('{src}')" if src else "F.lit(None).cast(StringType())"
            a(f"    df = df.withColumn('{tgt}', {expr})")

        # Null handling
        if not nh or nh == "error":
            _nn_cols_gen.append(tgt)
        elif nh == "fill_zero":
            a(f"    df = df.fillna({{'{tgt}': 0}})")
        elif nh == "fill_empty":
            a(f"    # NULL values remain as NULL (not replaced with empty string)")
        elif nh == "fill_default":
            a(f"    df = df.fillna({{'{tgt}': 0}})  # fill_default: update value as needed")
        a("")

    # ── Reorder to target schema ─────────────────────────────────────────
    a(f"    # Reorder to target schema")
    a(f"    _exp  = {tcols!r}")
    a(f"    _pres = [c for c in _exp if c in df.columns]")
    a(f"    _miss = [c for c in _exp if c not in df.columns]")
    a(f"    if _miss:")
    a(f"        logger.warning('  Missing target cols: %s', _miss)")
    a(f"    df = df.select(*_pres)")

    # ── Batched null validation (single Spark action) ─────────────────────
    if _nn_cols_gen:
        a("")
        a("    # ── Batch null validation (single Spark action) ──────────────────")
        a(f"    _nn_cols = {_nn_cols_gen!r}")
        a("    _null_exprs = [F.count(F.when(F.col(c).isNull(), 1)).alias(f'_null_{c}') for c in _nn_cols]")
        a("    _null_exprs.append(F.count('*').alias('_total_rows'))")
        a("    _null_result = df.select(*_null_exprs).first()")
        a("    _null_violations = {c: _null_result[f'_null_{c}'] for c in _nn_cols if _null_result[f'_null_{c}'] > 0}")
        a("    if _null_violations:")
        a("        for _col, _cnt in _null_violations.items():")
        a("            logger.error(\"  NULL in non-nullable '%s': %d rows\", _col, _cnt)")
        a("        raise ValueError(f'NULL values in non-nullable columns: {_null_violations}')")
        a("    logger.info('  Null check passed for %d non-nullable columns', len(_nn_cols))")
        a("    logger.info('  Output rows : %d', _null_result['_total_rows'])")
    else:
        # No non-nullable columns — still need row count
        a("")
        a("    _row_count = df.count()")
        a("    logger.info('  Output rows : %d', _row_count)")

    # ── Dialect coercion block (Snowflake only) ──────────────────────────
    coerce_lines = _build_dialect_coercion_block(tgt_col_types)
    if coerce_lines:
        a("")
        a("    # ── Dialect coercion: MySQL -> Snowflake value fixes ──────────────")
        a("    # Input is always MySQL dialect; coercions run only for 'snowflake'.")
        a("    if dialect == 'snowflake':")
        for cl in coerce_lines:
            a(f"    {cl}")

    a(f"    logger.info('  Output cols : %s', df.columns)")
    a(f"    logger.info('END TRANSFORM | dialect=%s', dialect)")
    a(f"    logger.info('=' * 70)")
    a(f"    return df")

    return "\n".join(L) + "\n"


def _build_dialect_coercion_block(tgt_col_types: dict[str, str]) -> list[str]:
    """
    Return indented PySpark lines (body of `if dialect == 'snowflake':` block)
    that coerce MySQL-dialect values to Snowflake-compatible values.

    Coercions:
      TINYINT(1) / BOOL / BOOLEAN  → cast to BooleanType
      CHAR(n)                       → rtrim trailing spaces
      DATE                          → null zero-dates ('0000-00-00')
      DATETIME / TIMESTAMP          → null zero-datetimes; cast to TimestampType
      TIME                          → HH:MM:SS string via date_format
      YEAR                          → cast to IntegerType
      DECIMAL / NUMERIC             → cast to DoubleType
    """
    lines: list[str] = []

    for col, raw_type in tgt_col_types.items():
        t    = raw_type.strip().lower()
        base = t.split("(")[0].rstrip()

        if base in ("tinyint", "bool", "boolean") and (
            "(1)" in t or base in ("bool", "boolean")
        ):
            lines.append(
                f"    df = df.withColumn('{col}', F.col('{col}').cast(BooleanType()))"
            )
            lines.append(
                f"    logger.debug('  [sf-coerce] {col}: tinyint(1)/bool -> BooleanType')"
            )

        elif base == "char":
            lines.append(
                f"    df = df.withColumn('{col}', F.rtrim(F.col('{col}')))"
            )
            lines.append(
                f"    logger.debug('  [sf-coerce] {col}: char -> rtrim whitespace')"
            )

        elif base == "date":
            lines.append(
                f"    df = df.withColumn('{col}',"
                f" F.when(F.col('{col}').cast(StringType()).startswith('0000-00-00'),"
                f" F.lit(None)).otherwise(F.col('{col}')))"
            )
            lines.append(
                f"    logger.debug('  [sf-coerce] {col}: date -> null zero-dates')"
            )

        elif base in ("datetime", "timestamp", "timestamp_ntz"):
            lines.append(
                f"    df = df.withColumn('{col}',"
                f" F.when(F.col('{col}').cast(StringType()).startswith('0000-00-00'),"
                f" F.lit(None)).otherwise(F.col('{col}').cast(TimestampType())))"
            )
            lines.append(
                f"    # Source timestamps are IST (from MySQL); Snowflake stores them converted to IST representation"
            )
            lines.append(
                f"    df = df.withColumn('{col}', F.from_utc_timestamp(F.col('{col}'), 'Asia/Kolkata'))"
            )
            lines.append(
                f"    logger.debug('  [sf-coerce] {col}: datetime/timestamp -> TimestampType (IST adjusted)')"
            )


        elif base == "time":
            lines.append(
                f"    df = df.withColumn('{col}', F.date_format(F.col('{col}'), 'HH:mm:ss'))"
            )
            lines.append(
                f"    logger.debug('  [sf-coerce] {col}: time -> HH:MM:SS string')"
            )

        elif base == "year":
            lines.append(
                f"    df = df.withColumn('{col}', F.col('{col}').cast(IntegerType()))"
            )
            lines.append(
                f"    logger.debug('  [sf-coerce] {col}: year -> IntegerType')"
            )

        elif base in ("decimal", "numeric"):
            lines.append(
                f"    df = df.withColumn('{col}', F.col('{col}').cast(DoubleType()))"
            )
            lines.append(
                f"    logger.debug('  [sf-coerce] {col}: decimal/numeric -> DoubleType')"
            )

    return lines


def _add_null_check(add, col: str, nh: str):
    if nh == "error":
        add(f'_nm = df_out["{col}"].isnull()')
        add(f"if _nm.any():")
        add(f'    _bad = df_out.index[_nm].tolist()')
        add(f'    logger.error(f"  NULL in non-nullable column \'{col}\' at rows: {{_bad}}")')
        add(f'    raise ValueError(f"NULL in \'{col}\' at rows: {{_bad}}")')
    elif nh == "fill_zero":
        add(f'df_out["{col}"] = df_out["{col}"].fillna(0)')
    elif nh == "fill_empty":
        add(f'df_out["{col}"] = df_out["{col}"].fillna("")')
    elif nh == "fill_default":
        add(f'_mode = df_out["{col}"].mode()')
        add(f'df_out["{col}"] = df_out["{col}"].fillna(_mode[0] if not _mode.empty else None)')


def _add_cast(add, col: str, force_dtype: str):
    dtype_map = {
        "int8": "Int8", "int16": "Int16", "int32": "Int32", "int64": "Int64",
        "float32": "float32", "float64": "float64",
        "str": "str", "string": "string",
    }
    pd_dtype = dtype_map.get(force_dtype.lower(), force_dtype)
    add(f'df_out["{col}"] = df_out["{col}"].astype("{pd_dtype}", errors="ignore")')


def _rule_to_pandas(
    rule: str | None,
    primary_src_col: str,
    all_df_cols: list[str],
    join_col_alias_map: dict | None = None,
    target_col: str = "",        # ← NEW: for error messages
    sheet_name: str = "",        # ← NEW: for error messages
) -> str:
    if not rule:
        return f'df["{primary_src_col}"]'
    expr = rule
    alias_map = join_col_alias_map or {}

    def _resolve(m):
        full = f"{m.group(1)}.{m.group(2)}".lower()
        return alias_map.get(full, m.group(2))

    expr = re.sub(
        r'\b([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)\b',
        _resolve,
        expr, flags=re.IGNORECASE,
    )

    needs_apply = bool(re.search(r'\bif\b.+?\belse\b', expr, re.IGNORECASE))

    if needs_apply:
        expr = re.sub(r'ROUND\(([^,]+),\s*(\d+)\s*\)',
                      lambda m: f'round({m.group(1).strip()}, {m.group(2)})', expr)
        expr = re.sub(r'UPPER\(\s*(\w+)\s*\)',
                      lambda m: f'upper__{m.group(1)}__', expr)
        expr = re.sub(r'CAST\(\s*(\w+)\s+AS\s+\w+\s*\)',
                      lambda m: m.group(1), expr, flags=re.IGNORECASE)

        for col in sorted(all_df_cols, key=len, reverse=True):
            expr = re.sub(
                r'(?<!\[")\b' + re.escape(col) + r'\b(?!")',
                f'row["{col}"]', expr,
            )
        expr = re.sub(r'(?<!\[")\bval\b', f'row["{primary_src_col}"]', expr)
        expr = re.sub(r'upper__(\w+)__', lambda m: f'row["{m.group(1)}"].upper()', expr)

        simple = re.match(
            r'1 if row\["(\w+)"\] == \'(\w+)\' else 0', expr.strip()
        )
        if simple:
            expr = f'(df["{simple.group(1)}"] == "{simple.group(2)}").astype(int)'
        else:
            expr = f'df.apply(lambda row: {expr}, axis=1)'

    else:
        expr = re.sub(r'CAST\(\s*(\w+)\s+AS\s+\w+\s*\)',
                      lambda m: m.group(1), expr, flags=re.IGNORECASE)

        for col in sorted(all_df_cols, key=len, reverse=True):
            expr = re.sub(
                r'(?<!\[")\b' + re.escape(col) + r'\b(?!")',
                f'df["{col}"]', expr,
            )
        expr = re.sub(r'(?<!\[")\bval\b', f'df["{primary_src_col}"]', expr)

        # ── Detect unresolved bare identifiers ────────────────────────────
        _still_bare = re.findall(r'(?<!\[")(?<!\.)\b([a-z_][a-z0-9_]*)\b(?!")', expr)
        _keywords   = {'if', 'else', 'and', 'or', 'not', 'in', 'is', 'None',
                       'True', 'False', 'round', 'int', 'str', 'float', 'len',
                       'row', 'df', 'axis', 'apply', 'lambda', 'astype', 'fillna'}
        _unresolved = [n for n in _still_bare
                       if n not in _keywords
                       and not n.startswith('df')
                       and not n.isdigit()]

        # ── FIX: raise immediately instead of writing broken __UNRESOLVED__ code ──
        if _unresolved:
            missing = sorted(set(_unresolved))
            loc = f"sheet '{sheet_name}', target column '{target_col}'" if sheet_name else f"target column '{target_col}'"
            raise ValueError(
                f"\n"
                f"  Unresolved column reference(s) in transform rule\n"
                f"  Location : {loc}\n"
                f"  Rule     : {rule}\n"
                f"  Missing  : {missing}\n"
                f"\n"
                f"  These name(s) are not present in the source schema or any\n"
                f"  join fetch_columns for this sheet.\n"
                f"\n"
                f"  How to fix:\n"
                f"    (a) Add the missing column(s) as a source row in your\n"
                f"        Excel mapping spec so they are read from the DB, OR\n"
                f"    (b) If it is a constant (e.g. 480), replace the bare name\n"
                f"        with the literal value directly in the transform_rule\n"
                f"        cell in the Excel spec.\n"
            )

        def _round_to_series(m_outer):
            inner = m_outer.group(1).strip()
            rp = re.match(r'^(.*),\s*(\d+)\s*$', inner, re.DOTALL)
            if rp:
                body, prec = rp.group(1).strip(), rp.group(2)
                return f'({body}).round({prec})'
            return f'({inner})'
        expr = re.sub(r'ROUND\((.+)\)', _round_to_series, expr, flags=re.DOTALL)

        expr = re.sub(r'str\((df\["[^"]+"\])\)', lambda m: f'{m.group(1)}.astype(str)', expr)
        
        # Handle zfill() - convert .astype(str).zfill(n) to .astype(str).str.zfill(n)
        expr = re.sub(r'\.astype\(str\)\.zfill\(', '.astype(str).str.zfill(', expr)
        
        expr = re.sub(r'UPPER\(\s*(df\["[^"]+"\])\s*\)',
                      lambda m: f'{m.group(1)}.str.upper()', expr)
        expr = re.sub(r'UPPER\(\s*(\w+)\s*\)',
                      lambda m: f'df["{m.group(1)}"].str.upper()', expr)

        def _concat(match):
            parts = []
            for a in [x.strip() for x in match.group(1).split(",")]:
                if (a.startswith("'") and a.endswith("'")) or (a.startswith('"') and a.endswith('"')):
                    parts.append(a)
                elif a.startswith('df['):
                    parts.append(a)
                else:
                    parts.append(f'df["{a}"]')
            return " + ".join(parts)
        expr = re.sub(r'CONCAT\(([^)]+)\)', _concat, expr)

    return expr


def _constant_to_py(rule: str) -> str:
    r = rule.strip().upper()
    if r in ("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()"):
        return "datetime.utcnow()"
    if r == "SYS_BATCH_ID":
        return "batch_id"
    if rule.startswith(("'", '"')):
        return rule
    try:
        float(rule); return rule
    except ValueError:
        return f'"{rule}"'


# ── LOAD SQL ───────────────────────────────────────────────────────────────

def build_load_sql(spec: dict, dialect: str) -> str:
    tname    = spec["target_schema"]["table"]
    pks      = spec["target_schema"]["primary_keys"]
    tgt_cols = [c["name"] for c in spec["target_schema"]["columns"]]
    col_list = ", ".join(tgt_cols)

    lines = [
        "-- ============================================================",
        f"-- STEP 5 : LOAD  |  target: {tname}",
        "-- ============================================================",
        f"-- Dialect   : {DIALECT_NOTES[dialect]}",
        f"-- Generated : {datetime.now():%Y-%m-%d %H:%M}",
        f"-- Strategy  : UPSERT (insert or update on PK conflict)",
        f"-- PKs       : {', '.join(pks) or 'none'}",
        "--",
        "",
    ]

    if dialect == "mysql":
        non_pk = [c for c in tgt_cols if c not in pks]
        ph     = ", ".join("%s" for _ in tgt_cols)
        upd    = ",\n    ".join(f"{c} = VALUES({c})" for c in non_pk)
        lines += [
            f"INSERT INTO {tname}",
            f"    ({col_list})",
            f"VALUES",
            f"    ({ph})",
            f"ON DUPLICATE KEY UPDATE",
            f"    {upd};",
            "",
            "-- Plain INSERT (no upsert):",
            f"-- INSERT INTO {tname} ({col_list})",
            f"-- VALUES ({', '.join('?' for _ in tgt_cols)});",
        ]

    elif dialect == "snowflake":
        sa, ta = "src", "tgt"
        bind   = ", ".join(f":{c}" for c in tgt_cols)
        join_c = " AND ".join(f"{ta}.{p} = {sa}.{p}" for p in pks)
        upd    = ",\n        ".join(f"{ta}.{c} = {sa}.{c}" for c in tgt_cols if c not in pks)
        ins_v  = ", ".join(f"{sa}.{c}" for c in tgt_cols)
        lines += [
            f"MERGE INTO {tname} AS {ta}",
            f"USING (SELECT {bind}) AS {sa} ({col_list})",
            f"    ON {join_c}",
            f"WHEN MATCHED THEN UPDATE SET",
            f"    {upd}",
            f"WHEN NOT MATCHED THEN",
            f"    INSERT ({col_list})",
            f"    VALUES ({ins_v});",
        ]

    elif dialect == "postgres":
        non_pk = [c for c in tgt_cols if c not in pks]
        ph     = ", ".join("%s" for _ in tgt_cols)
        upd    = ",\n    ".join(f"{c} = EXCLUDED.{c}" for c in non_pk)
        lines += [
            f"INSERT INTO {tname} ({col_list})",
            f"VALUES ({ph})",
            f"ON CONFLICT ({', '.join(pks)}) DO UPDATE SET",
            f"    {upd};",
        ]

    return "\n".join(lines)


# ── EXTRACT TARGET SQL ────────────────────────────────────────────────────

def build_extract_target(spec: dict, dialect: str) -> str:
    """Simple SELECT of all target columns — used instead of a LOAD step."""
    tname  = spec["target_schema"]["table"]
    cols   = [c["name"] for c in spec["target_schema"]["columns"]]
    col_names_lower = [c["name"].lower() for c in spec["target_schema"]["columns"]]
    pks    = spec["target_schema"]["primary_keys"]
    col_l  = ",\n".join(f"    {c}" for c in cols)
    # ORDER BY omitted — Spark join handles ordering internally.
    # order  = f"\nORDER BY {', '.join(pks)}" if pks else ""
    dlbl   = DIALECT_NOTES[dialect]

    _has_created = "created_at" in col_names_lower
    _has_updated = "updated_at" in col_names_lower

    pk_col = pks[0] if pks else None
    where_placeholder_lines = [
        "--",
        "-- [[FILTER_PLACEHOLDER]]",
        "-- The following WHERE clause is injected at runtime by custom_execution.py.",
        "-- Do NOT edit this block manually — it is overwritten on each run.",
        "--",
        "-- PK filter modes (PK_FILTER_MODE in custom_execution.py):",
        "--   full    : no PK WHERE clause",
    ]
    if pk_col:
        where_placeholder_lines += [
            f"--   pk_range: WHERE {pk_col} >= {{LOWER}} AND/OR {pk_col} <= {{UPPER}}",
            f"--             (either bound may be None → single-sided limit)",
            f"--   pk_set  : WHERE {pk_col} IN ({{PK_SET}})",
        ]
    where_placeholder_lines += [
        "--",
        "-- Date watermark modes (DATE_WATERMARK_MODE in custom_execution.py):",
        "--   full    : no date WHERE clause",
        "--   range   : WHERE {{DATE_FROM_COL}} >= '{{DATE_FROM}}'",
        "--             AND/OR {{DATE_TO_COL}} <= '{{DATE_TO}}'",
        "--             (either bound may be None → single-sided limit)",
        "--",
        "-- [[/FILTER_PLACEHOLDER]]",
    ]
    if _has_created or _has_updated:
        date_cols = [c for c in (["created_at"] if _has_created else []) + (["updated_at"] if _has_updated else [])]
        where_placeholder_lines.append(f"-- Date cols available in this table: {', '.join(date_cols)}")

    lines = [
        f"-- ============================================================",
        f"-- STEP 5 : EXTRACT TARGET  |  target: {tname}",
        f"-- ============================================================",
        f"-- Dialect   : {dlbl}",
        f"-- Generated : {datetime.now():%Y-%m-%d %H:%M}",
        f"--",
        f"-- Simple SELECT of all target columns for comparison against",
        f"-- the transformed source data (04_transform.py output).",
    ] + where_placeholder_lines + [
        f"",
        f"SELECT",
        col_l,
        # f"FROM {tname}{order};", #ORDER BY omitted — Spark join handles ordering internally.
        f"FROM {tname};",
    ]
    return "\n".join(lines)


# ── PIPELINE LOG ───────────────────────────────────────────────────────────

def build_pipeline_log(spec: dict, sheet_name: str) -> str:
    src      = spec["source_schema"]["table"]
    tgt      = spec["target_schema"]["table"]
    mappings = spec["column_mappings"]
    joins    = spec.get("joins", [])
    src_cols = spec["source_schema"]["columns"]
    tgt_cols = spec["target_schema"]["columns"]
    pks_src  = spec["source_schema"]["primary_keys"]
    pks_tgt  = spec["target_schema"]["primary_keys"]
    drops    = [m for m in mappings if m["transform_type"] == "drop"]

    lines = [
        f"# ETL Pipeline Log -- {sheet_name}",
        f"",
        f"> Generated : {datetime.now():%Y-%m-%d %H:%M}",
        f"> Source    : `{src}`",
        f"> Target    : `{tgt}`",
        f"> Joins     : {len(joins)}",
        f"",
        f"---",
        f"",
        f"## Step 1 -- Create Source Table",
        f"",
        f"| Column | Type | Nullable | PK |",
        f"|--------|------|----------|----|",
    ]
    for c in src_cols:
        lines.append(f"| `{c['name']}` | `{c['dtype']}` | {'Y' if c['nullable'] else 'N'} | {'PK' if c['is_pk'] else ''} |")

    lines += ["", f"Primary keys: {', '.join(f'`{p}`' for p in pks_src) or 'none'}", "", "---", ""]
    lines += [f"## Step 2 -- Create Target Table", "", f"| Column | Type | Nullable | Default | PK |",
              f"|--------|------|----------|---------|----|"]
    for c in tgt_cols:
        dv = f"`{c['default_val']}`" if c.get("default_val") else "--"
        lines.append(
            f"| `{c['name']}` | `{c.get('effective_dtype') or c['dtype']}` "
            f"| {'Y' if c['nullable'] else 'N'} | {dv} | {'PK' if c['name'] in pks_tgt else ''} |"
        )

    lines += ["", f"Primary keys: {', '.join(f'`{p}`' for p in pks_tgt) or 'none'}", "", "---", ""]
    lines += [f"## Step 3 -- Extract"]
    if drops:
        lines.append(f"\nDropped columns (not in SELECT): " +
                     ", ".join(f"`{m['source_columns'][0]}`" for m in drops if m["source_columns"]))
    if joins:
        lines += ["", "### Enrichment Joins", "",
                  "| Alias | Table | DB | Type | Left Key | Right Key | Fetched Columns |",
                  "|-------|-------|----|------|----------|-----------|-----------------|"]
        for jd in joins:
            lines.append(
                f"| `{jd['join_alias']}` | `{jd['join_table']}` | {jd.get('join_db') or '(same)'} "
                f"| {jd['join_type']} | `{jd['left_key']}` | `{jd['right_key']}` "
                f"| {', '.join(f'`{c}`' for c in jd['fetch_columns'])} |"
            )

    lines += ["", "---", "", "## Step 4 -- Transform Mappings", "",
              "| Source Col | Join Refs | Type | Rule | Target Col | Null Handling | Notes |",
              "|-----------|-----------|------|------|------------|---------------|-------|"]
    for m in mappings:
        sc   = ", ".join(f"`{c}`" for c in m["source_columns"]) or "--"
        jref = ", ".join(f"`{r}`" for r in m.get("join_columns", [])) or "--"
        tc   = f"`{m['target_column']}`" if m["target_column"] else "--"
        rule = f"`{m['transform_rule']}`" if m["transform_rule"] else "--"
        lines.append(f"| {sc} | {jref} | **{m['transform_type']}** | {rule} | {tc} | {m['null_handling'] or '--'} | {m['notes'] or '--'} |")

    lines += ["", "---", "", "## Step 5 -- Load Strategy", "",
              f"- UPSERT on PK conflict",
              f"- Target PKs: {', '.join(f'`{p}`' for p in pks_tgt) or 'none'}",
              "", "---", "", "## Step 6 -- Verification Checklist", "",
              "- [ ] Row count matches between source and extract",
              "- [ ] Join produced correct row count (no fan-out)",
              "- [ ] No NULLs in NOT NULL columns",
              "- [ ] Derived/join columns spot-checked",
              "- [ ] PK uniqueness confirmed in target",
              "- [ ] load_ts and batch_id present on all rows", ""]
    return "\n".join(lines)


def build_summary(data: dict) -> str:
    """Build the 00_summary.md overview document."""
    from datetime import datetime
    tables = data["tables"]
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        "# ETL Runbook -- Summary", "",
        f"> Generated : {now}",
        f"> Tables    : {len(tables)}", "",
        "## Files Generated Per Table", "",
        "| File | Purpose | Dialect |",
        "|------|---------|---------||",
        "| `01_create_source_table.sql` | Source CREATE TABLE           | MySQL |",
        "| `02_create_target_sf.sql`    | Target CREATE TABLE           | Snowflake |",
        "| `03_extract_source.sql`      | Source extraction SELECT      | MySQL |",
        "| `04_transform.py`            | PySpark column transforms     | Python |",
        "| `05_extract_target_sf.sql`   | Target SELECT for actual data | Snowflake |",
        "| `06_pipeline_log.md`         | Full per-table runbook        | — |",
        "", "---", "", "## Tables", "",
        "| Sheet | Source | Target | Src Cols | Tgt Cols | Joins | Drops | Derived | Constants |",
        "|-------|--------|--------|----------|----------|-------|-------|---------|-----------|",
    ]
    for sheet_name, spec in tables.items():
        mappings = spec["column_mappings"]
        src_cols = len([m for m in mappings if m.get("source_columns")])
        tgt_cols = len([m for m in mappings if m.get("target_column")])
        joins = len(spec.get("joins", []))
        drops = len([m for m in mappings if m["transform_type"] == "drop"])
        derived = len([m for m in mappings if m["transform_type"] == "derived"])
        constants = len([m for m in mappings if m["transform_type"] == "constant"])
        lines.append(
            f"| `{sheet_name}` | `{spec['source_table']}` | `{spec['target_table']}` "
            f"| {src_cols} | {tgt_cols} | {joins} | {drops} | {derived} | {constants} |"
        )
    lines += [
        "", "## Transform type reference", "",
        "| Type | Meaning |",
        "|------|---------|",
        "| `direct` | Copy column as-is (same name) |",
        "| `rename` | Copy column with a new target name |",
        "| `cast` | Apply an expression or type conversion |",
        "| `derived` | Compute from one or more source/join columns |",
        "| `drop` | Exclude column from target entirely |",
        "| `constant` | Insert a system-generated value (timestamp, batch ID) |",
        "", "## Join rule syntax (in transform_rule column)", "",
        "```",
        "alias.column              -- direct copy from joined table",
        "UPPER(alias.col)          -- expression on joined column",
        "alias.col1 + alias.col2   -- combine joined columns",
        "ROUND(alias.price, 2)     -- numeric expression on joined column",
        "```", "",
    ]
    return "\n".join(lines)


# ── Main ───────────────────────────────────────────────────────────────────

def generate(schemas_path: str, out_dir: str) -> None:
    data   = json.loads(Path(schemas_path).read_text(encoding="utf-8"))
    tables = data["tables"]
    base   = Path(out_dir)
    base.mkdir(parents=True, exist_ok=True)

    (base / "00_summary.md").write_text(build_summary(data), encoding="utf-8")
    print(f"  OK  00_summary.md")

    for sheet_name, spec in tables.items():
        folder = base / sheet_name
        folder.mkdir(exist_ok=True)
        files = {
            "01_create_source_table.sql": build_create_source(spec),
            "02_create_target_sf.sql":    build_create_target(spec, "snowflake"),
            "03_extract_source.sql":      build_extract_sql(spec),
            "04_transform.py":            build_transform_py(spec, sheet_name, dialect="mysql"),
            "05_extract_target_sf.sql":   build_extract_target(spec, "snowflake"),
            "06_pipeline_log.md":         build_pipeline_log(spec, sheet_name),
        }
        for fname, content in files.items():
            (folder / fname).write_text(content, encoding="utf-8")
        j = len(spec.get("joins", []))
        print(f"  OK  {sheet_name}/  ({len(spec['column_mappings'])} mappings{f'  [{j} join(s)]' if j else ''})")

    print(f"\n  Done. {len(tables)*6+1} files -> {out_dir}/")


def main(schemas_json: str, out: str) -> int:
    if not Path(schemas_json).exists():
        print(f"ERROR: {schemas_json} not found.", file=sys.stderr)
        return 1

    print(f"\nGenerating ETL runbook")
    print(f"  Source  : {schemas_json}")
    print(f"  Output  : {out}/")

    generate(schemas_json, out)
    return 0


if __name__ == "__main__":
    SCHEMAS_JSON = "vehicle_schemas.json"
    OUTPUT_DIR   = "etl_output"

    raise SystemExit(main(SCHEMAS_JSON, OUTPUT_DIR))
