# Excel Mapping Syntax Guide

> **Project**: ETL Testing — Databricks Multi-Source  
> **Format Version**: V3 Combined Aliased Extract  
> **Date**: 2026-04-23

---

## 1. File Naming

```
<target_db>.<target_schema>.<target_table>.xlsx
```

**Examples**:
- `analytics_dw.public.dim_vehicle_master.xlsx`
- `analytics_dw.public.fact_production.xlsx`

The parser extracts `target_db`, `target_schema`, and `target_table` from the filename.

---

## 2. Sheet Structure

Each workbook contains:

| Sheet | Required | Purpose |
|-------|----------|---------|
| `server1` (or any name) | ✅ At least 1 | Data mapping rows — one row per source→target column |
| `_joins` | Optional | Explicit JOIN definitions between source tables |
| *(others)* | — | Skipped (legends, version history, etc.) |

**Skipped sheet names**: `_joins`, `versionhistory`, `introduction`, `loading steps`, `sql-script`, `load-ref_category_type`, any starting with `retired` or `data prof`.

Server sheet names must match keys in `config/source_config.yaml`.

---

## 3. Server Sheet Layout

### Row 1: Section Headers

Visual grouping headers (optional merged cells):

```
| SOURCE                    |           | TRANSFORM        |           | TARGET                              |
```

### Row 2: Column Headers

| Column | Section | Required | Description |
|--------|---------|----------|-------------|
| `src_db` | Source | Y | Source database name |
| `src_schema` | Source | N | Source schema (blank = `db.table`) |
| `src_table` | Source | Y | Source table name |
| `src_col_name` | Source | Y | Source column name |
| `src_dtype` | Source | Y | Source data type |
| `src_nullable` | Source | N | `Y`/`N` (default `Y`) |
| `src_is_pk` | Source | N | `Y`/`N` — PK in source |
| `src_is_unique` | Source | N | `Y`/`N` |
| `transform_type` | Transform | Y | `direct`/`rename`/`cast`/`derived`/`constant` |
| `transform_rule` | Transform | Cond. | Required for `cast`/`derived`/`constant` |
| `tgt_col_name` | Target | Y | Target column name |
| `tgt_dtype` | Target | Y | Target data type (Snowflake) |
| `tgt_nullable` | Target | N | `Y`/`N` (default `Y`) |
| `tgt_is_pk` | Target | N | `Y`/`N` — PK in target |
| `tgt_default_val` | Target | N | Default value |
| `force_dtype` | Target | N | Override Spark dtype at compare |
| `null_handling` | Target | N | `error`/`fill_zero`/`fill_empty`/`fill:<value>` |
| `notes` | Target | N | Free text |

### Row 3+: Data Rows

One row per column mapping.

### Rules

1. **Every server sheet must produce ALL target columns** — same target schema across sheets
2. **`src_db`/`src_schema`/`src_table` are per-row** — different tables can be on the same server
3. **`constant` rows** have blank source fields; `transform_rule` is the value
4. **`direct` rows** have identical source→target names; `transform_rule` is blank
5. **`rename` rows** have different source→target names; `transform_rule` is blank
6. **`cast` rows** apply a function; `transform_rule` contains the expression
7. **`derived` rows** compute from one or more columns; `transform_rule` contains the expression

---

## 4. Transform Types

| Type | Source Fields | Transform Rule | Behavior |
|------|-------------|----------------|----------|
| `direct` | Required | Blank | Column passes through, aliased in SQL |
| `rename` | Required | Blank | Source→target name change, aliased in SQL |
| `cast` | Required | Required | Type conversion / function applied in PySpark |
| `derived` | Required | Required | Computed from columns in PySpark |
| `constant` | Blank | Required | New column created in PySpark |

### Supported Transform Rules

| Pattern | Example | Generated PySpark |
|---------|---------|-------------------|
| `ROUND(val, N)` | `ROUND(val, 2)` | `F.round(F.col('COL'), 2)` |
| `UPPER(val)` | `UPPER(val)` | `F.upper(F.col('COL'))` |
| `LOWER(val)` | `LOWER(val)` | `F.lower(F.col('COL'))` |
| `TRIM(val)` | `TRIM(val)` | `F.trim(F.col('COL'))` |
| `1 if val == 'X' else 0` | `1 if val == 'Y' else 0` | `F.when(...).otherwise(...)` |
| `colA + ' ' + colB` | `first_nm + ' ' + last_nm` | `F.concat(...)` |
| `CAST(val AS type)` | `CAST(val AS INT)` | `F.col('COL').cast(...)` |
| `ISNULL(col, default)` | `ISNULL(val, 0)` | `F.coalesce(...)` |
| `SUBSTRING(val, s, l)` | `SUBSTRING(val, 1, 3)` | `F.substring(...)` |
| `LEFT(val, n)` / `RIGHT(val, n)` | `LEFT(val, 5)` | `F.substring(...)` |
| `LPAD(val, n, c)` / `RPAD(val, n, c)` | `LPAD(val, 10, '0')` | `F.lpad(...)` |
| `replace("a","b")` chains | `replace("-","_")` | `F.regexp_replace(...)` |
| `CURRENT_TIMESTAMP` | *(constant)* | `F.current_timestamp()` |
| `SYS_BATCH_ID` | *(constant)* | `F.lit("ETL_VALIDATION")` |

---

## 5. Null Handling

| Value | Behavior |
|-------|----------|
| `error` | Raises error if NULLs found (non-nullable) |
| `fill_zero` | `df.fillna({col: 0})` |
| `fill_empty` | NULLs remain (nullable column) |
| `fill:<value>` | `df.fillna({col: value})` |

---

## 6. Global Transforms Section

Optional section at the **bottom** of each server sheet, after data rows.

### Marker Row
Column A contains `_global_transforms`.

### Sub-Headers (row after marker)

| Column | Required | Description |
|--------|----------|-------------|
| `Order` | Y | Integer execution order (1, 2, 3...) |
| `Operation` | Y | Transform operation name |
| `Parameters` | N | Operation-specific params |
| `Condition` | N | Column filter |
| `Notes` | N | Free text |

### Supported Operations

| Operation | Parameters | Effect |
|-----------|------------|--------|
| `TRIM` | — | `F.trim()` |
| `UPPER` | — | `F.upper()` |
| `LOWER` | — | `F.lower()` |
| `STRIP_WHITESPACE` | — | Collapse `\s+` → single space + trim |
| `REPLACE` | `"old","new"` | `F.regexp_replace(col, old, new)` |
| `REGEX_REPLACE` | `"pattern","replacement"` | `F.regexp_replace(...)` |
| `CAST` | `<spark_type>` | `col.cast(type)` |

### Condition Syntax

| Condition | Meaning |
|-----------|---------|
| *(blank or `all`)* | All columns |
| `dtype:VARCHAR` | VARCHAR/CHAR/TEXT columns |
| `col:*PATTERN*` | Glob match on column name |
| `exclude:COL1,COL2` | All columns except listed |

---

## 7. `_joins` Sheet

| Column | Required | Description |
|--------|----------|-------------|
| `sheet_name` | Y | Server sheet this join belongs to |
| `join_alias` | Y | Alias for joined table (e.g. `parts`, `sup`) |
| `join_table` | Y | Table name to join |
| `join_db` | N | Database of join table |
| `join_schema` | N | Schema of join table |
| `join_type` | Y | `LEFT`/`INNER`/`RIGHT`/`FULL` |
| `left_key` | Y | Column from main table |
| `right_key` | Y | Column from joined table |
| `fetch_columns` | Y | Comma-separated columns to SELECT |

Multiple rows with same `(sheet_name, join_alias, join_table)` create composite `ON ... AND ...` keys.

---

## 8. Verification Rules

Run `verify_excel_syntax.py` before parsing. Rules checked:

### File-Level Rules

| Rule | Severity | Check |
|------|----------|-------|
| S0 | ERROR | Filename matches `<db>.<schema>.<table>.xlsx` |
| S1 | ERROR | File is `.xlsx` and exists |
| S2 | ERROR | ≥1 server sheet found |

### Sheet-Level Rules

| Rule | Severity | Check |
|------|----------|-------|
| S3 | WARNING | Row 1 or 2 must contain header row |
| S4 | ERROR | Required headers present |

### Data Row Rules

| Rule | Severity | Check |
|------|----------|-------|
| D1 | ERROR | `tgt_col_name` not blank |
| D2 | ERROR | `tgt_dtype` not blank |
| D3 | ERROR | No duplicate target columns |
| D4 | ERROR | `transform_type` is valid value |
| D5 | WARNING | `constant` rows have blank source fields |
| D6 | WARNING | Non-constant rows have source fields |
| D7 | WARNING | `cast`/`derived`/`constant` have `transform_rule` |
| D8 | WARNING | ≥1 PK column exists |

### Cross-Sheet Rules

| Rule | Severity | Check |
|------|----------|-------|
| X1 | ERROR | Same target column set across sheets |
| X2 | ERROR | Same target dtypes across sheets |
| X3 | ERROR | Same PK markers across sheets |

### Joins Rules

| Rule | Severity | Check |
|------|----------|-------|
| X4 | ERROR | `sheet_name` matches a server sheet |
| X5 | ERROR | `join_type` is valid |
| X6 | ERROR | `join_alias` unique per sheet |
| X7 | ERROR | Required join fields not blank |

### Global Transform Rules

| Rule | Severity | Check |
|------|----------|-------|
| G1 | WARNING | `Order` is sequential from 1 |
| G2 | WARNING | `Operation` is recognized |

### Parameters Rules

| Rule | Severity | Check |
|------|----------|-------|
| P1 | ERROR | Required headers present (`sr_no`, `parameter_name`, `parameter_value`) |
| P2 | ERROR | `parameter_name` not blank |
| P3 | WARNING | No duplicate `parameter_name` entries |

---

## 9. `_parameters` Sheet

Optional sheet to define pipeline execution parameters directly in the Excel file.
When present, a `parameters.json` file is generated during runbook generation.
At runtime, parameters from this JSON are loaded first; any parameter not found
(or with an empty value) falls back to the Databricks widget value.

### Layout

| Column | Required | Description |
|--------|----------|-------------|
| `sr_no` | Y | Serial number (integer, for ordering) |
| `parameter_name` | Y | Parameter name — must match widget name (e.g. `TABLE_NAME`, `SUB_PATH`) |
| `parameter_value` | Y | Value for the parameter |

### Supported Parameter Names

| Parameter | Description | Example Value |
|-----------|-------------|---------------|
| `TABLE_NAME` | Target table folder name | `public_dim_vehicle_master` |
| `SUB_PATH` | Sub-path inside blob storage | `xl` |
| `STORAGE_ACCOUNT` | Azure storage account name | `etlstorage0907` |
| `CONTAINER` | Azure blob container name | `etl-source-data` |
| `VERIFY_SCHEMA` | Run schema verification | `true` / `false` |
| `PK_FILTER_MODE` | PK filter mode | `full` / `range` / `set` |
| `DATE_WATERMARK_MODE` | Date watermark mode | `full` / `range` |
| `PK_RANGE_LOWER` | PK range lower bound | `1000` |
| `PK_RANGE_UPPER` | PK range upper bound | `9999` |
| `DATE_FROM` | Date watermark start | `2024-01-01` |
| `DATE_FROM_COL` | Column for date-from filter | `created_date` |
| `DATE_TO` | Date watermark end | `2024-12-31` |
| `DATE_TO_COL` | Column for date-to filter | `modified_date` |
| `PARTIAL_COLS` | Comma-separated target columns | `VIN,MODEL_YEAR` |
| `RUN_SYNTAX_CHECK` | Run syntax check before parse | `true` / `false` |
| `EXCEL_FILE` | Excel file name | `analytics_dw.public.dim_vehicle_master.xlsx` |
| `SF_DATABASE` | Snowflake database override | `ANALYTICS_DW` |

### Example

| sr_no | parameter_name | parameter_value |
|-------|---------------|-----------------|
| 1 | TABLE_NAME | public_dim_vehicle_master |
| 2 | SUB_PATH | xl |
| 3 | STORAGE_ACCOUNT | etlstorage0907 |
| 4 | CONTAINER | etl-source-data |
| 5 | VERIFY_SCHEMA | true |
| 6 | SF_DATABASE | ANALYTICS_DW |

