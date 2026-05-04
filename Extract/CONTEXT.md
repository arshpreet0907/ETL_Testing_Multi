# Extract Pipeline — Architecture & Context Guide

> **Date**: 2026-04-29  
> **Status**: ✅ Fully implemented (Extract + Local Transform)  
> **Approach**: V3 Combined Aliased Extract — Multi-Server

---

## 1. What This Project Does

Extracts source data from one or more database servers, applies PySpark transforms, and saves CSV files ready for Snowflake loading and Databricks comparison.

### Pipeline Flow

```
Excel Mapping Spec (.xlsx)
    │
    ▼
custom_execution.py
    ├── 1. Set TABLE_NAME
    ├── 2. Auto-discover Excel file
    ├── 3. generate_etl_runbook.py → etl_output/ artifacts
    │       └── excel_schema_parser.py → parse Excel (calls verify_excel_syntax.py)
    ├── 4. Load parameters.json → fill config variables
    ├── 5. User overrides (optional — uncomment lines)
    └── 6. Extract per server → output/<table>/<subpath>/<server>/source_raw.csv
```

### Standalone Execution

```
excel_schema_parser.py     → standalone: parse Excel → JSON file
generate_etl_runbook.py    → standalone: parse Excel → SQL/PySpark artifacts
verify_excel_syntax.py     → standalone: validate Excel format
```

---

## 2. Folder Structure

```
Extract/
├── custom_execution.py           ← Main extract script (server loop)
├── custom_transform.py           ← Local transform script
├── CONTEXT.md                    ← This file
├── requirements.txt
├── config/
│   ├── pipeline_config.yaml      ← JVM/Hadoop paths, logging
│   └── source_config.yaml        ← Multi-server DB connections
├── excel_files/
│   ├── verify_excel_syntax.py    ← Syntax verifier (run before parser)
│   ├── excel_syntax_guide.md     ← Excel format specification
│   ├── excel_schema_parser.py    ← Parser: Excel → JSON
│   ├── generate_etl_runbook.py   ← Generator: JSON → SQL/PySpark artifacts
│   ├── *.xlsx                    ← Excel mapping specs
│   └── etl_output/               ← Generated runbook artifacts
│       └── <target_table>/
│           ├── 02_create_target_sf.sql      (shared Snowflake DDL)
│           ├── 05_extract_target_sf.sql     (shared target SELECT)
│           └── <server>/
│               ├── source_tables/
│               │   └── 01_src_*_ddl.sql     (partial source DDLs)
│               ├── 03_extract_source.sql    (combined aliased query)
│               ├── 04_transform.py          (PySpark transforms)
│               └── 06_pipeline_log.md       (audit log)
├── jars/                         ← JDBC drivers (auto-discovered)
├── output/                       ← Extraction results
│   └── <target_table>/
│       └── <server>/
│           ├── source_raw.csv
│           ├── source_raw.schema.json
│           └── transformed.csv
└── utils/
    ├── __init__.py
    ├── auto_config.py            ← V3 folder detection + config builder
    ├── config_loader.py          ← YAML loader
    ├── csv_writer.py             ← DataFrame → single CSV file
    ├── custom_execution_utils.py ← Pipeline step functions (V3 + legacy)
    ├── get_data.py               ← JDBC data extraction
    ├── logger.py                 ← Logging setup
    ├── query_filter.py           ← WHERE clause builder
    ├── verify_schema.py          ← Schema verification (partial + full)
    └── connections/
        ├── source_connection.py  ← JDBC options builder (multi-server)
        └── spark_session.py      ← Local PySpark session
```

---

## 3. Key Concepts

### Verification → Parsing → Generation Chain

```
custom_execution.py → generate_etl_runbook.py → excel_schema_parser.py → verify_excel_syntax.py
      (auto)                (auto)                     (auto)                    (auto)
```

- **`custom_execution.py`** sets TABLE_NAME, auto-discovers Excel, calls `generate_runbook()`, loads `parameters.json`
- **`generate_etl_runbook.py`** calls parser → produces all SQL/PySpark artifacts
- **`excel_schema_parser.py`** is the single source-of-truth parser — calls verifier automatically
- **`verify_excel_syntax.py`** validates Excel format (headers, types, joins, cross-sheet consistency)

### V3 Combined Aliased Extract

- One SQL query per server joins all source tables and aliases columns to target names
- CSV columns are already target-named — no rename transforms needed
- Transform receives single DataFrame: `apply_transforms(df: DataFrame)`
- One CSV per server; servers produce different rows for same target table

### Multi-Server Config (`config/source_config.yaml`)

```yaml
servers:
  server1:
    db_type: mysql         # or sqlserver
    host: localhost
    port: 3306
    database: etl_output_mysql_xl
    user: root
    password: root
    fetchsize: 50000
  # server2:
  #   db_type: sqlserver
  #   host: 192.168.1.50
  #   ...
```

Each key under `servers` matches a **server subdirectory** in `etl_output/<table>/`.

### Auto-Detection (V3 vs Legacy)

`auto_config.get_table_config()` auto-detects layout:
- **V3**: `<table>/<server>/03_extract_source.sql` exists → returns `{"servers": [...]}`
- **Legacy**: `<table>/03_extract_source.sql` at root → returns flat config

---

## 4. File-by-File Reference

### `custom_execution.py` — Extract Orchestrator

**Purpose**: Connect to source DBs, extract raw data, save CSV per server.

**Config**: Set `TABLE_NAME` at top (only required variable). Everything else is loaded from `parameters.json` generated by the runbook. Override any variable in the USER OVERRIDES section.

**Auto-bootstrap flow**:
1. Discover Excel file matching TABLE_NAME in `excel_files/`
2. Call `generate_runbook()` → regenerates all artifacts from Excel
3. Load `parameters.json` → fills SUBPATH, VERIFY_SCHEMA, PK_FILTER_MODE, etc.
4. User can override any value by uncommenting lines in Section 4

**V3 Flow** (in `main()`):
```
For each server in config["servers"]:
  Step 0: verify_partial_schemas() — check DDL columns exist in live DB
  Step 1: extract_source_v3()     — execute 03_extract_source.sql via JDBC
  Step 1.5: save_raw_source_csv() — write output/<table>/<subpath>/<server>/source_raw.csv
```

**Key imports**: `step_0_verify_partial_schemas`, `step_1_extract_source_v3`, `step_1_5_save_raw_source_csv_v3` from `custom_execution_utils`.

---

### `custom_transform.py` — Local Transform

**Purpose**: Load raw CSV → apply `04_transform.py` → save `transformed.csv`.

**Flow per server**:
1. Load `output/<table>/<server>/source_raw.csv` with `.schema.json` for types
2. `importlib.util.spec_from_file_location()` to load `04_transform.py`
3. Call `apply_transforms(df)` → transformed DataFrame
4. Save `output/<table>/<server>/transformed.csv`

---

### `utils/auto_config.py` — Config Builder

**`get_table_config(table_name)`** returns:

```python
# V3 return shape
{
    "table_name": "public_dim_vehicle_master",
    "target_ddl": "excel_files/etl_output/.../02_create_target_sf.sql",
    "target_query_file": "excel_files/etl_output/.../05_extract_target_sf.sql",
    "primary_keys": ["VEHICLE_KEY"],
    "exclude_cols": ["load_ts", "batch_id"],
    "servers": [
        {
            "server_name": "server1",
            "source_query_file": ".../server1/03_extract_source.sql",
            "transform_file": ".../server1/04_transform.py",
            "source_ddls": [".../source_tables/01_src_*_ddl.sql", ...],
            "output_dir": "output/public_dim_vehicle_master/server1",
        },
    ],
}
```

**Key functions**:
- `_detect_server_dirs(table_folder)` — finds subdirs with `03_extract_source.sql`
- `_build_v3_config()` / `_build_legacy_config()` — layout-specific builders
- `_parse_primary_keys_from_target_ddl()` — reads PKs from Snowflake DDL
- `parse_source_ddl_table_info(ddl_file)` — extracts `(database, table)` from partial DDL
- `build_filter_for_query()` — builds WHERE clause for PK/date filtering

---

### `utils/connections/source_connection.py` — JDBC Builder

**`get_source_connection(server_name=None)`**:
- Reads `config/source_config.yaml`
- If `server_name` + `servers:` key: reads `servers.<name>` block
- If no `server_name` but `servers:` exists: uses first server (with warning)
- Legacy fallback: reads flat top-level keys
- Returns `{"url": jdbc_url, "driver": ..., "user": ..., "password": ..., "fetchsize": ...}`
- Supports `db_type`: `mysql`, `sqlserver`

---

### `utils/verify_schema.py` — Schema Verification

**`verify_partial_ddls(spark, jdbc_opts, ddl_files, ...)`** (V3):
- Iterates partial DDL files
- For each: parses `db.table` from DDL, fetches live INFORMATION_SCHEMA
- Partial check: DDL columns ⊆ live columns (extras in live DB ignored)
- Uses `_compare_columns_partial()` (not `_compare_columns()` which flags extras)

**`verify_schema_from_ddl(...)`** (Legacy): Full verification, flags extras too.

---

### `utils/get_data.py` — JDBC Extraction

**`get_data(spark, query, server_name=None, ...)`**:
- Calls `get_source_connection(server_name=server_name)`
- Two read paths: single-partition (default) or partitioned JDBC
- Returns cached DataFrame with row count logged

---

### `utils/custom_execution_utils.py` — Pipeline Steps

**V3 steps** (per-server context):
- `step_0_verify_partial_schemas(spark, ctx)` — iterates `ctx["source_ddls"]`
- `step_1_extract_source_v3(spark, ctx)` — resolves SQL, applies filters, extracts
- `step_1_5_save_raw_source_csv_v3(source_df, ctx)` — saves CSV

**Legacy steps** (backward compatible):
- `step_0_verify_source_schema(spark, ctx, path_sub)`
- `step_1_extract_source(spark, ctx)`
- `step_1_5_save_raw_source_csv(source_df, ctx)`

**Helpers**:
- `resolve_query(query, query_file, type)` — reads SQL file, strips `USE` statements and semicolons
- `build_source_filter(config, ...)` — builds WHERE clause dict
- `detect_partition_bounds(spark, col, config, server_name)` — MIN/MAX pre-query

---

### `utils/csv_writer.py` — CSV Writer

**`save_dataframe_as_csv(df, file_path)`**:
- `coalesce(1)` → rename part file → clean path
- Also saves `.schema.json` alongside for type-safe reloading

---

### `utils/query_filter.py` — WHERE Clause Builder

**`build_where_clause(...)`**: Combines PK filter + date watermark into SQL WHERE string.
**`apply_filter_to_sql(base_sql, where_clause)`**: Injects WHERE before ORDER BY.

---

## 5. Generated Artifacts Reference

### `03_extract_source.sql` — Combined Query (per server)

```sql
WITH
cte_main AS (
    SELECT col1, col2, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM db.main_table
),
cte_other AS (
    SELECT colA, colB, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM db.other_table
)
SELECT
    cte_main.col1 AS TARGET_COL_1,
    cte_main.col2 AS TARGET_COL_2,
    cte_other.colA AS TARGET_COL_3
FROM cte_main
JOIN cte_other ON cte_main.rn = cte_other.rn;
```

### `04_transform.py` — PySpark Transform (per server)

```python
def apply_transforms(df: DataFrame) -> DataFrame:
    # direct/rename: no-op (already aliased in SQL)
    # cast: df = df.withColumn('COL', F.round(F.col('COL'), 2))
    # derived: df = df.withColumn('COL', F.when(...).otherwise(...))
    # constant: df = df.withColumn('LOAD_TS', F.current_timestamp())
    # global transforms: TRIM, UPPER, REPLACE on filtered columns
    # reorder to target schema
    # null validation
    return df
```

### `01_src_*_ddl.sql` — Partial Source DDLs

Only columns mapped in Excel spec. Used for schema verification.

### `02_create_target_sf.sql` — Snowflake Target DDL (shared)

Full target table DDL with Snowflake types, PKs, defaults.

---

## 6. Usage

### Extract raw data:
```bash
cd Extract/
# Edit TABLE_NAME in custom_execution.py
python custom_execution.py
# Output: output/<table>/<server>/source_raw.csv
```

### Transform locally:
```bash
# Edit TABLE_NAME in custom_transform.py
python custom_transform.py
# Output: output/<table>/<server>/transformed.csv
```

### Load into Snowflake:
1. Run `02_create_target_sf.sql` on Snowflake
2. Upload each server's `transformed.csv` into the target table

### Compare on Databricks:
Upload `source_raw.csv` + `.schema.json` to Azure Blob, run `Execution/main.py`.

---

## 7. Dependencies

```
pyspark
pyyaml
openpyxl        # for parser only
```

JDBC drivers in `jars/` (auto-discovered by `spark_session.py`):
- MySQL: `mysql-connector-j-*.jar`
- SQL Server: `mssql-jdbc-*.jar`

