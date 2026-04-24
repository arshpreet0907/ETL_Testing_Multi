"""
04_transform.py  —  server1 -> analytics_dw.public.dim_vehicle_master
Generated: 2026-04-24 11:31
Approach: V3 (Combined Aliased Extract)

Input DataFrame columns are already aliased to target names by the
extract SQL. COPY/RENAME transforms are skipped.
"""
from __future__ import annotations
import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    BooleanType, DecimalType, DoubleType,
    IntegerType, LongType, StringType, TimestampType,
)

logger = logging.getLogger(__name__)


def apply_transforms(df: DataFrame) -> DataFrame:
    """
    Apply transforms for server 'server1'.
    Target: analytics_dw.public.dim_vehicle_master

    Input DF columns are target-named (SQL aliased).
    COPY/RENAME transforms are no-ops.
    """
    logger.info('=' * 70)
    logger.info('START TRANSFORM | server1 -> analytics_dw.public.dim_vehicle_master')
    logger.info('  Input cols: %s', df.columns)

    # DIRECT: vehicle_id → VEHICLE_KEY — already aliased in SQL  # PK

    # RENAME: vin_number → VIN — already aliased in SQL

    # RENAME: model_nm → MODEL_NAME — already aliased in SQL

    # RENAME: variant_cd → VARIANT_CODE — already aliased in SQL

    # RENAME: model_yr → MODEL_YEAR — already aliased in SQL

    # RENAME: engine_type_cd → ENGINE_TYPE — already aliased in SQL

    # RENAME: plant_cd → PLANT_CODE — already aliased in SQL

    # CAST: base_price_amt → BASE_PRICE
    df = df.withColumn('BASE_PRICE', F.round(F.col('BASE_PRICE'), 2))

    # RENAME: status_cd → VEHICLE_STATUS — already aliased in SQL

    # RENAME: launch_dt → LAUNCH_DATE — already aliased in SQL

    # DERIVED: is_electric_flag → IS_ELECTRIC  # Y/N → 1/0
    df = df.withColumn('IS_ELECTRIC', F.when(F.col('IS_ELECTRIC') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'IS_ELECTRIC': 0})

    # RENAME: part_id → PRIMARY_PART_KEY — already aliased in SQL  # FK - primary part

    # NULL values in PRIMARY_PART_KEY remain as NULL

    # RENAME: part_no → PRIMARY_PART_NO — already aliased in SQL

    # NULL values in PRIMARY_PART_NO remain as NULL

    # RENAME: part_category → PART_CATEGORY — already aliased in SQL

    # NULL values in PART_CATEGORY remain as NULL

    # RENAME: unit_cost_amt → PART_UNIT_COST — already aliased in SQL

    # NULL values in PART_UNIT_COST remain as NULL

    # RENAME: qty_on_hand → PART_STOCK_QTY — already aliased in SQL

    df = df.fillna({'PART_STOCK_QTY': 0})

    # DERIVED: is_critical_flag → IS_CRITICAL_PART  # Y/N → 1/0
    df = df.withColumn('IS_CRITICAL_PART', F.when(F.col('IS_CRITICAL_PART') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'IS_CRITICAL_PART': 0})

    # RENAME: supplier_id → SUPPLIER_KEY — already aliased in SQL  # FK - primary supplier

    # NULL values in SUPPLIER_KEY remain as NULL

    # RENAME: supplier_nm → SUPPLIER_NAME — already aliased in SQL

    # NULL values in SUPPLIER_NAME remain as NULL

    # CAST: country_cd → SUPPLIER_COUNTRY  # Uppercased
    df = df.withColumn('SUPPLIER_COUNTRY', F.upper(F.col('SUPPLIER_COUNTRY')))
    # NULL values in SUPPLIER_COUNTRY remain as NULL

    # RENAME: rating_score → SUPPLIER_RATING — already aliased in SQL

    # NULL values in SUPPLIER_RATING remain as NULL

    # DERIVED: is_approved_flag → IS_APPROVED_SUPPLIER  # Y/N → 1/0
    df = df.withColumn('IS_APPROVED_SUPPLIER', F.when(F.col('IS_APPROVED_SUPPLIER') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'IS_APPROVED_SUPPLIER': 0})

    # RENAME: emp_id → PLANT_CONTACT_KEY — already aliased in SQL  # FK - plant contact

    # NULL values in PLANT_CONTACT_KEY remain as NULL

    # DERIVED: first_nm → PLANT_CONTACT_NAME  # Full name
    df = df.withColumn('PLANT_CONTACT_NAME', F.concat(F.col('PLANT_CONTACT_NAME'), F.lit(" "), F.col('last_nm')))
    # NULL values in PLANT_CONTACT_NAME remain as NULL

    # RENAME: dept_nm → CONTACT_DEPARTMENT — already aliased in SQL

    # NULL values in CONTACT_DEPARTMENT remain as NULL

    # RENAME: role_nm → CONTACT_ROLE — already aliased in SQL

    # NULL values in CONTACT_ROLE remain as NULL

    # CONSTANT: → LOAD_TS  # ETL load timestamp
    df = df.withColumn('LOAD_TS', F.current_timestamp())

    # CONSTANT: → BATCH_ID  # ETL batch identifier
    df = df.withColumn('BATCH_ID', F.lit("ETL_VALIDATION"))

    # ═══════════════════════════════════════════════════════════════
    # GLOBAL TRANSFORMS — applied to filtered column sets in order
    # ═══════════════════════════════════════════════════════════════

    # Global #1: TRIM | condition=all  # Trim all columns
    _gt_cols = ['VEHICLE_KEY', 'VIN', 'MODEL_NAME', 'VARIANT_CODE', 'MODEL_YEAR', 'ENGINE_TYPE', 'PLANT_CODE', 'BASE_PRICE', 'VEHICLE_STATUS', 'LAUNCH_DATE', 'IS_ELECTRIC', 'PRIMARY_PART_KEY', 'PRIMARY_PART_NO', 'PART_CATEGORY', 'PART_UNIT_COST', 'PART_STOCK_QTY', 'IS_CRITICAL_PART', 'SUPPLIER_KEY', 'SUPPLIER_NAME', 'SUPPLIER_COUNTRY', 'SUPPLIER_RATING', 'IS_APPROVED_SUPPLIER', 'PLANT_CONTACT_KEY', 'PLANT_CONTACT_NAME', 'CONTACT_DEPARTMENT', 'CONTACT_ROLE', 'LOAD_TS', 'BATCH_ID']
    for _c in _gt_cols:
        if _c in df.columns:
            df = df.withColumn(_c, F.trim(F.col(_c)))
    logger.debug('  [global #1] TRIM applied to %d cols', len(_gt_cols))

    # Global #2: UPPER | condition=dtype:VARCHAR  # Uppercase all string columns
    _gt_cols = ['VIN', 'MODEL_NAME', 'VARIANT_CODE', 'ENGINE_TYPE', 'PLANT_CODE', 'VEHICLE_STATUS', 'PRIMARY_PART_NO', 'PART_CATEGORY', 'SUPPLIER_NAME', 'SUPPLIER_COUNTRY', 'PLANT_CONTACT_NAME', 'CONTACT_DEPARTMENT', 'CONTACT_ROLE', 'BATCH_ID']
    for _c in _gt_cols:
        if _c in df.columns:
            df = df.withColumn(_c, F.upper(F.col(_c)))
    logger.debug('  [global #2] UPPER applied to %d cols', len(_gt_cols))

    # Global #3: REPLACE | condition=dtype:VARCHAR  # Normalise en-dashes
    _gt_cols = ['VIN', 'MODEL_NAME', 'VARIANT_CODE', 'ENGINE_TYPE', 'PLANT_CODE', 'VEHICLE_STATUS', 'PRIMARY_PART_NO', 'PART_CATEGORY', 'SUPPLIER_NAME', 'SUPPLIER_COUNTRY', 'PLANT_CONTACT_NAME', 'CONTACT_DEPARTMENT', 'CONTACT_ROLE', 'BATCH_ID']
    for _c in _gt_cols:
        if _c in df.columns:
            df = df.withColumn(_c, F.regexp_replace(F.col(_c), F.lit('–'), F.lit('_')))
    logger.debug('  [global #3] REPLACE applied to %d cols', len(_gt_cols))

    # Global #4: STRIP_WHITESPACE | condition=dtype:VARCHAR  # Collapse multiple spaces
    _gt_cols = ['VIN', 'MODEL_NAME', 'VARIANT_CODE', 'ENGINE_TYPE', 'PLANT_CODE', 'VEHICLE_STATUS', 'PRIMARY_PART_NO', 'PART_CATEGORY', 'SUPPLIER_NAME', 'SUPPLIER_COUNTRY', 'PLANT_CONTACT_NAME', 'CONTACT_DEPARTMENT', 'CONTACT_ROLE', 'BATCH_ID']
    for _c in _gt_cols:
        if _c in df.columns:
            df = df.withColumn(_c, F.regexp_replace(F.col(_c), r'\s+', ' '))
            df = df.withColumn(_c, F.trim(F.col(_c)))
    logger.debug('  [global #4] STRIP_WHITESPACE applied to %d cols', len(_gt_cols))

    # Reorder to target schema
    _exp = ['VEHICLE_KEY', 'VIN', 'MODEL_NAME', 'VARIANT_CODE', 'MODEL_YEAR', 'ENGINE_TYPE', 'PLANT_CODE', 'BASE_PRICE', 'VEHICLE_STATUS', 'LAUNCH_DATE', 'IS_ELECTRIC', 'PRIMARY_PART_KEY', 'PRIMARY_PART_NO', 'PART_CATEGORY', 'PART_UNIT_COST', 'PART_STOCK_QTY', 'IS_CRITICAL_PART', 'SUPPLIER_KEY', 'SUPPLIER_NAME', 'SUPPLIER_COUNTRY', 'SUPPLIER_RATING', 'IS_APPROVED_SUPPLIER', 'PLANT_CONTACT_KEY', 'PLANT_CONTACT_NAME', 'CONTACT_DEPARTMENT', 'CONTACT_ROLE', 'LOAD_TS', 'BATCH_ID']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # Batch null validation
    _nn_cols = ['VEHICLE_KEY', 'VIN', 'MODEL_NAME', 'VARIANT_CODE', 'MODEL_YEAR', 'ENGINE_TYPE', 'PLANT_CODE', 'BASE_PRICE', 'VEHICLE_STATUS', 'LAUNCH_DATE']
    _null_exprs = [F.count(F.when(F.col(c).isNull(), 1)).alias(f'_null_{c}') for c in _nn_cols if c in df.columns]
    _null_exprs.append(F.count('*').alias('_total_rows'))
    _null_result = df.select(*_null_exprs).first()
    _null_violations = {c: _null_result[f'_null_{c}'] for c in _nn_cols if c in df.columns and _null_result[f'_null_{c}'] > 0}
    if _null_violations:
        for _col, _cnt in _null_violations.items():
            logger.error("  NULL in non-nullable '%s': %d rows", _col, _cnt)
        raise ValueError(f'NULL values in non-nullable columns: {_null_violations}')
    logger.info('  Null check passed for %d cols', len(_nn_cols))
    logger.info('  Output rows: %d', _null_result['_total_rows'])
    logger.info('  Output cols: %s', df.columns)
    logger.info('END TRANSFORM')
    logger.info('=' * 70)
    return df
