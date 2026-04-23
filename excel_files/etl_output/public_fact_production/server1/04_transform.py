"""
04_transform.py  —  server1 -> analytics_dw.public.fact_production
Generated: 2026-04-24 01:13
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
    Target: analytics_dw.public.fact_production

    Input DF columns are target-named (SQL aliased).
    COPY/RENAME transforms are no-ops.
    """
    logger.info('=' * 70)
    logger.info('START TRANSFORM | server1 -> analytics_dw.public.fact_production')
    logger.info('  Input cols: %s', df.columns)

    # DIRECT: prod_order_id → PROD_ORDER_KEY — already aliased in SQL  # PK

    # RENAME: vehicle_id → VEHICLE_KEY — already aliased in SQL  # FK to dim_vehicle

    # RENAME: plant_cd → PLANT_CODE — already aliased in SQL

    # RENAME: order_dt → ORDER_DATE — already aliased in SQL

    # RENAME: qty_produced → PRODUCED_QTY — already aliased in SQL

    df = df.fillna({'PRODUCED_QTY': 0})

    # RENAME: qty_rejected → REJECTED_QTY — already aliased in SQL

    df = df.fillna({'REJECTED_QTY': 0})

    # RENAME: order_status_cd → ORDER_STATUS — already aliased in SQL

    # RENAME: efficiency_pct → EFFICIENCY_PCT — already aliased in SQL

    # NULL values in EFFICIENCY_PCT remain as NULL

    # DERIVED: qty_planned → YIELD_RATE_PCT  # Derived: produced/planned %
    df = df.withColumn('YIELD_RATE_PCT', F.when(F.col('YIELD_RATE_PCT') > F.lit(0), F.expr("ROUND((PRODUCED_QTY/YIELD_RATE_PCT)*100,2)")).otherwise(F.lit(0)))
    df = df.fillna({'YIELD_RATE_PCT': 0})

    # RENAME: inspection_id → INSPECTION_KEY — already aliased in SQL  # FK to QC

    # NULL values in INSPECTION_KEY remain as NULL

    # RENAME: inspection_dt → INSPECTION_DATE — already aliased in SQL

    # NULL values in INSPECTION_DATE remain as NULL

    # RENAME: result_cd → QC_RESULT — already aliased in SQL  # PASS/FAIL/REWORK

    # NULL values in QC_RESULT remain as NULL

    # RENAME: inspection_score → QC_SCORE — already aliased in SQL

    df = df.fillna({'QC_SCORE': 0})

    # DERIVED: rework_required_flag → QC_REWORK_FLAG  # Y/N → 1/0
    df = df.withColumn('QC_REWORK_FLAG', F.when(F.col('QC_REWORK_FLAG') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'QC_REWORK_FLAG': 0})

    # RENAME: rework_cost_amt → QC_REWORK_COST — already aliased in SQL

    df = df.fillna({'QC_REWORK_COST': 0})

    # RENAME: paint_log_id → PAINT_LOG_KEY — already aliased in SQL  # FK to paint

    # NULL values in PAINT_LOG_KEY remain as NULL

    # RENAME: color_cd → PAINT_COLOR_CODE — already aliased in SQL

    # NULL values in PAINT_COLOR_CODE remain as NULL

    # RENAME: oven_temp_celsius → PAINT_OVEN_TEMP_C — already aliased in SQL

    # NULL values in PAINT_OVEN_TEMP_C remain as NULL

    # DERIVED: defect_flag → PAINT_DEFECT_FLAG  # Y/N → 1/0
    df = df.withColumn('PAINT_DEFECT_FLAG', F.when(F.col('PAINT_DEFECT_FLAG') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'PAINT_DEFECT_FLAG': 0})

    # RENAME: paint_cost_amt → PAINT_COST — already aliased in SQL

    # NULL values in PAINT_COST remain as NULL

    # RENAME: assembly_log_id → ENGINE_LOG_KEY — already aliased in SQL  # FK to engine

    # NULL values in ENGINE_LOG_KEY remain as NULL

    # RENAME: engine_type_cd → ENGINE_TYPE — already aliased in SQL

    # NULL values in ENGINE_TYPE remain as NULL

    # DIRECT: torque_nm → ENGINE_TORQUE_NM — already aliased in SQL

    # NULL values in ENGINE_TORQUE_NM remain as NULL

    # RENAME: test_result_cd → ENGINE_TEST_RESULT — already aliased in SQL  # PASS/FAIL/RETEST

    # NULL values in ENGINE_TEST_RESULT remain as NULL

    # RENAME: assembly_cost_amt → ENGINE_ASSEMBLY_COST — already aliased in SQL

    # NULL values in ENGINE_ASSEMBLY_COST remain as NULL

    # DERIVED: defect_flag → ENGINE_DEFECT_FLAG  # Y/N → 1/0
    df = df.withColumn('ENGINE_DEFECT_FLAG', F.when(F.col('ENGINE_DEFECT_FLAG') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'ENGINE_DEFECT_FLAG': 0})

    # CONSTANT: → LOAD_TS  # ETL load timestamp
    df = df.withColumn('LOAD_TS', F.current_timestamp())

    # CONSTANT: → BATCH_ID  # ETL batch identifier
    df = df.withColumn('BATCH_ID', F.lit("ETL_VALIDATION"))

    # ═══════════════════════════════════════════════════════════════
    # GLOBAL TRANSFORMS — applied to filtered column sets in order
    # ═══════════════════════════════════════════════════════════════

    # Global #1: TRIM | condition=all  # Trim all columns
    _gt_cols = ['PROD_ORDER_KEY', 'VEHICLE_KEY', 'PLANT_CODE', 'ORDER_DATE', 'PRODUCED_QTY', 'REJECTED_QTY', 'ORDER_STATUS', 'EFFICIENCY_PCT', 'YIELD_RATE_PCT', 'INSPECTION_KEY', 'INSPECTION_DATE', 'QC_RESULT', 'QC_SCORE', 'QC_REWORK_FLAG', 'QC_REWORK_COST', 'PAINT_LOG_KEY', 'PAINT_COLOR_CODE', 'PAINT_OVEN_TEMP_C', 'PAINT_DEFECT_FLAG', 'PAINT_COST', 'ENGINE_LOG_KEY', 'ENGINE_TYPE', 'ENGINE_TORQUE_NM', 'ENGINE_TEST_RESULT', 'ENGINE_ASSEMBLY_COST', 'ENGINE_DEFECT_FLAG', 'LOAD_TS', 'BATCH_ID']
    for _c in _gt_cols:
        if _c in df.columns:
            df = df.withColumn(_c, F.trim(F.col(_c)))
    logger.debug('  [global #1] TRIM applied to %d cols', len(_gt_cols))

    # Global #2: UPPER | condition=dtype:VARCHAR  # Uppercase all string columns
    _gt_cols = ['PLANT_CODE', 'ORDER_STATUS', 'QC_RESULT', 'PAINT_COLOR_CODE', 'ENGINE_TYPE', 'ENGINE_TEST_RESULT', 'BATCH_ID']
    for _c in _gt_cols:
        if _c in df.columns:
            df = df.withColumn(_c, F.upper(F.col(_c)))
    logger.debug('  [global #2] UPPER applied to %d cols', len(_gt_cols))

    # Global #3: REPLACE | condition=dtype:VARCHAR  # Normalise en-dashes
    _gt_cols = ['PLANT_CODE', 'ORDER_STATUS', 'QC_RESULT', 'PAINT_COLOR_CODE', 'ENGINE_TYPE', 'ENGINE_TEST_RESULT', 'BATCH_ID']
    for _c in _gt_cols:
        if _c in df.columns:
            df = df.withColumn(_c, F.regexp_replace(F.col(_c), F.lit('–'), F.lit('−')))
    logger.debug('  [global #3] REPLACE applied to %d cols', len(_gt_cols))

    # Global #4: STRIP_WHITESPACE | condition=dtype:VARCHAR  # Collapse multiple spaces
    _gt_cols = ['PLANT_CODE', 'ORDER_STATUS', 'QC_RESULT', 'PAINT_COLOR_CODE', 'ENGINE_TYPE', 'ENGINE_TEST_RESULT', 'BATCH_ID']
    for _c in _gt_cols:
        if _c in df.columns:
            df = df.withColumn(_c, F.regexp_replace(F.col(_c), r'\s+', ' '))
            df = df.withColumn(_c, F.trim(F.col(_c)))
    logger.debug('  [global #4] STRIP_WHITESPACE applied to %d cols', len(_gt_cols))

    # Reorder to target schema
    _exp = ['PROD_ORDER_KEY', 'VEHICLE_KEY', 'PLANT_CODE', 'ORDER_DATE', 'PRODUCED_QTY', 'REJECTED_QTY', 'ORDER_STATUS', 'EFFICIENCY_PCT', 'YIELD_RATE_PCT', 'INSPECTION_KEY', 'INSPECTION_DATE', 'QC_RESULT', 'QC_SCORE', 'QC_REWORK_FLAG', 'QC_REWORK_COST', 'PAINT_LOG_KEY', 'PAINT_COLOR_CODE', 'PAINT_OVEN_TEMP_C', 'PAINT_DEFECT_FLAG', 'PAINT_COST', 'ENGINE_LOG_KEY', 'ENGINE_TYPE', 'ENGINE_TORQUE_NM', 'ENGINE_TEST_RESULT', 'ENGINE_ASSEMBLY_COST', 'ENGINE_DEFECT_FLAG', 'LOAD_TS', 'BATCH_ID']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # Batch null validation
    _nn_cols = ['PROD_ORDER_KEY', 'VEHICLE_KEY', 'PLANT_CODE', 'ORDER_DATE', 'ORDER_STATUS']
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
