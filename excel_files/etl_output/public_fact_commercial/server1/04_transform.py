"""
04_transform.py  —  server1 -> analytics_dw.public.fact_commercial
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
    Target: analytics_dw.public.fact_commercial

    Input DF columns are target-named (SQL aliased).
    COPY/RENAME transforms are no-ops.
    """
    logger.info('=' * 70)
    logger.info('START TRANSFORM | server1 -> analytics_dw.public.fact_commercial')
    logger.info('  Input cols: %s', df.columns)

    # DIRECT: sales_order_id → SALES_ORDER_KEY — already aliased in SQL  # PK

    # RENAME: vehicle_id → VEHICLE_KEY — already aliased in SQL  # FK to dim_vehicle

    # RENAME: customer_id → CUSTOMER_KEY — already aliased in SQL

    # RENAME: order_dt → SALE_DATE — already aliased in SQL

    # RENAME: sale_price_amt → SALE_PRICE — already aliased in SQL

    # RENAME: discount_pct → DISCOUNT_PCT — already aliased in SQL

    df = df.fillna({'DISCOUNT_PCT': 0})

    # RENAME: total_invoice_amt → TOTAL_INVOICE — already aliased in SQL

    # RENAME: payment_mode_cd → PAYMENT_MODE — already aliased in SQL

    # RENAME: order_status_cd → SALE_STATUS — already aliased in SQL

    # RENAME: region_cd → REGION — already aliased in SQL

    # RENAME: claim_id → CLAIM_KEY — already aliased in SQL  # FK to warranty

    # NULL values in CLAIM_KEY remain as NULL

    # RENAME: claim_dt → CLAIM_DATE — already aliased in SQL

    # NULL values in CLAIM_DATE remain as NULL

    # RENAME: defect_type_cd → CLAIM_DEFECT_TYPE — already aliased in SQL

    # NULL values in CLAIM_DEFECT_TYPE remain as NULL

    # RENAME: repair_cost_amt → CLAIM_REPAIR_COST — already aliased in SQL

    # NULL values in CLAIM_REPAIR_COST remain as NULL

    # RENAME: claim_status_cd → CLAIM_STATUS — already aliased in SQL

    # NULL values in CLAIM_STATUS remain as NULL

    # DERIVED: supplier_liability_flag → SUPPLIER_LIABLE  # Y/N → 1/0
    df = df.withColumn('SUPPLIER_LIABLE', F.when(F.col('SUPPLIER_LIABLE') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'SUPPLIER_LIABLE': 0})

    # RENAME: shipment_id → SHIPMENT_KEY — already aliased in SQL  # FK to shipment

    # NULL values in SHIPMENT_KEY remain as NULL

    # RENAME: carrier_nm → CARRIER_NAME — already aliased in SQL

    # NULL values in CARRIER_NAME remain as NULL

    # RENAME: shipment_dt → SHIPMENT_DATE — already aliased in SQL

    # NULL values in SHIPMENT_DATE remain as NULL

    # RENAME: freight_cost_amt → FREIGHT_COST — already aliased in SQL

    # NULL values in FREIGHT_COST remain as NULL

    # RENAME: status_cd → SHIPMENT_STATUS — already aliased in SQL

    # NULL values in SHIPMENT_STATUS remain as NULL

    # DERIVED: estimated_arrival_dt → IS_DELAYED  # 1 if arrived after ETA
    df = df.withColumn('IS_DELAYED', F.when(F.col('actual_arrival_dt').isNotNull() & (F.col('actual_arrival_dt') > F.col('IS_DELAYED')), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'IS_DELAYED': 0})

    # RENAME: ledger_id → LEDGER_KEY — already aliased in SQL  # FK to cost

    # NULL values in LEDGER_KEY remain as NULL

    # RENAME: cost_type_cd → COST_TYPE — already aliased in SQL  # Material/Labour/Overhead

    # NULL values in COST_TYPE remain as NULL

    # RENAME: posting_dt → COST_POSTING_DATE — already aliased in SQL

    # NULL values in COST_POSTING_DATE remain as NULL

    # RENAME: amount_lc → COST_AMOUNT_LOCAL — already aliased in SQL

    # NULL values in COST_AMOUNT_LOCAL remain as NULL

    # DIRECT: amount_usd → COST_AMOUNT_USD — already aliased in SQL

    # NULL values in COST_AMOUNT_USD remain as NULL

    # DERIVED: approved_flag → COST_APPROVED  # Y/N → 1/0
    df = df.withColumn('COST_APPROVED', F.when(F.col('COST_APPROVED') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'COST_APPROVED': 0})

    # CONSTANT: → LOAD_TS  # ETL load timestamp
    df = df.withColumn('LOAD_TS', F.current_timestamp())

    # CONSTANT: → BATCH_ID  # ETL batch identifier
    df = df.withColumn('BATCH_ID', F.lit("ETL_VALIDATION"))

    # ═══════════════════════════════════════════════════════════════
    # GLOBAL TRANSFORMS — applied to filtered column sets in order
    # ═══════════════════════════════════════════════════════════════

    # Global #1: TRIM | condition=all  # Trim all columns
    _gt_cols = ['SALES_ORDER_KEY', 'VEHICLE_KEY', 'CUSTOMER_KEY', 'SALE_DATE', 'SALE_PRICE', 'DISCOUNT_PCT', 'TOTAL_INVOICE', 'PAYMENT_MODE', 'SALE_STATUS', 'REGION', 'CLAIM_KEY', 'CLAIM_DATE', 'CLAIM_DEFECT_TYPE', 'CLAIM_REPAIR_COST', 'CLAIM_STATUS', 'SUPPLIER_LIABLE', 'SHIPMENT_KEY', 'CARRIER_NAME', 'SHIPMENT_DATE', 'FREIGHT_COST', 'SHIPMENT_STATUS', 'IS_DELAYED', 'LEDGER_KEY', 'COST_TYPE', 'COST_POSTING_DATE', 'COST_AMOUNT_LOCAL', 'COST_AMOUNT_USD', 'COST_APPROVED', 'LOAD_TS', 'BATCH_ID']
    for _c in _gt_cols:
        if _c in df.columns:
            df = df.withColumn(_c, F.trim(F.col(_c)))
    logger.debug('  [global #1] TRIM applied to %d cols', len(_gt_cols))

    # Global #2: UPPER | condition=dtype:VARCHAR  # Uppercase all string columns
    _gt_cols = ['PAYMENT_MODE', 'SALE_STATUS', 'REGION', 'CLAIM_DEFECT_TYPE', 'CLAIM_STATUS', 'CARRIER_NAME', 'SHIPMENT_STATUS', 'COST_TYPE', 'BATCH_ID']
    for _c in _gt_cols:
        if _c in df.columns:
            df = df.withColumn(_c, F.upper(F.col(_c)))
    logger.debug('  [global #2] UPPER applied to %d cols', len(_gt_cols))

    # Global #3: REPLACE | condition=dtype:VARCHAR  # Normalise en-dashes
    _gt_cols = ['PAYMENT_MODE', 'SALE_STATUS', 'REGION', 'CLAIM_DEFECT_TYPE', 'CLAIM_STATUS', 'CARRIER_NAME', 'SHIPMENT_STATUS', 'COST_TYPE', 'BATCH_ID']
    for _c in _gt_cols:
        if _c in df.columns:
            df = df.withColumn(_c, F.regexp_replace(F.col(_c), F.lit('–'), F.lit('−')))
    logger.debug('  [global #3] REPLACE applied to %d cols', len(_gt_cols))

    # Global #4: STRIP_WHITESPACE | condition=dtype:VARCHAR  # Collapse multiple spaces
    _gt_cols = ['PAYMENT_MODE', 'SALE_STATUS', 'REGION', 'CLAIM_DEFECT_TYPE', 'CLAIM_STATUS', 'CARRIER_NAME', 'SHIPMENT_STATUS', 'COST_TYPE', 'BATCH_ID']
    for _c in _gt_cols:
        if _c in df.columns:
            df = df.withColumn(_c, F.regexp_replace(F.col(_c), r'\s+', ' '))
            df = df.withColumn(_c, F.trim(F.col(_c)))
    logger.debug('  [global #4] STRIP_WHITESPACE applied to %d cols', len(_gt_cols))

    # Reorder to target schema
    _exp = ['SALES_ORDER_KEY', 'VEHICLE_KEY', 'CUSTOMER_KEY', 'SALE_DATE', 'SALE_PRICE', 'DISCOUNT_PCT', 'TOTAL_INVOICE', 'PAYMENT_MODE', 'SALE_STATUS', 'REGION', 'CLAIM_KEY', 'CLAIM_DATE', 'CLAIM_DEFECT_TYPE', 'CLAIM_REPAIR_COST', 'CLAIM_STATUS', 'SUPPLIER_LIABLE', 'SHIPMENT_KEY', 'CARRIER_NAME', 'SHIPMENT_DATE', 'FREIGHT_COST', 'SHIPMENT_STATUS', 'IS_DELAYED', 'LEDGER_KEY', 'COST_TYPE', 'COST_POSTING_DATE', 'COST_AMOUNT_LOCAL', 'COST_AMOUNT_USD', 'COST_APPROVED', 'LOAD_TS', 'BATCH_ID']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # Batch null validation
    _nn_cols = ['SALES_ORDER_KEY', 'VEHICLE_KEY', 'CUSTOMER_KEY', 'SALE_DATE', 'SALE_PRICE', 'TOTAL_INVOICE', 'PAYMENT_MODE', 'SALE_STATUS', 'REGION']
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
