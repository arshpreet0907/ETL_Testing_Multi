-- ============================================================
-- EXTRACT TARGET | analytics_dw.public.fact_production
-- Dialect: Snowflake
-- Generated: 2026-04-24 01:13
-- ============================================================

SELECT
    PROD_ORDER_KEY,
    VEHICLE_KEY,
    PLANT_CODE,
    ORDER_DATE,
    PRODUCED_QTY,
    REJECTED_QTY,
    ORDER_STATUS,
    EFFICIENCY_PCT,
    YIELD_RATE_PCT,
    INSPECTION_KEY,
    INSPECTION_DATE,
    QC_RESULT,
    QC_SCORE,
    QC_REWORK_FLAG,
    QC_REWORK_COST,
    PAINT_LOG_KEY,
    PAINT_COLOR_CODE,
    PAINT_OVEN_TEMP_C,
    PAINT_DEFECT_FLAG,
    PAINT_COST,
    ENGINE_LOG_KEY,
    ENGINE_TYPE,
    ENGINE_TORQUE_NM,
    ENGINE_TEST_RESULT,
    ENGINE_ASSEMBLY_COST,
    ENGINE_DEFECT_FLAG,
    LOAD_TS,
    BATCH_ID
FROM analytics_dw.public.fact_production;