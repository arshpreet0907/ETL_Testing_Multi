-- ============================================================
-- EXTRACT TARGET | analytics_dw.public.dim_vehicle_master
-- Dialect: Snowflake
-- Generated: 2026-04-24 01:13
-- ============================================================

SELECT
    VEHICLE_KEY,
    VIN,
    MODEL_NAME,
    VARIANT_CODE,
    MODEL_YEAR,
    ENGINE_TYPE,
    PLANT_CODE,
    BASE_PRICE,
    VEHICLE_STATUS,
    LAUNCH_DATE,
    IS_ELECTRIC,
    PRIMARY_PART_KEY,
    PRIMARY_PART_NO,
    PART_CATEGORY,
    PART_UNIT_COST,
    PART_STOCK_QTY,
    IS_CRITICAL_PART,
    SUPPLIER_KEY,
    SUPPLIER_NAME,
    SUPPLIER_COUNTRY,
    SUPPLIER_RATING,
    IS_APPROVED_SUPPLIER,
    PLANT_CONTACT_KEY,
    PLANT_CONTACT_NAME,
    CONTACT_DEPARTMENT,
    CONTACT_ROLE,
    LOAD_TS,
    BATCH_ID
FROM analytics_dw.public.dim_vehicle_master;