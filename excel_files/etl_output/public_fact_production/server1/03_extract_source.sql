-- ============================================================
-- EXTRACT SOURCE | server: server1
-- Target: analytics_dw.public.fact_production
-- Generated: 2026-04-24 01:13
-- ============================================================
-- Main table: etl_output_mysql_xl.production_orders AS cte_main
-- ROW_NUMBER JOIN etl_output_mysql_xl.quality_inspections AS cte_quality_inspections
-- ROW_NUMBER JOIN etl_output_mysql_xl.paint_shop_log AS cte_paint_shop_log
-- ROW_NUMBER JOIN etl_output_mysql_xl.engine_assembly_log AS cte_engine_assembly_log
-- Extra tables (ROW_NUMBER aligned): quality_inspections, paint_shop_log, engine_assembly_log
--
-- All source tables aliased upfront.
-- Columns aliased to target names where possible.
-- Non-trivial transforms applied in 04_transform.py.
use etl_output_mysql_xl;
WITH
cte_main AS (
    SELECT
        prod_order_id, vehicle_id, plant_cd, order_dt, qty_produced, qty_rejected, order_status_cd, efficiency_pct, qty_planned,
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM etl_output_mysql_xl.production_orders
),
cte_quality_inspections AS (
    SELECT
        inspection_id, inspection_dt, result_cd, inspection_score, rework_required_flag, rework_cost_amt,
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM etl_output_mysql_xl.quality_inspections
),
cte_paint_shop_log AS (
    SELECT
        paint_log_id, color_cd, oven_temp_celsius, defect_flag, paint_cost_amt,
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM etl_output_mysql_xl.paint_shop_log
),
cte_engine_assembly_log AS (
    SELECT
        assembly_log_id, engine_type_cd, torque_nm, test_result_cd, assembly_cost_amt, defect_flag,
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
    FROM etl_output_mysql_xl.engine_assembly_log
)
SELECT
    cte_main.prod_order_id AS PROD_ORDER_KEY,
    cte_main.vehicle_id AS VEHICLE_KEY,
    cte_main.plant_cd AS PLANT_CODE,
    cte_main.order_dt AS ORDER_DATE,
    cte_main.qty_produced AS PRODUCED_QTY,
    cte_main.qty_rejected AS REJECTED_QTY,
    cte_main.order_status_cd AS ORDER_STATUS,
    cte_main.efficiency_pct AS EFFICIENCY_PCT,
    cte_main.qty_planned AS YIELD_RATE_PCT,
    cte_quality_inspections.inspection_id AS INSPECTION_KEY,
    cte_quality_inspections.inspection_dt AS INSPECTION_DATE,
    cte_quality_inspections.result_cd AS QC_RESULT,
    cte_quality_inspections.inspection_score AS QC_SCORE,
    cte_quality_inspections.rework_required_flag AS QC_REWORK_FLAG,
    cte_quality_inspections.rework_cost_amt AS QC_REWORK_COST,
    cte_paint_shop_log.paint_log_id AS PAINT_LOG_KEY,
    cte_paint_shop_log.color_cd AS PAINT_COLOR_CODE,
    cte_paint_shop_log.oven_temp_celsius AS PAINT_OVEN_TEMP_C,
    cte_paint_shop_log.defect_flag AS PAINT_DEFECT_FLAG,
    cte_paint_shop_log.paint_cost_amt AS PAINT_COST,
    cte_engine_assembly_log.assembly_log_id AS ENGINE_LOG_KEY,
    cte_engine_assembly_log.engine_type_cd AS ENGINE_TYPE,
    cte_engine_assembly_log.torque_nm AS ENGINE_TORQUE_NM,
    cte_engine_assembly_log.test_result_cd AS ENGINE_TEST_RESULT,
    cte_engine_assembly_log.assembly_cost_amt AS ENGINE_ASSEMBLY_COST,
    cte_engine_assembly_log.defect_flag AS ENGINE_DEFECT_FLAG
FROM cte_main
JOIN cte_quality_inspections ON cte_main.rn = cte_quality_inspections.rn
JOIN cte_paint_shop_log ON cte_main.rn = cte_paint_shop_log.rn
JOIN cte_engine_assembly_log ON cte_main.rn = cte_engine_assembly_log.rn
;