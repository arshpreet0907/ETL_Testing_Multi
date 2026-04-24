-- ============================================================
-- EXTRACT SOURCE | server: server1
-- Target: analytics_dw.public.dim_vehicle_master
-- Generated: 2026-04-24 11:31
-- ============================================================
-- Main table: etl_output_mysql_xl.vehicle_master AS cte_main
-- ROW_NUMBER JOIN etl_output_mysql_xl.parts_inventory AS cte_parts_inventory
-- ROW_NUMBER JOIN etl_output_mysql_xl.supplier_master AS cte_supplier_master
-- ROW_NUMBER JOIN etl_output_mysql_xl.employee_master AS cte_employee_master
-- Extra tables (ROW_NUMBER aligned): parts_inventory, supplier_master, employee_master
--
-- All source tables aliased upfront.
-- Columns aliased to target names where possible.
-- Non-trivial transforms applied in 04_transform.py.
use etl_output_mysql_xl;
WITH
cte_main AS (
    SELECT
        vehicle_id, vin_number, model_nm, variant_cd, model_yr, engine_type_cd, plant_cd, base_price_amt, status_cd, launch_dt, is_electric_flag,
        ROW_NUMBER() OVER (ORDER BY vehicle_id) AS rn
    FROM etl_output_mysql_xl.vehicle_master
),
cte_parts_inventory AS (
    SELECT
        part_id, part_no, part_category, unit_cost_amt, qty_on_hand, is_critical_flag,
        ROW_NUMBER() OVER (ORDER BY part_id) AS rn
    FROM etl_output_mysql_xl.parts_inventory
),
cte_supplier_master AS (
    SELECT
        supplier_id, supplier_nm, country_cd, rating_score, is_approved_flag,
        ROW_NUMBER() OVER (ORDER BY supplier_id) AS rn
    FROM etl_output_mysql_xl.supplier_master
),
cte_employee_master AS (
    SELECT
        emp_id, first_nm, dept_nm, role_nm, last_nm,
        ROW_NUMBER() OVER (ORDER BY emp_id) AS rn
    FROM etl_output_mysql_xl.employee_master
)
SELECT
    cte_main.vehicle_id AS VEHICLE_KEY,
    cte_main.vin_number AS VIN,
    cte_main.model_nm AS MODEL_NAME,
    cte_main.variant_cd AS VARIANT_CODE,
    cte_main.model_yr AS MODEL_YEAR,
    cte_main.engine_type_cd AS ENGINE_TYPE,
    cte_main.plant_cd AS PLANT_CODE,
    cte_main.base_price_amt AS BASE_PRICE,
    cte_main.status_cd AS VEHICLE_STATUS,
    cte_main.launch_dt AS LAUNCH_DATE,
    cte_main.is_electric_flag AS IS_ELECTRIC,
    cte_parts_inventory.part_id AS PRIMARY_PART_KEY,
    cte_parts_inventory.part_no AS PRIMARY_PART_NO,
    cte_parts_inventory.part_category AS PART_CATEGORY,
    cte_parts_inventory.unit_cost_amt AS PART_UNIT_COST,
    cte_parts_inventory.qty_on_hand AS PART_STOCK_QTY,
    cte_parts_inventory.is_critical_flag AS IS_CRITICAL_PART,
    cte_supplier_master.supplier_id AS SUPPLIER_KEY,
    cte_supplier_master.supplier_nm AS SUPPLIER_NAME,
    cte_supplier_master.country_cd AS SUPPLIER_COUNTRY,
    cte_supplier_master.rating_score AS SUPPLIER_RATING,
    cte_supplier_master.is_approved_flag AS IS_APPROVED_SUPPLIER,
    cte_employee_master.emp_id AS PLANT_CONTACT_KEY,
    cte_employee_master.first_nm AS PLANT_CONTACT_NAME,
    cte_employee_master.dept_nm AS CONTACT_DEPARTMENT,
    cte_employee_master.role_nm AS CONTACT_ROLE,
    cte_employee_master.last_nm
FROM cte_main
JOIN cte_parts_inventory ON cte_main.rn = cte_parts_inventory.rn
JOIN cte_supplier_master ON cte_main.rn = cte_supplier_master.rn
JOIN cte_employee_master ON cte_main.rn = cte_employee_master.rn
;