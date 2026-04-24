-- ============================================================
-- EXTRACT SOURCE | server: server1
-- Target: analytics_dw.public.fact_commercial
-- Generated: 2026-04-24 11:31
-- ============================================================
-- Main table: etl_output_mysql_xl.sales_orders AS cte_main
-- ROW_NUMBER JOIN etl_output_mysql_xl.warranty_claims AS cte_warranty_claims
-- ROW_NUMBER JOIN etl_output_mysql_xl.logistics_shipments AS cte_logistics_shipments
-- ROW_NUMBER JOIN etl_output_mysql_xl.cost_ledger AS cte_cost_ledger
-- Extra tables (ROW_NUMBER aligned): warranty_claims, logistics_shipments, cost_ledger
--
-- All source tables aliased upfront.
-- Columns aliased to target names where possible.
-- Non-trivial transforms applied in 04_transform.py.
use etl_output_mysql_xl;
WITH
cte_main AS (
    SELECT
        sales_order_id, vehicle_id, customer_id, order_dt, sale_price_amt, discount_pct, total_invoice_amt, payment_mode_cd, order_status_cd, region_cd,
        ROW_NUMBER() OVER (ORDER BY sales_order_id) AS rn
    FROM etl_output_mysql_xl.sales_orders
),
cte_warranty_claims AS (
    SELECT
        claim_id, claim_dt, defect_type_cd, repair_cost_amt, claim_status_cd, supplier_liability_flag,
        ROW_NUMBER() OVER (ORDER BY claim_id) AS rn
    FROM etl_output_mysql_xl.warranty_claims
),
cte_logistics_shipments AS (
    SELECT
        shipment_id, carrier_nm, shipment_dt, freight_cost_amt, status_cd, estimated_arrival_dt, actual_arrival_dt,
        ROW_NUMBER() OVER (ORDER BY shipment_id) AS rn
    FROM etl_output_mysql_xl.logistics_shipments
),
cte_cost_ledger AS (
    SELECT
        ledger_id, cost_type_cd, posting_dt, amount_lc, amount_usd, approved_flag,
        ROW_NUMBER() OVER (ORDER BY ledger_id) AS rn
    FROM etl_output_mysql_xl.cost_ledger
)
SELECT
    cte_main.sales_order_id AS SALES_ORDER_KEY,
    cte_main.vehicle_id AS VEHICLE_KEY,
    cte_main.customer_id AS CUSTOMER_KEY,
    cte_main.order_dt AS SALE_DATE,
    cte_main.sale_price_amt AS SALE_PRICE,
    cte_main.discount_pct AS DISCOUNT_PCT,
    cte_main.total_invoice_amt AS TOTAL_INVOICE,
    cte_main.payment_mode_cd AS PAYMENT_MODE,
    cte_main.order_status_cd AS SALE_STATUS,
    cte_main.region_cd AS REGION,
    cte_warranty_claims.claim_id AS CLAIM_KEY,
    cte_warranty_claims.claim_dt AS CLAIM_DATE,
    cte_warranty_claims.defect_type_cd AS CLAIM_DEFECT_TYPE,
    cte_warranty_claims.repair_cost_amt AS CLAIM_REPAIR_COST,
    cte_warranty_claims.claim_status_cd AS CLAIM_STATUS,
    cte_warranty_claims.supplier_liability_flag AS SUPPLIER_LIABLE,
    cte_logistics_shipments.shipment_id AS SHIPMENT_KEY,
    cte_logistics_shipments.carrier_nm AS CARRIER_NAME,
    cte_logistics_shipments.shipment_dt AS SHIPMENT_DATE,
    cte_logistics_shipments.freight_cost_amt AS FREIGHT_COST,
    cte_logistics_shipments.status_cd AS SHIPMENT_STATUS,
    cte_logistics_shipments.estimated_arrival_dt AS IS_DELAYED,
    cte_cost_ledger.ledger_id AS LEDGER_KEY,
    cte_cost_ledger.cost_type_cd AS COST_TYPE,
    cte_cost_ledger.posting_dt AS COST_POSTING_DATE,
    cte_cost_ledger.amount_lc AS COST_AMOUNT_LOCAL,
    cte_cost_ledger.amount_usd AS COST_AMOUNT_USD,
    cte_cost_ledger.approved_flag AS COST_APPROVED,
    cte_logistics_shipments.actual_arrival_dt
FROM cte_main
JOIN cte_warranty_claims ON cte_main.rn = cte_warranty_claims.rn
JOIN cte_logistics_shipments ON cte_main.rn = cte_logistics_shipments.rn
JOIN cte_cost_ledger ON cte_main.rn = cte_cost_ledger.rn
;