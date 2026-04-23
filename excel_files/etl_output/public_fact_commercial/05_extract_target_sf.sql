-- ============================================================
-- EXTRACT TARGET | analytics_dw.public.fact_commercial
-- Dialect: Snowflake
-- Generated: 2026-04-24 01:13
-- ============================================================

SELECT
    SALES_ORDER_KEY,
    VEHICLE_KEY,
    CUSTOMER_KEY,
    SALE_DATE,
    SALE_PRICE,
    DISCOUNT_PCT,
    TOTAL_INVOICE,
    PAYMENT_MODE,
    SALE_STATUS,
    REGION,
    CLAIM_KEY,
    CLAIM_DATE,
    CLAIM_DEFECT_TYPE,
    CLAIM_REPAIR_COST,
    CLAIM_STATUS,
    SUPPLIER_LIABLE,
    SHIPMENT_KEY,
    CARRIER_NAME,
    SHIPMENT_DATE,
    FREIGHT_COST,
    SHIPMENT_STATUS,
    IS_DELAYED,
    LEDGER_KEY,
    COST_TYPE,
    COST_POSTING_DATE,
    COST_AMOUNT_LOCAL,
    COST_AMOUNT_USD,
    COST_APPROVED,
    LOAD_TS,
    BATCH_ID
FROM analytics_dw.public.fact_commercial;