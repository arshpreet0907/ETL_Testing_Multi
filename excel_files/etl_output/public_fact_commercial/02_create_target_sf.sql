-- ============================================================
-- Target table: analytics_dw.public.fact_commercial
-- Dialect: Snowflake
-- Generated: 2026-04-24 11:31
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics_dw.public.fact_commercial (
    SALES_ORDER_KEY                      NUMBER(10,0)               NOT NULL  ,-- PK,
    VEHICLE_KEY                          NUMBER(10,0)               NOT NULL,
    CUSTOMER_KEY                         NUMBER(10,0)               NOT NULL,
    SALE_DATE                            DATE                       NOT NULL,
    SALE_PRICE                           NUMBER(14,2)               NOT NULL,
    DISCOUNT_PCT                         NUMBER(5,2)                NOT NULL DEFAULT 0,
    TOTAL_INVOICE                        NUMBER(14,2)               NOT NULL,
    PAYMENT_MODE                         VARCHAR(20)                NOT NULL,
    SALE_STATUS                          VARCHAR(20)                NOT NULL,
    REGION                               VARCHAR(10)                NOT NULL,
    CLAIM_KEY                            NUMBER(10,0)              ,
    CLAIM_DATE                           DATE                      ,
    CLAIM_DEFECT_TYPE                    VARCHAR(20)               ,
    CLAIM_REPAIR_COST                    NUMBER(12,2)              ,
    CLAIM_STATUS                         VARCHAR(20)               ,
    SUPPLIER_LIABLE                      NUMBER(3,0)                DEFAULT 0,
    SHIPMENT_KEY                         NUMBER(10,0)              ,
    CARRIER_NAME                         VARCHAR(50)               ,
    SHIPMENT_DATE                        DATE                      ,
    FREIGHT_COST                         NUMBER(12,2)              ,
    SHIPMENT_STATUS                      VARCHAR(20)               ,
    IS_DELAYED                           NUMBER(3,0)                DEFAULT 0,
    LEDGER_KEY                           NUMBER(10,0)              ,
    COST_TYPE                            VARCHAR(30)               ,
    COST_POSTING_DATE                    DATE                      ,
    COST_AMOUNT_LOCAL                    NUMBER(16,2)              ,
    COST_AMOUNT_USD                      NUMBER(14,2)              ,
    COST_APPROVED                        NUMBER(3,0)                DEFAULT 0,
    LOAD_TS                              TIMESTAMP_NTZ              NOT NULL,
    BATCH_ID                             VARCHAR(50)                NOT NULL,
    PRIMARY KEY (SALES_ORDER_KEY)
);