-- ============================================================
-- Partial source DDL: etl_output_mysql_xl.sales_orders
-- Only columns referenced in mapping spec
-- Generated: 2026-04-24 01:13
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_output_mysql_xl.sales_orders (
    sales_order_id                     INT                        NOT NULL,  ,-- PK
    vehicle_id                         INT                        NOT NULL,
    customer_id                        INT                        NOT NULL,
    order_dt                           DATE                       NOT NULL,
    sale_price_amt                     DECIMAL(14,2)              NOT NULL,
    discount_pct                       DECIMAL(5,2)               NOT NULL,
    total_invoice_amt                  DECIMAL(14,2)              NOT NULL,
    payment_mode_cd                    VARCHAR(20)                NOT NULL,
    order_status_cd                    VARCHAR(20)                NOT NULL,
    region_cd                          VARCHAR(10)                NOT NULL
);