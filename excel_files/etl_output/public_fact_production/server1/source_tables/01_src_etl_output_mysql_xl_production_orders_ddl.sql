-- ============================================================
-- Partial source DDL: etl_output_mysql_xl.production_orders
-- Only columns referenced in mapping spec
-- Generated: 2026-04-24 01:13
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_output_mysql_xl.production_orders (
    prod_order_id                      INT                        NOT NULL,  ,-- PK
    vehicle_id                         INT                        NOT NULL,
    plant_cd                           VARCHAR(10)                NOT NULL,
    order_dt                           DATE                       NOT NULL,
    qty_produced                       INT                        NOT NULL,
    qty_rejected                       INT                        NOT NULL,
    order_status_cd                    VARCHAR(20)                NOT NULL,
    efficiency_pct                     DECIMAL(5,2)              ,
    qty_planned                        INT                        NOT NULL
);