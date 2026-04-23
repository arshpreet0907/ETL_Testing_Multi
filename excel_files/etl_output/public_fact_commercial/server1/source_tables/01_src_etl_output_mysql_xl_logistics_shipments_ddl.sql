-- ============================================================
-- Partial source DDL: etl_output_mysql_xl.logistics_shipments
-- Only columns referenced in mapping spec
-- Generated: 2026-04-24 01:13
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_output_mysql_xl.logistics_shipments (
    shipment_id                        INT                        NOT NULL,  ,-- PK
    carrier_nm                         VARCHAR(50)                NOT NULL,
    shipment_dt                        DATE                       NOT NULL,
    freight_cost_amt                   DECIMAL(12,2)              NOT NULL,
    status_cd                          VARCHAR(20)                NOT NULL,
    estimated_arrival_dt               DATE                      
);