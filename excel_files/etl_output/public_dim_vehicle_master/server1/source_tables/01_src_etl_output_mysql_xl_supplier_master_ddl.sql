-- ============================================================
-- Partial source DDL: etl_output_mysql_xl.supplier_master
-- Only columns referenced in mapping spec
-- Generated: 2026-04-24 11:31
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_output_mysql_xl.supplier_master (
    supplier_id                        INT                        NOT NULL,  ,-- PK
    supplier_nm                        VARCHAR(100)               NOT NULL,
    country_cd                         VARCHAR(30)                NOT NULL,
    rating_score                       DECIMAL(3,1)              ,
    is_approved_flag                   CHAR(1)                    NOT NULL
);