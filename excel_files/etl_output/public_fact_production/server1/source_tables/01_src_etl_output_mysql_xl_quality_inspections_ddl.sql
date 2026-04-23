-- ============================================================
-- Partial source DDL: etl_output_mysql_xl.quality_inspections
-- Only columns referenced in mapping spec
-- Generated: 2026-04-24 01:13
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_output_mysql_xl.quality_inspections (
    inspection_id                      INT                        NOT NULL,  ,-- PK
    inspection_dt                      DATE                       NOT NULL,
    result_cd                          VARCHAR(10)                NOT NULL,
    inspection_score                   DECIMAL(5,2)              ,
    rework_required_flag               CHAR(1)                    NOT NULL,
    rework_cost_amt                    DECIMAL(12,2)              NOT NULL
);