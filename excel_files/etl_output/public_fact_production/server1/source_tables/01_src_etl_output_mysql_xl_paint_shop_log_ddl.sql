-- ============================================================
-- Partial source DDL: etl_output_mysql_xl.paint_shop_log
-- Only columns referenced in mapping spec
-- Generated: 2026-04-24 01:13
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_output_mysql_xl.paint_shop_log (
    paint_log_id                       INT                        NOT NULL,  ,-- PK
    color_cd                           VARCHAR(20)                NOT NULL,
    oven_temp_celsius                  DECIMAL(5,2)               NOT NULL,
    defect_flag                        CHAR(1)                    NOT NULL,
    paint_cost_amt                     DECIMAL(12,2)              NOT NULL
);