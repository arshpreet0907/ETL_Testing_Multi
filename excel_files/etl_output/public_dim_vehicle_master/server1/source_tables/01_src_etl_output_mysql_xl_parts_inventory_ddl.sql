-- ============================================================
-- Partial source DDL: etl_output_mysql_xl.parts_inventory
-- Only columns referenced in mapping spec
-- Generated: 2026-04-24 11:31
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_output_mysql_xl.parts_inventory (
    part_id                            INT                        NOT NULL,  ,-- PK
    part_no                            VARCHAR(20)                NOT NULL,
    part_category                      VARCHAR(30)                NOT NULL,
    unit_cost_amt                      DECIMAL(12,2)              NOT NULL,
    qty_on_hand                        INT                        NOT NULL,
    is_critical_flag                   CHAR(1)                    NOT NULL
);