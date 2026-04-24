-- ============================================================
-- Partial source DDL: etl_output_mysql_xl.engine_assembly_log
-- Only columns referenced in mapping spec
-- Generated: 2026-04-24 11:31
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_output_mysql_xl.engine_assembly_log (
    assembly_log_id                    INT                        NOT NULL,  ,-- PK
    engine_type_cd                     VARCHAR(20)                NOT NULL,
    torque_nm                          DECIMAL(8,2)              ,
    test_result_cd                     VARCHAR(10)                NOT NULL,
    assembly_cost_amt                  DECIMAL(12,2)              NOT NULL,
    defect_flag                        CHAR(1)                    NOT NULL
);