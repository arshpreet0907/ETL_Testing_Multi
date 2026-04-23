-- ============================================================
-- Partial source DDL: etl_output_mysql_xl.employee_master
-- Only columns referenced in mapping spec
-- Generated: 2026-04-24 01:13
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_output_mysql_xl.employee_master (
    emp_id                             INT                        NOT NULL,  ,-- PK
    first_nm                           VARCHAR(50)                NOT NULL,
    dept_nm                            VARCHAR(50)                NOT NULL,
    role_nm                            VARCHAR(50)                NOT NULL
);