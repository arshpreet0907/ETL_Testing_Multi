-- ============================================================
-- Partial source DDL: etl_output_mysql_xl.vehicle_master
-- Only columns referenced in mapping spec
-- Generated: 2026-04-24 11:31
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_output_mysql_xl.vehicle_master (
    vehicle_id                         INT                        NOT NULL,  ,-- PK
    vin_number                         VARCHAR(17)                NOT NULL,
    model_nm                           VARCHAR(50)                NOT NULL,
    variant_cd                         VARCHAR(20)                NOT NULL,
    model_yr                           INT                        NOT NULL,
    engine_type_cd                     VARCHAR(20)                NOT NULL,
    plant_cd                           VARCHAR(10)                NOT NULL,
    base_price_amt                     DECIMAL(14,2)              NOT NULL,
    status_cd                          VARCHAR(20)                NOT NULL,
    launch_dt                          DATE                       NOT NULL,
    is_electric_flag                   CHAR(1)                    NOT NULL
);