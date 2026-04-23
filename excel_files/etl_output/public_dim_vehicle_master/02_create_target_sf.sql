-- ============================================================
-- Target table: analytics_dw.public.dim_vehicle_master
-- Dialect: Snowflake
-- Generated: 2026-04-24 01:13
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics_dw.public.dim_vehicle_master (
    VEHICLE_KEY                          NUMBER(10,0)               NOT NULL  ,-- PK,
    VIN                                  VARCHAR(17)                NOT NULL,
    MODEL_NAME                           VARCHAR(50)                NOT NULL,
    VARIANT_CODE                         VARCHAR(20)                NOT NULL,
    MODEL_YEAR                           NUMBER(10,0)               NOT NULL,
    ENGINE_TYPE                          VARCHAR(20)                NOT NULL,
    PLANT_CODE                           VARCHAR(10)                NOT NULL,
    BASE_PRICE                           NUMBER(14,2)               NOT NULL,
    VEHICLE_STATUS                       VARCHAR(20)                NOT NULL,
    LAUNCH_DATE                          DATE                       NOT NULL,
    IS_ELECTRIC                          NUMBER(3,0)                NOT NULL DEFAULT 0,
    PRIMARY_PART_KEY                     NUMBER(10,0)              ,
    PRIMARY_PART_NO                      VARCHAR(20)               ,
    PART_CATEGORY                        VARCHAR(30)               ,
    PART_UNIT_COST                       NUMBER(12,2)              ,
    PART_STOCK_QTY                       NUMBER(10,0)              ,
    IS_CRITICAL_PART                     NUMBER(3,0)                DEFAULT 0,
    SUPPLIER_KEY                         NUMBER(10,0)              ,
    SUPPLIER_NAME                        VARCHAR(100)              ,
    SUPPLIER_COUNTRY                     VARCHAR(30)               ,
    SUPPLIER_RATING                      NUMBER(3,1)               ,
    IS_APPROVED_SUPPLIER                 NUMBER(3,0)                DEFAULT 0,
    PLANT_CONTACT_KEY                    NUMBER(10,0)              ,
    PLANT_CONTACT_NAME                   VARCHAR(100)              ,
    CONTACT_DEPARTMENT                   VARCHAR(50)               ,
    CONTACT_ROLE                         VARCHAR(50)               ,
    LOAD_TS                              TIMESTAMP_NTZ              NOT NULL,
    BATCH_ID                             VARCHAR(50)                NOT NULL,
    PRIMARY KEY (VEHICLE_KEY)
);