-- ============================================================
-- Target table: analytics_dw.public.fact_production
-- Dialect: Snowflake
-- Generated: 2026-04-24 11:31
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics_dw.public.fact_production (
    PROD_ORDER_KEY                       NUMBER(10,0)               NOT NULL  ,-- PK,
    VEHICLE_KEY                          NUMBER(10,0)               NOT NULL,
    PLANT_CODE                           VARCHAR(10)                NOT NULL,
    ORDER_DATE                           DATE                       NOT NULL,
    PRODUCED_QTY                         NUMBER(10,0)               NOT NULL,
    REJECTED_QTY                         NUMBER(10,0)               NOT NULL DEFAULT 0,
    ORDER_STATUS                         VARCHAR(20)                NOT NULL,
    EFFICIENCY_PCT                       NUMBER(5,2)               ,
    YIELD_RATE_PCT                       NUMBER(5,2)               ,
    INSPECTION_KEY                       NUMBER(10,0)              ,
    INSPECTION_DATE                      DATE                      ,
    QC_RESULT                            VARCHAR(10)               ,
    QC_SCORE                             NUMBER(5,2)               ,
    QC_REWORK_FLAG                       NUMBER(3,0)                DEFAULT 0,
    QC_REWORK_COST                       NUMBER(12,2)               DEFAULT 0,
    PAINT_LOG_KEY                        NUMBER(10,0)              ,
    PAINT_COLOR_CODE                     VARCHAR(20)               ,
    PAINT_OVEN_TEMP_C                    NUMBER(5,2)               ,
    PAINT_DEFECT_FLAG                    NUMBER(3,0)                DEFAULT 0,
    PAINT_COST                           NUMBER(12,2)              ,
    ENGINE_LOG_KEY                       NUMBER(10,0)              ,
    ENGINE_TYPE                          VARCHAR(20)               ,
    ENGINE_TORQUE_NM                     NUMBER(8,2)               ,
    ENGINE_TEST_RESULT                   VARCHAR(10)               ,
    ENGINE_ASSEMBLY_COST                 NUMBER(12,2)              ,
    ENGINE_DEFECT_FLAG                   NUMBER(3,0)                DEFAULT 0,
    LOAD_TS                              TIMESTAMP_NTZ              NOT NULL,
    BATCH_ID                             VARCHAR(50)                NOT NULL,
    PRIMARY KEY (PROD_ORDER_KEY)
);