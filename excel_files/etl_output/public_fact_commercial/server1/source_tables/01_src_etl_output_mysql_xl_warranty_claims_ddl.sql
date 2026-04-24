-- ============================================================
-- Partial source DDL: etl_output_mysql_xl.warranty_claims
-- Only columns referenced in mapping spec
-- Generated: 2026-04-24 11:31
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_output_mysql_xl.warranty_claims (
    claim_id                           INT                        NOT NULL,  ,-- PK
    claim_dt                           DATE                       NOT NULL,
    defect_type_cd                     VARCHAR(20)                NOT NULL,
    repair_cost_amt                    DECIMAL(12,2)              NOT NULL,
    claim_status_cd                    VARCHAR(20)                NOT NULL,
    supplier_liability_flag            CHAR(1)                    NOT NULL
);