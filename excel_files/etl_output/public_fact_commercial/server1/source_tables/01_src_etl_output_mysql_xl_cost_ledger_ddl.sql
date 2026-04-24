-- ============================================================
-- Partial source DDL: etl_output_mysql_xl.cost_ledger
-- Only columns referenced in mapping spec
-- Generated: 2026-04-24 11:31
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_output_mysql_xl.cost_ledger (
    ledger_id                          INT                        NOT NULL,  ,-- PK
    cost_type_cd                       VARCHAR(30)                NOT NULL,
    posting_dt                         DATE                       NOT NULL,
    amount_lc                          DECIMAL(16,2)              NOT NULL,
    amount_usd                         DECIMAL(14,2)             ,
    approved_flag                      CHAR(1)                    NOT NULL
);