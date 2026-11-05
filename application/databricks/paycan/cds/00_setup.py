# Databricks notebook source
# DBTITLE 1,Add Widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

# COMMAND ----------

# DBTITLE 1,Import Utils
# MAGIC %run /paycan/common/utils

# COMMAND ----------

# DBTITLE 1,Import Config
unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))
print(unity_catalog_name)

# COMMAND ----------

# DBTITLE 1,Set User Metadata for table logging
# widget_config = {key: dbutils.notebook.entry_point.getCurrentBindings()[key] for key in dbutils.notebook.entry_point.getCurrentBindings()}

# spark.sql(f"SET spark.databricks.delta.commitInfo.userMetadata={widget_config}").show(1, False)

# COMMAND ----------

# DBTITLE 1,Create Standardized table - CNS Pos Val
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_cns_position_values
(
-- Core file fields
date DATE,
extract_period STRING,                -- ABNS / EODS
ledger_bpid STRING,
ledger_bpid_name STRING,
security_id STRING,                  -- ISIN
settlement_currency STRING,          -- CAD / USD
settlement_date DATE,
net_position DECIMAL(20,2),
price DECIMAL(20,2),
position_value DECIMAL(20,2),

-- Audit & control fields
audit_file_name STRING,
audit_file_date DATE,
audit_load_ts TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains CNS (Continuous Net Settlement) position values data. Each record represents a netted CNS position by ledger BPID and security, including settlement details, pricing, and calculated position value. Data includes both after-BNS (ABNS) and end-of-day (EODS) extracts. Position values are derived as the negative product of net position and price, reflecting buy/sell exposure. The dataset supports settlement exposure analysis, liquidity monitoring, and reconciliation of CNS activity across participants.';
""")

# COMMAND ----------

# DBTITLE 1,Adding table column comments - CNS Pos Val
comments = {
  "date": "Date of the CNS position (YYYYMMDD from source file)",
  "extract_period": "Data extract period of the file, either ABNS (after BNS) or EODS (end-of-day)",
  "ledger_bpid": "Company code and ledger code identifying the CNS position",
  "ledger_bpid_name": "Name of the ledger BPID (business participant identifier)",
  "security_id": "Security identifier (ISIN)",
  "settlement_currency": "Currency of the position settlement (CAD or USD)",
  "settlement_date": "Settlement date of the CNS position, may be current or future dated (YYYYMMDD)",
  "net_position": "Novated and netted CNS position quantity; negative indicates sell position, positive indicates buy position",
  "price": "Security price in the settlement currency used to value the net position",
  "position_value": "Calculated as -(net_position * price), representing the value of the CNS position",
    
  "audit_file_name": "Source file name used for ingestion",
  "audit_file_date": "Source file date (YYYY-MM-DD)",
  "audit_load_ts": "Timestamp when the record was loaded into the table"
}

for col, comment in comments.items():
    spark.sql(f"""
    ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_cns_position_values
    ALTER COLUMN {col}
    COMMENT '{comment.replace("'", "''")}';
    """)

# COMMAND ----------

# DBTITLE 1,Create Standardized table - Payment Obligations
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_payment_obligations
(
-- Core file fields
business_date DATE,                  -- “DATE” (yyyymmdd)
ledger_bpid STRING,                 -- “LEDGER BPID”
ledger_bpid_name STRING,            -- “LEDGER BPID NAME”
currency STRING,                    -- CAD / USD
payment_obligation DECIMAL(20,2),   -- +/- obligation amount

-- Derived / validation fields (recommended for ingestion layer)
-- file_date DATE,                     -- parsed from filename (yyyymmdd)

-- Audit & control fields
audit_file_name STRING,
audit_file_date DATE,
audit_load_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (business_date)
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains Bank of Canada payment obligation data. Each record represents a net payment obligation by ledger BPID and currency for a given business date. Negative values indicate the participant owes payment to CDS, while positive values indicate the participant will receive payment. The dataset supports payment exposure monitoring, liquidity analysis, and settlement reconciliation. Data is sorted in the source file by business_date, ledger_bpid, and currency.';
""")

# COMMAND ----------

# DBTITLE 1,Adding table column comments - Payment Obligations
comments = {
  "business_date": "Date of the payment obligation (YYYYMMDD from source file)",
  "ledger_bpid": "Company code and ledger code identifying the payment obligation",
  "ledger_bpid_name": "Name of the ledger BPID (business participant identifier)",
  "currency": "Currency of the payment obligation (CAD or USD)",
  "payment_obligation": "Net payment obligation amount; negative indicates the participant owes payment to CDS, positive indicates the participant will receive payment",
  "audit_file_name": "Source file name used for ingestion",
  "audit_file_date": "Source file date (YYYY-MM-DD)",
  "audit_load_ts": "Timestamp when the record was loaded into the table"
}

for col, comment in comments.items():
    spark.sql(f"""
    ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_payment_obligations
    ALTER COLUMN {col}
    COMMENT '{comment.replace("'", "''")}';
    """)

# COMMAND ----------

# DBTITLE 1,Create Standardized table - Line of Credit
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_line_of_credit
(
-- Core file fields
business_date DATE,                        -- “DATE” (yyyymmdd)

extender_ledger_bpid STRING,              -- “EXTENDER LEDGER BPID”
extender_ledger_bpid_name STRING,         -- “EXTENDER LEDGER BPID NAME”

receiver_ledger_bpid STRING,              -- “RECEIVER LEDGER BPID”
receiver_ledger_bpid_name STRING,         -- “RECEIVER LEDGER BPID NAME”

receiver_family_id STRING,                -- Optional
receiver_family_id_name STRING,           -- Conditional

line_of_credit_id STRING,                 -- Unique LOC ID

extended_credit_line DECIMAL(20,2),       -- Total extended credit
used_credit_line DECIMAL(20,2),           -- Utilized amount
available_credit_line DECIMAL(20,2),      -- Extended - Used (can be negative)
maximum_used_line DECIMAL(20,2),          -- Peak usage for the day

status STRING,                            -- AUTH / CFMD / WTDN

new_credit_line DECIMAL(20,2),            -- Pending increase amount
increase_status STRING,                   -- UNCF / AUTH / blank

settlement_facility STRING,               -- CDS / CDCC

-- Derived / validation fields
-- file_date DATE,                           -- parsed from filename (yyyymmdd)

-- Audit & control fields
audit_file_name STRING,
audit_file_date DATE,
audit_load_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (business_date)
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains Bank of Canada line of credit data between participant ledger BPIDs. Each record represents a credit facility extended from an extender to a receiver, including total, used, and available credit, along with status and settlement facility allocation. Negative available credit indicates overutilization. The dataset supports credit exposure monitoring, liquidity risk analysis, and participant-level credit tracking. Source data is sorted by business_date, extender_ledger_bpid, receiver_ledger_bpid, status, and extended_credit_line.';
""")

# COMMAND ----------

# DBTITLE 1,Adding table column comments - Line of Credit
comments = {
  "business_date": "Date of the line of credit data (YYYYMMDD from source file)",

  "extender_ledger_bpid": "Company code and ledger code of the credit extender",
  "extender_ledger_bpid_name": "Name of the credit extender (business participant identifier)",

  "receiver_ledger_bpid": "Company code and ledger code of the credit receiver",
  "receiver_ledger_bpid_name": "Name of the credit receiver (business participant identifier)",

  "receiver_family_id": "Company code representing the receiver’s family group; if it matches the first three characters of the extender ledger BPID, the receiver is an affiliate of the extender",
  "receiver_family_id_name": "Name of the receiver’s family group",

  "line_of_credit_id": "Unique identifier assigned to the line of credit",

  "extended_credit_line": "Total credit line amount extended by the extender to the receiver",
  "used_credit_line": "Amount of the credit line utilized by the receiver at the time of extraction (Payment Exchange)",
  "available_credit_line": "Available credit calculated as extended_credit_line minus used_credit_line; may be negative if overutilized",
  "maximum_used_line": "Maximum utilized amount of the credit line for the given business date",

  "status": "Status of the extended credit line: AUTH (authorized), CFMD (confirmed and usable), or WTDN (withdrawn)",

  "new_credit_line": "Pending increased credit line amount requiring authorization and confirmation; replaces extended_credit_line upon confirmation, otherwise zero or unchanged for decreases",
  "increase_status": "Status of the new credit line: UNCF (unconfirmed), AUTH (authorized), or blank if not applicable",

  "settlement_facility": "Settlement facility to which the confirmed credit line is allocated (e.g., CDS or CDCC)",

  "audit_file_name": "Source file name used for ingestion",
  "audit_file_date": "Source file date (YYYY-MM-DD)",
  "audit_load_ts": "Timestamp when the record was loaded into the table"
}

for col, comment in comments.items():
    spark.sql(f"""
    ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_line_of_credit
    ALTER COLUMN {col}
    COMMENT '{comment.replace("'", "''")}';
    """)

# COMMAND ----------

# DBTITLE 1,Create Standardized table - NSCC Pos Val
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_nscc_position_values
(
-- Core file fields
settlement_date DATE,                  -- “SETTLEMENT DATE” (yyyymmdd)
nscc_member_id STRING,                 -- NSCC MEMBER ID
cdsx_ledger_bpid STRING,               -- CDSX LEDGER BPID
cdsx_ledger_bpid_name STRING,          -- CDSX LEDGER BPID NAME
cusip STRING,                          -- Security identifier (CUSIP)

net_position BIGINT,                   -- Opening net position (can be negative)
market_value DECIMAL(20,2),            -- Market value of net position
currency_code STRING,                  -- USD

-- Derived / validation fields
-- file_date DATE,                        -- parsed from filename (yyyymmdd)

-- Audit & control fields
audit_file_name STRING,
audit_file_date DATE,
audit_load_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (settlement_date)
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains NSCC net position data mapped to CDSX ledger BPIDs. Each record represents an opening net position and its market value for a given settlement date, participant, and security (CUSIP). Positions may be positive or negative, indicating long or short exposure. The dataset supports cross-border settlement monitoring, reconciliation with CDSX positions, and exposure analysis. Source data is sorted by settlement_date, cdsx_ledger_bpid, and cusip.';
""")

# COMMAND ----------

# DBTITLE 1,Adding table column comments - NSCC Pos Val
comments = {
  "settlement_date": "Settlement date of the NSCC net position (YYYYMMDD from source file)",

  "nscc_member_id": "Participant identifier at NSCC",
  "cdsx_ledger_bpid": "CDSX ledger BPID corresponding to the NSCC member ID",
  "cdsx_ledger_bpid_name": "Name of the CDSX ledger BPID",

  "cusip": "Security identifier provided by NSCC; used when ISIN is not available for non-CDS-eligible securities",

  "net_position": "Opening net position for the settlement date; positive indicates long position, negative indicates short position",
  "market_value": "Market value of the opening net position in USD",

  "currency_code": "Currency of the market value (USD)",

  "audit_file_name": "Source file name used for ingestion",
  "audit_file_date": "Source file date (YYYY-MM-DD)",
  "audit_load_ts": "Timestamp when the record was loaded into the table"
}

for col, comment in comments.items():
    spark.sql(f"""
    ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_nscc_position_values
    ALTER COLUMN {col}
    COMMENT '{comment.replace("'", "''")}';
    """)

# COMMAND ----------

# DBTITLE 1,Create Standardized table - USD Deposit Bank Balances
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_usd_deposit_bank_balances
(
-- Core file fields
business_date DATE,                    -- "DATE" (yyyy-mm-dd)

wfc DECIMAL(20,2),                     -- Wells Fargo EOD USD balance
bny_hub DECIMAL(20,2),                 -- Bank of New York Hub account EOD USD balance
bny_ld DECIMAL(20,2),                  -- Bank of New York Liquidity Direct account EOD USD balance
cit DECIMAL(20,2),                     -- Citibank EOD USD balance
jpm DECIMAL(20,2),                     -- J.P. Morgan Chase EOD USD balance
stan DECIMAL(20,2),                    -- Standard Chartered EOD USD balance
stt DECIMAL(20,2),                     -- State Street EOD USD balance

-- Audit & control fields
audit_file_name STRING,
audit_file_date DATE,
audit_load_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (business_date)
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains Bank of Canada USD commercial bank balance data. Each record represents the end-of-day USD balances for a specified business date across configured commercial bank accounts, including Wells Fargo, Bank of New York, Citibank, J.P. Morgan Chase, Standard Chartered, and State Street. The dataset supports liquidity monitoring, reconciliation, and oversight reporting.';
""")

# COMMAND ----------

# DBTITLE 1,Adding table column comments - USD Deposit Bank Balances
comments = {
  "business_date": "Date of the USD bank balance data from the source DATE column",

  "wfc": "End-of-day USD balance at Wells Fargo",
  "bny_hub": "End-of-day USD balance at Bank of New York Hub account",
  "bny_ld": "End-of-day USD balance at Bank of New York Liquidity Direct account",
  "cit": "End-of-day USD balance at Citibank",
  "jpm": "End-of-day USD balance at J.P. Morgan Chase",
  "stan": "End-of-day USD balance at Standard Chartered",
  "stt": "End-of-day USD balance at State Street",

  "audit_file_name": "Source file name used for ingestion",
  "audit_file_date": "Source file date parsed from the raw folder date or ingestion pull date",
  "audit_load_ts": "Timestamp when the record was loaded into the table"
}

for col, comment in comments.items():
    spark.sql(f"""
    ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_usd_deposit_bank_balances
    ALTER COLUMN {col}
    COMMENT '{comment.replace("'", "''")}';
    """)

# COMMAND ----------

# DBTITLE 1,Create Standardized table - Settlement Fails
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_settlement_fails
(
-- Core file fields
date DATE,
settlement_type STRING,
currency STRING,
total_value DECIMAL(20,2),
failed_value DECIMAL(20,2),
failed_value_pct DECIMAL(20,2),

-- Audit & control fields
audit_file_name STRING,
audit_file_date DATE,
audit_load_ts TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains settlement fails data by business date, settlement type, and currency. Each record represents total settlement value, failed settlement value, and failed value percentage for a given settlement category. Settlement types include TFT non-exchange, TFT exchange, and CNS activity across equity and debt asset classes where applicable. The dataset supports settlement-fail monitoring, exposure analysis, and regulatory reporting.';
""")


# COMMAND ----------

# DBTITLE 1,Adding table column comments - Settlement Fails
comments = {
  "date": "Business date of the payment obligation parsed from YYYYMMDD source value",
  "settlement_type": "Grouping of settlement fails in Mode of Settlement - Transaction Type - Asset Class format; expected values include TFT NEXCH - EQUITY, TFT NEXCH - DEBT, TFT EXCH - EQUITY, TFT EXCH - DEBT, and CNS",
  "currency": "Currency of the settlement fails; expected values are CAD or USD",
  "total_value": "Value of total quantity, par value, or outstanding opening positions for the business date, settlement type, and currency combination",
  "failed_value": "Value of total failed quantity, par value, or outstanding opening positions for the business date, settlement type, and currency combination",
  "failed_value_pct": "Percentage of failed value over total value for the business date, settlement type, and currency combination",

  "audit_file_name": "Source file name used for ingestion",
  "audit_file_date": "Source file date (YYYY-MM-DD)",
  "audit_load_ts": "Timestamp when the record was loaded into the table"
}

for col, comment in comments.items():
    spark.sql(f"""
    ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_settlement_fails
    ALTER COLUMN {col}
    COMMENT '{comment.replace("'", "''")}';
    """)

# COMMAND ----------

# DBTITLE 1,Create Standardized table - NYL Daily Mark to Market by Participant
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_nyl_daily_mark_to_market_by_participant
(
-- Core file fields
date DATE,
ledger_bpid STRING,
ledger_bpid_name STRING,
nyl_mtm_usd DECIMAL(20,2),
mtm_cad_equiv DECIMAL(20,2),

-- Audit & control fields
audit_file_name STRING,
audit_file_date DATE,
audit_load_ts TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains NYL daily mark-to-market data by participant. Each record represents NYL mark-to-market USD value and its CAD equivalent for a given business date and ledger BPID. The dataset supports participant-level exposure analysis, liquidity monitoring, and reconciliation of NYL mark-to-market activity.';
""")

# COMMAND ----------

# DBTITLE 1,Adding table column comments - NYL Daily Mark to Market by Participant
comments = {
  "date": "Date of the NYL mark-to-market data parsed from YYYYMMDD source value",
  "ledger_bpid": "Company code and ledger code identifying the payment obligation",
  "ledger_bpid_name": "Name of the ledger BPID",
  "nyl_mtm_usd": "NYL mark-to-market value in USD",
  "mtm_cad_equiv": "NYL mark-to-market value converted to CAD equivalent using the applicable FX rate",

  "audit_file_name": "Source file name used for ingestion",
  "audit_file_date": "Source file date (YYYY-MM-DD)",
  "audit_load_ts": "Timestamp when the record was loaded into the table"
}

for col, comment in comments.items():
    spark.sql(f"""
    ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_nyl_daily_mark_to_market_by_participant
    ALTER COLUMN {col}
    COMMENT '{comment.replace("'", "''")}';
    """)

# COMMAND ----------

# DBTITLE 1,Create Standardized table - CNS Daily Mark to Market by Participant
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_cns_daily_mark_to_market_by_participant
(
-- Core file fields
date DATE,
ledger_bpid STRING,
ledger_bpid_name STRING,
settlement_date DATE,
cns_mtm_cad DECIMAL(20,2),
cns_mtm_usd DECIMAL(20,2),
cns_mtm_ccad DECIMAL(20,2),

-- Audit & control fields
audit_file_name STRING,
audit_file_date DATE,
audit_load_ts TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains CNS daily mark-to-market data by participant. Each record represents CNS mark-to-market CAD value, USD value, and combined CAD-equivalent value for a given business date, ledger BPID, and settlement date. The dataset supports participant-level exposure analysis, liquidity monitoring, and reconciliation of CNS mark-to-market activity.';
""")

# COMMAND ----------

# DBTITLE 1,Adding table column comments - CNS Daily Mark to Market by Participant
comments = {
  "date": "Date of the CNS mark-to-market data parsed from YYYYMMDD source value",
  "ledger_bpid": "Company code and ledger code identifying the payment obligation",
  "ledger_bpid_name": "Name of the ledger BPID",
  "settlement_date": "Settlement date parsed from YYYYMMDD source value",
  "cns_mtm_cad": "CNS mark-to-market value in CAD",
  "cns_mtm_usd": "CNS mark-to-market value in USD",
  "cns_mtm_ccad": "CNS mark-to-market combined value in CAD equivalent using the applicable FX rate",

  "audit_file_name": "Source file name used for ingestion",
  "audit_file_date": "Source file date (YYYY-MM-DD)",
  "audit_load_ts": "Timestamp when the record was loaded into the table"
}

for col, comment in comments.items():
    spark.sql(f"""
    ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_cns_daily_mark_to_market_by_participant
    ALTER COLUMN {col}
    COMMENT '{comment.replace("'", "''")}';
    """)

# COMMAND ----------

# DBTITLE 1,Create Standardized Table - Cleared and Novated in CNS
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_cleared_and_novated_in_cns
(
-- Core file fields
date DATE,
ledger_bpid STRING,
ledger_bpid_name STRING,
transaction_type STRING,
security_id STRING,
role STRING,
mode_of_settlement STRING,
currency STRING,
trade_date DATE,
intended_settlement_date DATE,
quantity DECIMAL(20,2),
settlement_amount DECIMAL(20,2),
ccad DECIMAL(20,2),

-- Audit & control fields
audit_file_name STRING,
audit_file_date DATE,
audit_load_ts TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains trades cleared and novated in CNS. Each record represents a CNS-cleared and novated transaction by business date, ledger BPID, security, role, trade date, intended settlement date, and currency. The dataset includes transaction type, settlement mode, quantity, settlement amount, and CAD-equivalent settlement amount, supporting CNS transaction monitoring, exposure analysis, and reconciliation.';
""")

# COMMAND ----------

# DBTITLE 1,Adding table column comments - Cleared and Novated in CNS
comments = {
  "date": "Date the transaction is cleared and novated, parsed from YYYYMMDD source value",
  "ledger_bpid": "Company code and ledger code identifying the CNS position",
  "ledger_bpid_name": "Name of the ledger BPID",
  "transaction_type": "Non-Exchange or Exchange indicator; expected values are Non-Exchange or Exchange",
  "security_id": "Security identifier, such as ISIN",
  "role": "Buyer or Seller indicator; expected values are B or S",
  "mode_of_settlement": "Transaction mode of settlement; expected value is CNS for all cleared and novated trades",
  "currency": "Transaction currency; expected values are CAD or USD",
  "trade_date": "Trade date of the transaction parsed from YYYYMMDD source value",
  "intended_settlement_date": "Intended settlement date or value date of the transaction parsed from YYYYMMDD source value",
  "quantity": "Par value or quantity of the transaction",
  "settlement_amount": "Settlement amount of the transaction",
  "ccad": "CAD-equivalent settlement amount; equals settlement amount when currency is CAD, or USD settlement amount converted into CAD when currency is USD",

  "audit_file_name": "Source file name used for ingestion",
  "audit_file_date": "Source file date (YYYY-MM-DD)",
  "audit_load_ts": "Timestamp when the record was loaded into the table"
}

for col, comment in comments.items():
    spark.sql(f"""
    ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_cleared_and_novated_in_cns
    ALTER COLUMN {col}
    COMMENT '{comment.replace("'", "''")}';
    """)

# COMMAND ----------

# DBTITLE 1,Create Standardized table - Settled in CDSX
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_settled_in_cdsx
(
-- Core file fields
ledger_bpid STRING,
ledger_bpid_name STRING,
transaction_type STRING,
role STRING,
status STRING,
mode_of_settlement STRING,
currency STRING,
settlement_date DATE,
settlement_quantity DECIMAL(20,2),
settlement_amount DECIMAL(20,2),
ccad DECIMAL(20,2),

-- Audit & control fields
audit_file_name STRING,
audit_file_date DATE,
audit_load_ts TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains transactions settled in CDSX. Each record represents a settled TFT transaction by ledger BPID, transaction type, role, settlement date, and currency. The dataset includes settlement quantity, settlement amount, and CAD-equivalent settlement amount, supporting settlement activity monitoring, exposure analysis, and reconciliation.';
""")

# COMMAND ----------

# DBTITLE 1,Adding table column comments - Settled in CDSX
comments = {
  "ledger_bpid": "Company code and ledger code identifying the CNS position",
  "ledger_bpid_name": "Name of the ledger BPID",
  "transaction_type": "Non-Exchange or Exchange indicator; expected values are Non-Exchange or Exchange",
  "role": "Buyer or Seller indicator; expected values are B or S",
  "status": "Settlement status; expected value is Settled",
  "mode_of_settlement": "Transaction mode of settlement; expected value is TFT for all settled trades",
  "currency": "Transaction currency; expected values are CAD or USD",
  "settlement_date": "Date the transaction settled, parsed from YYYYMMDD source value",
  "settlement_quantity": "Par value or quantity of the settled transaction",
  "settlement_amount": "Settlement amount of the transaction",
  "ccad": "CAD-equivalent settlement amount; equals settlement amount when currency is CAD, or USD settlement amount converted into CAD when currency is USD",

  "audit_file_name": "Source file name used for ingestion",
  "audit_file_date": "Source file date (YYYY-MM-DD)",
  "audit_load_ts": "Timestamp when the record was loaded into the table"
}

for col, comment in comments.items():
    spark.sql(f"""
    ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_settled_in_cdsx
    ALTER COLUMN {col}
    COMMENT '{comment.replace("'", "''")}';
    """)

# COMMAND ----------

# DBTITLE 1,Create Standardized table - New York Link Trade Stats
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_new_york_link_trade_stats
(
-- Core file fields
participant_bpid STRING,              -- CDS BPID (company code + ledger code)
nscc_account STRING,                  -- NSCC/DTC account number
market STRING,                        -- Market code
volume BIGINT,                        -- Count of trades
debits DECIMAL(20,2),                 -- Aggregate value of buy trades
credits DECIMAL(20,2),                -- Aggregate value of sell trades
sell_qty DECIMAL(20,2),               -- Sum of sell share quantity
buy_qty DECIMAL(20,2),                -- Sum of buy share quantity
trade_indicator STRING,               -- Netting indicator code

-- Audit & control fields
audit_file_name STRING,
audit_file_date DATE,
audit_load_ts TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains New York Link trade statistics by CDS participant, NSCC account, market, and trade indicator. Each record summarizes trade activity including trade volume, debit and credit values, and aggregate buy and sell quantities. Trade indicators identify the applicable netting or clearing treatment, including CNS, bilateral netting, and excluded trades. The dataset supports participant activity monitoring, settlement exposure analysis, operational reporting, and reconciliation of New York Link trading activity.';
""")

# COMMAND ----------

# DBTITLE 1,Adding table column comments - New York Link Trade Stats
comments = {
  "participant_bpid": "CDS participant BPID consisting of company code and ledger code identifying the NSCC account; example value ABC01",
  "nscc_account": "NSCC/DTC account number associated with the participant",
  "market": "Market code identifying the trading venue. Expected values include 010 (NYSE S/E), 020 (NYSE MKT/AMEX), 025 (NYSE 2), 030 (NYSE ARCA S/E), 035 (NYSE ARCA II), 040 (Chicago S/E), 050 (CBOE), 060 (Over the Counter), 070 (Options Clearing Corp.), 080 (National S/E), 090 (Prime Broker/OMGEO), 100 (Corresponding Clearing), 110 (NASDAQ), 130 (BATS), 135 (BATS BYX), 140 (NASDAQ OMX BX), 145 (NASDAQ OMX PHLX), 150 (EDGA), 160 (EDGX), and 920 (ETF Create/Redeem)",
  "volume": "Count of trades for the participant, market, and trade indicator combination",
  "debits": "Aggregate value amount of buy trades",
  "credits": "Aggregate value amount of sell trades",
  "sell_qty": "Total sell share quantity",
  "buy_qty": "Total buy share quantity",
  "trade_indicator": "Netting or clearing instruction code. Expected values are 1 (Excluded From All Netting), 2 (Bilateral Trade Date Netting Only), 3 (Excluded from Clearing), 4 (Participant Instructed Special Trade), 5 (Multilateral Netting), and 6 (Clear Against Central Counterparty/CNS)",
  "audit_file_name": "Source file name used for ingestion",
  "audit_file_date": "Source file date (YYYY-MM-DD)",
  "audit_load_ts": "Timestamp when the record was loaded into the table"
}

for col, comment in comments.items():
    spark.sql(f"""
    ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cds_new_york_link_trade_stats
    ALTER COLUMN {col}
    COMMENT '{comment.replace("'", "''")}';
    """)
