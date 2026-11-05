# Databricks notebook source
# MAGIC %md
# MAGIC # RTR (Payments Canada) - Standardized table creation
# MAGIC Source of truth: PC928.3 RTR EDW BoC Extract File Layout Design v3.1.
# MAGIC Columns/types match the RTR standardizing notebook (`rtr_schema.RTR`) output, incl. `file_date`, `file_name`, `update_time`.
# MAGIC **CREATE OR REPLACE resets data - run the standardizing notebook with `load_type=full` after deploying.**

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

# COMMAND ----------

# MAGIC %run /paycan/common/utils

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))
print(unity_catalog_name)

# COMMAND ----------

def apply_column_comments(table_name, comments):
    for col, comment in comments.items():
        spark.sql(f"""
        ALTER TABLE {unity_catalog_name}.standardized_db.{table_name}
        ALTER COLUMN {col}
        COMMENT '{comment.replace("'", "''")}';
        """)

AUDIT_COMMENTS = {
  "file_date": "Source file cycle date (YYYY-MM-DD) from the raw dated folder",
  "file_name": "Source file path used for ingestion",
  "update_time": "Timestamp when the record was loaded into the table"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clearing & Settlement (cs)

# COMMAND ----------

# account_cs_<cycle_date>.json | Full | overwrite
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_paycan_rtr_account_cs
(
-- Core file fields
account_id INT,
account_tier_type STRING,              -- POSITION / SETTLEMENT
participant_number STRING,
account_number STRING,
account_type STRING,                   -- DSP_S, ECA_S, ISP_P, DSP_C, ECA_C
currency STRING,
inactive_reason STRING,
effective_time TIMESTAMP,
expire_time TIMESTAMP,

-- Audit & control fields
file_date STRING,
file_name STRING,
update_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains RTR Clearing and Settlement account data from Payments Canada. Each record represents a version of a settlement or position account for a participant, with effective and expiry timestamps capturing current and historical records. Loaded from the daily full extract account_cs. The dataset supports participant account reference, settlement monitoring, and reconciliation.';
""")

apply_column_comments("tbl_paycan_rtr_account_cs", {
  "account_id": "Identifier for the account",
  "account_tier_type": "Account intent: POSITION (Position Account) or SETTLEMENT (Settlement Account)",
  "participant_number": "Unique identifier for the entity participant",
  "account_number": "Participant account identifier as provided by the user; could be a BIC, sort code, or equivalent",
  "account_type": "Settlement: DSP_S or ECA_S. Position: ISP_P, DSP_C or ECA_C",
  "currency": "ISO three-letter currency code",
  "inactive_reason": "Justification when the account is inactive",
  "effective_time": "Timestamp from which the record is valid",
  "expire_time": "Timestamp until which the record is valid",
  **AUDIT_COMMENTS
})

# COMMAND ----------

# transaction_cs_<cycle_date>.json | Delta | replace by file_date
# Nested records flattened by dfutils.flatten_all_col (<parent>_<child>)
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_paycan_rtr_transaction_cs
(
-- Core file fields
transaction_id STRING,
transaction_type STRING,               -- TWIR / TWOR / FUND / DFND / PTPT / ADJS
cns_ref_id STRING,
end_to_end_reference STRING,
uetr STRING,
cycle_id STRING,                       -- YYYYMMDD_NNN
cycle_date DATE,
status_timestamp TIMESTAMP,
transaction_status STRING,
reject_reason_code STRING,
transaction_amount DECIMAL(15,2),
exchange_code STRING,
exchange_name STRING,

-- sender (flattened)
sender_position_account_number STRING,
sender_position_account_type STRING,
sender_account_balance_after_transaction DECIMAL(15,2),
sender_reserve_balance_after_transaction DECIMAL(15,2),
sender_participant_number STRING,
sender_participant_short_name_en STRING,
sender_participant_short_name_fr STRING,
sender_participant_name_en STRING,
sender_participant_name_fr STRING,
sender_participant_irn STRING,

-- sender_settlement_agent (flattened)
sender_settlement_agent_settlement_account_number STRING,
sender_settlement_agent_settlement_account_type STRING,
sender_settlement_agent_account_balance_after_transaction DECIMAL(15,2),
sender_settlement_agent_reserve_balance_after_transaction DECIMAL(15,2),
sender_settlement_agent_participant_number STRING,
sender_settlement_agent_participant_short_name_en STRING,
sender_settlement_agent_participant_short_name_fr STRING,
sender_settlement_agent_participant_name_en STRING,
sender_settlement_agent_participant_name_fr STRING,
sender_settlement_agent_participant_irn STRING,

-- receiver (flattened)
receiver_position_account_number STRING,
receiver_position_account_type STRING,
receiver_account_balance_after_transaction DECIMAL(15,2),
receiver_reserve_balance_after_transaction DECIMAL(15,2),
receiver_participant_number STRING,
receiver_participant_short_name_en STRING,
receiver_participant_short_name_fr STRING,
receiver_participant_name_en STRING,
receiver_participant_name_fr STRING,
receiver_participant_irn STRING,

-- receiver_settlement_agent (flattened)
receiver_settlement_agent_settlement_account_number STRING,
receiver_settlement_agent_settlement_account_type STRING,
receiver_settlement_agent_account_balance_after_transaction DECIMAL(15,2),
receiver_settlement_agent_reserve_balance_after_transaction DECIMAL(15,2),
receiver_settlement_agent_participant_number STRING,
receiver_settlement_agent_participant_short_name_en STRING,
receiver_settlement_agent_participant_short_name_fr STRING,
receiver_settlement_agent_participant_name_en STRING,
receiver_settlement_agent_participant_name_fr STRING,
receiver_settlement_agent_participant_irn STRING,

-- commit (flattened)
commit_commit_id STRING,
commit_cycle_id STRING,
commit_cycle_date DATE,
commit_status_code STRING,
commit_reject_reason_code STRING,
commit_status_timestamp TIMESTAMP,
commit_sender_account_balance_after_transaction DECIMAL(15,2),
commit_sender_reserve_balance_after_transaction DECIMAL(15,2),
commit_sender_settlement_agent_account_balance_after_transaction DECIMAL(15,2),
commit_sender_settlement_agent_reserve_balance_after_transaction DECIMAL(15,2),
commit_receiver_account_balance_after_transaction DECIMAL(15,2),
commit_receiver_reserve_balance_after_transaction DECIMAL(15,2),
commit_receiver_settlement_agent_account_balance_after_transaction DECIMAL(15,2),
commit_receiver_settlement_agent_reserve_balance_after_transaction DECIMAL(15,2),

final_status_code STRING,

-- Audit & control fields
file_date STRING,
file_name STRING,
update_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (file_date)
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains RTR Clearing and Settlement transaction data from Payments Canada. Each record represents a C&S transaction (reservation and associated commit) with sender, receiver, and settlement agent account balances after the transaction. Loaded as a daily delta of completed transactions; in-flight reservations without a commit appear on the day they complete. The dataset supports settlement monitoring, liquidity analysis, and reconciliation with RTR Exchange payments via uetr.';
""")

apply_column_comments("tbl_paycan_rtr_transaction_cs", {
  "transaction_id": "Unique identifier of the transaction detail",
  "transaction_type": "TWIR (capacity transfer with reservation), TWOR (without reservation), FUND (funding), DFND (defunding), PTPT (participant to participant transfer), ADJS (ISP net position adjustments)",
  "cns_ref_id": "Reference ID generated by the C&S system",
  "end_to_end_reference": "End to end reference ID from the Exchange",
  "uetr": "Unique end to end transaction reference used to correlate transactions",
  "cycle_id": "Cycle identifier with date portion and sequence number (YYYYMMDD_NNN)",
  "cycle_date": "Cycle date in Toronto time",
  "status_timestamp": "Time of the status indicated in transaction_status",
  "transaction_status": "Transaction status code (e.g. RSVD, RJCT, STLD)",
  "reject_reason_code": "Rejection reason code for the transaction",
  "transaction_amount": "Transaction amount",
  "exchange_code": "Exchange from which the reservation was sent",
  "exchange_name": "Name of the exchange from which the reservation was sent",
  "sender_position_account_number": "Sender position account number",
  "sender_position_account_type": "Sender position account type: ISP_P, DSP_C, ECA_C, ECA_S",
  "sender_account_balance_after_transaction": "Sender position account balance after the transaction",
  "sender_reserve_balance_after_transaction": "Sender position reserve balance after the transaction",
  "sender_participant_number": "Sender participant unique identifier",
  "sender_participant_irn": "Sender participant IRN",
  "sender_settlement_agent_settlement_account_number": "Sender settlement agent settlement account number",
  "sender_settlement_agent_settlement_account_type": "Sender settlement agent account type: DSP_S or ECA_S",
  "sender_settlement_agent_account_balance_after_transaction": "Sender settlement account balance after the transaction",
  "sender_settlement_agent_reserve_balance_after_transaction": "Sender settlement reserve balance after the transaction",
  "receiver_position_account_number": "Receiver position account number",
  "receiver_position_account_type": "Receiver position account type: ISP_P, DSP_C, ECA_C, ECA_S",
  "receiver_account_balance_after_transaction": "Receiver position account balance after the transaction",
  "receiver_reserve_balance_after_transaction": "Receiver position reserve balance after the transaction",
  "receiver_participant_number": "Receiver participant unique identifier",
  "receiver_participant_irn": "Receiver participant IRN",
  "receiver_settlement_agent_settlement_account_number": "Receiver settlement agent settlement account number",
  "receiver_settlement_agent_settlement_account_type": "Receiver settlement agent account type: DSP_S or ECA_S",
  "receiver_settlement_agent_account_balance_after_transaction": "Receiver settlement account balance after the transaction",
  "receiver_settlement_agent_reserve_balance_after_transaction": "Receiver settlement reserve balance after the transaction",
  "commit_commit_id": "Unique identifier of the COMMIT associated with the RESERVE",
  "commit_cycle_id": "Commit cycle identifier (YYYYMMDD_NNN)",
  "commit_cycle_date": "Commit cycle date in Toronto time",
  "commit_status_code": "Commit status code (STLD, REVD, RJCT, MSTL, MREV)",
  "commit_reject_reason_code": "Reason code for commit failure or rejection",
  "commit_status_timestamp": "Timestamp when funds were finalized (camt.007)",
  "final_status_code": "Final status derived from commit status if a commit exists, otherwise the reserve status",
  **AUDIT_COMMENTS
})

# COMMAND ----------

# participant_cs_<cycle_date>.json | Full | overwrite
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_paycan_rtr_participant_cs
(
-- Core file fields
participant_id BIGINT,
participant_number STRING,
short_name_en STRING,
short_name_fr STRING,
name_en STRING,
name_fr STRING,
irn STRING,
status STRING,                         -- Active / Inactive
inactive_reason STRING,
participant_type STRING,               -- ISP / DSP / BOC / BSO
settlement_agent_flag BOOLEAN,
peer_to_peer_flag BOOLEAN,
effective_time STRING,                 -- kept as string to match std script
expire_time STRING,                    -- kept as string to match std script

-- Audit & control fields
file_date STRING,
file_name STRING,
update_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains RTR Clearing and Settlement participant data from Payments Canada, including onboarded DSPs, ISPs, and DSP settlement agents. Each record represents a version of a participant with effective and expiry times capturing current and historical records. Loaded from the daily full extract participant_cs. The dataset supports participant reference and settlement analysis.';
""")

apply_column_comments("tbl_paycan_rtr_participant_cs", {
  "participant_id": "Unique identifier for a participant record",
  "participant_number": "Permanent FIN of the participant",
  "short_name_en": "Participant short name in English",
  "short_name_fr": "Participant short name in French",
  "name_en": "Participant name in English",
  "name_fr": "Participant name in French",
  "irn": "IRN number for the institution",
  "status": "Participant status: Active or Inactive",
  "inactive_reason": "Justification when status is inactive",
  "participant_type": "Participant type: ISP, DSP, BOC or BSO",
  "settlement_agent_flag": "True if the participant is a settlement agent (DSP)",
  "peer_to_peer_flag": "True if the participant can carry out peer to peer transfers",
  "effective_time": "Timestamp when this version of the record became active (raw source value)",
  "expire_time": "Timestamp when this version of the record expired (raw source value)",
  **AUDIT_COMMENTS
})

# COMMAND ----------

# net_debit_cap_cs_<cycle_date>.json | Full | overwrite
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_paycan_rtr_net_debit_cap_cs
(
-- Core file fields
id BIGINT,
account_tier_type STRING,              -- POSITION / SETTLEMENT
account_number STRING,
net_debit_cap DECIMAL(15,2),
effective_time TIMESTAMP,
expire_time TIMESTAMP,

-- Audit & control fields
file_date STRING,
file_name STRING,
update_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains RTR Clearing and Settlement net debit cap data from Payments Canada. Net debit caps are limits a settlement agent sets for its indirect settlement participants (ISPs). Each record represents a version of a cap by account with effective and expiry times. Loaded from the daily full extract net_debit_cap_cs. The dataset supports credit limit monitoring and liquidity risk analysis.';
""")

apply_column_comments("tbl_paycan_rtr_net_debit_cap_cs", {
  "id": "Unique identifier",
  "account_tier_type": "POSITION (Position Account) or SETTLEMENT (Settlement Account)",
  "account_number": "Unique identifier for the participant account",
  "net_debit_cap": "Net debit cap amount; associated with ISP only and null for DSP",
  "effective_time": "Timestamp from which the record is valid",
  "expire_time": "Timestamp until which the record is valid",
  **AUDIT_COMMENTS
})

# COMMAND ----------

# watermark_cs_<cycle_date>.json | Full | overwrite
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_paycan_rtr_watermark_cs
(
-- Core file fields
watermark_id BIGINT,
account_tier_type STRING,              -- POSITION / SETTLEMENT
account_number STRING,
watermark_type STRING,
watermark_value DECIMAL(20,2),
watermark_threshold STRING,            -- e.g. 85%
effective_time STRING,                 -- kept as string to match std script
expire_time STRING,                    -- kept as string to match std script

-- Audit & control fields
file_date STRING,
file_name STRING,
update_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains RTR Clearing and Settlement watermark configuration data from Payments Canada. Each record represents a version of a position or settlement account watermark, including type, value, and notification threshold. Loaded from the daily full extract watermark_cs. The dataset supports account balance threshold monitoring.';
""")

apply_column_comments("tbl_paycan_rtr_watermark_cs", {
  "watermark_id": "Identifier for the watermark",
  "account_tier_type": "POSITION (Position Account) or SETTLEMENT (Settlement Account)",
  "account_number": "Account number",
  "watermark_type": "Watermark type: lower, upper or minimum",
  "watermark_value": "Value of the watermark",
  "watermark_threshold": "Threshold at which a notification is sent (e.g. 85%)",
  "effective_time": "Timestamp when this version of the record became active (raw source value)",
  "expire_time": "Timestamp when this version of the record expired (raw source value)",
  **AUDIT_COMMENTS
})

# COMMAND ----------

# exchange_cs_<cycle_date>.json | Full | overwrite
# ora_* read as string and try_cast to BIGINT in std script (step 3.3.1)
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_paycan_rtr_exchange_cs
(
-- Core file fields
id BIGINT,
exchange_code STRING,
exchange_name_en STRING,
exchange_name_fr STRING,
description_en STRING,
description_fr STRING,
validate_uetr_flag BOOLEAN,
ora_delay_time BIGINT,
ora_max_retry BIGINT,
effective_time TIMESTAMP,
expire_time TIMESTAMP,

-- Audit & control fields
file_date STRING,
file_name STRING,
update_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains RTR Clearing and Settlement exchange data from Payments Canada. Each record represents a version of an exchange configuration, including UETR validation and outstanding reservation alert settings, with effective and expiry times capturing historical changes. Loaded from the daily full extract exchange_cs. The dataset supports exchange reference and configuration tracking.';
""")

apply_column_comments("tbl_paycan_rtr_exchange_cs", {
  "id": "Unique identifier for the exchange record",
  "exchange_code": "Exchange ID from the source system",
  "exchange_name_en": "Exchange name in English",
  "exchange_name_fr": "Exchange name in French",
  "description_en": "Exchange description in English",
  "description_fr": "Exchange description in French",
  "validate_uetr_flag": "If true, the system checks the UETR and rejects transactions without one",
  "ora_delay_time": "Wait time before sending an outstanding reservation alert to the exchange",
  "ora_max_retry": "Maximum number of outstanding reservation alerts sent to the exchange",
  "effective_time": "Timestamp from which the record is valid",
  "expire_time": "Timestamp until which the record is valid",
  **AUDIT_COMMENTS
})

# COMMAND ----------

# reject_reason_codes_cs_<cycle_date>.json | Full | overwrite (v3.1 layout)
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_paycan_rtr_reject_reason_code_cs
(
-- Core file fields
reject_code STRING,
reject_code_name STRING,
reject_code_description_en STRING,
reject_code_description_fr STRING,

-- Audit & control fields
file_date STRING,
file_name STRING,
update_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains RTR reject reason codes and descriptions maintained by the Payments Canada ISO team. Each record represents a reject code with its name and English and French descriptions. Loaded from the daily full extract reject_reason_codes_cs. The dataset supports decoding of rejection reasons on RTR transactions.';
""")

apply_column_comments("tbl_paycan_rtr_reject_reason_code_cs", {
  "reject_code": "Value of the reject code (e.g. AB05)",
  "reject_code_name": "Name of the reject code",
  "reject_code_description_en": "Description of the reject code in English",
  "reject_code_description_fr": "Description of the reject code in French",
  **AUDIT_COMMENTS
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## RTR Exchange (exchange)

# COMMAND ----------

# competitive_service_ex_<cycle_date>.json | Full | overwrite
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_paycan_rtr_competitive_service_ex
(
-- Core file fields
competitive_service_id STRING,
name STRING,
key_contacts ARRAY<STRUCT<title: STRING, name: STRING, email: STRING, phone: STRING>>,
effective_time TIMESTAMP,
expire_time TIMESTAMP,

-- Audit & control fields
file_date STRING,
file_name STRING,
update_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains RTR Exchange competitive service data from Payments Canada. Each record represents a version of a competitive service with its key contacts and effective and expiry times capturing current and historical records. Loaded from the daily full extract competitive_service_ex. The dataset supports competitive service reference for RTR payments.';
""")

apply_column_comments("tbl_paycan_rtr_competitive_service_ex", {
  "competitive_service_id": "Unique identifier for each competitive service",
  "name": "Name of the competitive service",
  "key_contacts": "List of contacts (title, name, email, phone) for the competitive service",
  "effective_time": "Effective date and time of the record in Toronto time zone",
  "expire_time": "Expiry date and time of the record in Toronto time zone",
  **AUDIT_COMMENTS
})

# COMMAND ----------

# connection_service_provider_ex_<cycle_date>.json | Full | overwrite
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_paycan_rtr_connection_service_provider_ex
(
-- Core file fields
connection_service_provider_id STRING,
name STRING,
financial_institution_indicator STRING,
key_contacts ARRAY<STRUCT<title: STRING, name: STRING, email: STRING, phone: STRING>>,
effective_time TIMESTAMP,
expire_time TIMESTAMP,

-- Audit & control fields
file_date STRING,
file_name STRING,
update_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains RTR Exchange connection service provider data from Payments Canada. Each record represents a version of a connection service provider, indicating whether it is a financial institution, with key contacts and effective and expiry times. Loaded from the daily full extract connection_service_provider_ex. The dataset supports routing and participant connectivity reference.';
""")

apply_column_comments("tbl_paycan_rtr_connection_service_provider_ex", {
  "connection_service_provider_id": "Unique identifier for each connection service provider",
  "name": "Name of the connection service provider",
  "financial_institution_indicator": "Indicates whether the connection service provider is a participant / financial institution",
  "key_contacts": "List of contacts (title, name, email, phone) for the connection service provider",
  "effective_time": "Effective date and time of the record in Toronto time zone",
  "expire_time": "Expiry date and time of the record in Toronto time zone",
  **AUDIT_COMMENTS
})

# COMMAND ----------

# participant_ex_<cycle_date>.json | Full | overwrite
spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_paycan_rtr_participant_ex
(
-- Core file fields
participant_id STRING,
name STRING,
addresses ARRAY<STRUCT<street_number: STRING, street_name: STRING, building_name: STRING, city: STRING, province: STRING, postal_code: STRING>>,
key_contacts ARRAY<STRUCT<title: STRING, name: STRING, email: STRING, phone: STRING>>,
registration_date DATE,
active BOOLEAN,
authorized_to_originate BOOLEAN,
authorized_connection_service_providers ARRAY<STRUCT<connection_service_provider_id: STRING, name: STRING>>,
registered_competitive_services ARRAY<STRUCT<competitive_service_provider_id: STRING, name: STRING>>,
default_routing_method STRUCT<method: STRING, default_connection_service_provider_id: STRING>,
routing_rules ARRAY<STRUCT<competitive_service_id: STRING, method: STRING, connection_service_provider_id: STRING>>,
transit_number_routing_rules ARRAY<STRUCT<transit_number: STRING, connection_service_provider_id: STRING>>,
effective_time TIMESTAMP,
expire_time TIMESTAMP,

-- Audit & control fields
file_date STRING,
file_name STRING,
update_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains RTR Exchange participant data from Payments Canada. Each record represents a version of a participant including addresses, key contacts, registration and origination status, authorized connection service providers, registered competitive services, and routing rules, with effective and expiry times. Loaded from the daily full extract participant_ex. The dataset supports participant reference and payment routing analysis.';
""")

apply_column_comments("tbl_paycan_rtr_participant_ex", {
  "participant_id": "Unique identifier for each participant",
  "name": "Name of the participant",
  "addresses": "List of participant addresses",
  "key_contacts": "List of key contacts for the participant",
  "registration_date": "Date the participant was registered",
  "active": "Indicates if the participant is active",
  "authorized_to_originate": "Indicates if the participant is authorized to originate credit transfers",
  "authorized_connection_service_providers": "List of authorized connection service providers for the participant",
  "registered_competitive_services": "List of registered competitive services for the participant",
  "default_routing_method": "Default routing method and default connection service provider for the participant",
  "routing_rules": "List of routing rules by competitive service",
  "transit_number_routing_rules": "List of routing rules based on transit numbers",
  "effective_time": "Time when the record becomes effective",
  "expire_time": "Time when the record expires",
  **AUDIT_COMMENTS
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Payment (payment_ex_<cycle_date>.json | Delta | replace by file_date)
# MAGIC - Parent: `tbl_paycan_rtr_payment_ex` (1 row per uetr)
# MAGIC - Child: `tbl_paycan_rtr_payment_previous_instructing_agents_ex` (uetr + agent_index)
# MAGIC - Child: `tbl_paycan_rtr_payment_transaction_statuses_ex` (uetr + status + state + technical state)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_paycan_rtr_payment_ex
(
-- Core file fields
uetr STRING,
message_identification STRING,
end_to_end_identification STRING,
transaction_identification STRING,
clearing_and_settlement_assigned_reference STRING,
competitive_service_id STRING,
reconciliation_cycle_id STRING,
exchange_assigned_reference STRING,
sending_participant_id STRING,
sending_participant_csp_id STRING,
receiving_participant_id STRING,
receiving_participant_csp_id STRING,
payment_amount DECIMAL(15,2),
payment_currency_code STRING,
creation_time TIMESTAMP,
file_date STRING,

-- debtor_agent (flattened)
debtor_agent_bicfi STRING,
debtor_agent_clearing_system_identification_code STRING,
debtor_agent_clearing_system_member_id STRING,
debtor_agent_legal_entity_identifier STRING,
debtor_agent_name STRING,
debtor_agent_postal_address_postal_code STRING,
debtor_agent_postal_address_town_name STRING,
debtor_agent_postal_address_country STRING,

-- creditor_agent (flattened)
creditor_agent_bicfi STRING,
creditor_agent_clearing_system_identification_code STRING,
creditor_agent_clearing_system_member_id STRING,
creditor_agent_legal_entity_identifier STRING,
creditor_agent_name STRING,
creditor_agent_postal_address_postal_code STRING,
creditor_agent_postal_address_town_name STRING,
creditor_agent_postal_address_country STRING,

-- Audit & control fields
file_name STRING,
update_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (file_date)
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains RTR Exchange payment transactions from Payments Canada (parent table). Each record represents one final payment (ACSC or RJCT) identified by uetr, with references, sending and receiving participants, amount, and flattened debtor and creditor agent details. Loaded as a daily delta; in-flight payments appear on the day they complete. Child tables hold previous instructing agents and status lifecycle, joined on uetr and file_date.';
""")

apply_column_comments("tbl_paycan_rtr_payment_ex", {
  "uetr": "Unique end-to-end transaction reference",
  "message_identification": "Point-to-point message reference",
  "end_to_end_identification": "Unique ID assigned by the initiating party",
  "transaction_identification": "Unique identification assigned by the first instructing agent",
  "clearing_and_settlement_assigned_reference": "Reference ID created by the Clearing and Settlement system",
  "competitive_service_id": "Competitive service ID",
  "reconciliation_cycle_id": "Reconciliation ID for end-of-day settlement",
  "exchange_assigned_reference": "Reference ID created by RTR Exchange",
  "sending_participant_id": "Financial institution ID of the debtor",
  "sending_participant_csp_id": "Connection service provider ID for the debtor",
  "receiving_participant_id": "Financial institution ID of the creditor",
  "receiving_participant_csp_id": "Connection service provider ID for the creditor based on routing rules",
  "payment_amount": "Payment amount",
  "payment_currency_code": "Payment currency",
  "creation_time": "Timestamp when the transaction was created",
  "debtor_agent_bicfi": "BIC8/11 of the debtor agent",
  "debtor_agent_clearing_system_identification_code": "Debtor agent clearing system identification code",
  "debtor_agent_clearing_system_member_id": "Debtor agent clearing system member ID",
  "debtor_agent_legal_entity_identifier": "Debtor agent legal entity identifier",
  "debtor_agent_name": "Debtor agent name",
  "debtor_agent_postal_address_postal_code": "Debtor agent postal code",
  "debtor_agent_postal_address_town_name": "Debtor agent town name",
  "debtor_agent_postal_address_country": "Debtor agent country",
  "creditor_agent_bicfi": "BIC8/11 of the creditor agent",
  "creditor_agent_clearing_system_identification_code": "Creditor agent clearing system identification code",
  "creditor_agent_clearing_system_member_id": "Creditor agent clearing system member ID",
  "creditor_agent_legal_entity_identifier": "Creditor agent legal entity identifier",
  "creditor_agent_name": "Creditor agent name",
  "creditor_agent_postal_address_postal_code": "Creditor agent postal code",
  "creditor_agent_postal_address_town_name": "Creditor agent town name",
  "creditor_agent_postal_address_country": "Creditor agent country",
  **AUDIT_COMMENTS
})

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_paycan_rtr_payment_previous_instructing_agents_ex
(
-- Keys
uetr STRING,
file_date STRING,
agent_index BIGINT,

-- Core file fields
bicfi STRING,
clearing_system_identification_code STRING,
clearing_system_identification_proprietary STRING,
clearing_system_member_id STRING,
legal_entity_identifier STRING,
name STRING,
postal_code STRING,
town_name STRING,
country STRING,
identification STRING,
scheme_name_code STRING,
scheme_name_proprietary STRING,
issuer STRING,

-- Audit & control fields
file_name STRING,
update_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (file_date)
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains previous instructing agents for RTR Exchange payments (child of tbl_paycan_rtr_payment_ex on uetr and file_date). Each record represents one previous instructing agent in the payment chain, ordered by agent_index, with identification, clearing system, postal address, and other identification details. Exploded from payment_ex.previous_instructing_agents.';
""")

apply_column_comments("tbl_paycan_rtr_payment_previous_instructing_agents_ex", {
  "uetr": "Unique end-to-end transaction reference; joins to tbl_paycan_rtr_payment_ex",
  "agent_index": "Instructing agent index; falls back to array position if not provided",
  "bicfi": "BIC8/11 of the previous instructing agent",
  "clearing_system_identification_code": "Clearing system identification code",
  "clearing_system_identification_proprietary": "Identification code for a clearing system not yet in the list of clearing systems",
  "clearing_system_member_id": "Identification of a member of a clearing system",
  "legal_entity_identifier": "Legal entity identifier of the financial institution",
  "name": "Name of the agent",
  "postal_code": "Agent postal code",
  "town_name": "Agent town name",
  "country": "Agent country",
  "identification": "Unique and unambiguous identification of the agent",
  "scheme_name_code": "Identification scheme name in coded form",
  "scheme_name_proprietary": "Identification scheme name in free text form",
  "issuer": "Entity that assigns the identification",
  **AUDIT_COMMENTS
})

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {unity_catalog_name}.standardized_db.tbl_paycan_rtr_payment_transaction_statuses_ex
(
-- Keys
uetr STRING,
file_date STRING,

-- Core file fields
status_code STRING,                    -- PDNG / ACSP / ACSC / RJCT
status_description STRING,
status_reason_code STRING,
status_reason_desc STRING,
status_time TIMESTAMP,
transaction_states STRING,             -- single transaction_state value
transaction_state_time TIMESTAMP,
technical_state STRING,
technical_state_time TIMESTAMP,

-- Audit & control fields
file_name STRING,
update_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (file_date)
TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = "interval 60 days",
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'This table contains the status lifecycle of RTR Exchange payments (child of tbl_paycan_rtr_payment_ex on uetr and file_date). Each record represents one technical state within a transaction state within a transaction status, flattened from the three-level nested structure. The dataset supports payment lifecycle analysis, timing analysis, and rejection monitoring.';
""")

apply_column_comments("tbl_paycan_rtr_payment_transaction_statuses_ex", {
  "uetr": "Unique end-to-end transaction reference; joins to tbl_paycan_rtr_payment_ex",
  "status_code": "Transaction status code (PDNG, ACSP, ACSC, RJCT)",
  "status_description": "Description of the transaction status",
  "status_reason_code": "Reason code for the transaction status",
  "status_reason_desc": "Description of the status reason code",
  "status_time": "Date and time the status was active",
  "transaction_states": "Business state of the transaction within the status",
  "transaction_state_time": "Date and time the transaction state was active",
  "technical_state": "Technical state of the transaction within the business state",
  "technical_state_time": "Date and time the technical state was active",
  **AUDIT_COMMENTS
})
