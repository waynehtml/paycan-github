# Databricks notebook source
# DBTITLE 1,Parameters
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")

# COMMAND ----------

# DBTITLE 1,Load utility
# MAGIC %run ../../../common/utils

# COMMAND ----------

# DBTITLE 1,Get unity catalog name
unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))


# COMMAND ----------

# DBTITLE 1,participant_institution
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_participant_institution
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_participant_institution
""")

# COMMAND ----------

# DBTITLE 1,payment
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_payment
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_payment
""")

# COMMAND ----------

# DBTITLE 1,net_send_schedule
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_net_send_schedule
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_net_send_schedule
""")

# COMMAND ----------

# DBTITLE 1,operating_schedule
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_operating_schedule
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_operating_schedule
""")

# COMMAND ----------

# DBTITLE 1,operational_status
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_operational_status
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_operational_status
""")

# COMMAND ----------

# DBTITLE 1,net_send_override
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_net_send_override
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_net_send_override
""")

# COMMAND ----------

# DBTITLE 1,net_send_limit
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_net_send_limit
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_net_send_limit
""")

# COMMAND ----------

# DBTITLE 1,liquidity_threshold
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_liquidity_threshold
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_liquidity_threshold
""")

# COMMAND ----------

# DBTITLE 1,liquidity_position
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_liquidity_position
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_liquidity_position
""")

# COMMAND ----------

# DBTITLE 1,gridlock
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_gridlock
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_gridlock
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_credit_position
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_credit_position
""")

# COMMAND ----------

# DBTITLE 1,credit_activity
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_credit_activity
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_credit_activity
""")

# COMMAND ----------

# DBTITLE 1,loan
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_loan
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_loan
""")

# COMMAND ----------

# DBTITLE 1,lom_status
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_lom_status
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_lom_status
""")

# COMMAND ----------

# DBTITLE 1,minum_balance
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_minum_balance
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_minum_balance
""")

# COMMAND ----------

# DBTITLE 1,pause_payments
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_pause_payments
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_pause_payments
""")

# COMMAND ----------

# DBTITLE 1,transfer
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_transfer
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_transfer
""")

# COMMAND ----------

# DBTITLE 1,validation
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_validation
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_validation
""")

# COMMAND ----------

# DBTITLE 1,payment_summary
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_payment_summary
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_payment_summary
""")

# COMMAND ----------

# DBTITLE 1,transfer_summary
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_transfer_summary
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_transfer_summary
""")

# COMMAND ----------

# DBTITLE 1,credit_activity_summary
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_lynx_credit_activity_summary
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.lynx_credit_activity_summary
""")
