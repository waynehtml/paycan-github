# Databricks notebook source
# DBTITLE 1,Get env
dbutils.widgets.text("environment", "")

# COMMAND ----------

# DBTITLE 1,Run Utils
# MAGIC %run /paycan/common/utils

# COMMAND ----------

# DBTITLE 1,Set UC variable
unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

# DBTITLE 1,Create standardized table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {unity_catalog_name}.standardized_db.tbl_cls_overview (
  value_date  date not null,
  currency  string not null,
  number_of_sides_settled  int not null,
  value_original_ccy decimal(30,3) not null,
  number_unsettled int not null,
  value_unsettled decimal(30,3) not null,
  total_pay_ins decimal(30,3) not null,
  total_pay_outs decimal(30,3) not null,
  audit_load_file string not null,
  audit_load_ts timestamp not null,
  CONSTRAINT tbl_cls_overview_pk PRIMARY KEY(value_date, currency)
);
""")

spark.sql(f"""
ALTER TABLE {unity_catalog_name}.standardized_db.tbl_cls_overview SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled');
""")

spark.sql(f"""
ALTER TABLE {unity_catalog_name}.standardized_db.tbl_cls_overview ALTER COLUMN audit_load_ts SET DEFAULT current_timestamp();
""")


# COMMAND ----------

# DBTITLE 1,Create views
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vwu_cls_overview_total_usd_equiv 
AS
SELECT   *
FROM  {unity_catalog_name}.standardized_db.tbl_cls_overview
WHERE currency='Total (USD equiv)';
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vwu_cls_overview_individual_currency
AS
SELECT   *
FROM  {unity_catalog_name}.standardized_db.tbl_cls_overview
WHERE currency <> 'Total (USD equiv)';
""")

# COMMAND ----------

# DBTITLE 1,create loading log table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {unity_catalog_name}.standardized_db.tbl_loading_log (
  table_name  string not null,
  file_name  string not null,
  load_ts timestamp not null,
  load_result string not null
);
""")
