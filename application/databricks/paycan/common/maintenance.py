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

# DBTITLE 1,Maintenance works
spark.sql(f"""
ALTER VIEW {unity_catalog_name}.view_db.vw_cls_overview_individual_currency RENAME TO {unity_catalog_name}.view_db.vwu_cls_overview_individual_currency
""")

spark.sql(f"""
ALTER VIEW {unity_catalog_name}.view_db.vw_cls_overview_total_usd_equiv RENAME TO {unity_catalog_name}.view_db.vwu_cls_overview_total_usd_equiv
""")
