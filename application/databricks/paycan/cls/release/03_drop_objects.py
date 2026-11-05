# Databricks notebook source
dbutils.widgets.text("environment", "")


# COMMAND ----------

# MAGIC %run /paycan/common/utils

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

ad_suffix = get_ad_suffix(dbutils.widgets.get("environment"))

# COMMAND ----------

spark.sql(f"""
drop table  {unity_catalog_name}.standardized_db.tbl_cls_overview ;
""")


# COMMAND ----------

spark.sql(f"""
drop table  {unity_catalog_name}.standardized_db.tbl_loading_log ;
""")


# COMMAND ----------

spark.sql(f"""
drop view  {unity_catalog_name}.view_db.vwu_cls_overview_total_usd_equiv;
""")


# COMMAND ----------

spark.sql(f"""
drop view {unity_catalog_name}.view_db.vwu_cls_overview_individual_currency;
""")
