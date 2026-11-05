# Databricks notebook source
# DBTITLE 1,Set up Widgets
dbutils.widgets.text("environment", "")

# COMMAND ----------

# DBTITLE 1,Run Utils
# MAGIC %run /paycan/common/utils

# COMMAND ----------

# DBTITLE 1,Set up UC variable
unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

spark.conf.set('ad.catalog',unity_catalog_name)

# COMMAND ----------

# DBTITLE 1,Set AD group
ad_suffix = get_ad_suffix(dbutils.widgets.get("environment"))

ad_group = "lake_clsdata_rusr_" + ad_suffix;

# COMMAND ----------

# DBTITLE 1,Set Permissions on CLS Tables
spark.sql(f"""
GRANT SELECT ON  {unity_catalog_name}.standardized_db.tbl_cls_overview  TO {ad_group};
""")


# COMMAND ----------

# DBTITLE 1,Set Permissions on CLS Views
spark.sql(f"""
GRANT SELECT ON {unity_catalog_name}.view_db.vwu_cls_overview_total_usd_equiv TO {ad_group};
""")

spark.sql(f"""
GRANT SELECT ON {unity_catalog_name}.view_db.vwu_cls_overview_individual_currency TO {ad_group};
""")

# COMMAND ----------

# DBTITLE 1,List Permissions
# MAGIC %sql
# MAGIC SHOW GRANT ON TABLE ${ad.catalog}.standardized_db.tbl_cls_overview;
# MAGIC

# COMMAND ----------

# DBTITLE 1,List Permissions
# MAGIC %sql
# MAGIC SHOW GRANT ON  VIEW ${ad.catalog}.view_db.vwu_cls_overview_total_usd_equiv;

# COMMAND ----------

# DBTITLE 1,List Permissions
# MAGIC %sql
# MAGIC SHOW GRANT ON  VIEW ${ad.catalog}.view_db.vwu_cls_overview_individual_currency;
