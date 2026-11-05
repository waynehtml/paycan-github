# Databricks notebook source
# DBTITLE 1,Get Parameters
import json

# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")

if environment != 'prd':
    dbutils.notebook.exit("Success")


# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../../../common/utils

# COMMAND ----------

# DBTITLE 1,Get UC name
unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

# DBTITLE 1,Get CDCC reader AD group
ad_suffix = get_ad_suffix(dbutils.widgets.get("environment"))
ad_group = "lake_cdccdata_rusr_" + ad_suffix;

# COMMAND ----------

# DBTITLE 1,Grant read permission on all standardized cdcc tables to reader AD group
# set current database
spark.sql(f"USE {unity_catalog_name}.standardized_db")

# Get list of tmx_cdcc tables in the current database
tables = spark.sql("SHOW TABLES").filter("tableName LIKE '%_tmx_cdcc_%'").collect()

# Grant SELECT on each standardized cdcc table to reader AD group
for table in tables:
    spark.sql(f"GRANT SELECT on {table['tableName']} to  {ad_group}")


# COMMAND ----------

# DBTITLE 1,Grant read permission on all cdcc views to reader AD group
# set current database
spark.sql(f"USE {unity_catalog_name}.view_db")

# Get list of tmx_cdcc views in the current database
views = spark.sql("SHOW VIEWS").filter("viewName LIKE '%_tmx_cdcc_%'").collect()

# Grant SELECT on each standardized cdcc table to reader AD group
for view in views:
    spark.sql(f"GRANT SELECT on {view['viewName']} to  {ad_group}")
