# Databricks notebook source
# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../../../common/utils

# COMMAND ----------

# DBTITLE 1,Get UC name
unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

# DBTITLE 1,Drop cdcc views
from pyspark.sql.types import *

# set current database
spark.sql(f"USE {unity_catalog_name}.view_db")

# Get list of tmx_cdcc views in the current database
views = spark.sql("SHOW VIEWS ").filter("viewName LIKE '%_tmx_cdcc_%'").collect()

for view in views:
    spark.sql(f"DROP VIEW IF EXISTS {view['viewName']}")