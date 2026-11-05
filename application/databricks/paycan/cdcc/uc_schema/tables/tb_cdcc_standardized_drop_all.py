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

# DBTITLE 1,Drop all CDCC tables
# set current database
spark.sql(f"USE {unity_catalog_name}.standardized_db")

# Get list of tmx_cdcc tables in the current database
tables = spark.sql("SHOW TABLES").filter("tableName LIKE 'tbl_tmx_cdcc_%'").collect()

# Drop each table
for table in tables:
    spark.sql(f"DROP TABLE {table['tableName']}")