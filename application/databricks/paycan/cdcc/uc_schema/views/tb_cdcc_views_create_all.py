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

# DBTITLE 1,Create user views for each cdcc tables
# set current database
spark.sql(f"USE {unity_catalog_name}.standardized_db")

# Get list of tmx_cdcc tables in the current database
tables = spark.sql("SHOW TABLES ").filter("tableName LIKE 'tbl_tmx_cdcc%'").collect()

for table in tables:
    tbl_name = table['tableName']
    vwu_name = tbl_name.replace('tbl_', 'vwu_', 1)
    sql_stmt = f"CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.{vwu_name} AS SELECT * FROM {unity_catalog_name}.standardized_db.{tbl_name}"
    print(sql_stmt)
    spark.sql(sql_stmt)