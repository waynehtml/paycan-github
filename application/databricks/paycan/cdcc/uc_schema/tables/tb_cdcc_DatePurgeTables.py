# Databricks notebook source
# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("schema_name", "standardized_db")
dbutils.widgets.text("table_names", "tbl_loading_log_v2")
dbutils.widgets.text("column_name", "load_start_ts")
dbutils.widgets.text("delete_from_date", "2025-07-01")
dbutils.widgets.text("delete_end_date", "2025-06-01")


# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../../../common/utils

# COMMAND ----------

# DBTITLE 1,Get UC name
unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

# DBTITLE 1,Drop standardized tables
from pyspark.sql.types import *

def get_table_names(table_names_string) -> ArrayType:
    return table_names_string.split(",") 

schema_name = dbutils.widgets.get("schema_name")   
table_names = get_table_names(dbutils.widgets.get("table_names"))   
column_name = dbutils.widgets.get("column_name")
delete_from_date = dbutils.widgets.get("delete_from_date")
delete_end_date = dbutils.widgets.get("delete_end_date")    


for table_name in table_names:
    sql_stmt=f"DELETE FROM {unity_catalog_name}.{schema_name}.{table_name} WHERE {column_name} >= '{delete_from_date}' AND {column_name} <= '{delete_end_date}'"
    print(sql_stmt)
    spark.sql(sql_stmt)