# Databricks notebook source
# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")
dbutils.widgets.text("schema_name", "standardized_db")
dbutils.widgets.text("table_names", "tbl_loading_log_v2")


# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../../../common/utils

# COMMAND ----------

# DBTITLE 1,Get UC name
unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

# DBTITLE 1,Truncate standardized tables
from pyspark.sql.types import *

##get table names
def get_table_names(table_names_string) -> ArrayType:
    return table_names_string.split(",") 

schema_name = dbutils.widgets.get("schema_name")   
table_names = get_table_names(dbutils.widgets.get("table_names"))   

for table_name in table_names:
    spark.sql(f"TRUNCATE TABLE {unity_catalog_name}.{schema_name}.{table_name}")