# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import pandas as pd
import datetime
from pyspark.sql import functions as f
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("env","", "env")
#dbutils.widgets.text("storage_name","", "storage_name")
#dbutils.widgets.text("container","restricted", "container")
#dbutils.widgets.text("stn_path","standardized/external/payments_canada/acss_usbe/weekly", "stn_path")
#dbutils.widgets.text("curated_path","curated/paycan/acss_usbe/weekly", "curated_path")
dbutils.widgets.dropdown("file","acss_usbe_sets",  ["acss_usbe_sets", "acss_usbe_vol_val"])

env = dbutils.widgets.get("env")
print(f"env: {env}")
file_tag = dbutils.widgets.get("file")
print(f"file_tag: {file_tag}")
#container = dbutils.widgets.get("container")
#print(f"container: {container}")
#storage_name = dbutils.widgets.get("storage_name") 
#print(f"storage_name: {storage_name}")
#stn_path = dbutils.widgets.get("stn_path")
#print(f"stn_path: {stn_path}")
#curated_path = dbutils.widgets.get("curated_path")
#print(f"curated_path: {curated_path}")

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# Get lake storage if not provided
#if len(storage_name.strip()) == 0:
#    storage_name = get_lake_storage_account(env)
#print(f"storage_name: {storage_name}")

# input file path in standardized zone
#standardized_path = f"abfss://{container}@{storage_name}.dfs.core.windows.net/{stn_path}/*/{file_tag}"
#print(f"standardized_path: {standardized_path}")

# output file path in curated zone
#cur_path = f"abfss://{container}@{storage_name}.dfs.core.windows.net/{curated_path}/{file_tag}"
#print(f"cur_path: {cur_path}")

# table names
unity_catalog_name = get_unity_catalog_name(env)
# standardized table name
std_table_name = f"{unity_catalog_name}.standardized_db.tbl_paycan_{file_tag}"
# curated table name
curated_table_name = f"{unity_catalog_name}.curated_db.{file_tag}"

# COMMAND ----------

# DBTITLE 1,Lake processing
# read the standardized data from the lake
#print(f"Reading from {standardized_path} ")
#df_lake = spark.read.option('recursiveFileLookup', True).parquet(standardized_path)

# transformation
#w = Window().orderBy(lit('placeholder'))
#df_lake = df_lake.withColumn(file_tag+"_id", row_number().over(w)).withColumn("batch_id", row_number().over(w))
#df_lake.display()

# write to the curated file
#print(f"Writing to {cur_path} ")
#df_lake.write.format("parquet").mode("overwrite").save(cur_path)

# COMMAND ----------

# DBTITLE 1,UC Processing
# read the standardized data from the lake
print(f"Reading from {std_table_name} ")
df_uc = spark.read.format("delta").table(std_table_name)

# transformation
w = Window().orderBy(lit('placeholder'))
df_uc = df_uc.withColumn(file_tag+"_id", row_number().over(w)).withColumn("batch_id", row_number().over(w))
df_uc.display()

# write to the curated table
print(f"Writing to {curated_table_name} ")
df_uc.write.format("delta").mode("overwrite").partitionBy('file_date').saveAsTable(curated_table_name)

