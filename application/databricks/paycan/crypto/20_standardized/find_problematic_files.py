# Databricks notebook source
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.utils import AnalysisException
from functools import reduce

# COMMAND ----------

# MAGIC %run ../../common/utils

# COMMAND ----------

dbutils.widgets.removeAll()
# Widget Parameters
dbutils.widgets.text("log_path", "kaiko-bad")
dbutils.widgets.text("data_path", "raw/external/kaiko")
dbutils.widgets.dropdown("dataset", "Trade", ["Trade", "OrderBook"])
dbutils.widgets.text("exchange", "*")

# Get parameters
log_path = dbutils.widgets.get("log_path")
print(f"log_path: {log_path}")
data_path = dbutils.widgets.get("data_path")
print(f"data_path: {data_path}")
dataset = dbutils.widgets.get("dataset")
print(f"dataset: {dataset}")
exchange = dbutils.widgets.get("exchange")
print(f"exchange: {exchange}")

# COMMAND ----------

environment = get_env()
lake_storage = get_lake_storage_account(environment)
land_storage = get_land_st_name(environment)

log_full_path = f"abfss://land-paycan@{land_storage}.dfs.core.windows.net/{log_path}/{dataset}"
print(f"log_full_path: {log_full_path}")

data_base_path = f"abfss://data@{lake_storage}.dfs.core.windows.net/{data_path}"
print(f"data_base_path: {data_base_path}")
dataset_full_path = f"{data_base_path}/{dataset}"
print(f"dataset_full_path: {dataset_full_path}")

# COMMAND ----------

# DBTITLE 1,Load files
def get_files(file_path, format):
    print(f"Loading {format} files from {file_path}")
    try:
        # Load log files
        df = spark.read.format(format) \
            .option("recursiveFileLookup", "true") \
            .load(f"{file_path}")
        
        # Get unique file path
        df = df.select("path").distinct().orderBy("path")
    except AnalysisException as e:
        df = None
    
    return df

# Function to union multiple DataFrames safely
def safe_union(*dataframes):
    # Filter out None values
    valid_dataframes = [df for df in dataframes if df is not None]
    # Use reduce to chain union operations
    return reduce(DataFrame.union, valid_dataframes) if valid_dataframes else None


def remove_ffix(df, ffix):
    df = df.withColumn("path", regexp_replace(col("path"), ffix, ""))
    return df

# COMMAND ----------

# DBTITLE 1,Bad files
bad_files_path = f"{log_full_path}/*/bad_files/"
df_bad_files = get_files(bad_files_path,"json")
df_bad_files = remove_ffix(df_bad_files, data_base_path)
if df_bad_files is not None:
    print(f"bad files: {df_bad_files.count()}")
    df_bad_files.display()
else:
    print(f"No bad files found")


# COMMAND ----------

# DBTITLE 1,Files with bad records
bad_records_path = f"{log_full_path}/*/bad_records/"
df_bad_records = get_files(bad_records_path, "json")
df_bad_records = remove_ffix(df_bad_records, data_base_path)
if df_bad_records is not None:
    print(f"files with bad records: {df_bad_records.count()}")
    df_bad_records.display()
else:
    print(f"No bad records found")

# COMMAND ----------

# DBTITLE 1,gstmp files
gstmp_suffix = "_.gstmp"
gstmp_file_path = f"{dataset_full_path}/{exchange}/*/*{gstmp_suffix}"
df_gstmp_files = get_files(gstmp_file_path, "binaryFile")
df_gstmp_files = remove_ffix(df_gstmp_files, data_base_path)
df_gstmp_files = remove_ffix(df_gstmp_files, gstmp_suffix)
if df_gstmp_files is not None:
    print(f"gstmp files: {df_gstmp_files.count()}")
    df_gstmp_files.display()
else:
    print(f"No gSTMP files found")

# COMMAND ----------

# Union the DataFrames
df_all = safe_union(df_bad_files, df_bad_records, df_gstmp_files)
if df_all is not None:
    df_all = df_all.distinct().orderBy("path")
    print(f"All problematic files: {df_all.count()}")
    df_all.display()
else:
    print("No files found")
