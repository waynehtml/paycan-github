# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook loads standardized data into tbl_paycan_acss_intraday **
# MAGIC

# COMMAND ----------

# DBTITLE 1,functions
from pyspark.sql.functions import col, concat, conv, countDistinct, explode, length, lit, lpad, map_keys, map_values, split, substring, when, expr, to_date, coalesce, array, format_string, input_file_name
from pyspark.sql.types import StringType, DecimalType

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")
dbutils.widgets.text("replace", "false")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")
print(f"environment: {environment}")
replace = dbutils.widgets.get("replace")
print(f"replace: {replace}")

# COMMAND ----------

from delta.tables import *
from pyspark.sql import functions as F
from pyspark.sql.functions import split, element_at

# COMMAND ----------

# Get environment if not provided
if len(environment.strip()) == 0:
    environment = get_env()
print(f"environment: {environment}")

unity_catalog_name = get_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")

storage = get_lake_storage_account(environment)
print(f"storage: {storage}")
 

# COMMAND ----------

# DBTITLE 1,function: load_data
def call_function(function_name, *args, **kwargs):
    func = globals().get(function_name)
    if func is not None and callable(func):
        return func(*args, **kwargs)
    return None

def load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition=None):
    print(f"loading data from {file_rel_path} to {table_short_name}")

    # Compose the file path   
    file_full_path = f'abfss://restricted@{storage}.dfs.core.windows.net/{file_rel_path}'
    print(f"file_path: {file_full_path}")

    # Load data from file path
    print(f"reading data from {file_full_path}")
    df = spark.read.format("parquet").option("mergeSchema", "true").load(file_full_path)

    # dataset specific transformation
    tbl = table_short_name.split(".")[-1]
    ret = call_function(tbl, df)
    if (ret is not None):
        df = ret

    # Compose the table name
    table_full_name = f"{unity_catalog_name}.{table_short_name}"
    print(f"table_full_name: {table_full_name}")

    # Clear the table
    print(f"clearing {table_full_name}")
    table = delta.DeltaTable.forName(spark, table_full_name)
    table.delete()

    # Write data to the table
    print(f"writing data to {table_full_name}")
    if partition is None:
        df.write.format('delta').mode('append').saveAsTable(table_full_name)
    else:
        df.write.format('delta').mode('append').partitionBy(partition).saveAsTable(table_full_name)
    print(f"{spark.table(table_full_name).count()} records loaded to {table_full_name}")

    return df

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,tbl_paycan_acss_intraday
def tbl_paycan_acss_intraday(df: DataFrame):
    # add file_date column from file name
    df = df.withColumn(
        "file_date",
        to_date(element_at(split(F.input_file_name(), "/"), -2), "yyyy-MM-dd"),
    )
    
    # Cast sent_time to string
    # df = df.withColumn("sent_time", lpad(col("sent_time").cast("string"), 4, "0"))
    df = df.withColumn(
        "sent_time",
        format_string(
            "%02d:%02d",
            (col("sent_time") / 100).cast("int"),  # First two digits
            col("sent_time") % 100,                # Last two digits
        ),
    )
    return df

file_rel_path = f"standardized/external/payments_canada/acss/intraday/*"
table_short_name = f"standardized_db.tbl_paycan_acss_intraday"
partition = "cycle_date"
df = load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)
df.limit(100).display()

# COMMAND ----------

# DBTITLE 1,tbl_paycan_acss_usbe_sets
def tbl_paycan_acss_usbe_sets(df: DataFrame):
    # add file_date column from file name
    df = df.withColumn(
        "file_date",
        to_date(element_at(split(F.input_file_name(), "/"), -3), "yyyy-MM-dd"),
    )
    return df

file_rel_path = f"standardized/external/payments_canada/acss_usbe/weekly/*/acss_usbe_sets"
table_short_name = f"standardized_db.tbl_paycan_acss_usbe_sets"
partition = "file_date"
df = load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

df.limit(100).display()

# COMMAND ----------

# DBTITLE 1,acss_usbe_vol_val
def tbl_paycan_acss_usbe_vol_val(df: DataFrame):
    # add file_date column from file name
    df = df.withColumn(
        "file_date",
        to_date(element_at(split(F.input_file_name(), "/"), -3), "yyyy-MM-dd"),
    )
    return df

file_rel_path = f"standardized/external/payments_canada/acss_usbe/weekly/*/acss_usbe_vol_val"
table_short_name = f"standardized_db.tbl_paycan_acss_usbe_vol_val"
partition = "file_date"
df = load_data(storage, file_rel_path, unity_catalog_name, table_short_name, partition)

df.limit(100).display()
