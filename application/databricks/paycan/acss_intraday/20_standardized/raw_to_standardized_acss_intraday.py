# Databricks notebook source
from datetime import datetime as dt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from pyspark.sql import functions as f
from pyspark.sql.functions import row_number,lit
import pyspark.sql.types as t
from pyspark.sql import SparkSession

# COMMAND ----------

dbutils.widgets.text("environment","")
dbutils.widgets.text("storage_name","")
dbutils.widgets.text("cycle_date","")

environment = dbutils.widgets.get("environment")
print(f"environment: {environment}")
storage_name = dbutils.widgets.get("storage_name") #lakecanpdstdev01
print(f"storage_name: {storage_name}")
file_date = dbutils.widgets.get("cycle_date")
print(f"file_date: {file_date}")

# COMMAND ----------

# DBTITLE 1,load utils notebook
# MAGIC %run ../../common/utils

# COMMAND ----------

# MAGIC %run "/paycan/python_library/modules/02_standardize"

# COMMAND ----------

# DBTITLE 1,file paths
# Get environment if not provided
if len(environment.strip()) == 0:
    environment = get_env()
print(f"environment: {environment}")

# Get lake storage if not provided
if len(storage_name.strip()) == 0:
    storage_name = get_lake_storage_account(environment)
print(f"storage_name: {storage_name}")

# input - raw file path
raw_file_path = f"abfss://restricted@{storage_name}.dfs.core.windows.net/raw/external/payments_canada/acss/intraday/{file_date}/"
print(f"raw_file_path: {raw_file_path}")

# output - standardized file path
#std_file_path = f"abfss://restricted@{storage_name}.dfs.core.windows.net/standardized/external/payments_canada/acss/intraday/{file_date}/"
#print(f"std_file_path: {std_file_path}")

# output - target table name
unity_catalog_name = get_unity_catalog_name(environment)
target_table_name = f"{unity_catalog_name}.standardized_db.tbl_paycan_acss_intraday"
print(f"target_table_name: {target_table_name}")

# COMMAND ----------

# DBTITLE 1,Read raw file
df = spark.read.csv(raw_file_path, header = True)

# COMMAND ----------

datastn = Standardize(df)

# COMMAND ----------

df = datastn.stn_int("volume")
df = datastn.stn_int("sent_time")

# COMMAND ----------

df = datastn.stn_double("value")

# COMMAND ----------

df = datastn.stn_date("settlement_date","%Y%m%d")
df = datastn.stn_date("sent_date","%Y%m%d")
df = datastn.stn_date("cycle_date","%Y%m%d")

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,write to lake
#df.write.mode("overwrite").parquet(std_file_path)

# COMMAND ----------

# Remove existing data with same file_date
try :
    dt = delta.DeltaTable.forName(spark, target_table_name)
    dt.delete(col("file_date") == to_date(lit(file_date), "yyyy-MM-dd"))
except (Exception) as ex:
    print(f"Failed to remove the existing data from {target_table_name}, due to {ex}")

# Write data to target table
df = df.withColumn("file_date", to_date(lit(file_date), "yyyy-MM-dd"))
# Cast sent_time to hh:mm
df = df.withColumn(
    "sent_time",
    format_string(
        "%02d:%02d",
        (col("sent_time") / 100).cast("int"),  # First two digits
        col("sent_time") % 100,                # Last two digits
    ),
)
df.write.format("delta").mode("append").partitionBy('cycle_date').saveAsTable(target_table_name)
