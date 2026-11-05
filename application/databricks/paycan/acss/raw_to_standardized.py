# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import pandas as pd
import datetime
from pyspark.sql import functions as f
from pyspark.sql.functions import row_number,lit
from pyspark.sql.window import Window
import pyspark.sql.types as t

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

#all the components of the file path:
dbutils.widgets.text("env","", "env")
dbutils.widgets.text("cycle_date","", "cycle_date")
dbutils.widgets.text("container","restricted", "container")
dbutils.widgets.text("storage_name","", "storage_name")
dbutils.widgets.text("raw_path","raw/external/payments_canada/acss_usbe/weekly", "raw_path")
dbutils.widgets.dropdown("file","acss_usbe_sets",  ["acss_usbe_sets", "acss_usbe_vol_val"])
#dbutils.widgets.text("stn_path","standardized/external/payments_canada/acss_usbe/weekly", "stn_path")

env = dbutils.widgets.get("env")
print(f"env: {env}")
file_date = dbutils.widgets.get("cycle_date") #2021-11-08
print(f"file_date: {file_date}")
container = dbutils.widgets.get("container")
print(f"container: {container}")
storage_name = dbutils.widgets.get("storage_name")
print(f"storage_name: {storage_name}")
raw_path = dbutils.widgets.get("raw_path")
print(f"raw_path: {raw_path}")
file_tag = dbutils.widgets.get("file")
print(f"file_tag: {file_tag}")
#stn_path = dbutils.widgets.get("stn_path")
#print(f"stn_path: {stn_path}")

# COMMAND ----------

# DBTITLE 1,file paths
# Get environment if not provided
if len(env.strip()) == 0:
    env = get_env()
print(f"env: {env}")

# Get lake storage if not provided
if len(storage_name.strip()) == 0:
    storage_name = get_lake_storage_account(env)
print(f"storage_name: {storage_name}")

# input file path in raw zone
raw_file_path = "abfss://" + container+ "@" + storage_name + ".dfs.core.windows.net/" + raw_path + "/" + file_date
print(f"raw_file_path: {raw_file_path}")

# output file path in standardized zone
#std_file_path = "abfss://" + container+ "@" + storage_name + ".dfs.core.windows.net/" + stn_path + "/" + file_date
#print(f"std_file_path: {std_file_path}")

# target table name
unity_catalog_name = get_unity_catalog_name(env)
target_table_name = f"{unity_catalog_name}.standardized_db.tbl_paycan_{file_tag}"

# COMMAND ----------

def setSchema(dataframe, file):
    if file == "acss_usbe_sets":
        sc = spark.sparkContext
        schema = t.StructType(
            [
                t.StructField("system_code", t.StringType(), True),
                t.StructField("settlement_date", t.DateType(), True),
                t.StructField("sending_dc_irn", t.StringType(), True),
                t.StructField("sending_dc_name", t.StringType(), True),
                t.StructField("receiving_dc_irn", t.StringType(), True),
                t.StructField("receiving_dc_name", t.StringType(), True),
                t.StructField("sets_volume", t.LongType(), True),
                t.StructField("sets_value", t.DecimalType(30, 2), True),
                t.StructField("acss_usbe_sets_id", t.IntegerType(), True),
                t.StructField("batch_id", t.IntegerType(), True),
            ]
        )

    else:
        sc = spark.sparkContext
        schema = t.StructType(
            [
                t.StructField("system_code", t.StringType(), True),
                t.StructField("settlement_date", t.DateType(), True),
                t.StructField("sending_dc_irn", t.StringType(), True),
                t.StructField("sending_dc_name", t.StringType(), True),
                t.StructField("receiving_dc_irn", t.StringType(), True),
                t.StructField("receiving_dc_name", t.StringType(), True),
                t.StructField("stream_code", t.StringType(), True),
                t.StructField("stream_name", t.StringType(), True),
                t.StructField("volume", t.LongType(), True),
                t.StructField("value", t.DecimalType(30, 2), True),
                t.StructField("acss_usbe_vol_val_id", t.IntegerType(), True),
                t.StructField("batch_id", t.IntegerType(), True),
            ]
        )

    return spark.createDataFrame(sc.emptyRDD(), schema)

# COMMAND ----------

# DBTITLE 1,find file path with file tag
file_path = None
files = dbutils.fs.ls(raw_file_path)
for file in files:
    if file_tag in file.name:
        file_path = file.path
print(f"file_path: {file_path}")
if file_path is None:
    raise Exception(f"No file found with tag {file_tag} in {raw_file_path}")   

# COMMAND ----------

# DBTITLE 1,Read raw file
df = spark.read.format("csv").option("header", "true").load(file_path)
df.display()

# COMMAND ----------

# DBTITLE 1,transform data
if not df.rdd.isEmpty():

    # 2. Apply columns data type changes for sets
    if file_tag == "acss_usbe_sets":

        df = df.withColumn("acss_usbe_sets_id", f.lit(0))
        df = df.withColumn("batch_id", f.lit(0))
        df = df.withColumn(
            "acss_usbe_sets_id", df["acss_usbe_sets_id"].cast("long")
        )
        df = df.withColumn("batch_id", df["batch_id"].cast("int"))
        df = df.withColumn(
            "settlement_date", f.to_date(f.col("settlement_date"), "yyyyMMdd")
        )
        df = df.withColumn("sets_volume", df["sets_volume"].cast("long"))
        df = df.withColumn("sets_value", df["sets_value"].cast("Decimal(30,2)"))
        df = df.withColumnRenamed("receiveing_dc_name", "receiving_dc_name")
        w = Window().orderBy(lit("placeholder"))
        df = df.withColumn(
            "acss_usbe_sets_id", row_number().over(w)
        ).withColumn("batch_id", row_number().over(w))

    # 3. Apply columns data type changes for vol_val
    if file_tag == "acss_usbe_vol_val":
        df = df.withColumn("acss_usbe_vol_val_id", f.lit(0))
        df = df.withColumn("batch_id", f.lit(0))
        df = df.withColumn(
            "acss_usbe_vol_val_id", df["acss_usbe_vol_val_id"].cast("long")
        )
        df = df.withColumn("batch_id", df["batch_id"].cast("int"))
        df = df.withColumn(
            "settlement_date", f.to_date(f.col("settlement_date"), "yyyyMMdd")
        )
        df = df.withColumn("volume", df["volume"].cast("long"))
        df = df.withColumn("value", df["value"].cast("Decimal(30,2)"))
        w = Window().orderBy(lit("placeholder"))
        df = df.withColumn(
            "acss_usbe_vol_val_id", row_number().over(w)
        ).withColumn("batch_id", row_number().over(w))
else:
    df = setSchema(df, file)

# for testing
df.display()

# COMMAND ----------

# DBTITLE 1,write to lake
#std_file_full_path = f"{std_file_path}/{file_tag}"
#print(f"std_file_full_path: {std_file_full_path}")
#df.write.mode("overwrite").parquet(std_file_full_path)

# COMMAND ----------

# DBTITLE 1,Write to UC
if not df.rdd.isEmpty():
    # Remove existing data with same file_date
    try:
        print(f"Removing {file_date}/{file_tag} data from {target_table_name} ")
        dt = delta.DeltaTable.forName(spark, target_table_name)
        dt.delete(col("file_date") == to_date(lit(file_date), "yyyy-MM-dd"))
    except (Exception) as ex:
        print(f"Failed to remove the existing data from {target_table_name}, due to {ex}")

    # Write data to target table
    print(f"Writing {file_date}/{file_tag} data to {target_table_name} ")
    df = df.withColumn("file_date", to_date(lit(file_date), "yyyy-MM-dd"))
    df.write.format("delta").mode("append").partitionBy('file_date').saveAsTable(target_table_name)
else:
    print(f"No data to write to {target_table_name}")
