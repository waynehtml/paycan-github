# Databricks notebook source
from pyspark.sql.types import DecimalType
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("environment","dev", "environment")
dbutils.widgets.text("file_name","", "file_name")
environment = dbutils.widgets.get("environment")
file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %run ../../common/utils

# COMMAND ----------

# catalog name
unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# Storage
if environment == "dev":
    storage = "lakecacnpdstdev01"
elif environment == "stg":
    storage = "lakecacprdststg01"
elif environment == "prd":
    storage = "lakecacprdstprd01"
else:
    raise Exception("Wrong evnvironment")

# file name list
if len(file_name) > 0: 
    file_names = [file_name]
else:
    # no need to load summary files since they are overwritten every day.
    file_names = ['participant_institution', 'payment', 'net_send_schedule', 'operating_schedule', 'operational_status', 'net_send_override', 'net_send_limit', 'liquidity_threshold', 'liquidity_position', 'gridlock', 'credit_position', 'credit_activity', 'loan', 'lom_status', 'minum_balance', 'pause_payments', 'transfer', 'validation']

# COMMAND ----------

for file_name in file_names:
    print(file_name)
    file_path = f"abfss://restricted@{storage}.dfs.core.windows.net/curated/paycan/lynx/{file_name}"
    file_table_name = f"{unity_catalog_name}.curated_db.lynx_{file_name}"

    # Read parquet file
    df_parquet = spark.read.parquet(file_path)

    # Make necessary changes
    if file_name == "participant_institution":
        # Cast decimal(22,2) to decimal(30,2)
        df_parquet = df_parquet.withColumn("MAX_CREDIT_AMOUNT", df_parquet["MAX_CREDIT_AMOUNT"].cast(DecimalType(30,2)))

    # Make necessary changes
    if 'VALIDATION_STATUS' in df_parquet.columns:
        # Cast the boolean column to integer, align with SQL server
        df_parquet = df_parquet.withColumn("validation_status", F.col("validation_status").cast("int"))    

    # no need to load summary tables
    #if file_name == "payment_summary":
    #    # Cast decimal(22,2) to decimal(30,2)
    #    df_parquet = df_parquet.withColumn("amount", df_parquet["amount"].cast(DecimalType(30, 2)))
    #    # Cast string to long, to align with SQL server
    #    df_parquet = df_parquet.withColumn("priority", df_parquet["priority"].cast("long"))

    #if file_name == "transfer_summary":
    #    # Cast decimal(22,2) to decimal(30,2)
    #    df_parquet = df_parquet.withColumn("amount", df_parquet["amount"].cast(DecimalType(30, 2)))

    #if file_name == "credit_activity_summary":
    #    # Cast decimal(22,2) to decimal(30,2
    #    df_parquet = df_parquet.withColumn("total_amount", df_parquet["total_amount"].cast(DecimalType(30, 2)))
    #    # Cast decimal(22,2) to decimal(30,2
    #    df_parquet = df_parquet.withColumn("earmarked_amount", df_parquet["earmarked_amount"].cast(DecimalType(30, 2)))

    # Write to table
    df_parquet.write.format("delta").mode("overwrite").saveAsTable(file_table_name)
