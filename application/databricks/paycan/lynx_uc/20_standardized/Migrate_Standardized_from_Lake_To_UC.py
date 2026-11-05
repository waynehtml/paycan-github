# Databricks notebook source
from pyspark.sql.types import DecimalType

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
    file_names = ['participant_institution', 'payment', 'net_send_schedule', 'operating_schedule', 'operational_status', 'net_send_override', 'net_send_limit', 'liquidity_threshold', 'liquidity_position', 'gridlock', 'credit_position', 'credit_activity', 'loan', 'lom_status', 'minum_balance', 'pause_payments', 'transfer']

# COMMAND ----------

for file_name in file_names:
    print(file_name)
    file_path = f"abfss://restricted@{storage}.dfs.core.windows.net/standardized/external/payments_canada/lynx/{file_name}"
    file_table_name = f"{unity_catalog_name}.standardized_db.tbl_paycan_lynx_{file_name}"

    # Read parquet files
    if file_name == "net_send_override":
        schema = spark.table(file_table_name).schema
        df_parquet = spark.read \
            .option("inferSchema", "false") \
            .option("mode", "FAILFAST") \
            .schema(schema) \
            .parquet(file_path)
    else:
        df_parquet = spark.read.parquet(file_path)

    # Make necessary changes
    if file_name == "participant_institution":
        df_parquet = df_parquet.withColumn("MAX_CREDIT_AMOUNT", df_parquet["MAX_CREDIT_AMOUNT"].cast(DecimalType(30,2)))  
    
    # Write to table
    df_parquet.write.format("delta").mode("overwrite").partitionBy("BATCH_ID").saveAsTable(file_table_name)
