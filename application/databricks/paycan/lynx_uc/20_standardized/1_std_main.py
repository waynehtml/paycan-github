# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../../common/utils

# COMMAND ----------

# DBTITLE 1,Load validation notebook
# MAGIC %run ./2_std_data_validation

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("environment","dev", "environment")
dbutils.widgets.text("cycle_date","", "Cycle_date")
dbutils.widgets.text("file","", "file")
dbutils.widgets.text("config","", "config")
dbutils.widgets.text("storage_name","lakecacnpdstdev01", "storage_name")
dbutils.widgets.text("container_name","restricted", "container_name")
dbutils.widgets.text("raw_path","raw/external/payments_canada/lynx", "raw_path")

# COMMAND ----------

environment = dbutils.widgets.get("environment")
cycle_date = dbutils.widgets.get("cycle_date")
file = dbutils.widgets.get("file")
config_parameters = dbutils.widgets.get("config")
storage_name = dbutils.widgets.get("storage_name")
container_name = dbutils.widgets.get("container_name")
raw_path = dbutils.widgets.get("raw_path")

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))
config = json.loads(config_parameters)

# COMMAND ----------

# read raw file
file_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{raw_path}/{cycle_date}/{file}"
dataframe = (
    spark.read.option("header", True).option("encoding", "ISO-8859-1").csv(file_path)
)

# Process the file if not empty
if not dataframe.isEmpty():

    # Standardize the data - validate date type, change column names, add batch_id and sequence ID, and capitalize the column name
    file_name = file.replace(f"_{cycle_date}.csv", "")
    df = standardized_main(dataframe, file_name, cycle_date, config)

    # Special cast
    if file_name == "participant_institution":
        df = df.withColumn("max_credit_amount", col("max_credit_amount").cast(DecimalType(30,2)))

    # Merge data into standardized table
    table_name = f"{unity_catalog_name}.standardized_db.tbl_paycan_lynx_{file_name}"
    id_name = f"lynx_{file_name}_id"
    batch_id = cycle_date.replace("-", "")

    # remove all data with same batch_id
    delta_table = DeltaTable.forName(spark, f"{table_name}")
    delta_table.delete(f"batch_id = {batch_id}")

    # merge
    #delta_table.alias("target").merge(
    #    source=df.alias("source"),
    #    condition=f"target.batch_id = source.batch_id and target.{id_name} = source.{id_name}",
    #).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    df = df.withColumn("BATCH_ID", col("BATCH_ID").cast("int"))
    df.write.format("delta").mode("append").saveAsTable(table_name)
