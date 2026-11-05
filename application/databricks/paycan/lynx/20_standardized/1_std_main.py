# Databricks notebook source
# Author : rhaa
# Version : 3.2.0

# COMMAND ----------

import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Run Auth Notebook
#%run /ops/Utils/auth_util

# COMMAND ----------

# DBTITLE 1,Run Child Notebook
# MAGIC %run ./2_std_data_validation

# COMMAND ----------

dbutils.widgets.text("cycle_date","", "Cycle_date")
dbutils.widgets.text("file","", "file")
dbutils.widgets.text("config","", "config")
dbutils.widgets.text("storage_name","", "storage_name")
dbutils.widgets.text("container_name","", "container_name")
dbutils.widgets.text("raw_path","", "raw_path")
dbutils.widgets.text("stan_path","", "stan_path")


storage_name = dbutils.widgets.get("storage_name")
container_name = dbutils.widgets.get("container_name")
cycle_date = dbutils.widgets.get("cycle_date")
config_parameters = dbutils.widgets.get("config")
file = dbutils.widgets.get("file")
config = json.loads(config_parameters)
raw_path = dbutils.widgets.get("raw_path")
stan_path = dbutils.widgets.get("stan_path")

# COMMAND ----------

# DBTITLE 1,Adjust Default shuffle partitions
# set default partitions to match number of cores
spark.conf.set("spark.sql.shuffle.partitions",5)
spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# DBTITLE 1,Set partition overwrite mode
# set partition overwrite mode to dynamic to overwrite records within partition, for data reprocessing
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.get("spark.sql.sources.partitionOverwriteMode")

# COMMAND ----------

PAYCAN_SECRET_SCOPE = "lake-kv-paycan-scope"           
PAYCAN_SPN_ID = "spn-paycanservices-id"                       
PAYCAN_SPN_KEY = "spn-paycanservices-key"                  
LAKE_TENANT_ID = "lake-tenant-id"                                   

# COMMAND ----------

# DBTITLE 1,Enable SPN Access
#tryEnableSpnAccess(PAYCAN_SECRET_SCOPE, storage_name, PAYCAN_SPN_ID, PAYCAN_SPN_KEY,LAKE_TENANT_ID)

# COMMAND ----------

def main (storage_name, container_name, cycle_date, file, config, raw_path, stan_path):
  """
    Main function that will read from raw, perform validation, clean up, and write to standardized
    
    Parameters
    ----------
    storage_name: str
    container_name: str
    cycle_date: str
    file: str
    config_file: dict
    
    Returns
    ----------
    bool
  """
  
  
  raw_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{raw_path}/{cycle_date}"
  standardized_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{stan_path}"

  # load data to dataframe
  full_path = raw_path + "/" + file
  cycle_date_non_hyphenated = cycle_date.replace("-", "")
  
  dataframe = spark.read.option("header", True).csv(full_path)
  dataframe_size = dataframe.count()
  file = file.replace(f"_{cycle_date}.csv","")

  if(dataframe_size == 0):
    final = standardized_main_empty_file(dataframe, file, cycle_date, config)
    final.write.format("parquet").mode("overwrite").save(standardized_path + "/" + file + "/" + "BATCH_ID=" + cycle_date_non_hyphenated)
  
  else:
    final = standardized_main(dataframe, file, cycle_date, config)
    final.write.format("parquet").mode("overwrite").partitionBy("BATCH_ID").save(standardized_path + "/" + file)


  return True

# COMMAND ----------

main(storage_name, container_name, cycle_date, file, config,  raw_path, stan_path)
