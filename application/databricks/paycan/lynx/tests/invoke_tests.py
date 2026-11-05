# Databricks notebook source
# MAGIC %run ../utils/data_lake_access

# COMMAND ----------

# MAGIC %run /ops/Utils/auth_util

# COMMAND ----------

# MAGIC %run paycan/lynx/20_standardized/2_std_data_validation

# COMMAND ----------

# MAGIC %run paycan/lynx/30_curated/2_curate_business_validation

# COMMAND ----------

import json

config_file = dbutils.widgets.get("config")
storage_name = dbutils.widgets.get("storage")
container_name = dbutils.widgets.get("container")
file_date = dbutils.widgets.get("file_date")
file_name = dbutils.widgets.get("file_name")

batch_id = regexp_replace(lit(file_date),"-","")
config = json.loads(config_file)

# COMMAND ----------

tryEnableSpnAccess("lake-kv-paycan-scope", storage_name, "spn-paycanservices-id", "spn-paycanservices-key", "lake-tenant-id")

# COMMAND ----------

from pyspark.sql.functions import lit, regexp_replace, col
from pyspark.sql import DataFrame

# test data is in config-paycan container

raw_path = retrieve_path("raw",file_date,container_name,storage_name)
standardized_path = retrieve_path("standardized",file_date,container_name,storage_name)
curated_path = retrieve_path("curated",file_date,container_name,storage_name)
  
# load source data
rawDF = spark.read.format('csv').option("header", True).load(raw_path + "/" + file_name + "_" + file_date + ".csv")
standardizedDF = spark.read.format('parquet').load(standardized_path + "/" + file_name)
curatedDF = spark.read.format('parquet').load(curated_path + "/" + file_name)

# COMMAND ----------

# add necessary filterings to source data, if batch id is present, use it. Otherwise, use cycle date.
if("batch_id".upper() in (name.upper() for name in standardizedDF.columns)):
  qualified_rawDF = rawDF
  qualified_standardizedDF = standardizedDF.filter(col("batch_id")==batch_id).distinct()
  qualified_curatedDF = curatedDF.filter(col("batch_id")==batch_id)
else:
  qualified_rawDF = rawDF
  qualified_standardizedDF = standardizedDF.filter(col("cycle_date")==file_date).distinct()
  qualified_curatedDF = curatedDF.filter(col("cycle_date")==file_date)
  

cbv = Curate_Business_Validation()
var_uc = cbv.unique_column

# COMMAND ----------

# MAGIC %run ./includes/integration_tests

# COMMAND ----------

import sys, traceback

testResults = {}
Status = "Succeeded"
tests = ["test_"+file_name, "count_check", "null_check"]

for test in tests:
  try:
    test_to_call = getattr(Integration_Test, test)
    result = test_to_call()
    testResults[test] = {"Status":"Succeeded","Error":"None"}
  except:
    trace = traceback.format_exc()
    testResults[test] = {"Status":"Failed","Error":trace}
    Status = "Failed"

# COMMAND ----------

import json
strTestResults = json.dumps(testResults)

dbutils.notebook.exit(Status + strTestResults)

# COMMAND ----------

