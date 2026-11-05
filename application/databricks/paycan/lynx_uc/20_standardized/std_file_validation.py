# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import pandas as pd

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("environment","dev", "environment")
dbutils.widgets.text("cycle_date","", "cycle_date")
dbutils.widgets.text("holiday_config","", "holiday_config")
dbutils.widgets.text("storage_name_restricted","", "storage_name_restricted")
dbutils.widgets.text("file_container","", "file_container")
dbutils.widgets.text("file_path","", "file_path")

# COMMAND ----------

cycle_date = dbutils.widgets.get("environment")
cycle_date = dbutils.widgets.get("cycle_date")
config_parameters = dbutils.widgets.get("holiday_config")
storage_name_restricted = dbutils.widgets.get("storage_name_restricted")
file_container = dbutils.widgets.get("file_container")
file_path = dbutils.widgets.get("file_path")
holiday_config = json.loads(config_parameters)

# COMMAND ----------

def main (storage_name, container_name, cycle_date, holiday_config, file_path ):

  # Read all files that start with a specific prefix
  # check if the cycle_date is holiday
  try:
    holiday_config[cycle_date]
    holiday = True
  except:
    holiday = False
  print(f"{cycle_date} is holiday? {holiday}")


  # File path in raw zone
  raw_path = "abfss://" +container_name+ "@" +storage_name+ ".dfs.core.windows.net/"+file_path+"/"+cycle_date
  print(f"file path:{raw_path}")

  # Get all files
  all_files = dbutils.fs.ls(f"{raw_path}/")
  total_size = 0
  for file in all_files:
    print(f"Validating file: {file.name}")
    dataframe = spark.read.option("header", True).csv(raw_path + "/" + file.name)
    total_size += dataframe.count()
    if(not dataframe.rdd.isEmpty()):
      name = file.name
      name = name.replace(f"_{cycle_date}.csv","")
    else:
      print(f"{file.name} is empty")

  # if holiday, no file should be expected
  if holiday and total_size > 0:
    raise Exception(f"Date {cycle_date} is a holiday but receive non-empty files.")
  # if holiay, files should be expected
  elif not holiday and total_size == 0:
    raise Exception(f"Date {cycle_date} is not a holiday but all files are empty.")

# COMMAND ----------

main(storage_name_restricted, file_container, cycle_date,holiday_config, file_path)
