# Databricks notebook source
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import pandas as pd
import datetime

# COMMAND ----------

#%run /ops/Utils/auth_util

# COMMAND ----------

#all the components of the file path:

dbutils.widgets.text("storage_name","", "storage_name")
dbutils.widgets.text("container","", "container")
dbutils.widgets.text("file_path","", "file_path")
dbutils.widgets.text("file","", "file")
dbutils.widgets.text("cycle_date","", "cycle_date")

file = dbutils.widgets.get("file") 
container = dbutils.widgets.get("container") 
file_path = dbutils.widgets.get("file_path")
storage_name = dbutils.widgets.get("storage_name")
cycle_date = dbutils.widgets.get("cycle_date")

# COMMAND ----------

def main(container, storage_name, file_path, cycle_date):

  raw_file_path = "abfss://" + container+ "@" + storage_name + ".dfs.core.windows.net/" + file_path + "/" + cycle_date
   
  all_files = dbutils.fs.ls(raw_file_path)
  
  for i in all_files:
    if file in i.name: 
      
      data_path = i.path
      print(data_path)
  
  df = spark.read.format("csv").option("header", "true").load(data_path)
  
  if len(all_files) == 2:
    #File validation goes here
    print("Data passed File Validation")
   
  else:
    raise Exception("Data failed File Validation")
    #can we read then write immediately? do we have to pick a new path (name)

# COMMAND ----------

main(container, storage_name, file_path, cycle_date)
