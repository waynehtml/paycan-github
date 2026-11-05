# Databricks notebook source
# Author : rhaa
# Version : 1.2.0

# COMMAND ----------

import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import pandas as pd

# COMMAND ----------

dbutils.widgets.text("cycle_date","", "cycle_date")
dbutils.widgets.text("holiday_config","", "holiday_config")
dbutils.widgets.text("storage_name_config","", "storage_name_config")
dbutils.widgets.text("storage_name_restricted","", "storage_name_restricted")
dbutils.widgets.text("file_container","", "file_container")
dbutils.widgets.text("config_container","", "config_container")
dbutils.widgets.text("config_path","", "config_path")
dbutils.widgets.text("file_path","", "file_path")



storage_name_config = dbutils.widgets.get("storage_name_config")
storage_name_restricted = dbutils.widgets.get("storage_name_restricted")
file_container = dbutils.widgets.get("file_container")
config_container = dbutils.widgets.get("config_container")
config_path = dbutils.widgets.get("config_path")
cycle_date = dbutils.widgets.get("cycle_date")
config_parameters = dbutils.widgets.get("holiday_config")
file_path = dbutils.widgets.get("file_path")
holiday_config = json.loads(config_parameters)

# COMMAND ----------

# DBTITLE 1,Run Auth Notebook
#%run /ops/Utils/auth_util

# COMMAND ----------

# DBTITLE 1,Adjust Default shuffle partitions
# set default partitions to match number of cores
spark.conf.set("spark.sql.shuffle.partitions",5)
spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

PAYCAN_SECRET_SCOPE = "lake-kv-paycan-scope"           
PAYCAN_SPN_ID = "spn-paycanservices-id"                       
PAYCAN_SPN_KEY = "spn-paycanservices-key"                  
LAKE_TENANT_ID = "lake-tenant-id"                                   

# COMMAND ----------

# DBTITLE 1,Enable SPN Access
#tryEnableSpnAccess(PAYCAN_SECRET_SCOPE, storage_name_restricted, PAYCAN_SPN_ID, PAYCAN_SPN_KEY,LAKE_TENANT_ID)
#tryEnableSpnAccess(PAYCAN_SECRET_SCOPE, storage_name_config, PAYCAN_SPN_ID, PAYCAN_SPN_KEY,LAKE_TENANT_ID)

# COMMAND ----------

# DBTITLE 1,Set partition overwrite mode
# set partition overwrite mode to dynamic to overwrite records within partition, for data reprocessing
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.get("spark.sql.sources.partitionOverwriteMode")

# COMMAND ----------

def main (storage_name, container_name, cycle_date, holiday_config,storage_name_config,config_container,config_path, file_path ):
  
 
  
  raw_path = "abfss://" +container_name+ "@" +storage_name+ ".dfs.core.windows.net/"+file_path+"/"+cycle_date

    
  print(raw_path)
  all_files = []

  all_files = dbutils.fs.ls(f"{raw_path}/")
  
  #

# Read all files that start with a specific prefix
    ##### validate file_date if it is a holiday, if yes, abort, message will depend on total file size retrieved - Rahim (2021-06-02)
  holiday = False
  
  try:
    holiday_config[cycle_date]
    holiday = True
  except:
    pass
  
  empty = {}
  ###NOTE: dbutils.notebook.exit is how you pass variables backto ADF, if ADF gets False that means its an error if it gets True it means to move forward

  #Optimized code to run in O(N) instead of O(N^3)
  total_size = 0
  for file in all_files:


    dataframe = spark.read.option("header", True).csv(raw_path + "/" + file.name)
    #dataframe = read_data("csv", raw_path + "/" + file.name)
    total_size += dataframe.count()
    if(not dataframe.rdd.isEmpty()):
      name =file.name
      name = name.replace(f"_{cycle_date}.csv","")
      empty[name]=""

  if  holiday and total_size > 0:
    #Dont need to cast to string, python f string will do it automatically for you
    print(f"Date {cycle_date} is a holiday. Files total size is {total_size} rows in all files cumulative.")
    raise Exception 
    
  elif not holiday and total_size ==0:
    print("All files are empty and it is not holiday.")
    raise Exception 
    

      

  output_path = f"abfss://{config_container}@{storage_name_config}.dfs.core.windows.net/lynx_processed_files/{cycle_date}"
  df = pd.DataFrame(empty, index=[0])
  ddf = spark.createDataFrame(df)

 
      
  display(ddf)
  #ddf.write.mode('overwrite').format("json").save(output_path)

    
  



# COMMAND ----------

main(storage_name_restricted, file_container, cycle_date,holiday_config, storage_name_config, config_container, config_path, file_path)

# COMMAND ----------

# output_path = f"abfss://{config_container}@{storage_name_config}.dfs.core.windows.net/{config_path}/file_list/{cycle_date}"
# test = spark.read.format("json").load(output_path)
# display(test)

# COMMAND ----------


