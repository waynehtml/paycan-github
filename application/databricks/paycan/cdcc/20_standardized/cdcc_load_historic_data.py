# Databricks notebook source
# MAGIC %md
# MAGIC # Copy SQL Server DB data into UC tables
# MAGIC This notebook facilitates a one-time data transfer from existing SQL Server database tables to UC tables. The source data consists of CSV files exported from SQL Server tables, while the destination is UC tables.
# MAGIC
# MAGIC #####Source CSV File Naming Pattern
# MAGIC The CSV files adhere to the naming pattern: tablename.csv 
# MAGIC For example: cm.csv → cm is the table name.
# MAGIC
# MAGIC Due to SQL Server performance limitations, large tables positions and margins are split into multiple CSV files:
# MAGIC - Margins Table: Exported into seven files per year, named as margins_YYYY.csv (e.g., margins_2019.csv, margins_2020.csv).
# MAGIC - Positions Table: Exported into 85 files per month, spanning May 2019 to June 2025, named as positions_[YYYY][MM].csv (e.g., positions_202501.csv, positions_201906.csv).
# MAGIC
# MAGIC #####Destination UC Table Naming Pattern
# MAGIC The destination UC tables reside in:
# MAGIC - Catalog: oversight_cat
# MAGIC - Schema: standardized_db
# MAGIC
# MAGIC These tables follow the naming convention: tbl_tmx_cdcc_<tablename>.
# MAGIC For example, the full name of the positions table is: dev_oversight_cat.standardized_db.tbl_tmx_cdcc_positions
# MAGIC
# MAGIC #####raw files location and folders
# MAGIC The root folder of historic raw files is retricted/raw/external/tmx/cdcc/historic_data.
# MAGIC All incoming source CSV files will be in incoming sub-folder.
# MAGIC After this notebook, sucessfully loaded files will be moved to proceeded sub-folder, unsucessfully loaded files will be moved to rejected sub-folder  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Parameters Description
# MAGIC This notebook includes three parameters:
# MAGIC 1. #####environment
# MAGIC     - Determines the execution environment.
# MAGIC     - Possible values: dev, stg, prd.
# MAGIC
# MAGIC 2. #####raw_container
# MAGIC     - Determines raw zone container
# MAGIC
# MAGIC 3. #####raw_folder
# MAGIC     - Determines working raw zone directory

# COMMAND ----------

# DBTITLE 1,Load libraries
import io
import json
from pyspark.sql.types import *
import pandas as pd
import openpyxl as op
from openpyxl import load_workbook
from decimal import Decimal
import datetime

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("prm_raw_container", "restricted")
dbutils.widgets.text("prm_raw_directory", "raw/external/tmx/cdcc/historic_data/working")

env = dbutils.widgets.get("environment")
raw_container = dbutils.widgets.get("prm_raw_container")
raw_folder = dbutils.widgets.get("prm_raw_directory")

print(f"env: {env}")
print(f"raw_container: {raw_container}")
print(f"raw_folder: {raw_folder}")  


# COMMAND ----------

# DBTITLE 1,Load common utils
# MAGIC %run ../../common/utils

# COMMAND ----------

# DBTITLE 1,Set global variables
land_st_name = get_land_st_name(env)
raw_st_name = get_raw_st_name(env)
unity_catalog_name = get_unity_catalog_name(env)

table_name_prefix = "tbl_tmx_cdcc_" 
log_table_name = "tbl_loading_log_v2"

# COMMAND ----------

# DBTITLE 1,Load CSV file data
class working_file():
    def __init__(self, name, path):
        self.name = name
        self.path = path

## get working files
def get_working_files(tbl_name: str) -> ArrayType: 
    raw_path= "abfss://" + raw_container  + "@" + raw_st_name + ".dfs.core.windows.net/" + raw_folder 
    
    working_files = []
    # load ALL historic data files
    for f in dbutils.fs.ls(raw_path) :
        if f.isFile and (f.name.lower()).startswith(tbl_name) :
            working_files.append(working_file(f.name, f.path))    
    return working_files

## get table schema
def get_schema(table_name: str) -> StructType:
    spark.sql(f"USE {unity_catalog_name}.standardized_db")
    df = spark.table(table_name)
    tbl_schema = StructType.fromJson(json.loads(df.schema.json()))

    return tbl_schema

##get all table names
def get_table_names(table_name_prefix) -> ArrayType:
    spark.sql(f"USE {unity_catalog_name}.standardized_db")
    # Get list of tmx_cdcc tables in the current database
    tables = spark.sql("SHOW TABLES ").filter(f"LOWER(tableName)  LIKE '{table_name_prefix}%'").collect()
    table_names = [row.tableName for row in tables]
    return table_names

## move file to proceeded folder
def move_proceeded_file(source_path):
    dest_path = source_path.replace("/working/","/proceeded/")
    dbutils.fs.mv(source_path, dest_path) 

## append log 
def append_to_tbl_loading_log (load_start, load_end, result) :
    spark.sql(f"USE {unity_catalog_name}.standardized_db")
    schema = get_schema(log_table_name)
    notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    data = [(load_start, load_end , notebook_name, json.dumps(result))]
    df = spark.createDataFrame(data=data,schema=schema)
    df.write.mode("append").saveAsTable(log_table_name)

#Start loading
result = {}
result_details = [] 
load_start = datetime.datetime.now()

#load cdcc tables
try:
    # load tables
    table_names = get_table_names(table_name_prefix)
    for table_name in table_names : 
        tbl_name=table_name.removeprefix(table_name_prefix)
        for f in get_working_files(tbl_name) :
            t_schema = get_schema(table_name)
            df = spark.read.csv(f.path,  header=True,  schema=t_schema,   sep=",")
            df.write.mode("append").saveAsTable(table_name)
            num_rows = df.count()
            move_proceeded_file(f.path)
            new_entry = {"table": table_name, "file_name": f.name, "rows_processed": num_rows}
            result_details.append(new_entry) 

    # Define structured succeeded response
    result = {
        "status": "succeeded", 
        "details": result_details 
    }
  
except Exception as e:
    result = {
        "status": "failed",
        "details": str(e)
    }

result_str = json.dumps(result)
load_end = datetime.datetime.now()
# append log
append_to_tbl_loading_log (load_start, load_end, result_str)

print(result_str)
dbutils.notebook.exit(result_str)