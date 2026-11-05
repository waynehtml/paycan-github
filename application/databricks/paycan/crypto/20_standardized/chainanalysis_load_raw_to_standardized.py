# Databricks notebook source
# MAGIC %md
# MAGIC ## Load raw Kaiko trade data into UC by exchanges.
# MAGIC
# MAGIC ### Parameters:
# MAGIC - environment: environment, options: dev, stg or prd
# MAGIC - container: lake container, default: restricted
# MAGIC - path: raw folder of kaiko trade data, default: raw/external/chainanalysis/blockchain
# MAGIC - blockchain: blockchains to be loaded, default: empty, i.e. all blockchain in raw zone
# MAGIC > Omitted - all blockchain in the raw zone<br>
# MAGIC > AAVE,BNT - AAVE and BNT<br>
# MAGIC > AAVE\~BNT - all blockchain between AAVE and BNT<br>
# MAGIC > AAVE\~ - all blockchain after AAVE inclusively<br>
# MAGIC > \~BNT - all blockchain before BNT includsively<br>
# MAGIC - date - file date to be processed, eg. 2024-11,2024-12
# MAGIC - file - files to be process
# MAGIC > Omitted - all files in all blockchains<br>
# MAGIC > active-entities-monthly - single file<br>
# MAGIC > active-entities-monthly,age - comma seperated file names<br>
# MAGIC - mode - specify how data being written to delta, default: overwrite
# MAGIC > overwrite: existing data will be overwritten
# MAGIC > append: data will be appended to tables
# MAGIC
# MAGIC
# MAGIC ### How to Run:
# MAGIC Run this notebook with PL_Run_Notebook_UC ADF pipline with following parameters:
# MAGIC - notebook: notebook path, /paycan/crypto/20_standardized/chainanalysis_load_raw_to_standardized
# MAGIC - parameter: JSON string of parameters, e.g. 
# MAGIC {"environment":"dev|stg|prd"}

# COMMAND ----------

import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
import time
from datetime import timedelta
from datetime import datetime


# COMMAND ----------

# MAGIC %run ../../common/utils

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("environment", "")
dbutils.widgets.text("container","restricted")
dbutils.widgets.text("path","raw/external/chainanalysis/blockchain")
dbutils.widgets.text("date","")
dbutils.widgets.text("blockchain","") # for testing only
dbutils.widgets.text("file","")
dbutils.widgets.dropdown("mode","overwrite",["overwrite","append"])

environment = dbutils.widgets.get("environment")
print(f"environment: {environment}")
container = dbutils.widgets.get("container")
print(f"container: {container}")
path = dbutils.widgets.get("path")
print(f"path: {path}")
date = dbutils.widgets.get("date")
print(f"date: {date}")
blockchain = dbutils.widgets.get("blockchain")
print(f"blockchain: {blockchain}")
file = dbutils.widgets.get("file")
print(f"file: {file}")
mode = dbutils.widgets.get("mode")
print(f"mode: {mode}")

# COMMAND ----------

def get_subset(sorted_array, head=None, end=None):
    """
    Get subset of sorted array from head to end inclusively.
    If head is None, start from beginning.
    If end is None, end at last element.
    """
    # If array is empty, return empty array
    if not sorted_array:
        return []
        
    start_idx = 0
    end_idx = len(sorted_array)
    
    # Find start index if head provided
    if head is not None:
        for i, s in enumerate(sorted_array):
            if s == head:
                start_idx = i
                break
    
    # Find end index if end provided
    if end is not None:
        for i, s in enumerate(sorted_array):
            if s == end:
                end_idx = i
                break
    
    return sorted_array[start_idx:end_idx+1]

def extract_exchange_range(range):
    # Split the range string by the tilde (~)
    head, tail = range.split('~')
    
    # Replace empty strings with None for omitted parts
    head = head if head else None
    tail = tail if tail else None
    
    return head, tail

def get_subdirs(base_path):
    subdirs = [f.name.rstrip('/') for f in dbutils.fs.ls(base_path) if f.isDir()]
    return subdirs

def get_files(path, suffix):
    files = []
    for file_info in dbutils.fs.ls(path):
        if not file_info.isDir() and file_info.path.endswith(suffix):
            files.append(file_info.path)
    return files

def duration(start_time):
    # Calculate the duration
    duration_seconds = time.time() - start_time
    return timedelta(seconds=duration_seconds)

def split_and_strip(str, separator=','):
    return [item.strip() for item in str.split(separator)]

def extract_filename(abfss_path):
    """
    Extract the base filename without extension using string methods.
    
    Args:
        abfss_path: The ABFSS path string
        
    Returns:
        str: The base filename without extension
    """
    # Method 2: Using string methods
    # Split the path by '/' and get the last part
    filename_with_ext = abfss_path.split('/')[-1]
    # Split by '.' and get the filename without extension
    filename = filename_with_ext.split('.')[0]
    
    return filename.lower()

def remove_duplicates(string_array):
    """
    Remove duplicates using dict.fromkeys() method.
    In Python 3.7+, dictionaries preserve insertion order.
    """
    return list(dict.fromkeys(string_array))

# COMMAND ----------

# environment
if len(environment) == 0:
    environment = get_env()
print(f"environment: {environment}")

# catalog name
unity_catalog_name = get_crypto_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")

# lake storage account
storage = get_lake_storage_account(environment)

# land storage account
land_storage = get_land_st_name(environment)

# date folder
base_path = f"abfss://{container}@{storage}.dfs.core.windows.net/{path}/{date}"
print(f"base_path: {base_path}")

# COMMAND ----------

# DBTITLE 1,Get blockchains
# blockchains
if len(blockchain) == 0:
    # all exchanges
    blockchains = get_subdirs(base_path)
elif "," in blockchain:
    # comma separated list of exchanges
    blockchains = split_and_strip(blockchain)
elif "~" in blockchain:
    # range of exchanges
    (head, end) = extract_exchange_range(blockchain)
    blockchains = get_subdirs(base_path)
    blockchains = get_subset(blockchains,head,end)
else:
    # single exchange
    blockchains = [blockchain]

print(f"blockchains: {blockchains}")

# COMMAND ----------

# DBTITLE 1,Get files
# files
files = []
if len(file) == 0:
    for blockchain in blockchains:
        file_paths = get_files(f"{base_path}/{blockchain}","csv")
        for file_path in file_paths:
            files.append(extract_filename(file_path))
    files = remove_duplicates(files)
elif "," in file:
    files = split_and_strip(file)
else:
    files = [file]
files.sort()
print(f"files: {files}")

# COMMAND ----------

def load_files(date, target_table, source_files, mode):
    start_time = time.time()
    
    print(f"date: {date}")
    print(f"target_table: {target_table}")
    print(f"source_files: {source_files}")
    print(f"mode: {mode}")

    # Read all the files from all specified blockchains
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("ignoreCorruptFiles", "true") \
        .option("inferSchema", "true") \
        .load(source_files)

    # Add extra columns
    current_timestamp = datetime.fromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%S.000')
    df = df \
        .withColumn("file_date", regexp_extract(input_file_name(), ".*/([^/]+)/[^/]+/[^/]+$", 1)) \
        .withColumn("blockchain", regexp_extract(input_file_name(), ".*/[^/]+/([^/]+)/[^/]+$", 1)) \
        .withColumn("audit_timestamp", lit(current_timestamp).cast("timestamp")) \
        .withColumn("abs_file_path", input_file_name()) \
        .withColumn("rel_file_path", regexp_extract(input_file_name(), "([^/]+/[^/]+/[^/]+$)", 1))

    # if appending, delete the records for the date
    if mode == "append":
        print(f"Deleting file_date: {date}")
        delete_sql = f"DELETE FROM {target_table} WHERE file_date = '{date}'"
        spark.sql(delete_sql)

    # Write to table
    df.write \
        .format("delta") \
        .mode(mode) \
        .option("mergeSchema", "true") \
        .saveAsTable(target_table)
    
    print(f"Completed in {duration(start_time)}")

# COMMAND ----------

    
for file in files:
    print(f"Processing: {file}")

    # table name
    target_table = f"{unity_catalog_name}.standardized_db.tbl_chainanalysis_blockchain_{file.replace('-','_')}"

    # load the file into UC
    blockchain_folders = ",".join(blockchains)
    source_files = f"{base_path}/{{{blockchain_folders}}}/{file}.csv"
    load_files(date, target_table, source_files, mode)
