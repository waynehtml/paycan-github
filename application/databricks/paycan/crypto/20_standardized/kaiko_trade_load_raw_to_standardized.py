# Databricks notebook source
# MAGIC %md
# MAGIC ## Load raw Kaiko trade data into UC by exchanges.
# MAGIC
# MAGIC ### Parameters:
# MAGIC - **environment**: environment, options: dev, stg or prd
# MAGIC - container: lake container, default: restricted
# MAGIC - path: raw folder of kaiko trade data, default: raw/external/kaiko/Trade
# MAGIC - exchanges: exchanges to be loaded, default: empty, i.e. all exchange in raw zone
# MAGIC > Omitted - all exchanges in the raw zone<br>
# MAGIC > ACX,Binance - ACX and Binance<br>
# MAGIC > ACX\~Binance - all exchanges between ACX and Binance<br>
# MAGIC > ACX\~ - all exchanges after ACX inclusively<br>
# MAGIC > \~Binance - all exchanges before Binance includsively<br>
# MAGIC - date - date list to be processed, eg. 2024-11,2024-12
# MAGIC - force - a "copy into" option to control data reloading, default: false
# MAGIC > true: reload files that have been loaded
# MAGIC > false: skip files that have been loaded
# MAGIC
# MAGIC
# MAGIC ### How to Run:
# MAGIC Run this notebook with PL_Run_Notebook_UC ADF pipline with following parameters:
# MAGIC - notebook: notebook path, /paycan/crypto/20_standardized/kaiko_trade_load_raw_to_standardized
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

def duration(start_time):
    # Calculate the duration
    duration_seconds = time.time() - start_time
    return timedelta(seconds=duration_seconds)

def split_and_strip(str, separator=','):
    return [item.strip() for item in str.split(separator)]

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")
dbutils.widgets.text("container","data")
dbutils.widgets.text("path","raw/external/kaiko/Trade")
dbutils.widgets.text("exchanges","", "exchanges")
dbutils.widgets.text("date","")
dbutils.widgets.text("force","false")

environment = dbutils.widgets.get("environment")
print(f"environment: {environment}")
container = dbutils.widgets.get("container")
print(f"container: {container}")
path = dbutils.widgets.get("path")
print(f"path: {path}")
exchanges = dbutils.widgets.get("exchanges")
print(f"exchanges: {exchanges}")
date = dbutils.widgets.get("date")
print(f"date: {date}")
force = dbutils.widgets.get("force")
print(f"force: {force}")

# COMMAND ----------

if len(environment) == 0:
    environment = get_env()
print(f"environment: {environment}")

unity_catalog_name = get_crypto_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")

storage = get_lake_storage_account(environment)
base_path = f"abfss://{container}@{storage}.dfs.core.windows.net/{path}"
print(f"base_path: {base_path}")

land_storage = get_land_st_name(environment)
dataset = path.split("/")[-1]
tiemstamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
bad_path = f"abfss://land-paycan@{land_storage}.dfs.core.windows.net/kaiko-bad/{dataset}/{tiemstamp}"
print(f"bad_path: {bad_path}")

# COMMAND ----------

# DBTITLE 1,Get exchanges to load
if len(exchanges) == 0:
    # all exchanges
    exchange_list = get_subdirs(base_path)
elif "," in exchanges:
    # comma separated list of exchanges
    exchange_list = split_and_strip(exchanges)
elif "~" in exchanges:
    # range of exchanges
    (head, end) = extract_exchange_range(exchanges)
    exchange_list = get_subdirs(base_path)
    exchange_list = get_subset(exchange_list,head,end)
else:
    # single exchange
    exchange_list = [exchanges]

print(f"exchange_list: {exchange_list}")

# COMMAND ----------

# DBTITLE 1,Populate date list if provided
date_list_arg = []
if len(date):
    if "," in date:
        date_list_arg = split_and_strip(date)
    else:
        date_list_arg = [date]
print(f"date_list_arg: {date_list_arg}")

# COMMAND ----------

# DBTITLE 1,Compose copy options
# format options
format_options = {}
format_options["header"] = "true"
format_options["compression"] = "gzip"
format_options["ignoreCorruptFiles"] = "true"
format_options["badRecordsPath"] = bad_path
format_option_str = ', '.join([f"'{key}' = '{value}'" for key, value in format_options.items()])
print(f"format_option_str: {format_option_str}")

# copy options
copy_options = {}
copy_options["force"] = force
copy_options_str = ', '.join([f"'{key}' = '{value}'" for key, value in copy_options.items()])
print(f"copy_options_str: {copy_options_str}")

# COMMAND ----------

target_table = f"{unity_catalog_name}.standardized_db.tbl_kaiko_trade"

for exchange in exchange_list:
    print(f"\nProcessing exchange: {exchange}")
    start_time = time.time()
    current_timestamp = datetime.fromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%S.000')
    print(f"Timestamp: {current_timestamp}")

    # Define the source path
    exchange_path = f"{base_path}/{exchange}"

    # Get the list of dates
    if len(date_list_arg) > 0:
        # Use the dates in the parameter
        date_list = date_list_arg
        print(f"Using dates in parameter: {date_list}")
    else:
        # List all date folders in the exchange path
        date_list = get_subdirs(exchange_path)
        print(f"Using dates in exchange folder: {date_list}")
    
    # Process each date
    for date in date_list:
        # Remove existing exchange/date if force is enabled
        if force.strip().lower() == 'true':
            exchange_date = f"{exchange}/{date}"
            delete_sql = f"DELETE FROM {target_table} WHERE CONTAINS(input_file_name, '{exchange_date}')"
            print(f"Deleting existing data: {delete_sql}")
            spark.sql(delete_sql)
            print(f" in {duration(start_time)}")
        
        # Use the COPY INTO command to load data into the Delta table
        print(f"Processing date: {date}", end='')
        source_path = f"{exchange_path}/{date}/*.csv.gz"
        sql = f"""
            COPY INTO {target_table}
            FROM (SELECT 
                        CAST(id AS STRING) AS id,
                        exchange,
                        symbol,
                        to_timestamp(from_unixtime(date/1000)) AS date,
                        CAST(price AS DECIMAL(20, 8)) AS price,
                        CAST(amount AS DECIMAL(20, 8)) AS amount,
                        CAST(sell AS BOOLEAN) AS sell,
                        TIMESTAMP('{current_timestamp}') AS audit_timestamp,
                        input_file_name() AS input_file_name
                        FROM '{source_path}')
            FILEFORMAT = CSV
            FORMAT_OPTIONS({format_option_str})
            COPY_OPTIONS({copy_options_str})
        """
        spark.sql(sql)
        print(f" in {duration(start_time)}")

    print(f"Processing exchange: {exchange} in {duration(start_time)}")
