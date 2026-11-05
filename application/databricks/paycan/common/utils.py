# Databricks notebook source
from pyspark.sql.functions import col, DataFrame, to_date, to_timestamp, substring_index, trim, when, substring, lit, current_timestamp
from pyspark.sql.types import IntegerType, DecimalType
from datetime import date
from delta.tables import *

# COMMAND ----------

def get_unity_catalog_name(environment:str):
    catalog_name = environment + '_oversight_cat'
    return catalog_name

# COMMAND ----------

def get_crypto_unity_catalog_name(environment:str):
    catalog_name = environment + '_crypto_cat'
    return catalog_name

# COMMAND ----------

def get_land_st_name(environment:str):
    if (environment == "dev") :
        land_st_name = "lakecacnpdstdevland01"
    if (environment == "stg") :
        land_st_name = "lakecacprdststgland01"
    if (environment == "prd") : 
        land_st_name = "lakecacprdstprdland01"

    return land_st_name

# COMMAND ----------

def get_raw_st_name(environment:str):
    if (environment == "dev") :
        raw_st_name = "lakecacnpdstdev01"
    if (environment == "stg") :
        raw_st_name = "lakecacprdststg01"
    if (environment == "prd") : 
        raw_st_name = "lakecacprdstprd01"

    return raw_st_name

# COMMAND ----------

def get_ad_suffix(environment:str):
    if (environment == "dev") :
        ad_suffix = "inda"
    else: 
        ad_suffix = "inpa"

    return ad_suffix

# COMMAND ----------

def get_lake_storage_account(environment):
    storage_map = {
        "dev": "lakecacnpdstdev01",
        "stg": "lakecacprdststg01",
        "prd": "lakecacprdstprd01"
    }
    return storage_map.get(environment, "lakecacnpdstdev01")

# COMMAND ----------

def get_env():
    # Get the resource group name, sample: lake-cac-npd-rg-manageddbwdevuc01
    rg = spark.sparkContext.getConf().get("spark.databricks.clusterUsageTags.managedResourceGroup")

    # Split by '-' and get the real name
    rg_name = rg.split("-")[-1]

    # Define the keywords to search for
    keywords = ["dev", "stg", "prd"]

    # Check for any keyword in the last part
    found_keyword = next((keyword for keyword in keywords if keyword in rg_name), None)

    return found_keyword
