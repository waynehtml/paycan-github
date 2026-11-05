# Databricks notebook source
# MAGIC %pip install pandas

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %run ops/Utils/auth_util

# COMMAND ----------

## Variables Per Environment

## Dev
storage_name = dbutils.widgets.get('storage_name')
container_name = "restricted"
cur_path = "curated/paycan/lynx"


## Some Secrets, should be the same across envs?
PAYCAN_SECRET_SCOPE = "lake-kv-paycan-scope"
PAYCAN_SPN_ID = "spn-paycanservices-id"
PAYCAN_SPN_KEY = "spn-paycanservices-key"
LAKE_TENANT_ID = "lake-tenant-id"


# COMMAND ----------

def test_schema_valid():
    spn_access_enabled = tryEnableSpnAccess(PAYCAN_SECRET_SCOPE, storage_name, PAYCAN_SPN_ID, PAYCAN_SPN_KEY, LAKE_TENANT_ID)
    curated_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{cur_path}"
    credit_activity = spark.read.format("parquet").load(curated_path + "/credit_activity")
    
    schema = """
    BATCH_ID INTEGER,
    LYNX_CREDIT_ACTIVITY_ID LONG,
    CREDIT_ACTIVITY_ID LONG,
    CYCLE_DATE DATE,
    INSTITUTION_NUMBER LONG,
    INSTITUTION_IRN STRING,
    INSTITUTION_NAME STRING,
    INSTITUTION STRING,
    PARTICIPANT_NUMBER LONG,
    PARTICIPANT_BIC STRING,
    PARTICIPANT_NAME STRING,
    PARTICIPANT STRING,
    SYSTEM_REFERENCE_CD STRING,
    TOTAL_AMOUNT DECIMAL(30,2),
    EARMARKED_AMOUNT DECIMAL(30,2),
    EARMARKED_LOM STRING,
    LAST_MODIFIED_TS TIMESTAMP,
    STATUS_TYPE_CD STRING,
    STATUS_CHANGE_TIME TIMESTAMP,
    STATUS_REASON_CD STRING,
    STATUS_TYPE_CATEGORY STRING,
    VALIDATION_STATUS BOOLEAN
    """

    test_df = spark.createDataFrame(data = {}, schema=schema)
    test_df.schema

    assert credit_activity.schema == test_df.schema
    

# COMMAND ----------

test_schema_valid()

# COMMAND ----------


