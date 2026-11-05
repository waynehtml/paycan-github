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
    payment = spark.read.format("parquet").load(curated_path + "/payment")
    
    schema = """
    BATCH_ID INTEGER,
    LYNX_PAYMENT_ID LONG,
    PAYMENT_ID LONG,
    SYSTEM_REFERENCE STRING,
    CYCLE_DATE DATE,
    PAYER_INSTITUTION_NUMBER LONG,
    PAYER_INSTITUTION_IRN STRING,
    PAYER_INSTITUTION_NAME STRING,
    PAYER_INSTITUTION STRING,
    PAYER_PARTICIPANT_NUMBER LONG,
    PAYER_PARTICIPANT_BIC STRING,
    PAYER_PARTICIPANT_NAME STRING,
    PAYER_PARTICIPANT STRING,
    PAYER_REFERENCE STRING,
    SETTLEMENT_MECHANISM_CD STRING,
    RECEIVER_INSTITUTION_NUMBER LONG,
    RECEIVER_INSTITUTION_IRN STRING,
    RECEIVER_INSTITUTION_NAME STRING,
    RECEIVER_INSTITUTION STRING,
    RECEIVER_PARTICIPANT_NUMBER LONG,
    RECEIVER_PARTICIPANT_BIC STRING,
    RECEIVER_PARTICIPANT_NAME STRING,
    RECEIVER_PARTICIPANT STRING,
    SWIFT_MSG_TYPE_CD STRING,
    TRANSACTION_TYPE STRING,
    PCRN STRING,
    AMOUNT DECIMAL(30, 2),
    GRIDLOCK_SETTLED_ID LONG,
    PRIORITY LONG,
    LAST_MODIFIED_TS TIMESTAMP,
    STATUS_TYPE_CD STRING,
    STATUS_CHANGE_TIME_TS TIMESTAMP,
    STATUS_REASON STRING,
    STATUS_MILLISECOND_DURATION LONG,
    VALIDATION_STATUS BOOLEAN
    """

    test_df = spark.createDataFrame(data = {}, schema=schema)
    test_df.schema
    
    print("Diff Types: ")
    print(set(test_df.dtypes) ^ set(payment.dtypes))

    assert payment.schema == test_df.schema
    

# COMMAND ----------

test_schema_valid()

# COMMAND ----------


