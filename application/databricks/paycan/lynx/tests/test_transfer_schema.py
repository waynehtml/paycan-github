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
    transfer = spark.read.format("parquet").load(curated_path + "/transfer")
    
    schema = """
    BATCH_ID INTEGER,
    LYNX_TRANSFER_ID BIGINT,
    TRANSFER_ID BIGINT,
    SYSTEM_REFERENCE STRING,
    PAYER_REFERENCE STRING,
    CYCLE_DATE DATE,
    INSTITUTION_NUMBER BIGINT,
    INSTITUTION_IRN STRING,
    INSTITUTION_NAME STRING,
    INSTITUTION STRING,
    PARTICIPANT_NUMBER BIGINT,
    PARTICIPANT_BIC STRING,
    PARTICIPANT_NAME STRING,
    PARTICIPANT STRING,
    FROM_ACCOUNT_ID BIGINT,
    FROM_ACCOUNT_BBAN STRING,
    FROM_ACCOUNT_NAME STRING,
    FROM_SETTLE_MECH_CD STRING,
    TO_ACCOUNT_ID BIGINT,
    TO_ACCOUNT_BBAN STRING,
    TO_ACCOUNT_NAME STRING,
    TO_SETTLE_MECH_CD STRING,
    TRANSACTION_TYPE STRING,
    AMOUNT DECIMAL(30, 2),
    LAST_MODIFIED_TS TIMESTAMP,
    STATUS_TYPE_CD STRING,
    STATUS_CHANGE_TS TIMESTAMP,
    STATUS_REASON STRING,
    STATUS_MILLISECOND_DURATION BIGINT,
    VALIDATION_STATUS BOOLEAN
    """

    test_df = spark.createDataFrame(data = {}, schema=schema)
    test_df.schema
    
    print("Diff Types: ")
    print(set(test_df.dtypes) ^ set(transfer.dtypes))

    assert transfer.schema == test_df.schema
    



# COMMAND ----------

test_schema_valid()

# COMMAND ----------


