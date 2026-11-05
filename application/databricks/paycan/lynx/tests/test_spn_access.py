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

def test_spn_access_enabled():
    spn_access_enabled = tryEnableSpnAccess(PAYCAN_SECRET_SCOPE, storage_name, PAYCAN_SPN_ID, PAYCAN_SPN_KEY, LAKE_TENANT_ID)
    assert spn_access_enabled == True


# COMMAND ----------

test_spn_access_enabled()

# COMMAND ----------


