# Databricks notebook source
import json
import pyspark.sql.functions as F
from pyspark.sql.functions import lit

# COMMAND ----------

#%run ops/Utils/auth_util

# COMMAND ----------

dbutils.widgets.text("storage_name","", "storage_name")
dbutils.widgets.text("container_name","", "container_name")
dbutils.widgets.text("cur_path","", "cur_path")
storage_name = dbutils.widgets.get("storage_name")
container_name = dbutils.widgets.get("container_name")
cur_path = dbutils.widgets.get("cur_path")

# COMMAND ----------

PAYCAN_SECRET_SCOPE = "lake-kv-paycan-scope"
PAYCAN_SPN_ID = "spn-paycanservices-id"
PAYCAN_SPN_KEY = "spn-paycanservices-key"
LAKE_TENANT_ID = "lake-tenant-id"

# COMMAND ----------

#tryEnableSpnAccess(PAYCAN_SECRET_SCOPE, storage_name, PAYCAN_SPN_ID, PAYCAN_SPN_KEY, LAKE_TENANT_ID)

# COMMAND ----------

curated_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net/{cur_path}"
transfer = spark.read.format("parquet").load(curated_path + "/transfer")

# COMMAND ----------

transfer = transfer.filter(transfer['VALIDATION_STATUS']=='true')

# COMMAND ----------

transfer = transfer.filter(transfer['STATUS_TYPE_CD'].isin('Registered', 'Settled'))

# COMMAND ----------

transfer = transfer.withColumn('REGISTERED_', F.when((F.col('STATUS_TYPE_CD')=='Registered'), '1')\
                               .otherwise('0'))
transfer = transfer.withColumn('SETTLED_', F.when((F.col('STATUS_TYPE_CD')=='Settled'), '1')\
                               .otherwise('0'))

# COMMAND ----------

transfer = transfer.withColumn('REGISTERED_TIME_', F.when((F.col('REGISTERED_')==1), F.col('STATUS_CHANGE_TS'))\
                             .otherwise(lit(None)))
transfer = transfer.withColumn('SETTLED_TIME_', F.when((F.col('SETTLED_')==1), F.col('STATUS_CHANGE_TS'))\
                             .otherwise(lit(None)))

# COMMAND ----------

#Can only groupby on numeric columns
#To groupby on PROCESSED, SETTLED, DIARISED and REGISTERED, need to convert from string to byte
#To groupby on REGISTERED_TIME, DIARISED_TIME, SETTLED_TIME and PROCESSED_TIME, need to convert from string to timestamp, and then from timestamp to long
transfer = transfer.withColumn('REGISTERED_', transfer['REGISTERED_'].cast('Byte'))\
                   .withColumn('SETTLED_', transfer['SETTLED_'].cast('Byte'))\
                   .withColumn('REGISTERED_TIME_', transfer['REGISTERED_TIME_'].cast('Timestamp'))\
                   .withColumn('SETTLED_TIME_', transfer['SETTLED_TIME_'].cast('Timestamp'))

# COMMAND ----------

#need to convert from timestamp to long, to calculate duration
transfer = transfer.withColumn('REGISTERED_TIME_', transfer['REGISTERED_TIME_'].cast('Long'))\
                   .withColumn('SETTLED_TIME_', transfer['SETTLED_TIME_'].cast('Long'))

# COMMAND ----------

transfer = transfer.groupBy('BATCH_ID','TRANSFER_ID','PAYER_REFERENCE','CYCLE_DATE', 'INSTITUTION', 'PARTICIPANT', 'FROM_ACCOUNT_BBAN','FROM_ACCOUNT_NAME','FROM_SETTLE_MECH_CD','TO_ACCOUNT_BBAN','TO_ACCOUNT_NAME','TO_SETTLE_MECH_CD','TRANSACTION_TYPE','AMOUNT').max('REGISTERED_TIME_','SETTLED_TIME_','REGISTERED_','SETTLED_')
transfer = transfer.withColumnRenamed("max(REGISTERED_)","REGISTERED")\
                   .withColumnRenamed("max(SETTLED_)","SETTLED")\
                   .withColumnRenamed("max(REGISTERED_TIME_)","REGISTERED_TIME")\
                   .withColumnRenamed("max(SETTLED_TIME_)","SETTLED_TIME")

# COMMAND ----------

transfer = transfer.withColumn("BATCH_ID", transfer["BATCH_ID"].cast("Integer"))\
                  .withColumn("TRANSFER_ID", transfer["TRANSFER_ID"].cast("Long"))\
                  .withColumn("PAYER_REFERENCE", transfer["PAYER_REFERENCE"].cast("String"))\
                  .withColumn("CYCLE_DATE", transfer["CYCLE_DATE"].cast("Date"))\
                  .withColumn("INSTITUTION", transfer["INSTITUTION"].cast("String"))\
                  .withColumn("PARTICIPANT", transfer["PARTICIPANT"].cast("String"))\
                  .withColumn("FROM_ACCOUNT_BBAN", transfer["FROM_ACCOUNT_BBAN"].cast("String"))\
                  .withColumn("FROM_ACCOUNT_NAME", transfer["FROM_ACCOUNT_NAME"].cast("String"))\
                  .withColumn("FROM_SETTLE_MECH_CD", transfer["FROM_SETTLE_MECH_CD"].cast("String"))\
                  .withColumn("TO_ACCOUNT_BBAN", transfer["TO_ACCOUNT_BBAN"].cast("String"))\
                  .withColumn("TO_ACCOUNT_NAME", transfer["TO_ACCOUNT_NAME"].cast("String"))\
                  .withColumn("TO_SETTLE_MECH_CD", transfer["TO_SETTLE_MECH_CD"].cast("String"))\
                  .withColumn("TRANSACTION_TYPE", transfer["TRANSACTION_TYPE"].cast("String"))\
                  .withColumn("AMOUNT", transfer["AMOUNT"].cast("Decimal(22,2)"))\
                  .withColumn("REGISTERED_TIME", transfer["REGISTERED_TIME"].cast("Timestamp"))\
                  .withColumn("SETTLED_TIME", transfer["SETTLED_TIME"].cast("Timestamp"))\
                  .withColumn("REGISTERED", transfer["REGISTERED"].cast("Byte"))\
                  .withColumn("SETTLED", transfer["SETTLED"].cast("Byte"))

# COMMAND ----------

transfer.write.mode("overwrite").parquet(curated_path+"/transfer_summary")
