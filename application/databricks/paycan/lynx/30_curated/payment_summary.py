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
payment = spark.read.format("parquet").load(curated_path + "/payment")

# COMMAND ----------

payment = payment.filter(payment['VALIDATION_STATUS']=='true')

# COMMAND ----------

payment = payment.filter(payment['STATUS_TYPE_CD'].isin('Registered', 'Settled', 'Queued', 'Removed', 'Resequenced', 'Reprioritise', 'Insuf.funds', 'Discarded', 'Diarised'))

# COMMAND ----------

payment = payment.withColumn('PRIORITY', F.when((F.col('PRIORITY').isNull()), '99')\
                            .otherwise(F.col('PRIORITY')))

# COMMAND ----------

payment = payment.withColumn('REGISTERED_', F.when((F.col('STATUS_TYPE_CD')=='Registered'), '1')\
                               .otherwise('0'))
payment = payment.withColumn('SETTLED_', F.when((F.col('STATUS_TYPE_CD')=='Settled'), '1')\
                               .otherwise('0'))
payment = payment.withColumn('QUEUED_', F.when((F.col('STATUS_TYPE_CD')=='Queued'), '1')\
                               .otherwise('0'))
payment = payment.withColumn('REMOVED_', F.when((F.col('STATUS_TYPE_CD')=='Removed'), '1')\
                               .otherwise('0'))
payment = payment.withColumn('RESEQUENCED_', F.when((F.col('STATUS_TYPE_CD')=='Resequenced'), '1')\
                               .otherwise('0'))
payment = payment.withColumn('REPRIORITISE_', F.when((F.col('STATUS_TYPE_CD')=='Reprioritise'), '1')\
                               .otherwise('0'))
payment = payment.withColumn('INSUFFUNDS_', F.when((F.col('STATUS_TYPE_CD')=='Insuf.funds'), '1')\
                               .otherwise('0'))
payment = payment.withColumn('DISCARDED_', F.when((F.col('STATUS_TYPE_CD')=='Discarded'), '1')\
                               .otherwise('0'))
payment = payment.withColumn('DIARISED_', F.when((F.col('STATUS_TYPE_CD')=='Diarised'), '1')\
                               .otherwise('0'))

# COMMAND ----------

payment = payment.withColumn('REGISTERED_TIME_', F.when((F.col('REGISTERED_')==1), F.col('STATUS_CHANGE_TIME_TS'))\
                             .otherwise(lit(None)))
payment = payment.withColumn('SETTLED_TIME_', F.when((F.col('SETTLED_')==1), F.col('STATUS_CHANGE_TIME_TS'))\
                             .otherwise(lit(None)))
payment = payment.withColumn('QUEUED_TIME_', F.when((F.col('QUEUED_')==1), F.col('STATUS_CHANGE_TIME_TS'))\
                             .otherwise(lit(None)))
payment = payment.withColumn('REMOVED_TIME_', F.when((F.col('REMOVED_')==1), F.col('STATUS_CHANGE_TIME_TS'))\
                             .otherwise(lit(None)))
payment = payment.withColumn('RESEQUENCED_TIME_', F.when((F.col('RESEQUENCED_')==1), F.col('STATUS_CHANGE_TIME_TS'))\
                             .otherwise(lit(None)))
payment = payment.withColumn('REPRIORITISE_TIME_', F.when((F.col('REPRIORITISE_')==1), F.col('STATUS_CHANGE_TIME_TS'))\
                             .otherwise(lit(None)))
payment = payment.withColumn('INSUFFUNDS_TIME_', F.when((F.col('INSUFFUNDS_')==1), F.col('STATUS_CHANGE_TIME_TS'))\
                             .otherwise(lit(None)))
payment = payment.withColumn('DISCARDED_TIME_', F.when((F.col('DISCARDED_')==1), F.col('STATUS_CHANGE_TIME_TS'))\
                             .otherwise(lit(None)))
payment = payment.withColumn('DIARISED_TIME_', F.when((F.col('DIARISED_')==1), F.col('STATUS_CHANGE_TIME_TS'))\
                             .otherwise(lit(None)))

# COMMAND ----------

#Can only groupby on numeric columns
#To groupby on PROCESSED, SETTLED, QUEUED and REGISTERED, need to convert from string to byte
#To groupby on REGISTERED_TIME, QUEUED_TIME, SETTLED_TIME and PROCESSED_TIME, need to convert from string to timestamp, and then from timestamp to long
payment = payment.withColumn('REGISTERED_', payment['REGISTERED_'].cast('Byte'))\
                  .withColumn('SETTLED_', payment['SETTLED_'].cast('Byte'))\
                  .withColumn('QUEUED_', payment['QUEUED_'].cast('Byte'))\
                  .withColumn('REMOVED_', payment['REMOVED_'].cast('Byte'))\
                  .withColumn('RESEQUENCED_', payment['RESEQUENCED_'].cast('Integer'))\
                  .withColumn('REPRIORITISE_', payment['REPRIORITISE_'].cast('Byte'))\
                  .withColumn('INSUFFUNDS_', payment['INSUFFUNDS_'].cast('Byte'))\
                  .withColumn('DISCARDED_', payment['DISCARDED_'].cast('Byte'))\
                  .withColumn('DIARISED_', payment['DIARISED_'].cast('Byte'))\
                  .withColumn('REGISTERED_TIME_', payment['REGISTERED_TIME_'].cast('Timestamp'))\
                  .withColumn('SETTLED_TIME_', payment['SETTLED_TIME_'].cast('Timestamp'))\
                  .withColumn('QUEUED_TIME_', payment['QUEUED_TIME_'].cast('Timestamp'))\
                  .withColumn('REMOVED_TIME_', payment['REMOVED_TIME_'].cast('Timestamp'))\
                  .withColumn('RESEQUENCED_TIME_', payment['RESEQUENCED_TIME_'].cast('Timestamp'))\
                  .withColumn('REPRIORITISE_TIME_', payment['REPRIORITISE_TIME_'].cast('Timestamp'))\
                  .withColumn('INSUFFUNDS_TIME_', payment['INSUFFUNDS_TIME_'].cast('Timestamp'))\
                  .withColumn('DISCARDED_TIME_', payment['DISCARDED_TIME_'].cast('Timestamp'))\
                  .withColumn('DIARISED_TIME_', payment['DIARISED_TIME_'].cast('Timestamp'))

# COMMAND ----------

#need to convert from timestamp to long, to calculate duration
payment = payment.withColumn('REGISTERED_TIME_', payment['REGISTERED_TIME_'].cast('Long'))\
                  .withColumn('SETTLED_TIME_', payment['SETTLED_TIME_'].cast('Long'))\
                  .withColumn('QUEUED_TIME_', payment['QUEUED_TIME_'].cast('Long'))\
                  .withColumn('REMOVED_TIME_', payment['REMOVED_TIME_'].cast('Long'))\
                  .withColumn('RESEQUENCED_TIME_', payment['RESEQUENCED_TIME_'].cast('Long'))\
                  .withColumn('REPRIORITISE_TIME_', payment['REPRIORITISE_TIME_'].cast('Long'))\
                  .withColumn('INSUFFUNDS_TIME_', payment['INSUFFUNDS_TIME_'].cast('Long'))\
                  .withColumn('DISCARDED_TIME_', payment['DISCARDED_TIME_'].cast('Long'))\
                  .withColumn('DIARISED_TIME_', payment['DIARISED_TIME_'].cast('Long'))

# COMMAND ----------

payment = payment.groupBy('BATCH_ID','PAYMENT_ID','PAYER_REFERENCE','SYSTEM_REFERENCE','CYCLE_DATE', 'PAYER_INSTITUTION', 'PAYER_PARTICIPANT', 'SETTLEMENT_MECHANISM_CD','RECEIVER_INSTITUTION', 'RECEIVER_PARTICIPANT', 'SWIFT_MSG_TYPE_CD','TRANSACTION_TYPE', 'AMOUNT','GRIDLOCK_SETTLED_ID','PRIORITY').agg({'REGISTERED_TIME_':'max','SETTLED_TIME_':'max','QUEUED_TIME_':'max','REMOVED_TIME_':'max','RESEQUENCED_TIME_':'max','REPRIORITISE_TIME_':'max','INSUFFUNDS_TIME_':'max','DISCARDED_TIME_':'max','DIARISED_TIME_':'max','REGISTERED_':'max','SETTLED_':'max','QUEUED_':'max','REMOVED_':'max','REPRIORITISE_':'max','INSUFFUNDS_':'max','DISCARDED_':'max','DIARISED_':'max','RESEQUENCED_':'sum'})

#Need to rename REGISTERED_TIME, SETTLED_TIME, PROCESSED_TIME, QUEUED_TIME differently to avoid the Spark error, AnalysisException: Reference is ambiguous
payment = payment.withColumnRenamed("max(REGISTERED_)","REGISTERED")\
                 .withColumnRenamed("max(SETTLED_)","SETTLED")\
                 .withColumnRenamed("max(QUEUED_)","QUEUED")\
                 .withColumnRenamed("max(REMOVED_)","REMOVED")\
                 .withColumnRenamed("max(REPRIORITISE_)","REPRIORITISE")\
                 .withColumnRenamed("sum(RESEQUENCED_)","RESEQUENCED")\
                 .withColumnRenamed("max(INSUFFUNDS_)","INSUFFUNDS")\
                 .withColumnRenamed("max(DISCARDED_)","DISCARDED")\
                 .withColumnRenamed("max(DIARISED_)","DIARISED")\
                 .withColumnRenamed("max(REGISTERED_TIME_)","REGISTERED_TIME")\
                 .withColumnRenamed("max(SETTLED_TIME_)","SETTLED_TIME")\
                 .withColumnRenamed("max(QUEUED_TIME_)","QUEUED_TIME")\
                 .withColumnRenamed("max(REMOVED_TIME_)","REMOVED_TIME")\
                 .withColumnRenamed("max(RESEQUENCED_TIME_)","RESEQUENCED_TIME")\
                 .withColumnRenamed("max(REPRIORITISE_TIME_)","REPRIORITISE_TIME")\
                 .withColumnRenamed("max(INSUFFUNDS_TIME_)","INSUFFUNDS_TIME")\
                 .withColumnRenamed("max(DISCARDED_TIME_)","DISCARDED_TIME")\
                 .withColumnRenamed("max(DIARISED_TIME_)","DIARISED_TIME")

# COMMAND ----------

payment = payment.withColumn("BATCH_ID", payment["BATCH_ID"].cast("Integer"))\
                  .withColumn("PAYMENT_ID", payment["PAYMENT_ID"].cast("Long"))\
                  .withColumn("PAYER_REFERENCE", payment["PAYER_REFERENCE"].cast("String"))\
                  .withColumn("SYSTEM_REFERENCE", payment["SYSTEM_REFERENCE"].cast("String"))\
                  .withColumn("CYCLE_DATE", payment["CYCLE_DATE"].cast("Date"))\
                  .withColumn("PAYER_INSTITUTION", payment["PAYER_INSTITUTION"].cast("String"))\
                  .withColumn("PAYER_PARTICIPANT", payment["PAYER_PARTICIPANT"].cast("String"))\
                  .withColumn("SETTLEMENT_MECHANISM_CD", payment["SETTLEMENT_MECHANISM_CD"].cast("String"))\
                  .withColumn("RECEIVER_INSTITUTION", payment["RECEIVER_INSTITUTION"].cast("String"))\
                  .withColumn("SWIFT_MSG_TYPE_CD", payment["SWIFT_MSG_TYPE_CD"].cast("String"))\
                  .withColumn("TRANSACTION_TYPE", payment["TRANSACTION_TYPE"].cast("String"))\
                  .withColumn("AMOUNT", payment["AMOUNT"].cast("Decimal(22,2)"))\
                  .withColumn("GRIDLOCK_SETTLED_ID", payment["GRIDLOCK_SETTLED_ID"].cast("Long"))\
                  .withColumn("REGISTERED_TIME", payment["REGISTERED_TIME"].cast("Timestamp"))\
                  .withColumn("SETTLED_TIME", payment["SETTLED_TIME"].cast("Timestamp"))\
                  .withColumn("QUEUED_TIME", payment["QUEUED_TIME"].cast("Timestamp"))\
                  .withColumn("REMOVED_TIME", payment["REMOVED_TIME"].cast("Timestamp"))\
                  .withColumn("RESEQUENCED_TIME", payment["RESEQUENCED_TIME"].cast("Timestamp"))\
                  .withColumn("REPRIORITISE_TIME", payment["REPRIORITISE_TIME"].cast("Timestamp"))\
                  .withColumn("INSUFFUNDS_TIME", payment["INSUFFUNDS_TIME"].cast("Timestamp"))\
                  .withColumn("DISCARDED_TIME", payment["DISCARDED_TIME"].cast("Timestamp"))\
                  .withColumn("DIARISED_TIME", payment["DIARISED_TIME"].cast("Timestamp"))\
                  .withColumn("REGISTERED", payment["REGISTERED"].cast("Byte"))\
                  .withColumn("SETTLED", payment["SETTLED"].cast("Byte"))\
                  .withColumn("QUEUED", payment["QUEUED"].cast("Byte"))\
                  .withColumn("REMOVED", payment["REMOVED"].cast("Byte"))\
                  .withColumn("RESEQUENCED", payment["RESEQUENCED"].cast("Integer"))\
                  .withColumn("REPRIORITISE", payment["REPRIORITISE"].cast("Byte"))\
                  .withColumn("INSUFFUNDS", payment["INSUFFUNDS"].cast("Byte"))\
                  .withColumn("DISCARDED", payment["DISCARDED"].cast("Byte"))\
                  .withColumn("DIARISED", payment["DIARISED"].cast("Byte"))

# COMMAND ----------

payment.write.mode("overwrite").parquet(curated_path+"/payment_summary")
