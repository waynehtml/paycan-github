# Databricks notebook source
import json 
import pyspark.sql.functions as F
from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %run ../../common/utils

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("environment","dev", "environment")
environment = dbutils.widgets.get("environment")

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))
credit_activity_table = f"{unity_catalog_name}.curated_db.lynx_credit_activity"
credit_activity_summary_table = f"{unity_catalog_name}.curated_db.lynx_credit_activity_summary"

# COMMAND ----------

credit_activity = spark.read.table(credit_activity_table)

# COMMAND ----------

credit_activity.display()

# COMMAND ----------

credit_activity = credit_activity.filter(credit_activity['VALIDATION_STATUS']==1)

# COMMAND ----------

credit_activity = credit_activity.filter(credit_activity['STATUS_TYPE_CD'].isin('Registered', 'Processed'))

# COMMAND ----------

credit_activity = credit_activity.withColumn('REGISTERED_', F.when((F.col('STATUS_TYPE_CD')=='Registered'), '1')\
                               .otherwise('0'))
credit_activity = credit_activity.withColumn('PROCESSED_', F.when((F.col('STATUS_TYPE_CD')=='Processed'), '1')\
                               .otherwise('0'))

# COMMAND ----------

credit_activity = credit_activity.withColumn('REGISTERED_TIME_', F.when((F.col('REGISTERED_')==1), F.col('STATUS_CHANGE_TIME'))\
                             .otherwise(lit(None)))
credit_activity = credit_activity.withColumn('PROCESSED_TIME_', F.when((F.col('PROCESSED_')==1), F.col('STATUS_CHANGE_TIME'))\
                             .otherwise(lit(None)))

# COMMAND ----------

#Can only groupby on numeric columns
#To groupby on PROCESSED and REGISTERED, need to convert from string to byte
#To groupby on REGISTERED_TIME and PROCESSED_TIME, need to convert from string to timestamp, and then from timestamp to long
credit_activity = credit_activity.withColumn('REGISTERED_', credit_activity['REGISTERED_'].cast('Byte'))\
                  .withColumn('PROCESSED_', credit_activity['PROCESSED_'].cast('Byte'))\
                  .withColumn("REGISTERED_TIME_", credit_activity["REGISTERED_TIME_"].cast("Timestamp"))\
                  .withColumn("PROCESSED_TIME_", credit_activity["PROCESSED_TIME_"].cast("Timestamp"))

# COMMAND ----------

#need to convert from timestamp to long, to calculate duration
credit_activity = credit_activity.withColumn("REGISTERED_TIME_", credit_activity["REGISTERED_TIME_"].cast("Long"))\
                  .withColumn("PROCESSED_TIME_", credit_activity["PROCESSED_TIME_"].cast("Long"))

# COMMAND ----------

credit_activity = credit_activity.groupBy('BATCH_ID','CREDIT_ACTIVITY_ID', 'CYCLE_DATE', 'INSTITUTION', 'PARTICIPANT', 'TOTAL_AMOUNT','EARMARKED_AMOUNT','EARMARKED_LOM').max('REGISTERED_TIME_', 'PROCESSED_TIME_','REGISTERED_','PROCESSED_')
credit_activity = credit_activity.withColumnRenamed("max(REGISTERED_)","REGISTERED")\
                 .withColumnRenamed("max(PROCESSED_)","PROCESSED")\
                 .withColumnRenamed("max(REGISTERED_TIME_)","REGISTERED_TIME")\
                 .withColumnRenamed("max(PROCESSED_TIME_)","PROCESSED_TIME")

# COMMAND ----------

credit_activity = credit_activity.withColumn("BATCH_ID", credit_activity["BATCH_ID"].cast("Integer"))\
                  .withColumn("CREDIT_ACTIVITY_ID", credit_activity["CREDIT_ACTIVITY_ID"].cast("Long"))\
                  .withColumn("CYCLE_DATE", credit_activity["CYCLE_DATE"].cast("Date"))\
                  .withColumn("INSTITUTION", credit_activity["INSTITUTION"].cast("String"))\
                  .withColumn("PARTICIPANT", credit_activity["PARTICIPANT"].cast("String"))\
                  .withColumn("TOTAL_AMOUNT", credit_activity["TOTAL_AMOUNT"].cast("Decimal(30,2)"))\
                  .withColumn("EARMARKED_AMOUNT", credit_activity["EARMARKED_AMOUNT"].cast("Decimal(30,2)"))\
                  .withColumn("EARMARKED_LOM", credit_activity["EARMARKED_LOM"].cast("String"))\
                  .withColumn("REGISTERED_TIME", credit_activity["REGISTERED_TIME"].cast("Timestamp"))\
                  .withColumn("PROCESSED_TIME", credit_activity["PROCESSED_TIME"].cast("Timestamp"))\
                  .withColumn("REGISTERED", credit_activity["REGISTERED"].cast("Byte"))\
                  .withColumn("PROCESSED", credit_activity["PROCESSED"].cast("Byte"))

# COMMAND ----------

credit_activity.write.format("delta").mode("overwrite").saveAsTable(credit_activity_summary_table)
credit_activity.display()
