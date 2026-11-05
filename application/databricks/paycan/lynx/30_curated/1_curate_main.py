# Databricks notebook source
#%run /ops/Utils/auth_util

# COMMAND ----------

# DBTITLE 1,Define variables
dbutils.widgets.text("cycle_date","", "Cycle_date")
dbutils.widgets.text("storage","", "storage")
dbutils.widgets.text("container","", "container")
dbutils.widgets.text("file","", "file")
dbutils.widgets.text("std_dir","", "std_dir")
dbutils.widgets.text("cur_dir","", "cur_dir")
file_date = dbutils.widgets.get("cycle_date")
storage_name = dbutils.widgets.get("storage")
container_name = dbutils.widgets.get("container")
file_name = dbutils.widgets.get("file")
std_dir = dbutils.widgets.get("std_dir")
cur_dir = dbutils.widgets.get("cur_dir")
standardized_path = "abfss://" +container_name+ "@" +storage_name+ ".dfs.core.windows.net/"+std_dir
curated_path = "abfss://" +container_name+ "@" +storage_name+ ".dfs.core.windows.net/"+cur_dir

# COMMAND ----------

PAYCAN_SECRET_SCOPE = "lake-kv-paycan-scope"           
PAYCAN_SPN_ID = "spn-paycanservices-id"                       
PAYCAN_SPN_KEY = "spn-paycanservices-key"                  
LAKE_TENANT_ID = "lake-tenant-id"  
TESTING_STORAGE = "lakecacnpdstdevland01"

# COMMAND ----------

#tryEnableSpnAccess(PAYCAN_SECRET_SCOPE, storage_name, PAYCAN_SPN_ID, PAYCAN_SPN_KEY, LAKE_TENANT_ID)
#tryEnableSpnAccess(PAYCAN_SECRET_SCOPE, TESTING_STORAGE, PAYCAN_SPN_ID, PAYCAN_SPN_KEY, LAKE_TENANT_ID)

# COMMAND ----------

# DBTITLE 1,Run Child Notebook
# MAGIC %run ./2_curate_business_validation

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Check shuffle partitions and set according to cores in cluster

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",5)
spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

from pyspark.sql import functions as f
import pyspark.sql.types as t
def main (storage_name, container_name, file_date, file_name):
  
  #load data to dataframe, filter for desired date and cast int batch column
  dataframe = spark.read.format("parquet").load(standardized_path+"/"+file_name+"/BATCH_ID="+file_date)
  dataframe = dataframe.withColumn("BATCH_ID", f.lit(file_date).cast(t.IntegerType()))

  #load participant_institution for business rules 
  compare_participant_institution = spark.read.format("parquet").load(standardized_path+"/participant_institution"+"/BATCH_ID="+file_date)
  
  
  #instantiate validation table/business rules
  curate_class = Curate_Business_Validation()
  validation_table = create_validation_table()

  if file_name == "participant_institution":
    check_critical_empty(dataframe, ["INSTITUTION_NUMBER","INSTITUTION_NAME","INSTITUTION_DESCRIPTION","INSTITUTION_STATUS_CD","PARTICIPANT_NUMBER", "LAST_MODIFIED_TS"])
    dataframe = curate_class.participant_institution_business_validation(dataframe)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_PARTICIPANT_INSTITUTION_ID", ["FLAG_PART_INST_DUPLICATE"],"INSTITUTION_NUMBER")
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop("FLAG_PART_INST_DUPLICATE")
    write_file_to_curated(dataframe, "participant_institution")
    write_file_to_curated(validation_table, "validation", "participant_institution")


  elif file_name == "payment":
    check_critical_empty(dataframe, ["CYCLE_DATE","PAYER_INSTITUTION_NUMBER","PAYER_INSTITUTION_IRN","PAYER_INSTITUTION_NAME","PAYER_INSTITUTION_SHORT_NAME","PAYER_PARTICIPANT_NUMBER","PAYER_PARTICIPANT_BIC","PAYER_PARTICIPANT_NAME","PAYER_PARTICIPANT_SHORT_NAME","RECEIVER_INSTITUTION_NUMBER","RECEIVER_INSTITUTION_IRN","RECEIVER_INSTITUTION_NAME","RECEIVER_INSTITUTION_SHORT_NAME","RECEIVER_PARTICIPANT_NUMBER","RECEIVER_PARTICIPANT_BIC","RECEIVER_PARTICIPANT_NAME","RECEIVER_PARTICIPANT_SHORT_NAME", "LAST_MODIFIED_TS"])
    file_flags = ["FLAG_PYMNT_STATUS", "FLAG_REGISTRATION_STATUS", "FLAG_CYCLE_DATE", "FLAG_PAYER", "FLAG_SETTLEMENT", "FLAG_RECEIVER", "FLAG_AMOUNT", "FLAG_TOTAL_DURATION"]
    dataframe = curate_class.payment_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_PAYMENT_ID", file_flags,"PAYMENT_ID")
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "payment")
    write_file_to_curated(validation_table, "validation", "payment")


  elif file_name == "credit_position":
    check_critical_empty(dataframe, ["INSTITUTION_NUMBER", "INSTITUTION_IRN", "INSTITUTION_NAME", "INSTITUTION_SHORT_NAME", "PARTICIPANT_NUMBER", "PARTICIPANT_BIC", "PARTICIPANT_NAME", "PARTICIPANT_SHORT_NAME", "LAST_MODIFIED_TS", "CYCLE_DATE"])
    file_flags = ["FLAG_CREDIT_POSITION_ID", "FLAG_CYCLE_DATE", "FLAG_INSTITUTION"]
    dataframe = curate_class.credit_position_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_CREDIT_POSITION_ID", file_flags,"CREDIT_ENTRY_ID")
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "credit_position")
    write_file_to_curated(validation_table, "validation", "credit_position")


  elif file_name == "credit_activity":
    check_critical_empty(dataframe, ["INSTITUTION_NUMBER","INSTITUTION_IRN","INSTITUTION_NAME","INSTITUTION_SHORT_NAME","PARTICIPANT_NUMBER","PARTICIPANT_BIC","PARTICIPANT_NAME","PARTICIPANT_SHORT_NAME", "LAST_MODIFIED_TS", "CYCLE_DATE"])
    file_flags = ["FLAG_CREDIT_ACTIVITY_ID","FLAG_CYCLE_DATE","FLAG_INSTITUTION","FLAG_AMOUNT"]
    dataframe = curate_class.credit_activity_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_CREDIT_ACTIVITY_ID", file_flags,"CREDIT_ACTIVITY_ID")
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "credit_activity")
    write_file_to_curated(validation_table, "validation", "credit_activity")


  elif file_name == "net_send_override":
    check_critical_empty(dataframe, ["CYCLE_DATE","ACTIVE_DATE","DELETE_DATE","INSTITUTION_NUMBER","INSTITUTION_IRN","INSTITUTION_NAME","INSTITUTION_SHORT_NAME","PARTICIPANT_NUMBER","PARTICIPANT_BIC","PARTICIPANT_NAME","PARTICIPANT_SHORT_NAME","CONTRA_INSTITUTION_NUMBER","CONTRA_INSTITUTION_IRN","CONTRA_INSTITUTION_NAME","CONTRA_INSTITUTION_SHORT_NAME","CONTRA_PARTICIPANT_NUMBER","CONTRA_PARTICIPANT_BIC","CONTRA_PARTICIPANT_NAME","CONTRA_PARTICIPANT_SHORT_NAME","LAST_MODIFIED_TS"])
    file_flags =["FLAG_OVERRIDE_USED"]
    dataframe = curate_class.net_send_override_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_NET_SEND_OVERRIDE_ID", file_flags,"NET_SEND_OVERRIDE_ID") 
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "net_send_override")
    write_file_to_curated(validation_table, "validation", "net_send_override")


  elif file_name == "net_send_limit":
    check_critical_empty(dataframe, ["CYCLE_DATE","INSTITUTION_NUMBER","INSTITUTION_IRN","INSTITUTION_NAME","INSTITUTION_SHORT_NAME","PARTICIPANT_NUMBER","PARTICIPANT_BIC","PARTICIPANT_NAME","PARTICIPANT_SHORT_NAME","CONTRA_INSTITUTION_NUMBER","CONTRA_INSTITUTION_IRN","CONTRA_INSTITUTION_NAME","CONTRA_INSTITUTION_SHORT_NAME","CONTRA_PARTICIPANT_NUMBER","CONTRA_PARTICIPANT_BIC","CONTRA_PARTICIPANT_NAME","CONTRA_PARTICIPANT_SHORT_NAME","LIMIT_AMOUNT", "LAST_MODIFIED_TS"])
    file_flags = ["FLAG_NET_SEND_LIMIT_ID", "FLAG_REGISTERED_FINAL_DATE", "FLAG_INSTITUTION", "FLAG_CONTRA_INSTITUTION", "FLAG_SETTLEMENT", "FLAG_AMOUNT", "FLAG_EFFECTIVE_TIME"]
    dataframe = curate_class.net_send_limit_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_NET_SEND_LIMIT_ID", file_flags,"NET_SEND_LIMIT_ID") 
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "net_send_limit")
    write_file_to_curated(validation_table, "validation", "net_send_limit")


  elif file_name == "net_send_schedule":
    check_critical_empty(dataframe, ["ACTIVE_DATE","DELETE_DATE","INSTITUTION_NUMBER","INSTITUTION_IRN","INSTITUTION_NAME","INSTITUTION_SHORT_NAME","PARTICIPANT_NUMBER","PARTICIPANT_BIC","PARTICIPANT_NAME","PARTICIPANT_SHORT_NAME","CONTRA_INSTITUTION_NUMBER","CONTRA_INSTITUTION_IRN","CONTRA_INSTITUTION_NAME","CONTRA_INSTITUTION_SHORT_NAME","CONTRA_PARTICIPANT_NUMBER","CONTRA_PARTICIPANT_BIC","CONTRA_PARTICIPANT_NAME","CONTRA_PARTICIPANT_SHORT_NAME","LAST_MODIFIED_TS"])
    file_flags = ["FLAG_DATE_DEL", "FLAG_SETTLEMENT", "FLAG_FIXED_TIME", "FLAG_AMOUNT"]
    dataframe = curate_class.net_send_schedule_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_NET_SEND_SCHEDULE_ID", file_flags,"NET_SEND_ID") 
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "net_send_schedule")
    write_file_to_curated(validation_table, "validation", "net_send_schedule")


  elif file_name == "liquidity_threshold":
    check_critical_empty(dataframe,["CREATE_DATE","ACTIVE_DATE","DELETE_DATE","INSTITUTION_NUMBER","INSTITUTION_IRN","INSTITUTION_NAME","INSTITUTION_SHORT_NAME","PARTICIPANT_NUMBER","PARTICIPANT_BIC","PARTICIPANT_NAME","PARTICIPANT_SHORT_NAME", "LAST_MODIFIED_TS"])
    file_flags = ["FLAG_DATE_DELETE", "FLAG_THRESHOLD_AMOUNT", "FLAG_INSTITUTION", "FLAG_DUPLICATE"]
    dataframe = curate_class.liquidity_threshold_schedule_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_LIQUIDITY_THRESHOLD_ID", file_flags) 
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "liquidity_threshold")
    write_file_to_curated(validation_table, "validation", "liquidity_threshold")


  elif file_name == "liquidity_position":
    check_critical_empty(dataframe,["CYCLE_DATE","INSTITUTION_NUMBER","INSTITUTION_IRN","INSTITUTION_NAME","INSTITUTION_SHORT_NAME","LAST_MODIFIED_TS"])
    file_flags = ["FLAG_CYCLE_DATE", "FLAG_LIQUIDITY", "FLAG_INSTITUTION", "FLAG_MECHANISM", "FLAG_ENTRIES"]
    dataframe = curate_class.liquidity_position_schedule_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_LIQUIDITY_POSITION_ID", file_flags)
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "liquidity_position")
    write_file_to_curated(validation_table, "validation", "liquidity_position")


  elif file_name == "gridlock":
    check_critical_empty(dataframe,["CYCLE_DATE","INSTITUTION_NUMBER","INSTITUTION_IRN","INSTITUTION_NAME","INSTITUTION_SHORT_NAME","PARTICIPANT_NUMBER","PARTICIPANT_BIC","PARTICIPANT_NAME","PARTICIPANT_SHORT_NAME", "LAST_MODIFIED_TS", "START_TS", "END_TS"])
    file_flags = ["FLAG_GRIDLOCK_ID", "FLAG_BATCH", "FLAG_CYCLE_DATE", "FLAG_START_END_TIME", "FLAG_INSTITUTION", "FLAG_QUEUE_VALUE", "FLAG_QUEUE_VOLUME"]
    dataframe = curate_class.gridlock_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_GRIDLOCK_ID", file_flags,"GRIDLOCK_PARTICIPANT_ID") 
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "gridlock")
    write_file_to_curated(validation_table, "validation", "gridlock")


  elif file_name == "lom_status":
    check_critical_empty(dataframe,["CYCLE_DATE","INSTITUTION_NUMBER","INSTITUTION_IRN","INSTITUTION_NAME","INSTITUTION_SHORT_NAME","PARTICIPANT_NUMBER","PARTICIPANT_BIC","PARTICIPANT_NAME","PARTICIPANT_SHORT_NAME", "LAST_MODIFIED_TS"])
    file_flags = ["FLAG_CYCLE_DATE", "FLAG_INSTITUTION", "FLAG_SETTLEMENT"]
    dataframe = curate_class.lom_status_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_LOM_STATUS_ID", file_flags)
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "lom_status")
    write_file_to_curated(validation_table, "validation", "lom_status")


  elif file_name == "minum_balance":
    check_critical_empty(dataframe,["CYCLE_DATE", "INSTITUTION_NUMBER", "INSTITUTION_IRN","INSTITUTION_NAME","INSTITUTION_SHORT_NAME","PARTICIPANT_NUMBER", "PARTICIPANT_BIC","PARTICIPANT_NAME","PARTICIPANT_SHORT_NAME", "LAST_MODIFIED_TS"])
    file_flags = ["FLAG_CYCLE_DATE", "FLAG_INSTITUTION", "FLAG_AMOUNT"]
    dataframe = curate_class.minum_balance_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_MINUM_BALANCE_ID", file_flags)
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "minum_balance")
    write_file_to_curated(validation_table, "validation", "minum_balance")


  elif file_name == "loan":
    check_critical_empty(dataframe,["CYCLE_DATE","INSTITUTION_NUMBER","INSTITUTION_IRN","INSTITUTION_NAME","INSTITUTION_SHORT_NAME","PARTICIPANT_NUMBER","PARTICIPANT_BIC","PARTICIPANT_NAME","PARTICIPANT_SHORT_NAME","ENTRY_TS", "LAST_MODIFIED_TS"])
    file_flags = ["FLAG_UNIQUE", "FLAG_CYCLE_DATE", "FLAG_LOAN", "FLAG_REPAID", "FLAG_INSTITUTION"]
    dataframe = curate_class.loan_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_LOAN_ID", file_flags,"LOAN_ENTRY_ID") 
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "loan")
    write_file_to_curated(validation_table, "validation", "loan")


  elif file_name == "pause_payments":
    check_critical_empty(dataframe,["CYCLE_DATE","INSTITUTION_NUMBER","INSTITUTION_IRN","INSTITUTION_NAME","INSTITUTION_SHORT_NAME","PARTICIPANT_NUMBER","PARTICIPANT_BIC","PARTICIPANT_NAME","PARTICIPANT_SHORT_NAME","REGISTERED_TS","EFFECTIVE_TS","LAST_MODIFIED_TS","CONTRA_INSTITUTION_NUMBER","CONTRA_INSTITUTION_IRN","CONTRA_INSTITUTION_NAME","CONTRA_INSTITUTION_SHORT_NAME","CONTRA_PARTICIPANT_NUMBER","CONTRA_PARTICIPANT_BIC","CONTRA_PARTICIPANT_NAME","CONTRA_PARTICIPANT_SHORT_NAME"])
    file_flags = ["FLAG_CYCLE_DATE", "FLAG_INSTITUTION", "FLAG_CONTRA_INSTITUTION", "FLAG_UNIQUE"] 
    dataframe = curate_class.pause_payments_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_PAUSE_PAYMENTS_ID", file_flags,"PAUSE_PAYMENTS_ID")
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "pause_payments")
    write_file_to_curated(validation_table, "validation", "pause_payments")


  elif file_name == "transfer":
    check_critical_empty(dataframe,["CYCLE_DATE", "INSTITUTION_NUMBER","INSTITUTION_IRN","INSTITUTION_NAME","INSTITUTION_SHORT_NAME","PARTICIPANT_NUMBER","PARTICIPANT_BIC","PARTICIPANT_NAME","PARTICIPANT_SHORT_NAME","LAST_MODIFIED_TS","STATUS_CHANGE_TS"])
    file_flags = ["FLAG_TRANSER_DUPLICATE", "FLAG_CYCLE_DATE", "FLAG_INSTITUTION", "FLAG_AMOUNT"]   
    dataframe = curate_class.transfer_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_TRANSFER_ID", file_flags,"TRANSFER_ID")
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "transfer")
    write_file_to_curated(validation_table, "validation", "transfer")


  elif file_name == "operating_schedule":
    check_critical_empty(dataframe,["CYCLE_DATE", "LAST_MODIFIED_TS"])
    file_flags = ["FLAG_CYCLE_DATE", "FLAG_OPERATIONS", "FLAG_START_END"]
    dataframe = curate_class.operating_schedule_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_OPERATING_SCHEDULE_ID", file_flags,"WINDOW_LOG_ID") 
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "operating_schedule")
    write_file_to_curated(validation_table, "validation", "operating_schedule")


  elif file_name == "operational_status":
    check_critical_empty(dataframe,["CYCLE_DATE","INSTITUTION_NUMBER","INSTITUTION_IRN","INSTITUTION_NAME","INSTITUTION_SHORT_NAME","PARTICIPANT_NUMBER","PARTICIPANT_BIC","PARTICIPANT_NAME","PARTICIPANT_SHORT_NAME","EFFECTIVE_TS","LAST_MODIFIED_TS"])
    file_flags = ["FLAG_CYCLE_DATE", "FLAG_INSTITUTION", "FLAG_STATUS_LOG"]
    dataframe = curate_class.operational_status_business_validation(dataframe, compare_participant_institution)
    validation_table = write_to_validation_table(validation_table, dataframe, file_name, "LYNX_OPERATIONAL_STATUS_ID", file_flags,"STATUS_LOG_ID")
    dataframe = write_validation_status(dataframe, validation_table)
    dataframe = dataframe.drop(*file_flags)
    write_file_to_curated(dataframe, "operational_status")
    write_file_to_curated(validation_table, "validation", "operational_status")


  else:
    print("A file name was supplied that this notebook is not expecting.")
    raise Exception("A file name was supplied that this notebook is not expecting.")
      

# COMMAND ----------

def check_critical_empty(dataframe, critical_columns):
  nullColumns = []
  
  #if critical column has null append
  for column in critical_columns:
    
    if dataframe.filter(f.col(column).isNull()).count() > 0:
      nullColumns.append(column)
  
  #if any critical column has null print & raise exception
  if nullColumns:
    raise Exception(nullColumns, "columns have null. Abort pipeline.")
    

# COMMAND ----------

def write_file_to_curated(dataframe, file, file_in_validation=None):
  
  #if file exists, filter current files rows in it then append
  try:
    
    #cache file read or spark doesnt let us write to same path
    existingDf = spark.read.format("parquet").load(curated_path+"/"+file)
    existingDf.cache()
    
    #validation table filters batch+filename, file_name is data file
    if file == "validation":
      filteredDf = existingDf.filter(~((existingDf.BATCH_ID == file_date) & (existingDf.FILE_NAME == file_in_validation)))
      
      
    else:
      filteredDf = existingDf.filter(existingDf.BATCH_ID != file_date)
    
    
    dataframe = filteredDf.union(dataframe)
    
    
  except:
    print("File not written yet, no filtering necessary.")
  

  dataframe.write.format("parquet").mode("overwrite").save(curated_path+"/"+file)

# COMMAND ----------

def write_validation_status(dataframe, validation_table):
  
  #removes duplicate rows, rename column for join condition
  validation_table = validation_table.dropDuplicates(['BATCH_ID', 'FILE_PRIMARY_ID']) 
  validation_table = validation_table.withColumnRenamed("FILE_PRIMARY_ID", "LYNX_"+file_name.upper()+"_ID")
  
  
  #get df rows that exist in validation table, flag them as failed
  failedRows = dataframe.join(validation_table, ["BATCH_ID", "LYNX_"+file_name.upper()+"_ID"])
  failedRows = failedRows.withColumn("VALIDATION_STATUS", f.lit(False))
  
  
  #flag failed rows in original df, sucessful default to null
  failedRowsFlagged = dataframe.alias("original_df").join(failedRows, ["BATCH_ID", "LYNX_"+file_name.upper()+"_ID"], "left")
  
  #select orginial df columns, set null sucessesful rows to True
  cleanedDF = failedRowsFlagged.select("original_df.*", "VALIDATION_STATUS")
  cleanedDF = cleanedDF.na.fill({"VALIDATION_STATUS": True})
  return cleanedDF

# COMMAND ----------

def create_validation_table():
 
  sc = spark.sparkContext
  schema = t.StructType([
    t.StructField('BATCH_ID', t.IntegerType(), True),
    t.StructField('FILE_NAME', t.StringType(), True),
    t.StructField('FILE_PRIMARY_ID', t.LongType(), True),
    t.StructField('BUSINESS_ID', t.LongType(), True),
    t.StructField('FLAG_NAME', t.StringType(), True)
  ])
  
  return spark.createDataFrame(sc.emptyRDD(),schema)

# COMMAND ----------

def write_to_validation_table(validation_table, dataframe, file_name, primary_id, flags, business_id=None):
  for flag in flags:
    
    #select file record identifier if it exists, rename to BUSINESS_ID
    if business_id:
      df2 = dataframe.select(primary_id, "BATCH_ID", business_id).filter(flag +"== True")
      df2 = df2.withColumnRenamed(business_id, 'BUSINESS_ID')
    
    #default BUSINESS_ID to 0
    else:
      df2 = dataframe.select(primary_id, "BATCH_ID").filter(flag +"== True")
      df2 = df2.withColumn("BUSINESS_ID", f.lit(0).cast(t.LongType()))
      
    df2 = df2.withColumn("FILE_NAME", f.lit(file_name))
    df2 = df2.withColumn("FLAG_NAME", f.lit(flag))
    df2 = df2.withColumnRenamed(primary_id, 'FILE_PRIMARY_ID')
    validation_table = validation_table.unionByName(df2)
    
  validation_table = validation_table.withColumn("CREATETIME", f.current_timestamp())
  return validation_table

# COMMAND ----------

main(storage_name, container_name, file_date, file_name)
