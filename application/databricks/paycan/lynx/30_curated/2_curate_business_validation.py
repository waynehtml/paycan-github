# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql import window as w
import pyspark.sql.types as t
class Curate_Business_Validation(object):
  
  def participant_institution_business_validation(self, dataframe):
      dataframe = unique_column(dataframe, ["institution_number", "participant_number", "effective_ts", "expire_ts"], "FLAG_PART_INST_DUPLICATE")
      return dataframe
  
  def payment_business_validation(self, dataframe, compare_participant_institution):
      dataframe = unique_column(dataframe, ["PAYMENT_ID", "STATUS_TYPE_CD", "STATUS_CHANGE_TIME_TS"], "FLAG_PYMNT_STATUS")
      dataframe = unique_id_and_column_contains(dataframe, "PAYMENT_ID", dataframe.STATUS_TYPE_CD, "Registered", "FLAG_REGISTRATION_STATUS")
      dataframe = identical_column(dataframe, "FLAG_CYCLE_DATE")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_PAYER", "payer")
      dataframe = column_contains(dataframe, "SETTLEMENT_MECHANISM_CD", ["LSM", "RCM", "RTM", "UPM"], "FLAG_SETTLEMENT")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_RECEIVER", "receiver")
      dataframe = greater_than_or_equal_columns(dataframe, dataframe.AMOUNT, 0, "FLAG_AMOUNT")
      dataframe = settle_greater_than_registered(dataframe, "FLAG_TOTAL_DURATION")
      dataframe = format_transaction_type(dataframe)
      dataframe = dataframe.withColumnRenamed("PAYER_INSTITUTION_SHORT_NAME", "PAYER_INSTITUTION")
      dataframe = dataframe.withColumnRenamed("PAYER_PARTICIPANT_SHORT_NAME", "PAYER_PARTICIPANT")
      dataframe = dataframe.withColumnRenamed("RECEIVER_INSTITUTION_SHORT_NAME", "RECEIVER_INSTITUTION")
      dataframe = dataframe.withColumnRenamed("RECEIVER_PARTICIPANT_SHORT_NAME", "RECEIVER_PARTICIPANT")
      return dataframe

  def credit_position_business_validation(self, dataframe, compare_participant_institution):
      dataframe = unique_column(dataframe, ["credit_entry_id"], "FLAG_CREDIT_POSITION_ID")
      dataframe = identical_column(dataframe, "FLAG_CYCLE_DATE")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_INSTITUTION", "other")
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      dataframe = dataframe.withColumnRenamed("PARTICIPANT_SHORT_NAME", "PARTICIPANT")
      return dataframe
  
  def credit_activity_business_validation(self, dataframe, compare_participant_institution):
      dataframe = unique_column(dataframe, ["CREDIT_ACTIVITY_ID", "STATUS_TYPE_CD"], "FLAG_CREDIT_ACTIVITY_ID")
      dataframe = identical_column(dataframe, "FLAG_CYCLE_DATE")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_INSTITUTION", "other")
      dataframe = greater_than_or_equal_columns(dataframe, dataframe.TOTAL_AMOUNT, 0, "FLAG_AMOUNT")
      dataframe = create_status_type(dataframe, "STATUS_TYPE_CATEGORY")
      dataframe = dataframe.withColumnRenamed("PARTICIPANT_SHORT_NAME", "PARTICIPANT")
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      return dataframe
  
  def net_send_override_business_validation(self, dataframe, compare_participant_institution):
      dataframe = dataframe.withColumn("FLAG_OVERRIDE_USED", f.lit(1))
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      dataframe = dataframe.withColumnRenamed("PARTICIPANT_SHORT_NAME", "PARTICIPANT")
      dataframe = dataframe.withColumnRenamed("CONTRA_INSTITUTION_SHORT_NAME", "CONTRA_INSTITUTION")
      dataframe = dataframe.withColumnRenamed("CONTRA_PARTICIPANT_SHORT_NAME", "CONTRA_PARTICIPANT")
      return dataframe

    
  def net_send_limit_business_validation(self, dataframe, compare_participant_institution):
      dataframe = unique_column(dataframe, ["NET_SEND_LIMIT_ID"], "FLAG_NET_SEND_LIMIT_ID")
      dataframe = eval_exp(dataframe, (dataframe.REGISTERED_TS <= dataframe.FINAL_TS),"FLAG_REGISTERED_FINAL_DATE")
      dataframe = less_than(dataframe, dataframe.REGISTERED_TS, dataframe.EFFECTIVE_TS, "FLAG_EFFECTIVE_TIME")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_INSTITUTION", "other")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_CONTRA_INSTITUTION", "contra")
      dataframe = column_contains(dataframe, "SETTLEMENT_MECHANISM_CD", ["LSM"], "FLAG_SETTLEMENT")
      dataframe = greater_than_or_equal_columns(dataframe, dataframe.LIMIT_AMOUNT, 0, "FLAG_AMOUNT")
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      dataframe = dataframe.withColumnRenamed("PARTICIPANT_SHORT_NAME", "PARTICIPANT")
      dataframe = dataframe.withColumnRenamed("CONTRA_INSTITUTION_SHORT_NAME", "CONTRA_INSTITUTION")
      dataframe = dataframe.withColumnRenamed("CONTRA_PARTICIPANT_SHORT_NAME", "CONTRA_PARTICIPANT")
      return dataframe
    
  def net_send_schedule_business_validation(self, dataframe, compare_participant_institution):
      dataframe = greater_than_or_equal_columns(dataframe, dataframe.DELETE_DATE, dataframe.ACTIVE_DATE, "FLAG_DATE_DEL")
      dataframe = column_contains(dataframe, "SETTLEMENT_MECHANISM_CD", ["LSM"], "FLAG_SETTLEMENT")
      dataframe = event_time_and_fixed_time(dataframe, dataframe.EVENT_TRIGGER_CD, dataframe.FIXED_TIME, "FLAG_FIXED_TIME")
      dataframe = greater_than_or_equal_columns(dataframe, dataframe.AMOUNT, 0, "FLAG_AMOUNT")
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      dataframe = dataframe.withColumnRenamed("PARTICIPANT_SHORT_NAME", "PARTICIPANT")
      dataframe = dataframe.withColumnRenamed("CONTRA_INSTITUTION_SHORT_NAME", "CONTRA_INSTITUTION")
      dataframe = dataframe.withColumnRenamed("CONTRA_PARTICIPANT_SHORT_NAME", "CONTRA_PARTICIPANT")
      return dataframe
  
  def liquidity_threshold_schedule_business_validation(self, dataframe, compare_participant_institution):
      dataframe = unique_column(dataframe, dataframe.columns, "FLAG_DUPLICATE")
      dataframe = greater_than_or_equal_columns(dataframe, dataframe.DELETE_DATE, dataframe.ACTIVE_DATE, "FLAG_DATE_DELETE")
      dataframe = dataframe.withColumnRenamed("ACTIVE_DATE","CYCLE_DATE")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_INSTITUTION", "other")
      dataframe = dataframe.withColumnRenamed("CYCLE_DATE","ACTIVE_DATE")
      dataframe = greater_than_or_equal_columns(dataframe, dataframe.THRESHOLD_AMOUNT, 0, "FLAG_THRESHOLD_AMOUNT")
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      dataframe = dataframe.withColumnRenamed("PARTICIPANT_SHORT_NAME", "PARTICIPANT")
      return dataframe
    
  
  def liquidity_position_schedule_business_validation(self, dataframe, compare_participant_institution):
      dataframe = identical_column(dataframe, "FLAG_CYCLE_DATE")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_INSTITUTION", "institution")
      dataframe = column_equals_columns_subtracted(dataframe, dataframe.LIQUIDITY_POSITION, dataframe.RECEIVED_AMOUNT, dataframe.SENT_AMOUNT, "FLAG_LIQUIDITY")
      dataframe = unique_column(dataframe, ["CYCLE_DATE", "SETTLEMENT_MECHANISM_CD", "INSTITUTION_NAME"], "FLAG_MECHANISM")
      dataframe = count_unique(dataframe, 68, "FLAG_ENTRIES", "FLAG_MECHANISM")
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      return dataframe
  
  def gridlock_business_validation(self, dataframe, compare_participant_institution): 
      dataframe = unique_column(dataframe, ["GRIDLOCK_PARTICIPANT_ID"], "FLAG_GRIDLOCK_ID")  
      dataframe = columns_match_and_same_primary(dataframe, "GRIDLOCK_BATCH_ID", ["START_TS","GRIDLOCK_BATCH_ID"], "FLAG_BATCH")
      dataframe = identical_column(dataframe, "FLAG_CYCLE_DATE")
      dataframe = less_than(dataframe, dataframe.START_TS, dataframe.END_TS, "FLAG_START_END_TIME")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_INSTITUTION", "other")
      dataframe = eval_exp(dataframe, (dataframe.BEGIN_QUEUE_VALUE >= dataframe.END_QUEUE_VALUE) & (dataframe.END_QUEUE_VALUE >= 0) ,"FLAG_QUEUE_VALUE")
      dataframe = eval_exp(dataframe, (dataframe.BEGIN_QUEUE_VOLUME >= dataframe.END_QUEUE_VOLUME) & (dataframe.END_QUEUE_VALUE >= 0) ,"FLAG_QUEUE_VOLUME")
      payment = spark.read.format("parquet").load(standardized_path+"/payment"+"/BATCH_ID="+file_date)
      dataframe = matched_with_pay(dataframe, "MATCHED_WITH_PAY")
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      dataframe = dataframe.withColumnRenamed("PARTICIPANT_SHORT_NAME", "PARTICIPANT")
      return dataframe
    
  
  def lom_status_business_validation(self, dataframe, compare_participant_institution):
      dataframe = identical_column(dataframe, "FLAG_CYCLE_DATE")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_INSTITUTION", "other")
      dataframe = column_contains(dataframe, "SETTLEMENT_MECHANISM_CD", ["LSM"], "FLAG_SETTLEMENT")
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      dataframe = dataframe.withColumnRenamed("PARTICIPANT_SHORT_NAME", "PARTICIPANT")
      return dataframe
    
    
  def minum_balance_business_validation(self, dataframe, compare_participant_institution):
      dataframe = identical_column(dataframe, "FLAG_CYCLE_DATE")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_INSTITUTION", "other")
      dataframe = greater_than_or_equal_columns(dataframe, dataframe.MINIMUM_AMOUNT, 0, "FLAG_AMOUNT")
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      dataframe = dataframe.withColumnRenamed("PARTICIPANT_SHORT_NAME", "PARTICIPANT")
      return dataframe
    
    
  def loan_business_validation(self, dataframe, compare_participant_institution):
      dataframe = unique_column(dataframe, ["LOAN_ENTRY_ID"], "FLAG_UNIQUE")
      dataframe = identical_column(dataframe, "FLAG_CYCLE_DATE")
      dataframe = eval_exp(dataframe,((dataframe.ENTRY_TYPE == "Loan") & (dataframe.AMOUNT < 0)) | (dataframe.ENTRY_TYPE != "Loan"), "FLAG_LOAN")
      dataframe = eval_exp(dataframe, ((dataframe.ENTRY_TYPE == "Repaid") & (dataframe.AMOUNT > 0)) | (dataframe.ENTRY_TYPE != "Repaid"), "FLAG_REPAID")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_INSTITUTION", "other")
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      dataframe = dataframe.withColumnRenamed("PARTICIPANT_SHORT_NAME", "PARTICIPANT")
      dataframe = dataframe.withColumnRenamed("ACCOUNT_BBAN", "ACCOUNT_NAME")
      return dataframe

    
  def pause_payments_business_validation(self, dataframe, compare_participant_institution):
      dataframe = unique_column(dataframe, ["PAUSE_PAYMENTS_ID", "CONTRA_INSTITUTION_ACTION"], "FLAG_UNIQUE")
      dataframe = identical_column(dataframe, "FLAG_CYCLE_DATE")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_INSTITUTION", "other")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_CONTRA_INSTITUTION", "contra")
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      dataframe = dataframe.withColumnRenamed("PARTICIPANT_SHORT_NAME", "PARTICIPANT")
      dataframe = dataframe.withColumnRenamed("CONTRA_INSTITUTION_SHORT_NAME", "CONTRA_INSTITUTION")
      dataframe = dataframe.withColumnRenamed("CONTRA_PARTICIPANT_SHORT_NAME", "CONTRA_PARTICIPANT")
      return dataframe
 
    
  def transfer_business_validation(self, dataframe, compare_participant_institution):
      dataframe = unique_column(dataframe, ["TRANSFER_ID", "STATUS_TYPE_CD"], "FLAG_TRANSER_DUPLICATE")
      dataframe = identical_column(dataframe, "FLAG_CYCLE_DATE")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_INSTITUTION", "other")
      dataframe = greater_than(dataframe, dataframe.AMOUNT, 0, "FLAG_AMOUNT")
      dataframe = dataframe.withColumn('TO_ACCOUNT_NAME', f.regexp_replace('TO_ACCOUNT_NAME', '_CAD', ''))
      dataframe = dataframe.withColumn('FROM_ACCOUNT_NAME', f.regexp_replace('FROM_ACCOUNT_NAME', '_CAD', ''))
      dataframe = dataframe.withColumn("TRANSACTION_TYPE", f.substring_index(dataframe.TRANSACTION_TYPE, '(', 1))
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      dataframe = dataframe.withColumnRenamed("PARTICIPANT_SHORT_NAME", "PARTICIPANT")
      return dataframe
    
    
  def operating_schedule_business_validation(self, dataframe, compare_participant_institution):
      dataframe = unique_column(dataframe, dataframe.columns, "FLAG_OPERATIONS")
      dataframe = eval_exp(dataframe, (dataframe.START_TS <= dataframe.END_TS),"FLAG_START_END")
      dataframe = identical_column(dataframe, "FLAG_CYCLE_DATE")
      dataframe = convert_timestamp_milliseconds(dataframe, "DURATION")
      return dataframe

    
  def operational_status_business_validation(self, dataframe, compare_participant_institution):
      dataframe = identical_column(dataframe, "FLAG_CYCLE_DATE")
      dataframe = compare_to_participant_institution(dataframe, compare_participant_institution, "FLAG_INSTITUTION", "other")
      dataframe = unique_column(dataframe, ["STATUS_LOG_ID"], "FLAG_STATUS_LOG")
      dataframe = dataframe.withColumnRenamed("INSTITUTION_SHORT_NAME", "INSTITUTION")
      dataframe = dataframe.withColumnRenamed("PARTICIPANT_SHORT_NAME", "PARTICIPANT")
      return dataframe

# COMMAND ----------

def count_unique(dataframe, expected_count, flag, unique_flag):
  #filter for rows that are unique, acquire count
  real_count = dataframe.filter(unique_flag+"== False").count()
  
  #if correct amount of unique rows whole df is sucessful, otherwise all fail
  if real_count == expected_count:
    dataframe = dataframe.withColumn(flag, f.lit(0))
  
  else:
    dataframe = dataframe.withColumn(flag, f.lit(1))
  
  return dataframe

# COMMAND ----------

def matched_with_pay(dataframe, flag):
  
  #load payment file, retreive rows passing rule
  payment = spark.read.format("parquet").load(standardized_path+"/payment"+"/BATCH_ID="+file_date) 
  matchingRows = dataframe.join(payment, dataframe.GRIDLOCK_PARTICIPANT_ID == payment.GRIDLOCK_SETTLED_ID, 'leftsemi')
  
  #flag successfulRows
  successfulRows = matchingRows.withColumn(flag, f.lit("Y"))
  
  #flag failing rows in original df 
  joinedDf = dataframe.alias("gridlock").join(successfulRows, ["LYNX_"+file_name.upper()+"_ID", "BATCH_ID"], "leftouter")
  
  #select only original df columns, replace failed row null values
  cleanDataframe = joinedDf.select("gridlock.*", flag)
  cleanDataframe = cleanDataframe.na.fill({flag: "N"})
  return cleanDataframe

# COMMAND ----------

def create_status_type(dataframe, new_col_name):
  values = ["Registered", "Settled", "Queued","Processed"]
  df2 = dataframe.withColumn(new_col_name, f.when(dataframe.STATUS_TYPE_CD.isin(values), f.lit("With Status")).otherwise("No Status").cast(t.StringType()))
  return df2

# COMMAND ----------

def format_transaction_type(dataframe):
  df2 = dataframe.withColumn('TRANSACTION_TYPE', f.when((f.col('TRANSACTION_TYPE')=='Standard FSI (Single, including PI, LOM)'), 'LOM')\
                             .when((f.col('TRANSACTION_TYPE')=='Standard FSI (Single, LOM)'), 'LOM')\
                             .when((f.col('TRANSACTION_TYPE')=='Standard FSI (Single, RTM)'), 'RTM')\
                             .when((f.col('TRANSACTION_TYPE')=='Standard FSI (Single, no PI, RTM)'), 'RTM')\
                             .otherwise('NA'))
  return df2

# COMMAND ----------

def convert_timestamp_milliseconds(dataframe, new_col):
  #cast timestamp to double then subtract for seconds.milliseconds value
  secondsConversion = dataframe.withColumn("seconds", dataframe.END_TS.cast("double") - dataframe.START_TS.cast("double"))
  
  #multiply & cast seconds.milliseconds for only milliseconds conversion
  millisecondsConversion = secondsConversion.withColumn(new_col, (secondsConversion.seconds*1000).cast("long"))
  
  #drop leftover column
  cleanDF = millisecondsConversion.drop("seconds")
  return cleanDF

# COMMAND ----------

def settle_greater_than_registered(dataframe, flag):    
  
  #get only "Settled" rows
  onlySettledRows = dataframe.filter(dataframe.STATUS_TYPE_CD == "Settled")
  
  
  #get only ID's that fail rule
  failedRows = dataframe.alias("a").join(onlySettledRows.alias("b"), [f.col("a.PAYMENT_ID") == f.col("b.PAYMENT_ID"), f.col("a.STATUS_TYPE_CD") == "Registered", f.col("b.STATUS_CHANGE_TIME_TS") <= f.col("a.STATUS_CHANGE_TIME_TS")])
  
  #join df failing rule and df failing because of nulls
  failedNulls = dataframe.filter(dataframe.PAYMENT_ID.isNull() | dataframe.STATUS_CHANGE_TIME_TS.isNull() | dataframe.STATUS_TYPE_CD.isNull())
  failedRows = failedRows.select("a.*")
  failedRows = failedRows.union(failedNulls)
  
  #flag ID's that failed
  failedRows = failedRows.withColumn(flag, f.lit(1))
  
  
  #flag all duplicates of sucessful ID's, failed keys set to null by deafult
  allKeysFlagged = dataframe.alias("c").join(failedRows, ["PAYMENT_ID"], how="left_outer")
  
  #Replace default FLAG nulls with 0, select only original df
  cleanDataframe = allKeysFlagged.select("c.*", flag)
  cleanDataframe = cleanDataframe.na.fill({flag: 0})
  
  return cleanDataframe

# COMMAND ----------

def columns_match_and_same_primary(dataframe, column1, column2, flag):
  windowSpec = w.Window.partitionBy(column1)
  windowSpec2 = w.Window.partitionBy(column2)
  
  #count occurences of col1, count occurences of col1, col2 combo
  df2 = dataframe.withColumn("count1", f.count(f.col('*')).over(windowSpec)).withColumn("count2", f.count(f.col('*')).over(windowSpec2))
  
  #where both counts equal, col1 has all same values for col1+col2 combo
  df3 = df2.withColumn(flag, f.when(df2.count1 == df2.count2, f.lit(0)).otherwise(1))
  
  df4 = df3.drop('count1','count2')
  return df4

# COMMAND ----------

def eval_exp(dataframe, cond, flag):
  df2 = dataframe.withColumn(flag, f.when(cond, f.lit(0)).otherwise(1))
  return df2

# COMMAND ----------

def less_than(dataframe, column1, column2, flag):
  df2 = dataframe.withColumn(flag, f.when(column1 < column2, f.lit(0)).otherwise(1))
  return df2

# COMMAND ----------

def column_equals_columns_subtracted(dataframe, column1, column2, column3, flag):
  df2 = dataframe.withColumn(flag, f.when(column1 == (column2 - column3), f.lit(0)).otherwise(1))
  return df2

# COMMAND ----------

def equal_columns(dataframe, column1, column2, flag):
  df2 = dataframe.withColumn(flag, f.when(column1 == column2, f.lit(0)).otherwise(1))
  return df2

# COMMAND ----------

def event_time_and_fixed_time(dataframe, column1, column2, flag):
  df2 = dataframe.withColumn(flag, f.when(((column1.isNull()) & (column2.isNotNull())) | ((column1.isNotNull()) & (column2.isNull())), f.lit(0)).otherwise(1))
  return df2

# COMMAND ----------

def greater_than_or_equal_columns(dataframe, column1, column2, flag):
  df2 = dataframe.withColumn(flag, f.when(column1 >= column2, f.lit(0)).otherwise(1))
  return df2

# COMMAND ----------

def instutiton_query_type(dataframe, participant_institution, query):
  cond = None
  
  if query == "payer":
    cond = [dataframe.PAYER_INSTITUTION_SHORT_NAME == participant_institution.INSTITUTION_NAME, dataframe.PAYER_PARTICIPANT_SHORT_NAME == participant_institution.PARTICIPANT_NAME, dataframe.PAYER_INSTITUTION_NUMBER == participant_institution.INSTITUTION_NUMBER, dataframe.PAYER_INSTITUTION_IRN == participant_institution.INSTITUTION_IRN, dataframe.PAYER_PARTICIPANT_NUMBER == participant_institution.PARTICIPANT_NUMBER, dataframe.CYCLE_DATE >= f.to_date(participant_institution.EFFECTIVE_TS, 'yyyy-MM-dd'), (dataframe.CYCLE_DATE <= f.to_date(participant_institution.EXPIRE_TS, 'yyyy-MM-dd')) | (participant_institution.EXPIRE_TS.isNull())]
    
  elif query == "receiver":
    cond = [dataframe.RECEIVER_INSTITUTION_SHORT_NAME == participant_institution.INSTITUTION_NAME, dataframe.RECEIVER_PARTICIPANT_SHORT_NAME == participant_institution.PARTICIPANT_NAME, dataframe.RECEIVER_INSTITUTION_NUMBER == participant_institution.INSTITUTION_NUMBER, dataframe.RECEIVER_INSTITUTION_IRN == participant_institution.INSTITUTION_IRN, dataframe.RECEIVER_PARTICIPANT_NUMBER == participant_institution.PARTICIPANT_NUMBER, dataframe.CYCLE_DATE >= f.to_date(participant_institution.EFFECTIVE_TS, 'yyyy-MM-dd'), (dataframe.CYCLE_DATE <= f.to_date(participant_institution.EXPIRE_TS, 'yyyy-MM-dd')) | (participant_institution.EXPIRE_TS.isNull())]
  
  elif query == "contra":
    cond = [dataframe.CONTRA_INSTITUTION_SHORT_NAME == participant_institution.INSTITUTION_NAME, dataframe.CONTRA_PARTICIPANT_SHORT_NAME == participant_institution.PARTICIPANT_NAME, dataframe.CONTRA_INSTITUTION_NUMBER == participant_institution.INSTITUTION_NUMBER, dataframe.CONTRA_INSTITUTION_IRN == participant_institution.INSTITUTION_IRN, dataframe.CONTRA_PARTICIPANT_NUMBER == participant_institution.PARTICIPANT_NUMBER, dataframe.CYCLE_DATE >= f.to_date(participant_institution.EFFECTIVE_TS, 'yyyy-MM-dd'), (dataframe.CYCLE_DATE <= f.to_date(participant_institution.EXPIRE_TS, 'yyyy-MM-dd')) | (participant_institution.EXPIRE_TS.isNull())]
  
  elif query == "institution":
    cond = [dataframe.INSTITUTION_SHORT_NAME == participant_institution.INSTITUTION_NAME, dataframe.INSTITUTION_NUMBER == participant_institution.INSTITUTION_NUMBER, dataframe.INSTITUTION_IRN == participant_institution.INSTITUTION_IRN, dataframe.CYCLE_DATE >= f.to_date(participant_institution.EFFECTIVE_TS, 'yyyy-MM-dd'), (dataframe.CYCLE_DATE <= f.to_date(participant_institution.EXPIRE_TS, 'yyyy-MM-dd')) | (participant_institution.EXPIRE_TS.isNull())]
    
  else:
    cond = [dataframe.INSTITUTION_SHORT_NAME == participant_institution.INSTITUTION_NAME, dataframe.PARTICIPANT_SHORT_NAME == participant_institution.PARTICIPANT_NAME, dataframe.INSTITUTION_NUMBER == participant_institution.INSTITUTION_NUMBER, dataframe.INSTITUTION_IRN == participant_institution.INSTITUTION_IRN, dataframe.PARTICIPANT_NUMBER == participant_institution.PARTICIPANT_NUMBER, dataframe.CYCLE_DATE >= f.to_date(participant_institution.EFFECTIVE_TS, 'yyyy-MM-dd'), (dataframe.CYCLE_DATE <= f.to_date(participant_institution.EXPIRE_TS, 'yyyy-MM-dd')) | (participant_institution.EXPIRE_TS.isNull())]
  
  return cond

# COMMAND ----------

def compare_to_participant_institution(dataframe, participant_institution, flag, query): 
  
  #determine join condition
  cond = instutiton_query_type(dataframe, participant_institution, query)
  
  #get rows from left table passing rule, add column flagging sucesses
  successfulRows = dataframe.join(participant_institution, cond, how="leftsemi")
  successfulRowsFlagged = successfulRows.withColumn(flag, f.lit(0))

  
  #set flagged rows in original df, failed rows set to null by default
  allRowsFlagged = dataframe.alias(flag+"_DF").join(successfulRowsFlagged, ["LYNX_"+file_name.upper()+"_ID", "BATCH_ID"], "leftouter")
  
  
  #select only original columns+flag, set failed rows to 1
  cleanedDF = allRowsFlagged.select(flag+"_DF.*", flag)
  cleanedDF = cleanedDF.na.fill({flag: 1})
  return cleanedDF

# COMMAND ----------

def unique_column(dataframe, column, flag):
  
  #remove rows with null in focus cols 
  focusColsNoNulls = dataframe.dropna(subset=column)
   
  #get unique rows of focus cols
  windowSpec = w.Window.partitionBy(column)
  
  #set successful rows, those with values occuring once, to 0
  setSuccessfulRows = focusColsNoNulls.withColumn(flag, f.when(f.count(f.col('*')).over(windowSpec) == 1, f.lit(0)))
  
  #join on batch & ID incase historical files
  nulledFailedRows = dataframe.alias("original").join(setSuccessfulRows, ["LYNX_"+file_name.upper()+"_ID", "BATCH_ID"],'left')
  
  #remove duplicate columns, replace failure "nulls" with 1
  removeDuplicateColumns = nulledFailedRows.select("original.*", flag)
  cleanedDF = removeDuplicateColumns.na.fill({flag: 1})
  
  return cleanedDF

# COMMAND ----------

def identical_column(dataframe, flag):
  #convert integer to str then date type
  batch_converted = dataframe.withColumn("convert_batch", f.to_date(f.col("BATCH_ID").cast("string"), 'yyyyMMdd'))
  
  #compare cycle date to batch date, drop extra column 
  comparedDF = batch_converted.withColumn(flag, f.when(batch_converted.convert_batch == batch_converted.CYCLE_DATE, f.lit(0)).otherwise(1))
  
  cleanedDF = comparedDF.drop("convert_batch")
  return cleanedDF

# COMMAND ----------

def greater_than(dataframe, column, amount, flag):
  df2 = dataframe.withColumn(flag, f.when(column > amount, f.lit(0)).otherwise(1))
  return df2

# COMMAND ----------

def unique_id_and_column_contains(dataframe, unique_column, column, query, flag):
  
  #if unique_column contains query, mark with 0, 1 otherwise
  dataframe = dataframe.withColumn("contains_registered", f.when(column == query, f.lit(0)).otherwise(1))
  window = w.Window.partitionBy(unique_column).orderBy(f.asc('contains_registered'))
  
  #assign all unique_column ID's smallest value from their collection, assigns 0 not 1 when exists
  dataframe = dataframe.withColumn(flag, f.first('contains_registered').over(window))
  dataframe = dataframe.drop("contains_registered")
  return dataframe

# COMMAND ----------

def column_contains(dataframe, column, entries, flag):
  df = dataframe.withColumn(flag, f.when(f.col(column).isin(entries), f.lit(0)).otherwise(1))
  return df
