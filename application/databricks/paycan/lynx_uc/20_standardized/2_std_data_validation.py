# Databricks notebook source
# Author : rhaa
# Version : 2.0.5

# COMMAND ----------

import re
import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# DBTITLE 1,Get file info from config
def get_info(file, config):

  """ 
    Will get appropriate file information from config

    Parameters
    ----------

    file: str
    config_file: dict

    Returns
    ----------
    dict
  """
  

  return config[file]

# COMMAND ----------

# DBTITLE 1,Validate Columns Functions
def validate_col(df,file, config):
  """
    Will validate every column by going through every record of the column
    
    Parameters
    ----------
    df: DataFrame
    file: str
    config_file: dict
    
    Returns
    ----------
    DataFrame
  """
  is_int = udf(validate_int_udf)
  is_str = udf(validate_str_udf)
  is_numeric = udf(validate_numeric_udf)
  is_date = udf(validate_date_udf)
  is_time = udf(validate_time_udf)
  is_timestamp = udf(validate_timestamp_udf)
  check = None
  names = df.schema.names
  #check if file missing any columns
  headers = df.columns
  expected = spark.createDataFrame(config.items())
  check_header = expected.filter(~expected['_1'].isin(headers))
  
  if (not check_header.rdd.isEmpty()):
    check_header.select('_1').show()
    print("Missing column header(s) in the file")
    raise Exception
    
  for name in names:
    dtype = config[name]['data_type']

    if dtype == "INT64":
      
      #We are creating a new DF (check) which will have an extra column at the end that will have "False" in any cell that the data isnt valid
      check  = df.withColumn(f"is_Int_{name}", is_int(col(name)))
      
      #We now filter, look for any false value, they should all be true. 
      check_list = check.filter(check[f"is_Int_{name}"] == False).collect()
      
      #If there is any false present that means the data is not valid 
      if len(check_list)>0:
        print("Int")
        print(check_list)
        return False
      df = df.withColumn(name,col(name).cast(LongType()))
    elif dtype == "STRING":
      #We are creating a new DF (check) which will have an extra column at the end that will have "False" in any cell that the data isnt valid
      check  = df.withColumn(f"is_Str_{name}", is_str(col(name)))
      
      #We now filter, look for any false value, they should all be true. 
      check_list = check.filter(check[f"is_Str_{name}"] == False).collect()
      
      #If there is any false present that means the data is not valid 
      if len(check_list)>0:
        print("String")
        print(check_list[0])
        return False
      df = df.withColumn(name,col(name).cast("String"))
    elif dtype == "NUMERIC":
      #We are creating a new DF (check) which will have an extra column at the end that will have "False" in any cell that the data isnt valid
      check  = df.withColumn(f"is_Numeric_{name}", is_numeric(col(name)))
      
      #We now filter, look for any false value, they should all be true. 
      check_list = check.filter(check[f"is_Numeric_{name}"] == False).collect()
      
      #If there is any false present that means the data is not valid 
      if len(check_list)>0:
        print("Numeric")
        print(check_list[0])
        return False
      df = df.withColumn(name,col(name).cast(DecimalType(30,2)))
      
    elif dtype == "DATE":
      #We are creating a new DF (check) which will have an extra column at the end that will have "False" in any cell that the data isnt valid
      check  = df.withColumn(f"is_Date_{name}", is_date(col(name)))
      
      #We now filter, look for any false value, they should all be true. 
      check_list = check.filter(check[f"is_Date_{name}"] == False).collect()
      
      #If there is any false present that means the data is not valid 
      if len(check_list)>0:
        print("Date")
        print(check_list[0])
        return False
      df = df.withColumn(name,col(name).cast(DateType()))
    elif dtype == "TIME":
      
      #We are creating a new DF (check) which will have an extra column at the end that will have "False" in any cell that the data isnt valid
      check  = df.withColumn(f"is_Time_{name}", is_time(col(name)))
      
      #We now filter, look for any false value, they should all be true. 
      check_list = check.filter(check[f"is_Time_{name}"] == False).collect()
      
      #If there is any false present that means the data is not valid 
      if len(check_list)>0:
        
        print("Time")
        print(check_list[0])
        return False
      df = df.withColumn(name,col(name).cast(StringType()))
    else:
      #We are creating a new DF (check) which will have an extra column at the end that will have "False" in any cell that the data isnt valid
      check  = df.withColumn(f"is_Timestamp_{name}", is_timestamp(col(name)))
      
      #We now filter, look for any false value, they should all be true. 
      check_list = check.filter(check[f"is_Timestamp_{name}"] == False).collect()
      
      #If there is any false present that means the data is not valid 
      if len(check_list)>0:
        print("Timestamp")
        print(check_list[0])
        return False
      df = df.withColumn(name, regexp_replace(name, '0999-12-26 19:00:00.000 -05:00', '1900-01-01 00:00:00.000 -05:00'))
      
      #Strip the timezone from the timestamp, before applying the schema, to preserve the original data
      df = df.withColumn(name, col(name).substr(1,23))
      df = df.withColumn(name,col(name).cast(TimestampType()))
  return df

# COMMAND ----------

# DBTITLE 1,Validate Cells Functions
def validate_int_udf(value):
  """
    Validate int with type casting
    
    Parameters
    ----------
    value: int
    
    Returns
    ----------
    bool
  """
  #So if value is any of the choices below, it wont be able to convert to int but will still be correct dtype
  if value in [None,"", "null"]:
    return True
  #Using try/except to cover cases where the number might be in the form "123"
  try:
    int(value)
    return True 
  except:
    return False
  
def validate_str_udf(value):
  """
    Validate string with type casting
    
    Parameters
    ----------
    value: str
    
    Returns
    ----------
    bool
  """
  #So if value is any of the choices below, it wont be able to convert to str but will still be correct dtype
  if value in [None,"", "null"]:
    return True
  try:
    str(value)
    return True
  except:
    return False

def validate_numeric_udf(value):
  """
    Validate numeric with pattern matching
    
    Parameters
    ----------
    value: str
    
    Returns
    ----------
    bool
  """
  #So if value is any of the choices below, it wont pattern match properly
  if value in [None,"", "null"]:
    return True
  #This is the regex pattern to check for 1-2 decimals places 
  pattern = "^-*[0-9]*(\.[0-9]{1,2})$"
  if(not re.match(pattern, str(value))):
    return False 
  return True 

def validate_date_udf(value):
  
  """
    Validate date with pattern matching
    
    Parameters
    ----------
    value: str
    
    Returns
    ----------
    bool
  """
  #So if value is any of the choices below, it wont pattern match properly
  if value in [None,"", "null"]:
    return True
  #This is the regex pattern to check for date in format yyyy-mm-dd
  pattern = "^\d{4}-\d{2}-\d{2}$"
  if(not re.match(pattern, str(value))):
    return False 
  return True 

def validate_time_udf(value):
  
  """
    Validate time with pattern matching
    
    Parameters
    ----------
    value: str
    
    Returns
    ----------
    bool
  """
  #So if value is any of the choices below, it wont pattern match properly
  if value in [None,"", "null"]:
    return True
  #This is the regex pattern to check for time in format HH:mm:ss
  pattern = "(^([0-1]?\d|2[0-3]):([0-5]?\d):([0-5]?\d)$)|(^([0-5]?\d):([0-5]?\d)$)|(^[0-5]?\d$)"
  if(not re.match(pattern, str(value))):
    return False 
  return True

#Had to go back to pattern matching since we cant infer schema anymore, since spark was changing the data
def validate_timestamp_udf(value):
  """
    Validate timestamp with pattern matching
    
    Parameters
    ----------
    value: str
    
    Returns
    ----------
    bool
  """
  #So if value is any of the choices below, it wont pattern match properly
  if value in [None,"", "null"]:
    return True
  #Instead we use the isinstance
  pattern = "^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3} -\d{2}:\d{2}$"
  if(not re.match(pattern, str(value))):
    return False 
  return True 


# COMMAND ----------

def capitalize(df):
  """
    Capitalizes the column headers
    
    Parameters
    ----------
    df: DataFrame
   
    Returns
    ----------
    DataFrame
  """
  for col in df.columns:
    df = df.withColumnRenamed(col, col.upper())
  return df
  

# COMMAND ----------

def remove_infinity(df, col):
  """
    Replaces string "Infinity" with ""
    
    Parameters
    ----------
    df: DataFrame
    
    Returns
    ----------
    DataFrame
  """
  try:
    df[col]
    df  = df.withColumn(col,regexp_replace(col, "Infinity", "").cast(IntegerType()))
  except:
    pass
  return df

# COMMAND ----------

# DBTITLE 1,Function: Renaming Columns
def rename_columns(df, config):
  """
    Re maps the columns to the appropriate names
    
    Parameters
    ----------
    df: DataFrame
    config_file: dict
    
    Returns
    ----------
    DataFrame
  """
  for col in df.columns:
    new_format_dict = config[col]
    df = df.withColumnRenamed(col , new_format_dict["c_name"])
  
  return df

# COMMAND ----------

# DBTITLE 1,Function: Adding Batch ID
def add_batch_id(df, cycle_date):
  """
    Adds the batch id to the DF, the batch id is just the cycle date with the "-" removed
    
    Parameters
    ----------
    df: DataFrame
    cycle_date: str
    
    Returns
    ----------
    DataFrame
  """
  batch_id = regexp_replace(lit(cycle_date),"-","")
  df = df.withColumn("batch_id", lit(batch_id))
  return df

# COMMAND ----------

def add_lynx_id(dataframe, file_name):
  df = dataframe.withColumn("LYNX_"+file_name+"_ID", F.monotonically_increasing_id())
  return df

# COMMAND ----------

# DBTITLE 1,Main
def standardized_main(df, name, cycle_date, config_file):
  """
    Main function that will perform validation and cleaning dataframe
    
    Parameters
    ----------
    df: DataFrame
    name: str
    cycle_date: str
    config_file: dict
    
    Returns
    ----------
    DataFrame
  """
 
  
  #Get appropriate file data from config
  
  config = get_info(name, config_file)
  
  cleaned_df = remove_infinity(df, "max_credit_amt")
  
  validated_df = validate_col(cleaned_df, name, config)
  #First we Validate the column since if error arises, it is considered critical 
  #No point in changing column names etc just to throw error later
  if not (validated_df):
    return False 
  
  
  standardized_df = rename_columns(validated_df,  config)
  standardized_df = add_batch_id(standardized_df,cycle_date)
  standardized_df = add_lynx_id(standardized_df,name)
  standardized_df = capitalize(standardized_df)
  return standardized_df

#standardized_main(df_payment, file, cycle_date)

# COMMAND ----------

def standardized_main_empty_file(df, name, cycle_date, config_file):
  """
    Main function that will create an empty dataframe with a schema as determined from the config file and skip validation as the csv is empty
    
    Parameters
    ----------
    df: DataFrame
    name: str
    cycle_date: str
    config_file: dict
    
    Returns
    ----------
    DataFrame
  """
 
  
  #Get appropriate file data from config
  
  config = get_info(name, config_file)
  
  #Create an empty data set to use as the data component of the empty dataframe
  emptyRDD = spark.sparkContext.emptyRDD()
  
  #Create a schema Struct that pulls the column name from the config file and determines the Spark data type function to call from the spark_val k,v pair that is nested
  schema = StructType([StructField(key, eval(value['spark_val']+'()')) for key, value in config.items()])
  
  #Set the dataframe to be the schema defined by the StructType
  df = spark.createDataFrame(emptyRDD, schema)
  
  #df.printSchema()
  #df.show()
  
  #Perform standard formatting functions to rename and insert columns to ensure naming and column consistency with validated data
  standardized_df = rename_columns(df,  config)
  standardized_df = add_batch_id(standardized_df,cycle_date)
  standardized_df = add_lynx_id(standardized_df,name)
  standardized_df = capitalize(standardized_df)
  
  #standardized_df.printSchema()
  
  return standardized_df
