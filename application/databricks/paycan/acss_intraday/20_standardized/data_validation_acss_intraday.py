# Databricks notebook source
from datetime import datetime as dt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from pyspark.sql import functions as f
from pyspark.sql.functions import row_number,lit
import pyspark.sql.types as t
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %run "/paycan/python_library/modules/01_validate"

# COMMAND ----------


dbutils.widgets.text("storage_name","", "storage_name")
dbutils.widgets.text("cycle_date","", "cycle_date")

storage_name = dbutils.widgets.get("storage_name") #lakecanpdstdev01
cycle_date = dbutils.widgets.get("cycle_date")


# COMMAND ----------

raw_file_path = f"abfss://restricted@{storage_name}.dfs.core.windows.net/raw/external/payments_canada/acss/intraday/{cycle_date}/"

# COMMAND ----------

df = spark.read.csv(raw_file_path, header = True)

# COMMAND ----------

data_validation = DataValidation(df)

# COMMAND ----------

data_validation.validate_empty_file()

# COMMAND ----------

data_validation.validate_col_count(expected_col_count = 21)

# COMMAND ----------

data_validation.validate_column_headers(valid_header_list = ['system_code',
                                                             'delivery_id',
                                                             'cycle_date',
                                                             'record_type',
                                                             'settlement_date',
                                                             'version_number',
                                                             'sending_inst_irn',
                                                             'sending_inst_name_en',
                                                             'receiving_inst_irn',
                                                             'receiving_inst_name_en',
                                                             'region_code',
                                                             'region_name',
                                                             'sent_date',
                                                             'sent_time',
                                                             'payment_id',
                                                             'stream_code',
                                                             'stream_name',
                                                             'crdb_type',
                                                             'settle_type',
                                                             'value',
                                                             'volume'])

# COMMAND ----------

data_validation.validate_date_column(column_name = 'cycle_date', date_format = 'yyyyMMdd')
data_validation.validate_date_column(column_name = 'settlement_date', date_format = 'yyyyMMdd')
data_validation.validate_date_column(column_name = 'sent_date', date_format = 'yyyyMMdd')

# COMMAND ----------

data_validation.validate_float_column(column_name = 'value')

# COMMAND ----------

data_validation.validate_int_column(column_name='sent_time')
data_validation.validate_int_column(column_name='volume')
