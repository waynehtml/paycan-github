# Databricks notebook source
# Fetch widget values
environment = dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %run /paycan/common/utils

# COMMAND ----------

raw_st_name = get_raw_st_name(dbutils.widgets.get("environment"))

# COMMAND ----------

uc_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

import io
import json
from pyspark.sql.types import *
import pandas as pd
import openpyxl as op
from openpyxl import load_workbook
from decimal import Decimal
import datetime

class working_file():
    def __init__(self, name, path):
        self.name = name
        self.path = path

## get working files
def get_working_files() -> ArrayType: 
    raw_container="restricted"
    raw_folder = "raw/external/cls_group/working"
    #raw_folder = "raw/external/payments_canada/cls/working"
    raw_path= "abfss://" + raw_container  + "@" + raw_st_name + ".dfs.core.windows.net/" + raw_folder
    
    working_files = []
    for f in dbutils.fs.ls(raw_path) :
        if f.isFile and f.name.endswith('.xlsx') :
            working_files.append(working_file(f.name, f.path))
    return working_files

## get schema for the datasets in the excel
def get_schema_overview() -> StructType:
    config_file = "/Workspace/paycan/cls/release/overview_schema.json"
    with open(config_file, "r") as f: 
        d = json.load(f)
        f.close()    
    return StructType.fromJson(d)

## append record to UC table standardized_db.tbl_cls_overview
def append_to_tbl_cls_overview (overview_data) :
    table_name= uc_name + ".standardized_db.tbl_cls_overview"
    # overview - create PySpark df df_overview
    df_overview = spark.createDataFrame(data=overview_data, schema=get_schema_overview())
    df_overview.write.mode("append").saveAsTable(table_name)

## append record to loading log table standardized_db.tbl_cls_overview
def append_to_tbl_loading_log (file_name, status) :
    table_name= uc_name + ".standardized_db.tbl_loading_log"
    data = [("standardized_db.tbl_cls_overview", file_name, datetime.datetime.now(), status)]
    schema = StructType([ \
        StructField("table_name",StringType(),True), \
        StructField("file_name",StringType(),True), \
        StructField("load_ts",TimestampType(), True),\
        StructField("load_result", StringType(),True) \
    ])
    df = spark.createDataFrame(data=data,schema=schema)
    df.write.mode("append").saveAsTable(table_name)

# Check whether this file is processed already.
def file_count(file_name) -> IntegerType:
    table_name= uc_name + ".standardized_db.tbl_cls_overview"
    str_query = "select count(audit_load_file) from " + table_name + " where audit_load_file = '" + file_name +"'"
    rec_count = spark.sql(str_query).collect()[0][0]
    return rec_count

# Check whether the dataset contains processed data .
def date_count(date_str) -> IntegerType:
    #overview_data
    table_name= uc_name + ".standardized_db.tbl_cls_overview"
    str_query = "select count(value_date) from " + table_name + " where value_date = to_date('" + date_str+"','yyyy-MM-dd HH:mm:ss')"
    rec_count = spark.sql(str_query).collect()[0][0]
    return rec_count

def reject_file(f) :  
    append_to_tbl_loading_log (f.name, "failed")
    date_str = datetime.datetime.now().strftime('%Y%m%d')
    time_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    dest_file = f.path.replace("/working/","/rejected/"+date_str+"/")
    dest_file = dest_file + time_str
    dbutils.fs.mv(f.path, dest_file) 
 
def proceed_file(f) :
    append_to_tbl_cls_overview (overview_data)
    append_to_tbl_loading_log (f.name, "succeed" )    
    date_str = datetime.datetime.today().strftime('%Y')
    dest_file = f.path.replace("/working/","/processed/"+date_str+"/")
    dbutils.fs.mv(f.path, dest_file) 

#Loop all working files
#The method pandas.read_excel does not support using wasbs or abfss scheme URL to access the file. 
#We will use PySpark to read the file from abfss schema URL, then create in-memory binary streams 
# as it could be read by pandas. The data flow is as below
# abfss EXCEL file -> PySpark df -> in-memory object -> pandas df  

#This variable will idnetify whether there is any error encounted during the files process
status_overall = "no error"

for f in get_working_files() :
    try:
        xlsx_file_bin = spark.read.format("binaryFile").load(f.path)
        xlsx_file = io.BytesIO(xlsx_file_bin.first().content)
        xl_df = pd.ExcelFile(xlsx_file, engine="openpyxl") 

        # Check whether this file is processed already.
        # If this file has been proceeded, 
        # 1. move it to rejected/yyyymmdd folder
        # 2. set status_overall = "encountered error"
        # 3. skip to next file
        if (file_count(f.name) > 0) :
            reject_file(f)
            status_overall = "encountered error"
            continue     


        # Initialize record data
        overview_data = []

        ###Loop for all sheets
        for sheet in xl_df.sheet_names:
            #creating pandas dataframe object sheet_df to hold current sheet
            sheet_df = xl_df.parse(sheet, header=None)
            
            ##load overview data
            for x in range(12,35):
                if(x==31 or x==33) :
                    continue
                if (x==32) :
                    tmp = str(sheet_df.iat[x, 1])
                    continue
                if (x==34) :
                    new_record = (sheet_df.iat[1, 1], str(sheet_df.iat[x, 0]), int(tmp.replace("nan", "0")), 
                            Decimal(str(sheet_df.iat[x, 2]).replace("nan", "0")), int(str(sheet_df.iat[x, 3]).replace("nan", "0")), 
                            Decimal(str(sheet_df.iat[x, 4]).replace("nan", "0")), Decimal(str(sheet_df.iat[x, 5]).replace("nan", "0")), 
                            Decimal(str(sheet_df.iat[x, 6]).replace("nan", "0")), f.name, datetime.datetime.now() 
                        )
                    overview_data.append(new_record)
                    continue
                new_record = (sheet_df.iat[1, 1], str(sheet_df.iat[x, 0]), int(str(sheet_df.iat[x, 1]).replace("nan", "0")), 
                        Decimal(str(sheet_df.iat[x, 2]).replace("nan", "0")), int(str(sheet_df.iat[x, 3]).replace("nan", "0")), 
                        Decimal(str(sheet_df.iat[x, 4]).replace("nan", "0")), Decimal(str(sheet_df.iat[x, 5]).replace("nan", "0")), 
                        Decimal(str(sheet_df.iat[x, 6]).replace("nan", "0")), f.name, datetime.datetime.now()
                        )
                overview_data.append(new_record)

        xl_df.close()
        proceed_file(f)

    except:
        reject_file(f)
        status_overall = "encountered error"

#print(status_overall)
dbutils.notebook.exit (status_overall)