# Databricks notebook source
import json
import os
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, concat, current_date, to_date, date_format, lit
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from typing import List
from decimal import Decimal
import pandas as pd
import numpy as np
from io import BytesIO
import openpyxl

# COMMAND ----------

# MAGIC %run /paycan/common/utils

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# COMMAND ----------

dbutils.widgets.text("storage_name","", "storage_name")
dbutils.widgets.text("data_container","", "data_container")
dbutils.widgets.text("raw_path","", "raw_path")
dbutils.widgets.text("environment","", "environment")
dbutils.widgets.text("file_date","", "file_date")
dbutils.widgets.text("table_name","", "table_name")

data_container = dbutils.widgets.get("data_container")
storage_name = dbutils.widgets.get("storage_name")
raw_path = dbutils.widgets.get("raw_path")
environment = dbutils.widgets.get("environment")
file_date = dbutils.widgets.get("file_date")
file_date_str = f"{file_date[:4]}-{file_date[4:6]}-{file_date[6:]}"
table_name = dbutils.widgets.get("table_name")

## Catalog + schema
unit_catalog_name = get_unity_catalog_name(environment)
std_cat_schema = (unit_catalog_name + "." + "standardized_db")

# COMMAND ----------

## get table schema
def get_schema(table_name: str) -> StructType:
    spark.sql(f"USE {unit_catalog_name}.standardized_db")
    df = spark.table(table_name)
    tbl_schema = StructType.fromJson(json.loads(df.schema.json()))

    return tbl_schema

tmx_cdcc_collateral_table_schema = get_schema("tbl_tmx_cdcc_collateral"); 
tmx_cdcc_vm_table_schema = get_schema("tbl_tmx_cdcc_vm"); 
tmx_cdcc_members_table_schema = get_schema("tbl_tmx_cdcc_members"); 
tmx_cdcc_margins_table_schema = get_schema("tbl_tmx_cdcc_margins"); 
tmx_cdcc_positions_table_schema = get_schema("tbl_tmx_cdcc_positions"); 
tmx_cdcc_liquidity_data_table_schema = get_schema("tbl_tmx_cdcc_liquidity_data");   


# COMMAND ----------

def import_margin_file(file_path: str, file_name: str, table_schema: StructType) -> DataFrame:

    ## load file into dataframe
    df = spark.read.format("csv").option("header", True).option("delimiter", ";").load(file_path)

    df = df.withColumnRenamed("Date", "date")
    df = df.withColumnRenamed("ActivityName", "activity_name")
    df = df.withColumnRenamed("CollateralPoolType", "collateral_pool_type")
    df = df.withColumnRenamed("ClearingAccount", "clearing_account")
    df = df.withColumnRenamed("CustomerAccountName", "customer_account_name")
    df = df.withColumnRenamed("MemberNameId", "member_name_id")
    df = df.withColumnRenamed("RiskModel", "risk_model")
    df = df.withColumnRenamed("AccountType", "account_type")
    df = df.withColumnRenamed("SubAccount", "sub_account")
    df = df.withColumnRenamed("ReportingLevel", "reporting_level")
    df = df.withColumnRenamed("InitialMarginOffsetGroup", "initial_margin_offset_group")
    df = df.withColumnRenamed("CombinedCommodityCode", "combined_commodity_code")
    df = df.withColumnRenamed("FundTypeDetail", "fund_type_detail")
    df = df.withColumnRenamed("BaseMarginAmount", "base_margin_amount")
    df = df.withColumnRenamed("RequirementAmount", "requirement_amount")

    ## Format date column
    df = df.withColumn("date", to_date(col("date"), "yyyyMMdd"))
    df = df.withColumn("date", date_format(col("date"), "yyyy-MM-dd"))

    ## add file_date column
    df = df.withColumn("file_date", lit(file_date_str))

    ## Force conversion to columns
    for field in table_schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))    

   
    return df
    

# COMMAND ----------

def import_members_file(file_path: str, file_name: str, table_schema: StructType) -> DataFrame:

    ## load file into dataframe
    df = spark.read.format("csv").option("header", True).option("delimiter", ";").load(file_path)

    df = df.withColumnRenamed("Date", "date")
    df = df.withColumnRenamed("ActivityName", "activity_name")
    df = df.withColumnRenamed("MemberNameId", "member_name_id")
    df = df.withColumnRenamed("FundType", "fund_type")
    df = df.withColumnRenamed("RequirementAmount", "requirement_amount")
    df = df.withColumnRenamed("AssetValue", "asset_value")
    df = df.withColumnRenamed("InventoryAssetSummaryTotal", "inventory_asset_summary_total")

    ## Format date column
    df = df.withColumn("date", to_date(col("date"), "yyyyMMdd"))
    df = df.withColumn("date", date_format(col("date"), "yyyy-MM-dd"))
    
    ## add file_date column
    df = df.withColumn("file_date", lit(file_date_str))

    ## Force conversion to columns
    for field in table_schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))        
   
    return df

# COMMAND ----------

def import_position_file(file_path: str, file_name: str, table_schema: StructType) -> DataFrame:

    ## load file into dataframe
    df = spark.read.format("csv").option("header", True).option("delimiter", ";").load(file_path)

    df = df.withColumnRenamed("Date", "date")
    df = df.withColumnRenamed("PositionType", "position_type")
    df = df.withColumnRenamed("PositionId", "position_id")
    df = df.withColumnRenamed("CollateralPoolType", "collateral_pool_type")
    df = df.withColumnRenamed("ClearingAccount", "clearing_account")
    df = df.withColumnRenamed("MemberName", "member_name")
    df = df.withColumnRenamed("RiskModel", "risk_model")
    df = df.withColumnRenamed("AccountType", "account_type")
    df = df.withColumnRenamed("SubAccount", "sub_account")
    df = df.withColumnRenamed("AccountingType", "accounting_type")
    df = df.withColumnRenamed("ProductSymbol", "product_symbol")
    df = df.withColumnRenamed("ProductType", "product_type")
    df = df.withColumnRenamed("UsageType", "usage_type")
    df = df.withColumnRenamed("UnderlyingSymbol", "underlying_symbol")
    df = df.withColumnRenamed("ExpiryDate", "expiry_date")
    df = df.withColumnRenamed("StrikePrice", "strike_price")
    df = df.withColumnRenamed("CallPutCode", "call_put_code")
    df = df.withColumnRenamed("PremiumMultiplier", "premium_multiplier")
    df = df.withColumnRenamed("ContractSize", "contract_size")
    df = df.withColumnRenamed("Long","long")
    df = df.withColumnRenamed("Short","short")
    df = df.withColumnRenamed("LongCash","long_cash")
    df = df.withColumnRenamed("ShortCash","short_cash")
    df = df.withColumnRenamed("MarginableLong","marginable_long")
    df = df.withColumnRenamed("MarginableShort","marginable_short")
    df = df.withColumnRenamed("ProcessedSelectedMarginPrice", "processed_selected_margin_price")
    df = df.withColumnRenamed("UnderlyingSelectedMarginPrice", "underlying_selected_margin_price")
    df = df.withColumnRenamed("CombinedCommodityCode", "combined_commodity_code")
    df = df.withColumnRenamed("TransactionType", "transaction_type")
    df = df.withColumnRenamed("SettlementDate", "settlement_date")
    df = df.withColumnRenamed("CustomerAccountName", "customer_account_name")
    df = df.withColumnRenamed("ActivityName", "activity_name")    

    ## Format date column
    df = df.withColumn("date", to_date(col("date"), "yyyyMMdd"))
    df = df.withColumn("date", date_format(col("date"), "yyyy-MM-dd"))

    ## add file_date column
    df = df.withColumn("file_date", lit(file_date_str))
    
    ## Force conversion to columns
    for field in table_schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))   

    return df

# COMMAND ----------

def import_cash_settlement_file(file_path: str, cash_set_sheet_name : str, cel_from_to : str, file_name: str, table_schema : StructType, axis_sheet_name : str) -> DataFrame:

    ## load file into dataframe
    df = excel_to_df_multiple_sheets(file_path, cash_set_sheet_name, cel_from_to, 1, axis_sheet_name)
    spark_df = spark.createDataFrame(data=df)

    spark_df = spark_df.withColumnRenamed("Date", "date")
    spark_df = spark_df.withColumnRenamed("Member", "member")
    spark_df = spark_df.withColumnRenamed("EOD Daily VM Cash Settlement: (-)Pay (+)Excess", "eod_daily_vm_cash_settlement")
    
    ## add file_date column
    spark_df = spark_df.withColumn("file_date", lit(file_date_str))

    ## Force conversion to columns
    for field in table_schema.fields:
        if field.name in spark_df.columns:
            spark_df = spark_df.withColumn(field.name, col(field.name).cast(field.dataType))  

   
    return spark_df

# COMMAND ----------

def import_collateral_file(file_path: str, cash_set_sheet_name : str, cel_from_to : str, file_name: str, table_schema : StructType) -> DataFrame:

    ## load file into dataframe
    df = excel_file_to_dataframe(file_path, cash_set_sheet_name, cel_from_to)

    spark_df = spark.createDataFrame(data=df)

    ## Rename columns
    spark_df = spark_df.withColumnRenamed("Date", "date")
    spark_df = spark_df.withColumnRenamed("member", "member")
    spark_df = spark_df.withColumnRenamed("ActivityName", "activity_name")
    spark_df = spark_df.withColumnRenamed("FundType", "fund_type")
    spark_df = spark_df.withColumnRenamed("AssetType", "asset_type")
    spark_df = spark_df.withColumnRenamed("ProductType", "product_type")
    spark_df = spark_df.withColumnRenamed("ProductSymbol", "product_symbol")
    spark_df = spark_df.withColumnRenamed("BondIssuer", "bond_issuer")
    spark_df = spark_df.withColumnRenamed("ISIN", "isin")    
    spark_df = spark_df.withColumnRenamed("LongDescription", "long_description")
    spark_df = spark_df.withColumnRenamed("DecimalQuantity", "decimal_quantity")
    spark_df = spark_df.withColumnRenamed("Value", "value")
    spark_df = spark_df.withColumnRenamed("MaturityDate", "maturity_date")
    spark_df = spark_df.withColumnRenamed("IssueDate", "issue_date")
    spark_df = spark_df.withColumnRenamed("TimeToMaturityAtIssuanceInYears", "time_to_maturity_at_issuance_in_years")
    spark_df = spark_df.withColumnRenamed("TimeToMaturity", "time_to_maturity")
    spark_df = spark_df.withColumnRenamed("TimeToMaturityInYears", "time_to_maturity_in_years")

    ## add file_date column
    spark_df = spark_df.withColumn("file_date", lit(file_date_str))

    ## Force conversion to columns
    for field in table_schema.fields:
        if field.name in spark_df.columns:
            spark_df = spark_df.withColumn(field.name, col(field.name).cast(field.dataType))

    return spark_df

# COMMAND ----------

def import_cdcc_liquidity_data_file(file_path: str, cash_set_sheet_name : str, cel_from_to : str, file_name: str, table_schema : StructType) -> DataFrame:

    ## load file into dataframe
    pd_df = excel_file_to_dataframe(file_path, cash_set_sheet_name, cel_from_to)

    ## Convert to Spark Dataframe     
    spark_df = spark.createDataFrame(data=pd_df)

    ## Rename columns
    spark_df = spark_df.withColumnRenamed("Date", "date")
    spark_df = spark_df.withColumnRenamed("ScenarioName", "scenario_name")
    spark_df = spark_df.withColumnRenamed("ScenarioIdx", "scenario_idx")
    spark_df = spark_df.withColumnRenamed("ParentMemberNameId", "parent_member_name_id")
    spark_df = spark_df.withColumnRenamed("Need_FI_Dplus1", "need_fi_dplus1")
    spark_df = spark_df.withColumnRenamed("Need_FI_Dplus2", "need_fi_dplus2")
    spark_df = spark_df.withColumnRenamed("Need_FI_Dplus3", "need_fi_dplus3")
    spark_df = spark_df.withColumnRenamed("Need_FI_Dplus4", "need_fi_dplus4")
    spark_df = spark_df.withColumnRenamed("Need_FUTURES_Dplus1", "need_futures_dplus1")
    spark_df = spark_df.withColumnRenamed("Need_FUTURES_Dplus2", "need_futures_dplus2")
    spark_df = spark_df.withColumnRenamed("Need_FUTURES_Dplus3", "need_futures_dplus3")
    spark_df = spark_df.withColumnRenamed("Need_FUTURES_Dplus4", "need_futures_dplus4")
    spark_df = spark_df.withColumnRenamed("Need_OPTIONS_Dplus1", "need_options_dplus1")
    spark_df = spark_df.withColumnRenamed("Need_OPTIONS_Dplus2", "need_options_dplus2")
    spark_df = spark_df.withColumnRenamed("Need_OPTIONS_Dplus3", "need_options_dplus3")
    spark_df = spark_df.withColumnRenamed("Need_OPTIONS_Dplus4", "need_options_dplus4")
    spark_df = spark_df.withColumnRenamed("Need_EQY_Dplus1", "need_eqy_dplus1")
    spark_df = spark_df.withColumnRenamed("Need_EQY_Dplus2", "need_eqy_dplus2")
    spark_df = spark_df.withColumnRenamed("Need_EQY_Dplus3", "need_eqy_dplus3")
    spark_df = spark_df.withColumnRenamed("Need_EQY_Dplus4", "need_eqy_dplus4")
    spark_df = spark_df.withColumnRenamed("Source_FI_Dplus1", "source_fi_dplus1")
    spark_df = spark_df.withColumnRenamed("Source_FI_Dplus2", "source_fi_dplus2")
    spark_df = spark_df.withColumnRenamed("Source_EQY_Dplus1", "source_eqy_dplus1")
    spark_df = spark_df.withColumnRenamed("Source_EQY_Dplus2", "source_eqy_dplus2")
    spark_df = spark_df.withColumnRenamed("EQY_ColValue", "eqy_col_value")
    spark_df = spark_df.withColumnRenamed("FI_ColValue", "fi_col_value")
    spark_df = spark_df.withColumnRenamed("Member_CF_FI_Value", "member_cf_fi_value")
    spark_df = spark_df.withColumnRenamed("Cash_ColValue", "cash_col_value")
    spark_df = spark_df.withColumnRenamed("SurvivorCF_FI_Value", "survivor_cf_fi_value")
    spark_df = spark_df.withColumnRenamed("SurvivorCF_Cash_Value", "survivor_cf_cash_value")
    spark_df = spark_df.withColumnRenamed("SurvivorSLF_Cash_Value", "survivor_slf_cash_value")
    spark_df = spark_df.withColumnRenamed("Survivor_SLF_FI_Value", "survivor_slf_fi_value")
    spark_df = spark_df.withColumnRenamed("ExposureFI", "exposure_fi")
    spark_df = spark_df.withColumnRenamed("exposureDerivatives", "exposure_derivatives")
    spark_df = spark_df.withColumnRenamed("TotalCashResourcesDerivatives", "total_cash_resources_derivatives")

    ## add file_date column
    spark_df = spark_df.withColumn("file_date", lit(file_date_str))
    
    ## Force conversion to columns
    for field in table_schema.fields:
        if field.name in spark_df.columns:
            spark_df = spark_df.withColumn(field.name, col(field.name).cast(field.dataType))    

    
    return spark_df

# COMMAND ----------

def import_liquidity_repport_file(in_df: DataFrame, in_day_dmp : str, table_schema : StructType) -> DataFrame:

    ## Rename columns
    in_df = in_df.withColumnRenamed("Date", "date")
    in_df = in_df.withColumnRenamed("DerivativesExposure", "largest_derivatives_exposure")
    in_df = in_df.withColumnRenamed("Scenario", "scenario")
    in_df = in_df.withColumnRenamed("Member", "member")
    in_df = in_df.withColumnRenamed("AvailableResources", "available_resources")
    in_df = in_df.withColumnRenamed("Futuresexposure", "futures_exposure")
    in_df = in_df.withColumnRenamed("Optionsexposure", "options_exposure")
    in_df = in_df.withColumn("day_dmp", lit(in_day_dmp))

    ## Format columns
    ## Force conversion to columns
    for field in table_schema.fields:
        if field.name in in_df.columns:
            in_df = in_df.withColumn(field.name, col(field.name).cast(field.dataType))
   
    return in_df

# COMMAND ----------

def excel_file_to_dataframe(in_path_to_file : str, in_sheet_name : str, in_cel_to_from : str) -> DataFrame:
    binary_df = spark.read.format("binaryFile").load(in_path_to_file)
    file_content = binary_df.select("content").first()[0]
    excel_file = BytesIO(file_content)
    df = pd.read_excel(excel_file, sheet_name=in_sheet_name, usecols=in_cel_to_from, header=0)
    df.rename(columns={col: col.replace(' ', '') for col in df.columns}, inplace=True)    
    
    return df

# COMMAND ----------

def excel_to_df_multiple_sheets(in_path_to_file : str, in_sheet_name : str, in_cel_to_from : str, in_header : int, axis_sheet_name : str) -> DataFrame:
    
    binary_df = spark.read.format("binaryFile").load(in_path_to_file)
    file_content = binary_df.select("content").first()[0]
    excel_file = BytesIO(file_content)
    dfs = pd.read_excel(excel_file, sheet_name=in_sheet_name, usecols=in_cel_to_from, header=in_header)
    df = (pd.concat(dfs).reset_index(level=1, drop=True).rename_axis(axis_sheet_name).reset_index())
    return df

# COMMAND ----------

def delete_existing_table_data(table_name:str):
    sql_script = f"delete from {table_name} where file_date='{file_date_str}'"
    spark.sql(sql_script)

# COMMAND ----------

## Read files from Raw path
raw_full_path = f"abfss://{data_container}@{storage_name}.dfs.core.windows.net/{raw_path}"

all_files = dbutils.fs.ls(raw_full_path)

print(all_files)

if table_name == "" or table_name == None:
    delete_existing_table_data("tbl_tmx_cdcc_collateral"); 
    delete_existing_table_data("tbl_tmx_cdcc_vm"); 
    delete_existing_table_data("tbl_tmx_cdcc_members"); 
    delete_existing_table_data("tbl_tmx_cdcc_margins"); 
    delete_existing_table_data("tbl_tmx_cdcc_positions"); 
    delete_existing_table_data("tbl_tmx_cdcc_liquidity_data");   
else:
    delete_existing_table_data(table_name);

for file in all_files:
    if(file.isDir()):
        ## import data from direcory
        all_sub_files = dbutils.fs.ls(file.path)
        for sub_file in all_sub_files:

            sub_file_name = sub_file.name
            sub_file_path = sub_file.path

            if(sub_file_name.startswith("Margin")):
                if (table_name == "" or table_name == None or table_name == "tbl_tmx_cdcc_margins"):
                    ret_df = import_margin_file(sub_file_path, sub_file_name, tmx_cdcc_margins_table_schema)                
                    margin_table = "tbl_tmx_cdcc_margins"
                    full_qualified_table_name = f"{std_cat_schema}.{margin_table}"

                    ## Save dataset to UC table
                    print("Saving " + str(ret_df.count()) + " items to table " + margin_table + " from file: " + sub_file_name)
                    ret_df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(full_qualified_table_name)
                            
            elif(sub_file_name.startswith("Member")):
                if (table_name == "" or table_name == None or table_name == "tbl_tmx_cdcc_members"):
                    ret_df = import_members_file(sub_file_path, sub_file_name, tmx_cdcc_members_table_schema)
                    member_table = "tbl_tmx_cdcc_members"
                    full_qualified_table_name = f"{std_cat_schema}.{member_table}"                

                    ## Save dataset to UC table
                    print("Saving " + str(ret_df.count()) + " items to table " + member_table + " from file: " + sub_file_name)
                    ret_df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(full_qualified_table_name)

            elif(sub_file_name.startswith("Position")):
                if (table_name == "" or table_name == None or table_name == "tbl_tmx_cdcc_positions"):
                    ret_df = import_position_file(sub_file_path, sub_file_name, tmx_cdcc_positions_table_schema)
                    position_table = "tbl_tmx_cdcc_positions"
                    full_qualified_table_name = f"{std_cat_schema}.{position_table}"

                    ## Save dataset to UC table
                    print("Saving " + str(ret_df.count()) + " items to table " + position_table + " from file: " + sub_file_name)
                    ret_df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(full_qualified_table_name)
            
    else:
        file_name = file.name
        file_location = file.path

        ## Added NOT_LOAD_ to the file name to avoid LiquidityReport_YYYYMMDD.xlsx 
        if(file_name.startswith("NOT_LOAD_LiquidityReport")):            
            liq_sheet_name_1 = "1.3.1 Derivatives Exp Day_1 DMP"
            cel_from_to = "A:G"
            df1 = excel_file_to_dataframe(file_location, liq_sheet_name_1, cel_from_to)
            ## Convert pandas to spark dataframe
            df_1 = spark.createDataFrame(data=df1)
            df_liq_1 = import_liquidity_repport_file(df_1, "day_1", cdcc_liquidity_report_table_schema)
            
            liq_sheet_name_2 = "1.3.2 Derivatives Exp Day_2 DMP"
            cel_from_to = "A:G"
            df2 = excel_file_to_dataframe(file_location, liq_sheet_name_2, cel_from_to)
            ## Convert pandas to spark dataframe
            df_2 = spark.createDataFrame(data=df2)
            df_liq_2 = import_liquidity_repport_file(df_2, "day_2", cdcc_liquidity_report_table_schema)

            ## Join dataframes
            df_liq_final = df_liq_1.union(df_liq_2)
            
            ## Save Liquidity report
            liquidity_report_table_name = "tbl_tmx_cdcc_liquidity_report"
            full_qualified_table_name = f"{std_cat_schema}.{liquidity_report_table_name}"
            
            ## Save dataframe to UC table
            print("Saving " + str(df_liq_final.count()) + " items to table " + liquidity_report_table_name + " from file: " + file_name)
            df_liq_final.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(full_qualified_table_name)
            
        elif(file_name.startswith("Cash Settlement")):
            if (table_name == "" or table_name == None or table_name == "tbl_tmx_cdcc_vm"):
                vm_table_name = "tbl_tmx_cdcc_vm"
                full_qualified_table_name = f"{std_cat_schema}.{vm_table_name}"
                cash_set_sheet_name = None
                cel_from_to = "A:B"
                axis_sheet_name = "Date"
                df = import_cash_settlement_file(file_location, cash_set_sheet_name, cel_from_to, file_name, tmx_cdcc_vm_table_schema, axis_sheet_name)
                
                ## Clead std table before load new data
                print("Saving " + str(df.count()) + " items to table " + vm_table_name + " from file: " + file_name)
                df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(full_qualified_table_name)
        
        elif(file_name.startswith("CDCC_LiquidityData")):
            if (table_name == "" or table_name == None or table_name == "tbl_tmx_cdcc_liquidity_data"):
                CDCC_LiquidityData = "tbl_tmx_cdcc_liquidity_data"
                full_qualified_table_name = f"{std_cat_schema}.{CDCC_LiquidityData}"
                cel_from_to = "A:AH"
                CDCC_Liquidity_sheet_name = "Sheet1"
                df = import_cdcc_liquidity_data_file(file_location, CDCC_Liquidity_sheet_name, cel_from_to, file_name, tmx_cdcc_liquidity_data_table_schema)

                ## Save data to UC table
                print("Saving " + str(df.count()) + " items to table " + CDCC_LiquidityData + " from file: " + file_name)
                df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(full_qualified_table_name)

        elif(file_name.startswith("Collateral")):
            if (table_name == "" or table_name == None or table_name == "tbl_tmx_cdcc_collateral"):
                collateral_table = "tbl_tmx_cdcc_collateral"
                full_qualified_table_name = f"{std_cat_schema}.{collateral_table}"
                cel_from_to = "A:Q"
                collateral_sheet_name = "Sheet1"                
                df = import_collateral_file(file_location, collateral_sheet_name, cel_from_to, file_name, tmx_cdcc_collateral_table_schema)

                ## Save dataset to UC table
                print("Saved " + str(df.count()) + " items to table " + collateral_table + " from file: " + file_name) 
                df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(full_qualified_table_name)