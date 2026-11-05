# Databricks notebook source
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.types import IntegerType, LongType, DoubleType, StringType, DateType, TimestampType

# COMMAND ----------

class Standardize():
    def __init__(self, df : DataFrame):
        self.df = df

    def add_file_name(self, column : str) -> DataFrame:
        """
        adds an ingestion file name to the dataframe as new column

        PARAMS:
        column (string) :: new column name
        """
        self.df = self.df.withColumn(column, input_file_name())
        self.df = self.df.withColumn(column, substring_index(column, '/', -1))
        self.df = self.df.withColumn(column, substring_index(column, '.', 1))    
        return self.df

    def add_ingestion_timestamp(self, column : str) -> DataFrame:
        """
        adds an ingestion timestamp to the dataframe as a new column

        PARAMS:
        column (string) :: new column name
        """
        self.df = self.df.withColumn(column, F.current_timestamp())
        return self.df

    def trim_spaces(self) -> DataFrame:
        """
        trims spaces from every column in the dataframe
        """
        self.df = self.df.select([trim(col(c)).alias(c) for c in self.df.columns])
        return self.df

    def stn_column_lowercase(self) -> DataFrame:
        """
        converts all column headers in dataframe to lowercase
        """
        self.df = self.df.toDF(*[c.lower() for c in df.columns])
        return self.df

    def rename_columns(self, column_dict : dict) -> DataFrame:
        """
        renames specified column header(s) in a dataframe

        PARAMS:
        column_dict (dictionary) :: dictionary that specifies how to rename the column headers i.e. column_dict = { "column_name_1" : "new_column_name_1", "column_name_2" : "new_column_name_2" }
        """
        for column in column_dict:
            self.df = self.df.withColumnRenamed(column, column_dict.get(column))
        return self.df

    def replace_empty_value(self) -> DataFrame:
        """
        replaces empty values in dataframe with null values
        """
        self.df = self.df.select([when(col(c)=='', None).otherwise(col(c)).alias(c) for c in self.df.columns])
        return self.df

    def extract_characters(self, column : str, index_a : int, index_b : int) -> DataFrame:
        """
        extracts substring from a column by specified indexes

        PARAMS:
        column (string) :: column to extract the substring from
        index_a (integer) :: starting index
        index_b (integer) :: ending index
        """
        self.df[column]
        self.df = self.df.withColumn(column, col(column).substr(index_a, index_b))
        return self.df

    def stn_date(self, column : str, source_format : str) -> DataFrame:
        """
        ensure all date types in column are of specified format

        PARAMS:
        column (string) :: column of date type
        source_format (string) :: expected raw date format i.e. "%Y%m%d"
        """
        self.df[column]
        date_func = udf(lambda x: datetime.strptime(x, source_format) if x is not None else None, DateType())
        self.df = self.df.withColumn(column, date_func(col(column)))
        return self.df

    def stn_timestamp(self, column : str) -> DataFrame:
        """
        standardizes timestamp type column in the dataframe

        PARAMS:
        column (string) :: column of timestamp type
        """
        self.df[column]
        self.df = self.df.withColumn(column, col(column).cast("timestamp"))
        return self.df

    def stn_double(self, column : str) -> DataFrame:
        """
        standardizes double type column in the dataframe

        PARAMS:
        column (string) :: column of double type 
        """
        self.df[column]
        float_func = udf(lambda x: float(x) if x is not None else None, DoubleType())
        self.df = self.df.withColumn(column, float_func(col(column)))
        return self.df

    def stn_int(self, column : str) -> DataFrame:
        """
        standardizes integer type column in the dataframe

        PARAMS:
        column (string) :: column of integer type 
        """
        self.df[column]
        int_func = udf(lambda x: int(x) if x is not None else None, IntegerType())
        self.df = self.df.withColumn(column, int_func(col(column)))
        return self.df

    def stn_long(self, column : str) -> DataFrame:
        """
        standardizes long column in Dataframe

        PARAMS:
        column (string) :: column of long type 
        """
        self.df[column]
        long_func = udf(lambda x: int(x) if x is not None else None, LongType())
        self.df = self.df.withColumn(column, long_func(col(column)))
        return self.df