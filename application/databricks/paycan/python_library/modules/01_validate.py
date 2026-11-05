# Databricks notebook source
from typing import List
from pyspark.sql.dataframe import *
from pathlib import Path
import re
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import *
from collections import OrderedDict 
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
from functools import reduce

# COMMAND ----------

class FileSystemValidation():
    def __init__(self, path : str):
        self.path = path        
        if not self._is_valid_path():
            raise Exception(f"invalid path: {self.path}")

    def _ls(self, path: str, recursive : bool = False) -> List:
        folder_listing = dbutils.fs.ls(path)
        if recursive:
            folder_listing = list(map(lambda x:x.path, folder_listing))
            column_header = Row('filename')
            df = spark.createDataFrame([column_header(x) for x in folder_listing])
            folder_listing = df.select('filename').rdd.flatMap(lambda x:x).collect()
        return folder_listing

    def _is_valid_path(self) -> bool:
        try:
            self._ls(self.path)
        except Exception as e:
            if 'java.io.FileNotFoundException' in str(e):
                return False
            else:
                raise e        
        return True

# COMMAND ----------

class FileValidation(FileSystemValidation):
    def get_item_count(self, recursive : bool = False) -> int:
        return len(self.get_file_name(recursive))
    
    def validate_item_count(self, recursive : bool = False, valid_item_count : int = 0) -> bool:
        """
        checks the expected number of file(s) in a directory, raises an exception if the integer count between the expected and actual don't match

        PARAMS:
        recursive: boolean value of True to iterate through the sub-directory, and False if otherwise
        valid_item_count (integer) :: expected number of file(s), default value of 0
        """
        item_count = self.get_item_count(recursive)        
        if item_count != valid_item_count:
            raise Exception(f"Invalid count; Expected {valid_item_count}, but received {item_count}")           
        return True
    
    def get_file_extension(self, recursive : bool = False) -> List[str]:
        folder_listing = self._ls(self.path, recursive)
        column_header = Row('filename')
        df = spark.createDataFrame([column_header(x) for x in folder_listing])
        df = df.withColumn('filename', substring_index('filename', '.', -1))
        folder_listing = df.select('filename').rdd.flatMap(lambda x: x).distinct().collect()
        return folder_listing
    
    def validate_file_extension(self, recursive : bool = False, valid_file_extension : List[str] = None) -> bool:
        """
        checks the expected file extension(s) in a directory, raises an exception if the list of file extension(s) between the expected and actual don't match

        PARAMS:
        recursive: boolean value of True to iterate through the sub-directory, and False if otherwise
        valid_file_extension (list) :: list of expected file extension(s)
        """
        file_extension = self.get_file_extension(recursive)        
        if file_extension != valid_file_extension:
            raise Exception(f"Invalid file extension; Expected {valid_file_extension}, but received {file_extension} from the vendor")            
        return True        
    
    def get_file_name(self, recursive: bool = False) -> List:
        folder_listing = self._ls(self.path, recursive)
        column_header = Row('filename')
        df = spark.createDataFrame([column_header(x) for x in folder_listing])
        df = df.withColumn('filename', substring_index('filename', '/', -1))
        folder_listing = df.select('filename').rdd.flatMap(lambda x: x).collect()
        return folder_listing
    
    def validate_file_name(self, recursive : bool = False, valid_file_name : List[str] = None) -> bool:
        """
        checks the expected file name(s) in a directory, raises an exception if the list of file name(s) between the expected and actual don't match

        PARAMS:
        recursive: boolean value of True to iterate through the sub-directory, and False if otherwise
        valid_file_name (list) :: list of expected file name(s)
        """
        file_name = self.get_file_name(recursive)        
        if file_name != valid_file_name:
            raise Exception(f"Invalid file name; Expected {valid_file_name}, but received {file_name} from the vendor")            
        return True
    
    def validate_file_name_regex(self, recursive : bool = False, regex_pattern : List[str] = None) -> bool:
        """
        checks the expected file name regex in a directory, raises an exception if the list of file name regex between the expected and actual don't match

        PARAMS:
        recursive: boolean value of True to iterate through the sub-directory, and False if otherwise
        regex_pattern (list) :: list of expected file name regex
        """
        file_name = self.get_file_name(recursive)
        df = spark.createDataFrame(file_name, StringType()).toDF('file_name')
        starts_with = reduce(lambda x, y: x | y, [col('file_name').startswith(s) for s in regex_pattern])
        df = df.where(starts_with)       
        if(df.count() == 0):
            raise Exception(f"Invalid file names not matching regex; {regex_pattern}")            
        return True

    def get_folder_name(self, recursive: bool = False) -> List:
        folder_listing = self._ls(self.path, recursive)
        column_header = Row('filename')
        df = spark.createDataFrame([column_header(x) for x in folder_listing])
        df = df.withColumn('filename', substring_index('filename', '/', -2))
        df = df.withColumn('filename', substring_index('filename', '/', 1))
        folder_listing = df.select('filename').rdd.flatMap(lambda x:x).collect()
        return folder_listing
    
    def validate_folder_name(self, recursive : bool = False, valid_folder_name : List[str] = None) -> bool:
        """
        checks the expected folder name(s) in a directory, raises an exception if the list of folder name(s) between the expected and actual don't match

        PARAMS:
        recursive: boolean value of True to iterate through the sub-directory, and False if otherwise
        valid_folder_name (list) :: list of expected folder name(s)
        """
        folder_name = self.get_folder_name(recursive)
        if folder_name != valid_folder_name:
            raise Exception(f"Invalid folder name; Expected {valid_folder_name}, but received {folder_name}")    
        return True

# COMMAND ----------

class DataValidation():
    def __init__(self, df : DataFrame):
        self.df = df
    
    def validate_empty_file(self) -> bool:
        """
        checks if the dataframe is empty, raises exception if it is empty
        """
        if self.df.count() == 0:
            raise Exception("the file(s) is/are empty")

    def count_columns(self) -> int:
        return len(self.df.columns)

    def validate_col_count(self, expected_col_count : int) -> bool:
        """
        checks the expected number of columns in the dataframe, raises exception if number of columns between the expected and actual don't match

        PARAMS:
        expected_col_count (integer) :: expected number of column(s) in the dataframe
        """
        col_count = self.count_columns()
        if col_count != expected_col_count:
            raise Exception(f"Invalid column count; Expected {expected_col_count} columns, but received {col_count}")            
        return True
    
    def validate_column_headers(self, valid_header_list : List[str] = None) -> bool:
        """
        checks the expected column headers in the dataframe, raises exception if column headers between the expected and actual don't match

        PARAMS:
        valid_header_list (list) :: list of expected column header(s)
        """
        header_list = []
        for col_name, col_type in self.df.dtypes:
            header_list.append(col_name)        
        if header_list != valid_header_list:
            raise Exception(f"Invalid column headers; Expected {valid_header_list}, but received {header_list} from the vendor")           
        return True
            
    def validate_unique_identifier(self, unique_identifier_list : List[str] = None) -> bool:
        """
        checks if the column(s) is/are unique, raises exception if duplicates are found

        PARAMS:
        unique_identifier_list (list) :: list of column(s) to check for uniqueness
        """
        distinct_row_count = self.df.select(unique_identifier_list).distinct().count()
        row_count = self.df.count()
        if row_count != distinct_row_count:
            raise Exception(f"{row_count - distinct_row_count} duplicate(s) found in the unique identifier, {unique_identifier_list}")        
        return True
           
    def validate_float_column(self, column_name : str) -> bool:
        """
        checks if the specified column is of float type, raises exception if it isn't

        PARAMS:
        column_name (string) :: column to check for float type
        """
        try:
            self.df[column_name]
            temp_df = self.df.filter(col(column_name).isNotNull())
            temp_df = temp_df.withColumn(column_name, temp_df[column_name].cast('Float'))
            invalid_row_count = temp_df.filter(col(column_name).isNull()).count()
        except Exception as e:
            raise Exception(f"Float validation failed with error: {e}")
        if invalid_row_count>0:
            raise Exception(f"Invalid float format for the column {column_name}")            
        return True

    def validate_int_column(self, column_name : str) -> bool:
        """
        checks if the specified column is of integer type, raises exception if it isn't

        PARAMS:
        column_name (string) :: column to check for integer type
        """
        try:
            self.df[column_name]
            temp_df = self.df.filter(col(column_name).isNotNull())
            temp_df = temp_df.withColumn(column_name, temp_df[column_name].cast('Integer'))
            invalid_row_count = temp_df.filter(col(column_name).isNull()).count()
        except Exception as e:
            raise Exception(f"Integer validation failed with error: {e}")
        if invalid_row_count>0:
            raise Exception(f"Invalid integer format for the column {column_name}")            
        return True

    def validate_long_column(self, column_name : str) -> bool:
        """
        checks if the specified column is of long type, raises exception if it isn't

        PARAMS:
        column_name (string) :: column to check for long type
        """
        try:
            self.df[column_name]
            temp_df = self.df.filter(col(column_name).isNotNull())
            temp_df = temp_df.withColumn(column_name, temp_df[column_name].cast('Long'))
            invalid_row_count = temp_df.filter(col(column_name).isNull()).count()
        except Exception as e:
            raise Exception(f"Long validation failed with error: {e}")
        if invalid_row_count>0:
            raise Exception(f"Invalid long format for the column {column_name}")            
        return True

    def validate_date_column(self, column_name : str, date_format : str) -> bool:
        """
        checks if the specified column is of the expected date format, raises exception if it isn't

        PARAMS:
        column_name (string) :: column to check for date type
        date_format (string) :: expected date format, i.e. 'yyyy-MM-dd'
        """
        old_time_parser_policy = spark.conf.get("spark.sql.legacy.timeParserPolicy")
        try:
            spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
            self.df[column_name]
            temp_df = self.df.filter(col(column_name).isNotNull())
            temp_df = temp_df.select(col(column_name), to_date(col(column_name), date_format).alias("__TEMP_DATE__"))
            invalid_row_count = temp_df.filter(col("__TEMP_DATE__").isNull()).count()
        except Exception as e:
            raise Exception(f"Date validation failed with error: {e}")
        finally:
            spark.conf.set("spark.sql.legacy.timeParserPolicy", old_time_parser_policy)
        if invalid_row_count > 0:
            raise Exception(f"Invalid date validation for the column {column_name}")
        return True
    
    def validate_timestamp_column(self, column_name : str, timestamp_format : str) -> bool:    
        """
        checks if the specified column is of the expected timestamp format, raises exception if it isn't

        PARAMS:
        column_name (string) :: column to check for timestamp type
        timestamp_format (string) :: expected timestamp format, i.e. 'yyyy-MM-dd HH:mm:ss'
        """
        old_time_parser_policy = spark.conf.get("spark.sql.legacy.timeParserPolicy")
        try:
            spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
            self.df[column_name]
            temp_df = self.df.filter(col(column_name).isNotNull())
            temp_df = temp_df.select(col(column_name), to_timestamp(col(column_name), timestamp_format).alias("__TEMP_TIMESTAMP__"))
            invalid_row_count = temp_df.filter(col("__TEMP_TIMESTAMP__").isNull()).count()
        except Exception as e:
            raise Exception(f"Date validation failed with error: {e}")
        finally:
            spark.conf.set("spark.sql.legacy.timeParserPolicy", old_time_parser_policy)
        if invalid_row_count > 0:
            raise Exception(f"Invalid timestamp validation for the column {column_name}")
        return True
      
    def validate_range(self, column_name : str, min : int = None, max : int = None):
        """
        checks if the specified column is of the expected numerical range, raises exception if it isn't

        PARAMS:
        column_name (string) :: column to check for timestamp type
        min (integer) :: expected minimum integer value
        max (integer) :: expected maximum integer value
        """
        def range_func(x : int, min : int, max : int) -> int:
            if not (max >= x >= min):
                raise Exception(f"{x} does not fall between {max} and {min}")
            return True            
        udf_range = udf(lambda x, min, max : range_func(x, min, max), IntegerType())        
        try:
            self.df[column_name]
            self.df.select(column_name, udf_range(column_name, lit(min), lit(max))).collect()
        except Exception as e:
            raise Exception(f"Invalid numerical range; The column {column_name} is not within the range of {min} and {max}")            
        return True