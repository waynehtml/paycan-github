# Databricks notebook source
!pip install fuzzywuzzy python-Levenshtein

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import max as _max, udf, col, lit, count, sum as _sum, avg, min as _min, round as _round, sha2, row_number, rank, dense_rank, percent_rank, ntile, cume_dist, lag, lead, concat, concat_ws, when
from typing import List
from cryptography.fernet import Fernet
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from fuzzywuzzy import fuzz
from pyspark.sql.column import Column

# COMMAND ----------

class Curated():
    df : DataFrame = None
    window_spec : Window = None

    def __init__(self, df : DataFrame):
        self.df = df

    def concatenate_column(self, new_column_name : str = None, column_list : List[str] = [], separator : str = None):
        """
        appends two or more columns into a new column

        PARAMS:
        new_column_name (string) :: new column name
        column_list (list) :: list of columns to append
        separator (string) :: words separator
        """
        column_list = [col(i) for i in column_list]
        if separator:
            self.df = self.df.select(concat_ws(separator, *column_list).alias(new_column_name), "*")
        else:
            self.df = self.df.select(concat(*column_list).alias(new_column_name), "*")
        return self.df

    def __dedupe_param_verify(self, **kwargs):
        valid_keep_str = ['first', 'last']
        if not isinstance(kwargs['primary_keys'], list):
            raise Exception(f"primary keys parameter must be passed as a list")        
        if not kwargs['keep'] in valid_keep_str:
            raise Exception(f"invalid parameter for 'keep', valid entries are {valid_keep_str}")
        if not kwargs['partition_col'] in self.df.columns:
            raise Exception(f"partition column {kwargs['partition_col']} not in dataframe: {self.df.columns}")

    def dedupe(self, primary_keys : List[str], keep : str = None, partition_col : str = None) -> DataFrame:
        """
        drops duplicated rows from the dataframe

        PARAMS:
        primary_keys (list) :: list of primary/composite keys for dropping
        keep (string) :: available options
                         (1) 'first'
                         (2) 'last'
                         determining whether to keep the first or the last instance of the data
        partition_col (string) :: partition column to determine which duplicate to keep
        """
        self.__dedupe_param_verify(primary_keys=primary_keys, keep=keep, partition_col=partition_col)
        if keep == "first":
            keep = "ASC"
        elif keep == "last":
            keep = "DESC"
        primary_keys = ", ".join(primary_keys)
        self.df.createOrReplaceTempView("tmp")
        sql = f"""
        SELECT * FROM (
            SELECT *, 
                    ROW_NUMBER() OVER (PARTITION BY {primary_keys} ORDER BY {partition_col} {keep}) as rn
            FROM tmp
        ) as sq
        """
        temp_df = spark.sql(sql)
        return_df = temp_df.filter(temp_df.rn > 1)
        return_df = return_df.drop("rn")
        self.df = temp_df.filter(temp_df.rn == 1)
        self.df = self.df.drop("rn")
        return self.df

    def __obfuscate_param_verify(self, **kwargs):
        valid_encryption_types = ['md5']
        if not kwargs['encryption_type'] in valid_encryption_types:
            raise Exception(f"invalid encryption type passed {kwargs['encryption_type']}, valid encryption types: {valid_encryption_types}")

    def __md5_encryption(self, column : str, salt : str = None):
        if salt is None:
            salt = ""       
        self.df = self.df.withColumn(column, sha2(concat(lit(salt), column), 256))
        return self.df

    def obfuscate(self, column : str = None, encryption_type : str = None, encryption_key : str = None) -> DataFrame:
        """
        obfuscates specified column in a dataframe

        PARAMS:
        column (string) :: column to be obfuscated
        encryption_type (string) :: only md5 available at the moment
        encryption_key (string) :: will be random if not specified
        """
        self.__obfuscate_param_verify(column = column, encryption_type = encryption_type, encryption_key = encryption_key)
        if encryption_type == 'md5':
            return self.__md5_encryption(column = column, salt = encryption_key)
    
    def __aggregate_param_verify(self, **kwargs):
        valid_aggregate_types = ['min', 'max', 'average', 'sum', 'count']
        if not kwargs['agg_type'] in valid_aggregate_types:
            raise Exception(f"given aggregate type {kwargs['agg_type']} is not in valid list: {valid_aggregate_types}")

    def aggregate(self, column_name : str = None, agg_type : str = None) -> DataFrame:
        """
        aggregates specified column in a dataframe

        PARAMS:
        column (string) :: column to be aggregated on
        agg_type (string) :: available aggregation methods
                            (1) 'min'
                            (2) 'max'
                            (3) 'average'
                            (4) 'sum'
                            (5) 'count'
        """
        self.__aggregate_param_verify(agg_type=agg_type)
        if agg_type == 'count':
            return self.df.groupBy(column_name).agg(count('*').alias("count"))
        elif agg_type == 'sum':
            return self.df.select(_sum(column_name))
        elif agg_type == 'average':
            return self.df.select(avg(column_name))
        elif agg_type == 'max':
            return self.df.select(_max(column_name))
        elif agg_type == 'min':
            return self.df.select(_min(column_name))

    def __filter_param_verify(self, **kwargs):
        valid_operator_list = ['==', '>', '<', '>=', '<=']
        if not kwargs['operator'] in valid_operator_list:
            raise Exception(f"invalid range type passed: {kwargs['operator']}, valid ranges: {valid_operator_list}")

    def filter(self, column : str = None, operator : str = None, value : str = None) -> DataFrame:
        """
        filters on specified column in a dataframe

        PARAMS:
        column (string) :: column to be filtered on
        operator (string) :: available operators
                             (1) '=='
                             (2) '>'
                             (3) '<'
                             (4) '>='
                             (5) '<='
        value (string) :: value within valid range
        """
        self.__filter_param_verify(column=column, operator=operator, value=value)
        if operator == '==':
            return self.df.filter(col(column) == value)
        elif operator == '>':
            return self.df.filter(col(column) > value)
        elif operator == '<':
            return self.df.filter(col(column) < value)
        elif operator == '>=':
            return self.df.filter(col(column) >= value)
        elif operator == '<=':
            return self.df.filter(col(column) <= value)
        else:
            raise Exception(f"unhandled operator {operator}")

    def fuzzy(s1, s2):
        return fuzz.token_sort_ratio(s1,s2)
    
    def fuzzy_match(self, column_name : str = None, similarity : int = 80, literal_match : str = None, column_match : str = None, keep_similarity : bool = False) -> DataFrame:
        """
        fuzzy matches on specified column or string value in a dataframe

        PARAMS:
        column (string) :: source column to fuzzy match on
        similarity (integer) :: default value is score of 80 or above if not specified otherwise
        literal_match (string) :: string value to be fuzzy matched on the parameter 'column'
        column_match (string) :: target column to fuzzy match on
        keep_similarity (boolean) :: if True, then an exact similarity score will be calculated
        """
        FuzzyUDF = udf(Curated.fuzzy, StringType())
        df = self.df
        if literal_match:
            df = df.withColumn("similarity_score", FuzzyUDF(col(column_name), lit(literal_match)))
            df = df.filter(df.similarity_score > similarity)
            if not keep_similarity:
                df = df.drop("similarity_score")
        if column_match:
            df = df.withColumn("similarity_score", FuzzyUDF(col(column_name), col(column_match)))
            df = df.filter(df.similarity_score > similarity)
            if not keep_similarity:
                df = df.drop("similarity_score")
        return df
    
    def __set_moving_window_param_verify(self, **kwargs):
        valid_range_types = ['days', 'seconds', 'months', 'weeks', 'years', 'minutes', 'hours']
        if not kwargs['range_type'] in valid_range_types:
            raise Exception(f"invalid range type passed: {kwargs['range_type']}, valid ranges: {valid_range_types}")
    
    def _convert_range_to_int(self, range_type : str, range_value : int):
        if range_type == "seconds":
            multiplier = 1
        elif range_type == "minutes":
            multiplier = 60
        elif range_type == "hours":
            multiplier = 1440
        elif range_type == "days":
            multiplier = 86400
        elif range_type == "weeks":
            multiplier = 604800
        elif range_type == "months":
            multiplier = 2629744
        elif range_type == "years":
            multiplier = 31556929
        else:
            raise Exception(f"range not converted to numerical")
        return multiplier * range_value

    def set_moving_window(self, partition_col : str = None, timestamp_col : str = None, range_type : str = None, range_value : int = None):
        """
        sets the moving window based on partition and timestamp columns
        to be used with window_function to specify the type of function

        PARAMS:
        partition_col (string) :: column to partition by
        timestamp_col (string) :: column that specifies the timestamp on which the window's range is created
        range_type (string) :: available range types
                               (1) 'seconds'
                               (2) 'minutes'
                               (3) 'hours'
                               (4) 'days'
                               (5) 'weeks'
                               (6) 'months'
                               (7) 'years'
        range_value (integer) :: the length of the starting numerical range
        """
        self.__set_moving_window_param_verify(range_type = range_type)
        range_start = -self._convert_range_to_int(range_type, range_value)
        self.window_spec = Window.partitionBy(partition_col).orderBy(col(timestamp_col).cast('long')).rangeBetween(range_start, 0)

    def set_fixed_window(self, partition : str = None, order : str = None):
        """
        sets the fixed window based on partition and order column

        PARAMS:
        partition (string) :: column to partition by
        order (string) :: column to order by
        """
        self.window_spec = Window.partitionBy(partition).orderBy(order)

    def __check_window_set(self):
        if self.window_spec is None:
            raise Exception(f"set window spec first with 'set_fixed_window' or 'set_moving_window'")

    def __window_function_param_verify(self, **kwargs):
        valid_function_names = ['row_number', 'rank', 'dense_rank', 'percent_rank', 'ntile', 'cume_dist', 'lag', 'lead', 'average']
        if not kwargs['function'] in valid_function_names:
            raise Exception(f"invalid function type passed: {kwargs['function']}, valid functions: {valid_function_names}")

    def window_function(self, new_col_name : str = None, function : str = None, n : int = None, column : str = None):
        """
        calls the window_function only after specifying the use of either set_moving_window or set_fixed_window in 2 options
        (1) curated.set_moving_window()
            curated.window_function()
        (2) curated.set_fixed_window()
            curated.window_function()

        PARAMS:
        new_col_name (string) :: column to be populated with the calculated values
        function (string) :: available functions
                             (1) 'row_number' : assigns a sequential integer to each row within the partition of the data
                             (2) 'rank' : assigns a rank to each row within the partition of the data
                             (3) 'dense_rank' : assigns a rank to each row within the partition of the data with no gaps in the ranking values
                             (4) 'percent_rank' : calculates a percent rank for each row using the formula (rank of current row - 1)/(number of rows in window partition -1)
                             (5) 'ntile' : assigns a bucket number to each row representing the group in which it belongs to
                             (6) 'cume_dist' : calculates a cumulative distribution value for each row using the formula (number of rows with value less than or equal to the current row value)/(number of rows in the partition of the data)
                             (7) 'lag' : provides access to a row at a specified physical offset that comes before the current row
                             (8) 'lead' : provides access to a row at a specified physical offset that follows the current row
                             (9) 'average' : calculates an average for each row within the partition of the data
        n (integer) :: needs to be populated when using functions 
                       (1) 'ntile'
                       (2) 'lag'
                       (3) 'lead'
                       where the default value is 1
        column (string) :: needs to be populated when using functions 
                           (1) 'lag'
                           (2) 'lead'
                           (3) 'average'
        """
        self.__window_function_param_verify(new_col_name=new_col_name, function=function)
        self.__check_window_set()
        if function == 'row_number':
            return self.w_row_number(new_col_name)
        elif function == 'rank':
            return self.w_rank(new_col_name)
        elif function == 'dense_rank':
            return self.w_dense_rank(new_col_name)
        elif function == 'percent_rank':
            return self.w_percent_rank(new_col_name)
        elif function == 'ntile':
            if n is None:
                raise Exception(f"NTILE function requires passing in tile number as 'n=x'")
            return self.w_ntile(new_col_name, n)
        elif function == 'cume_dist':
            return self.w_cume_dist(new_col_name)
        elif function == 'lag':
            if n is None:
                raise Exception(f"LAG function requires passing in lag number as 'n=x'")
            if column is None:
                raise Exception(f"LAG function requires passing in column to analyze as 'column=xxxx'")
            return self.w_lag(new_col_name, n, column)
        elif function == 'lead':
            if n is None:
                raise Exception(f"LEAD function requires passing in lead number as 'n=x'")
            if column is None:
                raise Exception(f"LEAD function requires passing in column to analyze as 'column=xxxx'")
            return self.w_lead(new_col_name, n, column)
        elif function == 'average':
            if column is None:
                raise Exception(f"LEAD function requires passing in column to analyze as 'column=xxxx'")
            return self.w_avg(new_col_name, column)

    def w_row_number(self, new_col_name : str = "row_number"):
        self.df = self.df.withColumn(new_col_name, row_number().over(self.window_spec))
        return self.df

    def w_rank(self, new_col_name : str = "rank"):
        self.df = self.df.withColumn(new_col_name, rank().over(self.window_spec))
        return self.df

    def w_dense_rank(self, new_col_name : str = "dense_rank"):
        self.df = self.df.withColumn(new_col_name, dense_rank().over(self.window_spec))
        return self.df

    def w_percent_rank(self, new_col_name : str = "percent_rank"):
        self.df = self.df.withColumn(new_col_name, percent_rank().over(self.window_spec))
        return self.df

    def w_ntile(self, new_col_name : str = "ntile", n : int = 1):
        self.df = self.df.withColumn(new_col_name, ntile(n).over(self.window_spec))
        return self.df

    def w_cume_dist(self, new_col_name : str = "cume_dist"):
        self.df = self.df.withColumn(new_col_name, cume_dist().over(self.window_spec))
        return self.df

    def w_lag(self, new_col_name : str = "lag", n : int = 1, column : str = None):
        self.df = self.df.withColumn(new_col_name, lag(column, n).over(self.window_spec))
        return self.df

    def w_lead(self, new_col_name : str = "lead", n : int = 1, column : str = None):
        self.df = self.df.withColumn(new_col_name, lead(column, n).over(self.window_spec))
        return self.df

    def w_avg(self, new_col_name : str = 'average', column : str = None):
        self.df = self.df.withColumn(new_col_name, avg(column).over(self.window_spec))
        return self.df

# COMMAND ----------

class Derived():
    df : DataFrame = None

    def __init__(self, df : DataFrame):
        self.df = df

    def merge(self, df : DataFrame) -> DataFrame:
        """
        merges two dataframes

        PARAMS:
        df (dataframe) :: dataframe to merge with
        """
        self.df = self.df.unionByName(df, allowMissingColumns=True)
        return self.df

    def join(self, df : DataFrame, matching_column : Column, type_of_join : str) -> DataFrame:
        """
        joins another dataframe based on conditions and a given function

        PARAMS:
        df (dataframe) :: dataframe to join
        matching_column (Column) :: a join expression df1.column == df2.column
        type_of_join (string) :: availble join types 
                                (1) 'inner' : joins two dataframes where rows get dropped if the values don't match in both datasets
                                (2) 'outer' : returns all rows in both dataframe
                                (3) 'left' : returns all rows from the left dataframe regardless of match found in the right dataframe
                                (4) 'right' : returns all rows from the right dataframe regardless of the match found in the left dataframe
                                (5) 'anti' : returns un-matched records between two dataframes
                                (6) 'leftanti' : returns only columns from the left dataframe for non-matched records
                                (7) 'leftsemi' : returns all columns from the left dataframe and ignores all columns from the right dataframe
        """
        return self.df.join(df, matching_column, type_of_join)
    
    def concatenate(self, df : DataFrame):
        """
        appends two dataframes into one

        PARAMS:
        df (dataframe) :: second dataframe to be concatenated to the first
        """
        self.df = self.df.unionByName(df)
        return self.df