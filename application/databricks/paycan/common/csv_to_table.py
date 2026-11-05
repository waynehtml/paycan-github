# Databricks notebook source
# Widget Parameters
dbutils.widgets.removeAll()
dbutils.widgets.text("file", "abfss://land-paycan@lakecacnpdstdevland01.dfs.core.windows.net/kaiko/kaiko_exchange.csv")
dbutils.widgets.text("header", "true")
dbutils.widgets.text("table", "dev_crypto_cat.standardized_db.tbl_kaiko_exchange")
dbutils.widgets.dropdown("mode", "overwrite", ["overwrite", "append"])

# Get parameters
file = dbutils.widgets.get("file")
print(f"file: {file}")
header = dbutils.widgets.get("header")
print(f"header: {header}")
table = dbutils.widgets.get("table")
print(f"table: {table}")
mode = dbutils.widgets.get("mode")
print(f"mode: {mode}")

# COMMAND ----------

# DBTITLE 1,Validate inputs
if len(file)==0:
    raise ValueError("file is required")
if len(header)==0:
    raise ValueError("header is required")
if len(table)==0:
    raise ValueError("table is required")
if len(mode)==0:
    raise ValueError("mode is required")

# COMMAND ----------

# Function to get schema from reference table
def get_schema(table_name):
    try:
        schema = spark.table(table_name).schema
        return schema
    except Exception as e:
        raise Exception(f"Error getting schema from {table_name}: {str(e)}")

# Function to read CSV with schema
def read_csv_with_schema(path, schema, header):
    try:
        df = spark.read \
            .option("header", header) \
            .option("inferSchema", "false") \
            .option("mode", "FAILFAST") \
            .schema(schema) \
            .csv(path)
        return df
    except Exception as e:
        raise Exception(f"Error reading CSV: {str(e)}")

# Function to write DataFrame to table
def write_to_table(df, table_name, mode):
    try:
        df.write \
            .mode(mode) \
            .saveAsTable(table_name)
    except Exception as e:
        raise Exception(f"Error writing to table: {str(e)}")



# COMMAND ----------

# MAGIC %md
# MAGIC # Main execution

# COMMAND ----------

# DBTITLE 1,Get schema from table
schema = get_schema(table)
print(f"schema: {schema}")


# COMMAND ----------

# DBTITLE 1,Read CSV
df = read_csv_with_schema(file, schema, header)
print(f"Successfully read data from {file}")
df.display()

# COMMAND ----------

# DBTITLE 1,Write to target table
write_to_table(df, table, mode)
print(f"Successfully wrote data to {table} in {mode} mode")

