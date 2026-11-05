# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC _This is the initial deployment of Kaiko orderbook pipeline._
# MAGIC
# MAGIC * Tasks: 
# MAGIC   * create tbl_kaiko_orderbook table
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC * Dependency:
# MAGIC   * NA

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")
dbutils.widgets.text("replace_if_exist", "false")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")
print(f"environment: {environment}")
replace_if_exist = dbutils.widgets.get("replace_if_exist")
print(f"replace_if_exist: {replace_if_exist}")

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../common/utils

# COMMAND ----------

# Environment
if len(environment) == 0:
    environment = get_env()
print(f"environment: {environment}")

# Unity Catalog name
unity_catalog_name = get_crypto_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")

# COMMAND ----------

# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_kaiko_orderbook"

# COMMAND ----------

# DBTITLE 1,Create standardized tables
# since replace_if_exist is false by defaut, table will not be replaced
create_table_sql = f"""
{'CREATE OR REPLACE TABLE' if replace_if_exist.lower() == 'true' else 'CREATE TABLE IF NOT EXISTS'} {table_name} (
    exchange string,
    symbol string,
    date timestamp,
    type string,
    price decimal(20,8),
    amount decimal(20,8),
    audit_timestamp timestamp,
    abs_file_path string,
    rel_file_path string
)
USING DELTA
PARTITIONED BY (exchange)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
)
"""

# Execute the SQL command to create the table
print(f"Creating table: {create_table_sql}")
spark.sql(create_table_sql)

# COMMAND ----------

# DBTITLE 1,ZORDER
# Run OPTIMIZE command
optimize_sql = f"""
OPTIMIZE {table_name} ZORDER BY (rel_file_path, date)
"""

# Execute the SQL command
print(f"Optimizeing table: {optimize_sql}")
spark.sql(optimize_sql)
