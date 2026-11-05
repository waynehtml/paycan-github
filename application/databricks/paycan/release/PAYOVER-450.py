# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC _This is the initial deployment of Kaiko Trade pipeline._
# MAGIC
# MAGIC * Tasks: 
# MAGIC   * create tbl_kaiko_trade table
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC * Dependency:
# MAGIC   * CUDT-449 must be deployed first

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")
print(f"environment: {environment}")

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../common/utils

# COMMAND ----------

if len(environment) == 0:
    environment = get_env()
print(f"environment: {environment}")

# Unity Catalog name
unity_catalog_name = get_crypto_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")

# COMMAND ----------

# DBTITLE 1,Create standardized tables
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_kaiko_trade"
create_table_sql = f"""
CREATE OR REPLACE TABLE {table_name} (
    id string,
    exchange string,
    symbol string,
    date timestamp,
    price decimal(20,8),
    amount decimal(20,8),
    sell boolean,
    audit_timestamp timestamp,
    input_file_name string    
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.logRetentionDuration' = 'interval 5 day',
    'delta.deletedFileRetentionDuration' = 'interval 5 day',
    'delta.checkpointInterval' = '100'
)
"""

# Execute the SQL command to create the table
spark.sql(create_table_sql)
