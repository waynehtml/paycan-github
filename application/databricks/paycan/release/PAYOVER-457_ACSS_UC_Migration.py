# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook creates tables for isa and m7 standardized data **
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * None
# MAGIC * Steps:
# MAGIC   * run this notebook after code deployment
# MAGIC * Tasks: 
# MAGIC   * create table tbl_isa_reissnotes
# MAGIC   *  
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC   * replace - false or true, replace if tables exist

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")
dbutils.widgets.text("replace", "false")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")
print(f"environment: {environment}")
replace = dbutils.widgets.get("replace")
print(f"replace: {replace}")

# COMMAND ----------

# Get environment if not provided
if len(environment.strip()) == 0:
    environment = get_env()
print(f"environment: {environment}")

unity_catalog_name = get_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")
 

# COMMAND ----------

# DBTITLE 1,replace or skip if exists
# Define create action
create_action = 'CREATE OR REPLACE TABLE' if replace.lower() == 'true' else 'CREATE TABLE IF NOT EXISTS'
print(f"create_action: {create_action}")

# COMMAND ----------

# DBTITLE 1,tbl_paycan_acss_intraday
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_paycan_acss_intraday"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    system_code STRING,
    delivery_id STRING,
    cycle_date DATE,
    record_type STRING,
    settlement_date DATE,
    version_number STRING,
    sending_inst_irn STRING,
    sending_inst_name_en STRING,
    receiving_inst_irn STRING,
    receiving_inst_name_en STRING,
    region_code STRING,
    region_name STRING,
    sent_date DATE,
    sent_time STRING,
    payment_id STRING,
    stream_code STRING,
    stream_name STRING,
    crdb_type STRING,
    settle_type STRING,
    value DOUBLE,
    volume INT
) partitioned by (cycle_date)
 TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name',
    'delta.deletedFileRetentionDuration' = 'interval 60 days',
    'delta.logRetentionDuration' = 'interval 60 days'
);
"""
print(f"sql: {sql}")

# Execute the SQL command to create the table
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,tbl_paycan_acss_usbe_sets
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_paycan_acss_usbe_sets"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    system_code STRING,
    settlement_date DATE,
    sending_dc_irn STRING,
    sending_dc_name STRING,
    receiving_dc_irn STRING,
    receiving_dc_name STRING,
    sets_volume LONG,
    sets_value DECIMAL(30,2),
    acss_usbe_sets_id INT,
    batch_id INT
) partitioned by (file_date)
 TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name',
    'delta.deletedFileRetentionDuration' = 'interval 60 days',
    'delta.logRetentionDuration' = 'interval 60 days'
);
"""
print(f"sql: {sql}")

# Execute the SQL command to create the table
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,tbl_paycan_acss_usbe_vol_val
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_paycan_acss_usbe_vol_val"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    system_code STRING,
    settlement_date DATE,
    sending_dc_irn STRING,
    sending_dc_name STRING,
    receiving_dc_irn STRING,
    receiving_dc_name STRING,
    stream_code STRING,
    stream_name STRING,
    volume LONG,
    value DECIMAL(30,2),
    acss_usbe_vol_val_id INT,
    batch_id INT
) partitioned by (file_date)
 TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name',
    'delta.deletedFileRetentionDuration' = 'interval 60 days',
    'delta.logRetentionDuration' = 'interval 60 days'
);
"""
print(f"sql: {sql}")

# Execute the SQL command to create the table
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,acss_usbe_sets
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.curated_db.acss_usbe_sets"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    system_code STRING,
    settlement_date DATE,
    sending_dc_irn STRING,
    sending_dc_name STRING,
    receiving_dc_irn STRING,
    receiving_dc_name STRING,
    sets_volume LONG,
    sets_value DECIMAL(30,2),
    acss_usbe_sets_id INT,
    batch_id INT
) partitioned by (file_date)
 TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name',
    'delta.deletedFileRetentionDuration' = 'interval 60 days',
    'delta.logRetentionDuration' = 'interval 60 days'
);
"""
print(f"sql: {sql}")

# Execute the SQL command to create the table
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,acss_usbe_vol_val
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.curated_db.acss_usbe_vol_val"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    system_code STRING,
    settlement_date DATE,
    sending_dc_irn STRING,
    sending_dc_name STRING,
    receiving_dc_irn STRING,
    receiving_dc_name STRING,
    stream_code STRING,
    stream_name STRING,
    volume LONG,
    value DECIMAL(30,2),
    acss_usbe_vol_val_id INT,
    batch_id INT
) partitioned by (file_date)
 TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name',
    'delta.deletedFileRetentionDuration' = 'interval 60 days',
    'delta.logRetentionDuration' = 'interval 60 days'
);
"""
print(f"sql: {sql}")

# Execute the SQL command to create the table
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,vw_acss_usbe_sets
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_acss_usbe_sets
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.acss_usbe_sets
""")

# COMMAND ----------

# DBTITLE 1,vw_acss_usbe_vol_val
spark.sql(f"""
CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.vw_acss_usbe_vol_val
AS
SELECT
*
FROM {unity_catalog_name}.curated_db.acss_usbe_vol_val
""")
