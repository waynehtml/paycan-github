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

# DBTITLE 1,Table names
old_table_name = f"{unity_catalog_name}.standardized_db.tbl_kaiko_trade"
print(f"old_table_name: {old_table_name}")
new_table_name = f"{old_table_name}_ex"
print(f"new_table_name: {new_table_name}")

# COMMAND ----------

# DBTITLE 1,delete old table
delete_table_sql = f"""
DROP TABLE IF EXISTS {old_table_name}
"""

# Execute the SQL command
print(f"Deleting old table: {delete_table_sql}")
spark.sql(delete_table_sql)

# COMMAND ----------

# DBTITLE 1,rename new table
rename_table_sql = f"""
ALTER TABLE {new_table_name} RENAME TO {old_table_name}
"""

# Execute the SQL command
print(f"Renaming new table: {rename_table_sql}")
spark.sql(rename_table_sql)
