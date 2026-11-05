# Databricks notebook source
# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run /paycan/common/utils

# COMMAND ----------

# DBTITLE 1,Get UC name
unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))
##spark.conf.set('ad.catalog',unity_catalog_name)

# COMMAND ----------

# DBTITLE 1,Update table liquidity_data
spark.sql(f"""
    UPDATE {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_liquidity_data 
    SET file_date = next_day(date, 'MO')
    WHERE file_date is null
""")

spark.sql(f"""
    UPDATE {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_liquidity_data 
    SET file_date = '2025-02-18'
    WHERE file_date = '2025-02-17'
""")

spark.sql(f"""
    UPDATE {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_liquidity_data 
    SET file_date = '2025-05-20'
    WHERE file_date = '2025-05-19'
""")

# COMMAND ----------

# DBTITLE 1,Update other tables
table_names = ["tbl_tmx_cdcc_members", "tbl_tmx_cdcc_margins", "tbl_tmx_cdcc_positions", "tbl_tmx_cdcc_vm", "tbl_tmx_cdcc_collateral"]

# Build full table names with catalog + schema + table
full_table_names = [f"{unity_catalog_name}.standardized_db.{t}" for t in table_names]

# SQL template to update file_date column
sql_template = """
UPDATE {table} 
SET file_date = 
  CASE 
    WHEN dayofweek(date) = 2 THEN date
    ELSE next_day(date, 'MO')
  END
WHERE file_date is null
"""

# Loop through and execute the SQL for each table
for tbl in full_table_names:
    query = sql_template.format(table=tbl)
    spark.sql(query)

sql_template = """
UPDATE {table} 
SET file_date = '2025-02-18'
WHERE date>= '2025-02-11' and date <= '2025-02-18'
"""

for tbl in full_table_names:
    query = sql_template.format(table=tbl)
    spark.sql(query)
              

sql_template = """
UPDATE {table} 
SET file_date = '2025-05-20'
WHERE date>= '2025-05-13' and date <= '2025-05-20'
"""

for tbl in full_table_names:
    query = sql_template.format(table=tbl)
    spark.sql(query)
