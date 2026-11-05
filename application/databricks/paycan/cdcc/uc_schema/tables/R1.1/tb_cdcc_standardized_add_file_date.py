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

# DBTITLE 1,Alter table collateral
spark.sql(f"""
ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_collateral 
ADD COLUMNS (file_date string)
;
""")


# COMMAND ----------

# DBTITLE 1,Alter table vm
spark.sql(f"""
ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_vm
ADD COLUMNS (file_date string)
;
""")


# COMMAND ----------

# DBTITLE 1,Alter table liquidity_data
spark.sql(f"""
ALTER TABLE {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_liquidity_data 
ADD COLUMNS (file_date string)
;
""")


# COMMAND ----------

# DBTITLE 1,Alter table members
spark.sql(f"""
ALTER TABLE  {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_members
ADD COLUMNS (file_date string)
;
""")


# COMMAND ----------

# DBTITLE 1,Alter table margins
spark.sql(f"""
ALTER TABLE  {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_margins
ADD COLUMNS (file_date string)
;
""")


# COMMAND ----------

# DBTITLE 1,Alter table positions
spark.sql(f"""
ALTER TABLE  {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_positions
ADD COLUMNS (file_date string)
;
""")