# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC _This is a sample deployment instrction. Please replace with your content of the Jira task to be deployed._
# MAGIC
# MAGIC * Tasks: 
# MAGIC   * add two columns in standardized table
# MAGIC   * clean up unused data
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC   * date - date of the file to updated
# MAGIC * Dependency:
# MAGIC   * CUDT-001 has to be deployed first
# MAGIC * [Other]
# MAGIC   * anything that needs to be mentioned to the release manager

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../common/utils

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))

# COMMAND ----------

# DBTITLE 1,create standardized tables
# MAGIC %run ../lynx_uc/schema/tables/tb_lynx_standardized

# COMMAND ----------

# DBTITLE 1,create curated tables
# MAGIC %run ../lynx_uc/schema/tables/tb_lynx_curated

# COMMAND ----------

# DBTITLE 1,create lynx views
# MAGIC %run ../lynx_uc/schema/views/vw_lynx_views
