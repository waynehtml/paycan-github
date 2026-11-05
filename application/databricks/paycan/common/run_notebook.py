# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

import json

# COMMAND ----------

# DBTITLE 1,Setup widget to retrieve parameters from ADF
dbutils.widgets.removeAll()
dbutils.widgets.text("notebook", "")
dbutils.widgets.text("parameter", "")

# COMMAND ----------

# DBTITLE 1,Get parameters
# notebook parameter is the notebook path, required
notebook = dbutils.widgets.get("notebook")
print(f"notebook is {notebook}")
if not notebook:
    raise Exception("notebook was not provided.")

# parameter is a json string, optional
parameter_str = dbutils.widgets.get("parameter")
print(f"parameter_str is {parameter_str}")

# Deserialize parameter string into a dictionary
parameter = {}
if parameter_str:
    parameter = json.loads(parameter_str)

# if environment not provided, use the detected environment
if 'environment' not in parameter:
    parameter['environment'] = get_env()
print(f"parameter is {parameter}")    

# COMMAND ----------

# DBTITLE 1,Call target notebook
# call the target notebook
dbutils.notebook.run(notebook, 0, parameter)
