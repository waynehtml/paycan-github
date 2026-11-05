# Databricks notebook source
# MAGIC %md
# MAGIC # Folder operation utility
# MAGIC This notebook could delete, copy and move folders and sub-folders recursivelly. This notebook should work in both UC and non-UC workspace.
# MAGIC
# MAGIC * Parameters:
# MAGIC   * command: "copy|more|delete"
# MAGIC   * container: "raw|restricted"
# MAGIC   * storage: "lakecacnpdstdev01|lakecacprdststg01|lakecacprdstprd01"
# MAGIC   * source: folder path, for example raw/internal/rcmp
# MAGIC   * destination: folder path, for example raw/internal/rcmp_bak
# MAGIC

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run /ops/Utils/auth_util

# COMMAND ----------

dbutils.widgets.removeAll()

# create databricks widgets
dbutils.widgets.dropdown("command", "copy", ["copy", "delete", "move"])
dbutils.widgets.text("src_container", "restricted")
dbutils.widgets.text("src_storage", "lakecacnpdstdev01")
dbutils.widgets.text("src_path", "")
dbutils.widgets.text("des_container", "restricted")
dbutils.widgets.text("des_storage", "lakecacnpdstdev01")
dbutils.widgets.text("des_path", "")

# get user input values from widgets/ADF base parameters
cmd = dbutils.widgets.get("command")
src_container = dbutils.widgets.get("src_container")
src_storage = dbutils.widgets.get("src_storage")
src_path = dbutils.widgets.get("src_path")
des_container = dbutils.widgets.get("des_container")
des_storage = dbutils.widgets.get("des_storage")
des_path = dbutils.widgets.get("des_path")

# COMMAND ----------

# DBTITLE 1,Authentication
kv_scope = "lake-kv-paycan-scope"
spn_id_secret = "spn-paycanservices-id"
spn_key_secret = "spn-paycanservices-key"
tenant_id_secret = "lake-tenant-id"

# try to enable access to storage via SPN authentication.
tryEnableSpnAccess(kv_scope, src_storage, spn_id_secret, spn_key_secret, tenant_id_secret)
if src_storage.lower() != des_storage.lower():
  tryEnableSpnAccess(kv_scope, des_storage, spn_id_secret, spn_key_secret, tenant_id_secret)


# COMMAND ----------

def copy(src, des):
  print(f"Copying {src} to {des}")
  dbutils.fs.cp(src, des, recurse=True)

def delete(src):
  print(f"Deleting folder: {src}")
  dbutils.fs.rm(src, recurse=True)

def move(src, des):
  copy(src, des)
  delete(src)


# COMMAND ----------

# DBTITLE 1,backup the m7 bn data
src = f"abfss://{src_container}@{src_storage}.dfs.core.windows.net/{src_path}"
des = f"abfss://{des_container}@{des_storage}.dfs.core.windows.net/{des_path}"

if len(cmd) == 0:
  raise Exception("cmd is required")

if cmd.lower() == "copy":
  if len(src_path) == 0:
    raise Exception("Source path is required for copy")
  if len(des_path) == 0:
    raise Exception("Destination path is required for copy")
  copy(src, des)
elif cmd.lower() == "delete":
  if len(src_path) == 0:
    raise Exception("Source path is required for delete")
  delete(src)
elif cmd.lower() == "move":
  if len(src_path) == 0:
    raise Exception("Source path is required for move")
  if len(des_path) == 0:
    raise Exception("Destination path is required for move")
  move(src, des)
else:
  print(f"Unknown command: {cmd}")
