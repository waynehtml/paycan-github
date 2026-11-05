# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("cmd", "")
dbutils.widgets.text("show_sql_only", "0")
dbutils.widgets.text("param1", "")
dbutils.widgets.text("param2", "")

cmd = dbutils.widgets.get("cmd")
print(f"cmd:{cmd}")
show_sql_only = dbutils.widgets.get("show_sql_only")
print(f"show_sql_only:{show_sql_only}")
param1 = dbutils.widgets.get("param1")
print(f"param1:{param1}")
param2 = dbutils.widgets.get("param2")
print(f"param2:{param2}")

# COMMAND ----------

def is_view(obj):
    table_info = spark.catalog.getTable(obj)
    return table_info.tableType == "VIEW"

# COMMAND ----------

if len(cmd) == 0:
    raise Exception("cmd is required")

# COMMAND ----------

# DBTITLE 1,Delete table or view
if (cmd == "delete" or cmd=="del" or cmd=="drop" or cmd=="remove" or cmd=="rm"):
    if (len(param1) == 0):
        raise Exception("param1 is required")

    if is_view(param1):
        sql = f"DROP VIEW IF EXISTS {param1};"
    else:
        sql = f"DROP TABLE IF EXISTS {param1};"
    print(sql)

    if show_sql_only == "0":
        spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Show Permission
if (cmd == "show" or cmd=="grant"):
    if (len(param1) == 0):
        raise Exception("param1 is required")
    
    sql = f"SHOW GRANT ON {param1}"
    print(sql)
    df = spark.sql(sql)
    df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Truncated Table
if (cmd == "trunc" or cmd=="truncate" or cmd=="empty" or cmd=="clear"):
    if (len(param1) == 0):
        raise Exception("param1 is required")

    sql = f"TRUNCATE TABLE {param1}"
    print(sql)
    if show_sql_only == "0":
        spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Copy Table
if (cmd == "copy" or cmd=="cp"):
    if (len(param1) == 0):
        raise Exception("param1 is required")
    if (len(param2) == 0):
        raise Exception("param2 is required")

    # Backup table by writing to a new table
    print(f"Copying {param1} to {param2}.")
    sql = f"CREATE OR REPLACE TABLE {param2} AS SELECT * FROM {param1}"
    if show_sql_only == "0":
        spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,Execute sql
if (cmd == "sql"):
    if (len(param1) == 0):
        raise Exception("param1 is required")

    # Backup table by writing to a new table
    print(f"Executing sql: {param1}")
    if show_sql_only == "0":
        ret = spark.sql(param1)
        display(ret)
