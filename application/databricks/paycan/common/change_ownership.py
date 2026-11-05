# Databricks notebook source
# MAGIC %md
# MAGIC # changes all tables/views to a specific owner
# MAGIC - catalog: UC catalog to be searched, default: dev_currency_cat
# MAGIC - owner: owner idenity, defaut Dev_SPN_MI_Platform for ADF MI
# MAGIC - exclusion: prefixes separated ";", defaut test_
# MAGIC - show_sql_only: show sql only for testing or execute sql, default 0

# COMMAND ----------

# DBTITLE 1,Set Parameters
dbutils.widgets.removeAll()
dbutils.widgets.text("catalog", "dev_currency_cat")
dbutils.widgets.text("owner", "Dev_SPN_MI_Platform")
dbutils.widgets.text("exclusion", "test_")
dbutils.widgets.text("show_sql_only", "0")

catalog = dbutils.widgets.get("catalog")
print(f"catalog:{catalog}")
owner = dbutils.widgets.get("owner")
print(f"owner:{owner}")
exclusion = dbutils.widgets.get("exclusion")
print(f"exclusion:{exclusion}")
exclusions = exclusion.split(";")
print(f"exclusions:{exclusions}")
show_sql_only = dbutils.widgets.get("show_sql_only")
print(f"show_sql_only:{show_sql_only}")

# COMMAND ----------

# DBTITLE 1,Find all objects not belong to owner
sql_find_objs_without_tbl_own_fmd = f"""
SELECT concat(table_catalog,'.', table_schema, '.', table_name) as name, t.table_type as type, t.table_owner as owner 
FROM system.information_schema.tables as t 
where t.table_catalog = '{catalog}' and t.table_schema != 'information_schema' and t.table_owner != '{owner}' ORDER BY t.table_schema, t.table_type, t.table_name
"""
print(sql_find_objs_without_tbl_own_fmd)
df_objs = spark.sql(sql_find_objs_without_tbl_own_fmd)
display(df_objs)

# COMMAND ----------

# DBTITLE 1,Assign objects ownership
objs = [row.asDict() for row in df_objs.collect()]
for obj in objs:
    name = obj['name']
    type = obj['type']

    # exclude objects that start with exclusion
    short_name = name.split(".")[-1]
    found = any(short_name.lower().startswith(prefix.lower()) for prefix in exclusions)
    if not found:
        if type == 'MANAGED':
            type = "TABLE"
        sql = f"ALTER {type} {name} OWNER TO `{owner}`"
        print(f"sql:{sql}")
        if show_sql_only == "0":
            spark.sql(sql)
