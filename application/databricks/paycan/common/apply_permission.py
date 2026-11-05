# Databricks notebook source
# DBTITLE 1,Set up Widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("environment", "")
dbutils.widgets.dropdown("generate_sql_only", "no", ["no", "yes"])
dbutils.widgets.text("land_storage","")
dbutils.widgets.text("land_container","land-paycan")
dbutils.widgets.text("config_file","permission.json")

# COMMAND ----------

# DBTITLE 1,Import libaries
import re
from pyspark.sql.functions import col, explode

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,Utility functions
# Log function
def log(message):
    print(message);

# Compose group's full name
# Global references: groups
def get_group_full_name(group_name, env):
    group = next(filter(lambda group: group['name'] == group_name, groups));
    
    if env == "dev" or env == "sbx":
        suffix = "inda"
    else: 
        suffix = "inpa"

    return group["name"] + "_" + suffix;

# COMMAND ----------

# DBTITLE 1,Find object dependency
# Tokenize string with multiple separators
def tokenize_string(text, separators):
    pattern = '|'.join(map(re.escape, separators))
    tokens = re.split(pattern, text)
    return [token for token in tokens if token]

# check if a object is view
def is_view(obj):
    table_info = spark.catalog.getTable(obj)
    return table_info.tableType == "VIEW"

# remove comment lines starting with "--"
def remove_comment(input):
    # Split the input string into lines
    lines = input.split("\n")
    # Remove lines starting with "--" using list comprehension
    filtered_lines = [line for line in lines if not line.startswith("--")]
    # Join the filtered lines back into a string
    output = "\n".join(filtered_lines)
    return output

def remove_inline_comment(input):
    output = re.sub(r'--.*', '', input, flags=re.DOTALL)
    return output

def check_table_or_view(table):
    ret = 0
    
    sql = f"SHOW CREATE TABLE {table}";
    create_stmt = spark.sql(sql)


    return ret

# build a view's dependency recursively
def built_view_dept(view):
    log(f"view: {view}");
    
    # Get view's create statement
    sql = f"SHOW CREATE TABLE {view}";
    create_stmt = spark.sql(sql).first()['createtab_stmt'];

    # remove comment
    create_stmt = remove_comment(create_stmt);

    #remove inline comment
    create_stmt = remove_inline_comment(create_stmt);

    # tokenize the text
    separators = [',', ' ', '(', ')','\n','\t'];
    tokens = tokenize_string(create_stmt, separators);

    # Define a filtering function
    def starts_with_token(s):
        return any((s.startswith(token) and s.count(".")==2) for token in catalogs);

    # Filter elements that start with specified catalog
    objs = list(filter(starts_with_token, tokens));

    # remove view itself to avoid duplicate and keep right sequence
    if view in objs:
        objs.remove(view);

    # output all dependent objects
    log(f"Dependent objects: {objs}");

    # keep find dependency if obj is view
    for obj in objs:
        if is_view(obj):
            built_view_dept(obj);
        else:
            dep_objs.append(obj);
    
    # Add the view as the last itme
    dep_objs.append(view);

def run_perm_sql(env,obj, grant_groups,revoke_groups):
    # Run revoke sql
    if revoke_groups:
        for group in revoke_groups:
            group_full_name = get_group_full_name(group, env);
            sql = f"";
            sql = f"REVOKE SELECT ON {obj} FROM {group_full_name};";
            log(f"{sql}");

            # Run
            if not generate_sql_only:
                result = spark.sql(sql);
    
    # Run grant sql
    if grant_groups:
        for group in grant_groups:
            group_full_name = get_group_full_name(group, env);
            sql = f"GRANT SELECT ON {obj} TO {group_full_name};"
            log(f"{sql}");

            # Run
            if not generate_sql_only:
                result = spark.sql(sql);

def merge_group(data, name):
  # Use a dictionary to group grants by obj
  result_dict = {}

  for entry in data:
      obj = entry['obj']
      grant = entry['group']
      result_dict.setdefault(obj, []).append(grant)

  # Convert the dictionary back to a list of dictionaries
  result = [{'obj': obj, name: grants} for obj, grants in result_dict.items()]

  return result

def apply_permission(env, catalog_name, perms):
    if not perms:
        log("no permission applied.");
    else:
        for perm in perms:
            log(f"Permission: {perm}");
            # Build dependent object lists
            global dep_objs;
            dep_objs = [];
            obj_full_name = f"{catalog_name}.{perm['obj']}"
            built_view_dept(obj_full_name);

            # remove duplicate dependency, actually, this step is optional
            unique_dep_objs = []
            for item in dep_objs:
                if item not in unique_dep_objs:
                    unique_dep_objs.append(item)

            # Output dependency
            log("");
            log(f"Full dependency: {unique_dep_objs}");

            # run sql
            log("")
            log("Runing sql:");
            for obj in unique_dep_objs:
                grant_groups = None;
                if "grant" in perm:
                    grant_groups = perm["grant"];
                revoke_groups = None;
                if "revoke" in perm:
                    revoke_groups = perm["revoke"]
                run_perm_sql(env, obj, grant_groups, revoke_groups);
            # Add empty line in log message
            log("");

# COMMAND ----------

# MAGIC %md
# MAGIC #Main process

# COMMAND ----------

# DBTITLE 1,Initiate parameters
print("Initializing parameters:")
# env
env = dbutils.widgets.get("environment");
if len(env) == 0:
    env = get_env()
print(f"env={env}");

# generate_sql_only
generate_sql_only = 1 if dbutils.widgets.get("generate_sql_only") == "yes" else 0;
print(f"generate_sql_only={generate_sql_only}");

# land_storage
land_storage = dbutils.widgets.get("land_storage");
if len(land_storage) == 0:
    land_storage = get_land_st_name(env)
print(f"land_storage={land_storage}")

# land_container
land_container = dbutils.widgets.get("land_container");
print(f"land_container={land_container}")

# config_file
config_file = dbutils.widgets.get("config_file");
print(f"config_file={config_file}")

# COMMAND ----------

# DBTITLE 1,load configurations
# Read permssion.json file
config_url = f"abfss://{land_container}@{land_storage}.dfs.core.windows.net/{config_file}"
config_df = spark.read.format("json").option("multiLine", True).load(config_url)

# Extract catalogs
catalog_df = config_df.select(explode(col("catalog")).alias("catalog")).select(col("catalog.name").alias("name"))
catalogs = catalog_df.rdd.map(lambda row: row["name"]).collect()
catalogs = [env + s for s in catalogs]
print(f"catalogs: {catalogs}")

# Extract groups
group_df = config_df.select(explode(col("group")).alias("group")).select(col("group.name").alias("name"),col("group.type").alias("type"))
groups = group_df.rdd.map(lambda row: {"name": row["name"], "type": row["type"]}).collect()
print(f"groups: {groups}")

# Extract permissions
permissions_grant_df = config_df.select(explode(col("permissions")).alias("permission")).select(col("permission.name").alias("name"),explode(col("permission.grant")).alias("grant"))
permissions_grant = permissions_grant_df.rdd.map(lambda row: {"obj": row["name"],"group": row["grant"]}).collect()
permissions_grant = merge_group(permissions_grant, "grant")
print(f"permission_grant: {permissions_grant}")

# COMMAND ----------

# DBTITLE 1,Appy permission
apply_permission(env, catalogs[0], permissions_grant);
