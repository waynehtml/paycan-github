# Databricks notebook source
# DBTITLE 1,Import Common Databricks Tools
# MAGIC %pip install "oeds-common~=1.0" 
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import python packages
import os
import sys
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, input_file_name

# COMMAND ----------

# DBTITLE 1,Import Common Python Packages
# Add python utils to sys.path
python_utils_dir = "/Workspace/oedataservices/utility/python/src"
# Add the python directories to sys.path only if they're not already in
if python_utils_dir not in sys.path:
    sys.path.append(python_utils_dir)

# Import the oeds libraries
from oeds.core.env_utils import ENV
import oeds.core.uc_utils as ucutils
import oeds.core.misc_utils as misc
import oeds.core.df_utils as dfutils
import oeds.core.file_utils as fileutils

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("env","")
dbutils.widgets.text("container","restricted")
dbutils.widgets.text("storage","")
dbutils.widgets.text("file_date","")
dbutils.widgets.dropdown("dataset", "cs", ["cs", "exchange"])
dbutils.widgets.dropdown("load_type", "daily", ["daily", "full"])

# COMMAND ----------

# DBTITLE 1,Get Parameters
env = dbutils.widgets.get("env")
if len(env.strip()) == 0:
    env = ENV.NAME
print(f"env: {env}")

container = dbutils.widgets.get("container")
if len(container.strip()) == 0:
    raise Exception("Parameter container is required.")
print(f"container: {container}")

storage = dbutils.widgets.get("storage")
if len(storage.strip()) == 0:
    storage = ENV.LAKEST
print(f"storage: {storage}")

file_date = dbutils.widgets.get("file_date")

dataset = dbutils.widgets.get("dataset")
if len(dataset.strip()) == 0:
    raise Exception("Parameter dataset is required.")
print(f"dataset: {dataset}")

load_type = dbutils.widgets.get("load_type")
print(f"load_type: {load_type}")

if load_type == "daily" and len(file_date.strip()) == 0:
    raise Exception("file_date is required for daily load.")


# COMMAND ----------

# DBTITLE 1,Import dataset definitions
from rtr_schema import RTR

# COMMAND ----------

# DBTITLE 1,Dataset config
DS_CONFIG = RTR.DATASETS[dataset]

# COMMAND ----------

# DBTITLE 1,Process Files
# ============================================================
# 3. Process files
# ============================================================
from pyspark.sql.functions import input_file_name, regexp_extract, current_timestamp
from pyspark.sql import functions as F

for file_info in DS_CONFIG["files"]:
    print(f"\n### Processing: {dataset} - {file_info['name']} ###\n")

    # --------------------------------------------------------
    # 3.1 Build file path
    # --------------------------------------------------------
    if load_type == "daily":
        file_url = fileutils.adsl_url(
            storage,
            container,
            f"{DS_CONFIG['raw_path']}/{file_date}/{file_info['file_name']}"
        ).replace("{DATE}", file_date)
    else:
        # full load = bulk read all dated folders
        file_url = fileutils.adsl_url(
            storage,
            container,
            f"{DS_CONFIG['raw_path']}/*/{file_info['file_name']}"
        ).replace("{DATE}", "*")

    print(f"## Read file from: {file_url}")

    # --------------------------------------------------------
    # 3.2 Read raw files
    # --------------------------------------------------------
    schema = file_info["schema"][0] if isinstance(file_info["schema"], list) else file_info["schema"]

    # read_encoding = "UTF-8" if file_info["name"] == "reject_reason_code" else "windows-1252"
    read_encoding = file_info.get("encoding", "UTF-8")

    df = (
        spark.read
            .options(mode="FAILFAST", encoding=read_encoding, lineSep="\n")
            .schema(schema)
            .json(file_url)
    )

    # --------------------------------------------------------
    # 3.3 Add / derive file_date
    # --------------------------------------------------------
    if load_type == "full":
        # expects dated folder structure like .../<YYYY-MM-DD>/filename.json
        df = df.withColumn(
            "file_date",
            regexp_extract(input_file_name(), r"/(\d{4}-\d{2}-\d{2})/", 1)
        )

        missing_file_date_count = df.filter(F.col("file_date") == "").limit(1).count()
        if missing_file_date_count > 0:
            raise Exception(
                "Failed to derive file_date from input file path during full load. "
                "Expected a dated folder pattern like /YYYY-MM-DD/."
            )
    else:
        df = df.withColumn("file_date", F.lit(file_date))

    if dataset == "cs" and file_info["name"] == "exchange":
        for c in ("ora_delay_time", "ora_max_retry"):
            df = df.withColumn(c, F.expr(f"try_cast({c} AS BIGINT)"))
        df.printSchema() # temp: ora_delay_time / ora_max_retry should be bigint

    # --------------------------------------------------------
    # 3.4 Payment special handling
    # --------------------------------------------------------
    if file_info['name'] == 'payment':
        # extract schemas
        schema_payment_raw, schema_payment, schema_previous_instructing_agents, schema_transaction_statuses = file_info['schema']

        # create child dataframes
        df_payment_ex = df.drop("previous_instructing_agents", "transaction_statuses").select(
            "uetr", "message_identification", "end_to_end_identification",
            "transaction_identification", "clearing_and_settlement_assigned_reference",
            "competitive_service_id", "reconciliation_cycle_id", "exchange_assigned_reference",
            "sending_participant_id", "sending_participant_csp_id",
            "receiving_participant_id", "receiving_participant_csp_id",
            "payment_amount", "payment_currency_code", "creation_time",
            "file_date",

            F.col("debtor_agent.bicfi").alias("debtor_agent_bicfi"),
            F.col("debtor_agent.clearing_system_identification_code").alias("debtor_agent_clearing_system_identification_code"),
            F.col("debtor_agent.clearing_system_member_id").alias("debtor_agent_clearing_system_member_id"),
            F.col("debtor_agent.legal_entity_identifier").alias("debtor_agent_legal_entity_identifier"),
            F.col("debtor_agent.name").alias("debtor_agent_name"),
            F.col("debtor_agent.postal_address.postal_code").alias("debtor_agent_postal_address_postal_code"),
            F.col("debtor_agent.postal_address.town_name").alias("debtor_agent_postal_address_town_name"),
            F.col("debtor_agent.postal_address.country").alias("debtor_agent_postal_address_country"),

            F.col("creditor_agent.bicfi").alias("creditor_agent_bicfi"),
            F.col("creditor_agent.clearing_system_identification_code").alias("creditor_agent_clearing_system_identification_code"),
            F.col("creditor_agent.clearing_system_member_id").alias("creditor_agent_clearing_system_member_id"),
            F.col("creditor_agent.legal_entity_identifier").alias("creditor_agent_legal_entity_identifier"),
            F.col("creditor_agent.name").alias("creditor_agent_name"),
            F.col("creditor_agent.postal_address.postal_code").alias("creditor_agent_postal_address_postal_code"),
            F.col("creditor_agent.postal_address.town_name").alias("creditor_agent_postal_address_town_name"),
            F.col("creditor_agent.postal_address.country").alias("creditor_agent_postal_address_country"),
        )

        df_payment_previous_instructing_agents = (
            df.select("uetr", "file_date", F.posexplode_outer("previous_instructing_agents").alias("pos", "agent"))
            .where(F.col("agent").isNotNull())
            .select(
                "uetr",
                "file_date",
                F.coalesce(F.col("agent.agent_index"), F.col("pos")).cast("long").alias("agent_index"),
                F.col("agent.bicfi").alias("bicfi"),
                F.col("agent.clearing_system_identification_code").alias("clearing_system_identification_code"),
                F.col("agent.clearing_system_identification_proprietary").alias("clearing_system_identification_proprietary"),
                F.col("agent.clearing_system_member_id").alias("clearing_system_member_id"),
                F.col("agent.legal_entity_identifier").alias("legal_entity_identifier"),
                F.col("agent.name").alias("name"),
                F.col("agent.postal_address.postal_code").alias("postal_code"),
                F.col("agent.postal_address.town_name").alias("town_name"),
                F.col("agent.postal_address.country").alias("country"),
                F.col("agent.other.identification").alias("identification"),
                F.col("agent.other.scheme_name_code").alias("scheme_name_code"),
                F.col("agent.other.scheme_name_proprietary").alias("scheme_name_proprietary"),
                F.col("agent.other.issuer").alias("issuer"),
            )
        )

        df_payment_transaction_statuses = (
            df
            .select(
                "uetr",
                "file_date",
                F.explode_outer("transaction_statuses").alias("sts")
            )
            .where(F.col("sts").isNotNull())
            .select(
                "uetr",
                "file_date",
                F.col("sts.status_code").alias("status_code"),
                F.col("sts.status_description").alias("status_description"),
                F.col("sts.status_reason_code").alias("status_reason_code"),
                F.col("sts.status_reason_desc").alias("status_reason_desc"),
                F.col("sts.status_time").alias("status_time"),
                F.explode_outer("sts.transaction_states").alias("ts")
            )
            .where(F.col("ts").isNotNull())
            .select(
                "uetr",
                "file_date",
                "status_code",
                "status_description",
                "status_reason_code",
                "status_reason_desc",
                "status_time",
                F.col("ts.transaction_state").alias("transaction_states"),
                F.col("ts.transaction_state_time").alias("transaction_state_time"),
                F.explode_outer("ts.technical_states").alias("tech")
            )
            .where(F.col("tech").isNotNull())
            .select(
                "uetr",
                "file_date",
                "status_code",
                "status_description",
                "status_reason_code",
                "status_reason_desc",
                "status_time",
                "transaction_states",
                "transaction_state_time",
                F.col("tech.technical_state").alias("technical_state"),
                F.col("tech.technical_state_time").alias("technical_state_time"),
            )
        )

    # --------------------------------------------------------
    # 3.5 Validate
    # --------------------------------------------------------
    if file_info['name'] == 'payment':
        print("\n## Validating NULL violations - payment")
        null_report_payment = dfutils.check_not_null(df_payment_ex, schema_payment)
        null_report_payment_previous_instructing_agents = dfutils.check_not_null(
            df_payment_previous_instructing_agents, schema_previous_instructing_agents
        )
        null_report_payment_transaction_statuses = dfutils.check_not_null(
            df_payment_transaction_statuses, schema_transaction_statuses
        )

        errors = []

        if null_report_payment:
            errors.append(f"Null values in non-nullable column(s): {null_report_payment}")

        if null_report_payment_previous_instructing_agents:
            errors.append(
                f"Null values in non-nullable column(s): {null_report_payment_previous_instructing_agents}"
            )

        if null_report_payment_transaction_statuses:
            errors.append(
                f"Null values in non-nullable column(s): {null_report_payment_transaction_statuses}"
            )

        if errors:
            raise Exception("\n".join(errors))
        else:
            print("No null violations found - payment")

    else:
        print("\n## Validating NULL violations")
        null_report = dfutils.check_not_null(df, file_info['schema'])
        if null_report:
            raise Exception(f"Null values in non-nullable column(s): {null_report}")
        else:
            print("No null violations found")

    # --------------------------------------------------------
    # 3.6 Transformation
    # --------------------------------------------------------
    # 3.6.1 Rename columns
    # N/A

    # 3.6.2 Flatten nested columns
    if dataset == 'cs' and file_info['name'] == 'transaction':
        print(f"\n## Flattening nested columns")
        df = dfutils.flatten_all_col(df)

    # --------------------------------------------------------
    # 3.7 Add audit columns
    # --------------------------------------------------------
    def add_audit_cols(df_in):
        return (
            df_in
            .withColumn("file_name", input_file_name())
            .withColumn("update_time", current_timestamp())
        )

    if file_info['name'] == 'payment':
        print("\n## Adding audit columns - payment")
        df_payment_ex = add_audit_cols(df_payment_ex)
        df_payment_previous_instructing_agents = add_audit_cols(df_payment_previous_instructing_agents)
        df_payment_transaction_statuses = add_audit_cols(df_payment_transaction_statuses)
    else:
        print("\n## Adding audit columns")
        df = add_audit_cols(df)

    # --------------------------------------------------------
    # 3.8 Write to UC
    # --------------------------------------------------------
    catalog = ucutils.resolve_catalog('paycan', env)

    def get_distinct_file_dates(df_in):
        vals = [r["file_date"] for r in df_in.select("file_date").where(F.col("file_date").isNotNull()).distinct().collect()]
        vals = [v for v in vals if str(v).strip() != ""]
        return sorted(vals)

    def write_df(df_out, full_table_name, base_mode):
        """
        Handles both daily and full load semantics for batch_update:
        - replace mode requires batch_column + batch_id, one day at a time
        - overwrite mode replaces entire table
        """
        base_mode = base_mode.lower()

        if load_type == "daily":
            print(f"## daily write mode: {base_mode}")
            kwargs = {
                "df": df_out,
                "full_table_name": full_table_name,
                "mode": base_mode,
            }

            if base_mode == "replace":
                kwargs["batch_column"] = "file_date"
                kwargs["batch_id"] = file_date

            ucutils.batch_update(**kwargs)
            return

        # full load
        if base_mode == "replace":
            # delta-style table: process each day separately
            distinct_dates = get_distinct_file_dates(df_out)
            if not distinct_dates:
                raise Exception(f"No file_date values found for full-load replace into {full_table_name}")

            print(f"## Full load replace by file_date for {len(distinct_dates)} day(s)")
            for dt in distinct_dates:
                print(f"## Write batch file_date={dt} to table: {full_table_name}")
                df_batch = df_out.filter(F.col("file_date") == F.lit(dt))

                ucutils.batch_update(
                    df=df_batch,
                    full_table_name=full_table_name,
                    mode="replace",
                    batch_column="file_date",
                    batch_id=dt
                )

        elif base_mode == "overwrite":
            # snapshot files are full dumps; keep only the latest day
            latest_date = df_out.agg(F.max("file_date")).first()[0]
            if not latest_date:
                raise Exception(f"No file_date values found for full-load overwrite into {full_table_name}")

            print(f"## Full load overwrite using latest file_date={latest_date}: {full_table_name}")
            ucutils.batch_update(
                df=df_out.filter(F.col("file_date") == F.lit(latest_date)),
                full_table_name=full_table_name,
                mode="overwrite"
            )

        else:
            raise Exception(f"Unsupported table_update_mode '{base_mode}' for table {full_table_name}")

    if file_info['name'] == 'payment':
        print("\n## Writing payment tables to UC")

        # ---- payment_ex
        full_table_name_ex = f"{catalog}.{file_info['table_name'][0]}"
        print(f"## Write target: {full_table_name_ex}")
        write_df(df_payment_ex, full_table_name_ex, file_info["table_update_mode"])
        print(f"## Write Complete: {full_table_name_ex}")

        # ---- payment_previous_instructing_agents
        full_table_name_pia = f"{catalog}.{file_info['table_name'][1]}"
        print(f"## Write target: {full_table_name_pia}")
        write_df(df_payment_previous_instructing_agents, full_table_name_pia, file_info["table_update_mode"])
        print(f"## Write Complete: {full_table_name_pia}")

        # ---- payment_transaction_statuses
        full_table_name_ts = f"{catalog}.{file_info['table_name'][2]}"
        print(f"## Write target: {full_table_name_ts}")
        write_df(df_payment_transaction_statuses, full_table_name_ts, file_info["table_update_mode"])
        print(f"## Write Complete: {full_table_name_ts}")

    else:
        full_table_name = f"{catalog}.{file_info['table_name']}"
        print(f"\n## Write target: {full_table_name}")
        write_df(df, full_table_name, file_info["table_update_mode"])
        print(f"## Write Complete: {full_table_name}")
