# Databricks notebook source
# MAGIC %md
# MAGIC # CDS Raw-to-Standardized Ingestion Notebook
# MAGIC
# MAGIC ## Supports
# MAGIC
# MAGIC - **Daily load**: loads one `pull_date` folder.
# MAGIC - **Full load**: loads all date-separated folders under each raw path.
# MAGIC - **File selection**: supports loading either all files or one selected file type using `file_key`.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as f
from pyspark.sql.functions import col, lit, to_date, current_timestamp, trim, when, substring, to_timestamp
from pyspark.sql.types import *
import delta

# COMMAND ----------

# DBTITLE 1,Import utils
# MAGIC %run paycan/common/utils

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.removeAll()
dbutils.widgets.text("env", "", "env")
dbutils.widgets.text("pull_date", "", "pull_date")  # yyyy-MM-dd, required for daily
dbutils.widgets.text("container", "restricted", "container")
dbutils.widgets.text("storage_name", "", "storage_name")

dbutils.widgets.dropdown("load_type", "daily", ["daily", "full"])

dbutils.widgets.dropdown(
    "file_key",
    "all",
    [
        "all",
        "CNSPOSNVAL",
        "CNSPOSNVAL_EODS", 
        "CNSPOSNVAL_ABNS",
        "PAYMENTOBLIGATIONS",
        "LINEOFCREDIT",
        "NSCCPOSNVAL", 
        "USDBANKBAL", 
        "STLMTFAILS", 
        "NYLMTM", 
        "CNSMTM", 
        "CLRDTRDS", 
        "STLDTRDS",
        "RCBSE01720EN"
    ]
)

env = dbutils.widgets.get("env")
pull_date = dbutils.widgets.get("pull_date")
container = dbutils.widgets.get("container")
storage_name = dbutils.widgets.get("storage_name")
load_type = dbutils.widgets.get("load_type").lower()
file_key = dbutils.widgets.get("file_key")
if file_key in ["CNSPOSNVAL_EODS", "CNSPOSNVAL_ABNS"]:
    file_key = "CNSPOSNVAL"

print(f"env: {env}")
print(f"pull_date: {pull_date}")
print(f"container: {container}")
print(f"storage_name: {storage_name}")
print(f"load_type: {load_type}")
print(f"file_key: {file_key}")


# COMMAND ----------

# DBTITLE 1,Resolve environment, storage, and Unity Catalog
if len(env.strip()) == 0:
    env = get_env()

if len(storage_name.strip()) == 0:
    storage_name = get_lake_storage_account(env)

unity_catalog_name = get_unity_catalog_name(env)

print(f"resolved env: {env}")
print(f"resolved storage_name: {storage_name}")
print(f"unity_catalog_name: {unity_catalog_name}")


# COMMAND ----------

# DBTITLE 1,File config
FILE_CONFIGS = {
    "CNSPOSNVAL": {
        "raw_path": "raw/external/tmx/cds_oversight/CNSPOSNVAL",
        "file_patterns": ["CNSPOSNVAL"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_tmx_cds_cns_position_values",
        "column_map": {
            "DATE": "date",
            "EXTRACT PERIOD": "extract_period",
            "LEDGER BPID": "ledger_bpid",
            "LEDGER BPID NAME": "ledger_bpid_name",
            "SECURITY ID": "security_id",
            "SETTLEMENT CURRENCY": "settlement_currency",
            "SETTLEMENT DATE": "settlement_date",
            "NET POSITION": "net_position",
            "PRICE": "price",
            "POSITION VALUE": "position_value"
        },
        "date_columns": {
            "date": "yyyyMMdd",
            "settlement_date": "yyyyMMdd"
        },
        "decimal_columns": {
            "net_position": "decimal(20,2)",
            "price": "decimal(20,2)",
            "position_value": "decimal(20,2)"
        },
        "bigint_columns": [],
        "string_columns": [
            "extract_period",
            "ledger_bpid",
            "ledger_bpid_name",
            "security_id",
            "settlement_currency"
        ]
    },
    "PAYMENTOBLIGATIONS": {
        "raw_path": "raw/external/tmx/cds_oversight/PAYMENTOBLIGATIONS",
        "file_patterns": ["PAYMENTOBLIGATIONS"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_tmx_cds_payment_obligations",
        "column_map": {
            "DATE": "business_date",
            "LEDGER BPID": "ledger_bpid",
            "LEDGER BPID NAME": "ledger_bpid_name",
            "CURRENCY": "currency",
            "PAYMENT OBLIGATION": "payment_obligation"
        },
        "date_columns": {
            "business_date": "yyyyMMdd"
        },
        "decimal_columns": {
            "payment_obligation": "decimal(20,2)"
        },
        "bigint_columns": [],
        "string_columns": [
            "ledger_bpid",
            "ledger_bpid_name",
            "currency"
        ]
    },

    "LINEOFCREDIT": {
        "raw_path": "raw/external/tmx/cds_oversight/LINEOFCREDIT",
        "file_patterns": ["LINEOFCREDIT"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_tmx_cds_line_of_credit",
        "column_map": {
            "DATE": "business_date",
            "EXTENDER LEDGER BPID": "extender_ledger_bpid",
            "EXTENDER LEDGER BPID NAME": "extender_ledger_bpid_name",
            "RECEIVER LEDGER BPID": "receiver_ledger_bpid",
            "RECEIVER LEDGER BPID NAME": "receiver_ledger_bpid_name",
            "RECEIVER FAMILY ID": "receiver_family_id",
            "RECEIVER FAMILY ID NAME": "receiver_family_id_name",
            "LINE OF CREDIT ID": "line_of_credit_id",
            "EXTENDED CREDIT LINE": "extended_credit_line",
            "USED CREDIT LINE": "used_credit_line",
            "AVAILABLE CREDIT LINE": "available_credit_line",
            "MAXIMUM USED LINE": "maximum_used_line",
            "STATUS": "status",
            "NEW CREDIT LINE": "new_credit_line",
            "INCREASE STATUS": "increase_status",
            "SETTLEMENT FACILITY": "settlement_facility"
        },
        "date_columns": {
            "business_date": "yyyyMMdd"
        },
        "decimal_columns": {
            "extended_credit_line": "decimal(20,2)",
            "used_credit_line": "decimal(20,2)",
            "available_credit_line": "decimal(20,2)",
            "maximum_used_line": "decimal(20,2)",
            "new_credit_line": "decimal(20,2)"
        },
        "bigint_columns": [],
        "string_columns": [
            "extender_ledger_bpid",
            "extender_ledger_bpid_name",
            "receiver_ledger_bpid",
            "receiver_ledger_bpid_name",
            "receiver_family_id",
            "receiver_family_id_name",
            "line_of_credit_id",
            "status",
            "increase_status",
            "settlement_facility"
        ]
    },

    "NSCCPOSNVAL": {
        "raw_path": "raw/external/tmx/cds_oversight/NSCCPOSNVAL",
        "file_patterns": ["NSCCPOSNVAL"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_tmx_cds_nscc_position_values",
        "column_map": {
            "SETTLEMENT DATE": "settlement_date",
            "NSCC MEMBER ID": "nscc_member_id",
            "CDSX LEDGER BPID": "cdsx_ledger_bpid",
            "CDSX LEDGER BPID NAME": "cdsx_ledger_bpid_name",
            "CUSIP": "cusip",
            "NET POSITION": "net_position",
            "MARKET VALUE": "market_value",
            "CURRENCY CODE": "currency_code"
        },
        "date_columns": {
            "settlement_date": "yyyyMMdd"
        },
        "decimal_columns": {
            "market_value": "decimal(20,2)"
        },
        "bigint_columns": [
            "net_position"
        ],
        "string_columns": [
            "nscc_member_id",
            "cdsx_ledger_bpid",
            "cdsx_ledger_bpid_name",
            "cusip",
            "currency_code"
        ]
    },
    "USDBANKBAL": {
        "raw_path": "raw/external/tmx/cds_oversight/USDBANKBAL",
        "file_patterns": ["USDBANKBAL"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_tmx_cds_usd_deposit_bank_balances",
        "column_map": {
            "DATE": "business_date",
            "WFC": "wfc",
            "BNY HUB": "bny_hub",
            "BNY LD": "bny_ld",
            "CIT": "cit",
            "JPM": "jpm",
            "STAN": "stan",
            "STT": "stt"
        },
        "date_columns": {
            "business_date": "yyyy-MM-dd"
        },
        "decimal_columns": {
            "wfc": "decimal(20,2)",
            "bny_hub": "decimal(20,2)",
            "bny_ld": "decimal(20,2)",
            "cit": "decimal(20,2)",
            "jpm": "decimal(20,2)",
            "stan": "decimal(20,2)",
            "stt": "decimal(20,2)"
        },
        "bigint_columns": [],
        "string_columns": []
    }, 
    "STLMTFAILS": {
        "raw_path": "raw/external/tmx/cds_oversight/STLMTFAILS",
        "file_patterns": ["STLMTFAILS"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_tmx_cds_settlement_fails",
        "column_map": {
            "BUSINESS DATE": "date",
            "SETTLEMENT TYPE": "settlement_type",
            "CURRENCY": "currency",
            "TOTAL VALUE": "total_value",
            "FAILED VALUE": "failed_value",
            "FAILED VALUE %": "failed_value_pct"
        },
        "date_columns": {
            "date": "yyyyMMdd"
        },
        "decimal_columns": {
            "total_value": "decimal(20,2)",
            "failed_value": "decimal(20,2)",
            "failed_value_pct": "decimal(20,2)"
        },
        "bigint_columns": [],
        "string_columns": [
            "settlement_type",
            "currency"
        ]
    }, 
    "NYLMTM": {
        "raw_path": "raw/external/tmx/cds_oversight/NYLMTM",
        "file_patterns": ["NYLMTM"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_tmx_cds_nyl_daily_mark_to_market_by_participant",
        "column_map": {
            "DATE": "date",
            "CDS LEDGER BPID": "ledger_bpid",
            "CDS LEDGER BPID NAME": "ledger_bpid_name",
            "NYL MTM USD": "nyl_mtm_usd",
            "MTM CAD EQUIV": "mtm_cad_equiv"
        },
        "date_columns": {
            "date": "yyyyMMdd"
        },
        "decimal_columns": {
            "nyl_mtm_usd": "decimal(20,2)",
            "mtm_cad_equiv": "decimal(20,2)"
        },
        "bigint_columns": [],
        "string_columns": [
            "ledger_bpid",
            "ledger_bpid_name"
        ]
    }, 
    "CNSMTM": {
        "raw_path": "raw/external/tmx/cds_oversight/CNSMTM",
        "file_patterns": ["CNSMTM"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_tmx_cds_cns_daily_mark_to_market_by_participant",
        "column_map": {
            "DATE": "date",
            "LEDGER BPID": "ledger_bpid",
            "CDS LEDGER BPID NAME": "ledger_bpid_name",
            "SETTLEMENT DATE": "settlement_date",
            "CNS MTM CAD": "cns_mtm_cad",
            "CNS MTM USD": "cns_mtm_usd",
            "CNS MTM CCAD": "cns_mtm_ccad"
        },
        "date_columns": {
            "date": "yyyyMMdd",
            "settlement_date": "yyyyMMdd"
        },
        "decimal_columns": {
            "cns_mtm_cad": "decimal(20,2)",
            "cns_mtm_usd": "decimal(20,2)",
            "cns_mtm_ccad": "decimal(20,2)"
        },
        "bigint_columns": [],
        "string_columns": [
            "ledger_bpid",
            "ledger_bpid_name"
        ]
    }, 
    "CLRDTRDS": {
        "raw_path": "raw/external/tmx/cds_oversight/CLRDTRDS",
        "file_patterns": ["CLRDTRDS"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_tmx_cds_cleared_and_novated_in_cns",
        "column_map": {
            "BUSINESS DATE": "date",
            "LEDGER BPID": "ledger_bpid",
            "LEDGER BPID NAME": "ledger_bpid_name",
            "TRANSACTION TYPE": "transaction_type",
            "SECURITY ID": "security_id",
            "ROLE": "role",
            "MODE OF SETTLEMENT": "mode_of_settlement",
            "CURRENCY": "currency",
            "TRADE DATE": "trade_date",
            "INTENDED SETTLEMENT DATE": "intended_settlement_date",
            "QUANTITY": "quantity",
            "SETTLEMENT AMOUNT": "settlement_amount",
            "CCAD": "ccad"
        },
        "date_columns": {
            "date": "yyyyMMdd",
            "trade_date": "yyyyMMdd",
            "intended_settlement_date": "yyyyMMdd"
        },
        "decimal_columns": {
            "quantity": "decimal(20,2)",
            "settlement_amount": "decimal(20,2)",
            "ccad": "decimal(20,2)"
        },
        "bigint_columns": [],
        "string_columns": [
            "ledger_bpid",
            "ledger_bpid_name",
            "transaction_type",
            "security_id",
            "role",
            "mode_of_settlement",
            "currency"
        ]
    }, 
    "STLDTRDS": {
        "raw_path": "raw/external/tmx/cds_oversight/STLDTRDS",
        "file_patterns": ["STLDTRDS"],
        "target_table": f"{unity_catalog_name}.standardized_db.tbl_tmx_cds_settled_in_cdsx",
        "column_map": {
            "LEDGER BPID": "ledger_bpid",
            "LEDGER BPID NAME": "ledger_bpid_name",
            "TRANSACTION TYPE": "transaction_type",
            "ROLE": "role",
            "STATUS": "status",
            "MODE OF SETTLEMENT": "mode_of_settlement",
            "CURRENCY": "currency",
            "SETTLEMENT DATE": "settlement_date",
            "SETTLEMENT QUANTITY": "settlement_quantity",
            "SETTLEMENT AMOUNT": "settlement_amount",
            "CCAD": "ccad"
        },
        "date_columns": {
            "settlement_date": "yyyyMMdd"
        },
        "decimal_columns": {
            "settlement_quantity": "decimal(20,2)",
            "settlement_amount": "decimal(20,2)",
            "ccad": "decimal(20,2)"
        },
        "bigint_columns": [],
        "string_columns": [
            "ledger_bpid",
            "ledger_bpid_name",
            "transaction_type",
            "role",
            "status",
            "mode_of_settlement",
            "currency"
        ]
    },
    "RCBSE01720EN": {
    "raw_path": "raw/external/tmx/cds_oversight/RCBSE01720EN",
    "file_patterns": ["RCBSE01720EN"],
    "target_table": f"{unity_catalog_name}.standardized_db.tbl_tmx_cds_new_york_link_trade_stats",
    "skip_rows": 1,
    "column_map": {
        "PARTICIPANT BP ID": "participant_bpid",
        "NSCC ACCOUNT": "nscc_account",
        "MARKET": "market",
        "VOLUME": "volume",
        "DEBITS": "debits",
        "CREDITS": "credits",
        "SELL QTY": "sell_qty",
        "BUY QTY": "buy_qty",
        "TRADE INDICATOR": "trade_indicator"
    },
    "date_columns": {},
    "decimal_columns": {
        "debits": "decimal(20,2)",
        "credits": "decimal(20,2)",
        "sell_qty": "decimal(20,2)",
        "buy_qty": "decimal(20,2)"
    },
    "bigint_columns": [
        "volume"
    ],
    "string_columns": [
        "participant_bpid",
        "nscc_account",
        "market",
        "trade_indicator"
    ]
}
}


# COMMAND ----------

# DBTITLE 1,Ingestion helpers
def normalize_col_name(name):
    """
    Normalizes source file headers so minor spacing/case differences
    do not break the column mapping.
    """
    return " ".join(name.strip().upper().split())


def config_list(value):
    """
    Supports either list config values or legacy semicolon-delimited strings.
    """
    if value is None:
        return []

    if isinstance(value, list):
        return value

    if isinstance(value, str):
        return [x.strip() for x in value.split(";") if x.strip()]

    return []


def get_raw_base_path(config):
    """
    Builds the ADLS base path for a configured file type.
    """
    return (
        f"abfss://{container}@{storage_name}.dfs.core.windows.net/"
        f"{config['raw_path']}"
    )


def file_matches_config(file_name, config):
    """
    Checks whether a file belongs to the current file config based on filename pattern.
    """
    file_name_upper = file_name.upper()

    return any(
        pattern.upper() in file_name_upper
        for pattern in config["file_patterns"]
    )


def get_files_to_load(file_config_key, config):
    """
    Finds files to load.

    daily:
      scans one folder:
      <raw_path>/<pull_date>/

    full:
      scans all date-separated folders:
      <raw_path>/<date_folder>/
    """
    raw_base_path = get_raw_base_path(config)
    files_to_load = []

    print(f"Scanning raw path for {file_config_key}: {raw_base_path}")

    if load_type == "daily":
        if len(pull_date.strip()) == 0:
            raise Exception("pull_date is required when load_type = daily")

        daily_path = f"{raw_base_path}/{pull_date}"
        print(f"Daily path: {daily_path}")

        for file in dbutils.fs.ls(daily_path):
            if not file.isDir() and file_matches_config(file.name, config):
                files_to_load.append({
                    "file_key": file_config_key,
                    "file_path": file.path,
                    "audit_file_name": file.name,
                    "audit_file_date": pull_date,
                })

    elif load_type == "full":
        date_folders = dbutils.fs.ls(raw_base_path)

        for folder in date_folders:
            if folder.isDir():
                folder_date = folder.name.replace("/", "")
                print(f"Scanning date folder: {folder_date}")

                for file in dbutils.fs.ls(folder.path):
                    if not file.isDir() and file_matches_config(file.name, config):
                        files_to_load.append({
                            "file_key": file_config_key,
                            "file_path": file.path,
                            "audit_file_name": file.name,
                            "audit_file_date": folder_date,
                        })

    else:
        raise Exception(f"Unsupported load_type: {load_type}")

    return files_to_load


def read_raw_file(file_path):
    """
    Reads a raw delimited file.
    """
    if file_key == "RCBSE01720EN":
        return (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("sep", ",")
            .option("skipRows", "1")
            .load(file_path)
        )
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("sep", ",")
        .load(file_path)
    )


def standardize_columns(df, config):
    """
    Normalizes raw headers and renames them to target table columns.
    """
    for raw_col in df.columns:
        normalized_col = normalize_col_name(raw_col)

        if raw_col != normalized_col:
            df = df.withColumnRenamed(raw_col, normalized_col)

    normalized_column_map = {
        normalize_col_name(raw_col): target_col
        for raw_col, target_col in config["column_map"].items()
    }

    for raw_col, target_col in normalized_column_map.items():
        if raw_col in df.columns:
            df = df.withColumnRenamed(raw_col, target_col)
        else:
            raise Exception(f"Missing expected source column: {raw_col}")

    return df


def clean_values(df):
    """
    Matches standard function behavior:
      - trims all column values
      - converts empty strings to null
    """
    df = df.select([
        trim(col(c).cast("string")).alias(c)
        for c in df.columns
    ])

    df = df.select([
        when(col(c) == "", None).otherwise(col(c)).alias(c)
        for c in df.columns
    ])

    return df


def drop_configured_columns(df, config):
    """
    Drops configured unwanted columns, if supplied.
    This is optional because select_target_columns also removes extras later.
    """
    drop_columns = config_list(config.get("drop_columns"))

    if len(drop_columns) > 0:
        df = df.drop(*drop_columns)

    return df


def apply_casts(df, config):
    """
    Applies configured string, date, decimal, float, integer, bigint,
    and timestamp casts.
    """
    for col_name in config_list(config.get("string_columns")):
        df = df.withColumn(col_name, col(col_name).cast("string"))

    for col_name, date_format in config.get("date_columns", {}).items():
        if any(x in date_format for x in ["-", "/", "\\"]):
            df = df.withColumn(
                col_name,
                to_date(substring(col(col_name), 1, 10), date_format)
            )
        else:
            df = df.withColumn(
                col_name,
                to_date(substring(col(col_name), 1, 8), date_format)
            )

    for col_name, data_type in config.get("decimal_columns", {}).items():
        df = df.withColumn(col_name, col(col_name).cast(data_type))

    for col_name in config_list(config.get("float_columns")):
        df = df.withColumn(col_name, col(col_name).cast("float"))

    for col_name in config_list(config.get("integer_columns")):
        df = df.withColumn(col_name, col(col_name).cast("integer"))

    for col_name in config_list(config.get("bigint_columns")):
        df = df.withColumn(col_name, col(col_name).cast("bigint"))

    for col_name, timestamp_format in config.get("timestamp_columns", {}).items():
        df = df.withColumn(
            col_name,
            to_timestamp(col(col_name), timestamp_format)
        )

    return df


def add_audit_columns(df, config, file_info):
    """
    Adds standard audit/control columns.
    """
    df = (
        df
        .withColumn("audit_file_name", lit(file_info["audit_file_name"]))
        .withColumn(
            "audit_file_date",
            to_date(lit(file_info["audit_file_date"]), "yyyy-MM-dd")
        )
        .withColumn("audit_load_ts", current_timestamp())
    )

    return df


def select_target_columns(df, config):
    """
    Selects only columns expected by the target table, in target order.
    """
    target_columns = list(config["column_map"].values())

    target_columns.extend([
        "audit_file_name",
        "audit_file_date",
        "audit_load_ts",
    ])

    return df.select(*target_columns)


def drop_standard_duplicates(df, config):
    """
    Matches standard duplicate handling:
      - if merge_key_columns exists, dedupe by those columns
      - otherwise dedupe by full row
    """
    merge_key_columns = config_list(config.get("merge_key_columns"))

    if len(merge_key_columns) > 0:
        return df.dropDuplicates(merge_key_columns)

    return df.dropDuplicates()


def transform_file(df_raw, config, file_info):
    """
    Applies all transformations for one raw file.
    """
    try:
        df = standardize_columns(df_raw, config)
        df = clean_values(df)
        df = drop_configured_columns(df, config)
        df = apply_casts(df, config)
        df = add_audit_columns(df, config, file_info)
        df = select_target_columns(df, config)
        df = drop_standard_duplicates(df, config)

        return df

    except Exception as e:
        raise Exception("Error in standardizing raw dataframe: " + str(e))


def union_dataframes(df_list):
    """
    Unions multiple transformed files for the same target table.
    """
    if len(df_list) == 0:
        return None

    df_all = df_list[0]

    for df in df_list[1:]:
        df_all = df_all.unionByName(df)

    return df_all


def delete_existing_data(config):
    """
    Soft-key delete-before-append logic.

    daily:
      deletes rows for the selected audit_file_date.

    full:
      deletes all rows from the selected target table.
    """
    target_table = config["target_table"]
    dt = delta.DeltaTable.forName(spark, target_table)

    if load_type == "daily":
        print(
            f"Deleting existing data from {target_table} "
            f"where audit_file_date = {pull_date}"
        )

        dt.delete(
            col("audit_file_date") == to_date(lit(pull_date), "yyyy-MM-dd")
        )

    elif load_type == "full":
        print(f"Deleting all existing data from {target_table}")

        dt.delete()


def write_to_delta(df, config):
    """
    Appends transformed data to the target Delta table.

    audit_file_date acts as the soft key because delete_existing_data()
    removes the existing slice before this append runs.
    """
    target_table = config["target_table"]

    print(f"Writing data to {target_table}")

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )


def ingest_one_file_type(file_config_key, config):
    """
    Main ingestion driver for one file type.
    """
    print("=" * 100)
    print(f"Starting ingestion for file_key = {file_config_key}")
    print("=" * 100)

    files_to_load = get_files_to_load(file_config_key, config)

    print(f"Files found for {file_config_key}: {len(files_to_load)}")

    if len(files_to_load) == 0:
        print(f"No files found for {file_config_key}. Skipping.")
        return

    transformed_dfs = []

    for file_info in files_to_load:
        print(f"Reading file: {file_info['file_path']}")

        df_raw = read_raw_file(file_info["file_path"])

        if df_raw.rdd.isEmpty():
            print(f"File is empty. Skipping: {file_info['file_path']}")
            continue

        df_transformed = transform_file(df_raw, config, file_info)
        transformed_dfs.append(df_transformed)

    df_all = union_dataframes(transformed_dfs)

    if df_all is None or df_all.rdd.isEmpty():
        print(f"No non-empty data found for {file_config_key}. Nothing to write.")
        return

    print(f"Preview transformed data for {file_config_key}")
    df_all.limit(10).display()

    delete_existing_data(config)
    write_to_delta(df_all, config)

    print(f"Completed ingestion for file_key = {file_config_key}")
    

# COMMAND ----------

# DBTITLE 1,Execute ingestion
if file_key == "all":
    selected_configs = FILE_CONFIGS.items()
else:
    if file_key not in FILE_CONFIGS:
        raise Exception(f"Unsupported file_key: {file_key}")

    selected_configs = [(file_key, FILE_CONFIGS[file_key])]

completed_file_keys = []

for selected_file_key, selected_config in selected_configs:
    ingest_one_file_type(selected_file_key, selected_config)
    completed_file_keys.append(selected_file_key)


# COMMAND ----------

# DBTITLE 1,Results
print(
    f"Ingestion complete. "
    f"Processed {len(completed_file_keys)} file config(s): {', '.join(completed_file_keys)}. "
    f"load_type={load_type}, "
    f"pull_date={pull_date if load_type == 'daily' else 'N/A'}, "
    f"env={env}, "
    f"storage={container}@{storage_name}, "
    f"catalog={unity_catalog_name}."
)
