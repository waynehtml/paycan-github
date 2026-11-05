# Databricks notebook source
# MAGIC %md
# MAGIC # TMX CDS Oversight Raw File Validation
# MAGIC
# MAGIC Per-file validation only.
# MAGIC
# MAGIC ## Validates only
# MAGIC
# MAGIC 1. Non-empty physical file
# MAGIC 2. File name contains the configured file pattern
# MAGIC 3. Expected source columns exist from a brief Spark CSV load

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("env", "", "env")
dbutils.widgets.text("pull_date", "", "pull_date")  # yyyy-MM-dd
dbutils.widgets.text("container", "data", "container")
dbutils.widgets.text("storage_name", "", "storage_name")


dbutils.widgets.dropdown(
    "file_key",
    "--please-select--",
    [
        "--please-select--",
        "CNSPOSNVAL_ABNS",
        "CNSPOSNVAL_EODS",
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


# COMMAND ----------

env = dbutils.widgets.get("env").strip()
pull_date = dbutils.widgets.get("pull_date").strip()
container = dbutils.widgets.get("container").strip()
storage_name = dbutils.widgets.get("storage_name").strip()
file_key = dbutils.widgets.get("file_key").strip().upper()

print(f"env: {env}")
print(f"pull_date: {pull_date}")
print(f"container: {container}")
print(f"storage_name: {storage_name}")
print(f"file_key: {file_key}")


# COMMAND ----------

# MAGIC %run paycan/common/utils

# COMMAND ----------

if len(env) == 0:
    env = get_env()

if len(storage_name) == 0:
    storage_name = get_lake_storage_account(env)

if len(pull_date) == 0:
    raise Exception("pull_date is required. Expected format: yyyy-MM-dd")

print(f"resolved env: {env}")
print(f"resolved storage_name: {storage_name}")
print(f"pull_date: {pull_date}")


# COMMAND ----------

# DBTITLE 1,Lightweight Configs
# Only fields needed for file-level validation are retained:
# - raw_path
# - file_patterns
# - expected_columns

FILE_CONFIGS = {
    "CNSPOSNVAL_ABNS": {
        "raw_path": "raw/external/tmx/cds_oversight/CNSPOSNVAL",
        "file_patterns": ["CNSPOSNVAL_ABNS"],
        "expected_columns": [
            "DATE",
            "EXTRACT PERIOD",
            "LEDGER BPID",
            "LEDGER BPID NAME",
            "SECURITY ID",
            "SETTLEMENT CURRENCY",
            "SETTLEMENT DATE",
            "NET POSITION",
            "PRICE",
            "POSITION VALUE"
        ]
    },
    "CNSPOSNVAL_EODS": {
        "raw_path": "raw/external/tmx/cds_oversight/CNSPOSNVAL",
        "file_patterns": ["CNSPOSNVAL_EODS"],
        "expected_columns": [
            "DATE",
            "EXTRACT PERIOD",
            "LEDGER BPID",
            "LEDGER BPID NAME",
            "SECURITY ID",
            "SETTLEMENT CURRENCY",
            "SETTLEMENT DATE",
            "NET POSITION",
            "PRICE",
            "POSITION VALUE"
        ]
    },
    "PAYMENTOBLIGATIONS": {
        "raw_path": "raw/external/tmx/cds_oversight/PAYMENTOBLIGATIONS",
        "file_patterns": ["PAYMENTOBLIGATIONS"],
        "expected_columns": [
            "DATE",
            "LEDGER BPID",
            "LEDGER BPID NAME",
            "CURRENCY",
            "PAYMENT OBLIGATION"
        ]
    },

    "LINEOFCREDIT": {
        "raw_path": "raw/external/tmx/cds_oversight/LINEOFCREDIT",
        "file_patterns": ["LINEOFCREDIT"],
        "expected_columns": [
            "DATE",
            "EXTENDER LEDGER BPID",
            "EXTENDER LEDGER BPID NAME",
            "RECEIVER LEDGER BPID",
            "RECEIVER LEDGER BPID NAME",
            "RECEIVER FAMILY ID",
            "RECEIVER FAMILY ID NAME",
            "LINE OF CREDIT ID",
            "EXTENDED CREDIT LINE",
            "USED CREDIT LINE",
            "AVAILABLE CREDIT LINE",
            "MAXIMUM USED LINE",
            "STATUS",
            "NEW CREDIT LINE",
            "INCREASE STATUS",
            "SETTLEMENT FACILITY"
        ]
    },

    "NSCCPOSNVAL": {
        "raw_path": "raw/external/tmx/cds_oversight/NSCCPOSNVAL",
        "file_patterns": ["NSCCPOSNVAL"],
        "expected_columns": [
            "SETTLEMENT DATE",
            "NSCC MEMBER ID",
            "CDSX LEDGER BPID",
            "CDSX LEDGER BPID NAME",
            "CUSIP",
            "NET POSITION",
            "MARKET VALUE",
            "CURRENCY CODE"
        ]
    },

    "USDBANKBAL": {
        "raw_path": "raw/external/tmx/cds_oversight/USDBANKBAL",
        "file_patterns": ["USDBANKBAL"],
        "expected_columns": [
            "DATE",
            "WFC",
            "BNY HUB",
            "BNY LD",
            "CIT",
            "JPM",
            "STAN",
            "STT"
        ]
    },

    "STLMTFAILS": {
        "raw_path": "raw/external/tmx/cds_oversight/STLMTFAILS",
        "file_patterns": ["STLMTFAILS"],
        "expected_columns": [
            "BUSINESS DATE",
            "SETTLEMENT TYPE",
            "CURRENCY",
            "TOTAL VALUE",
            "FAILED VALUE",
            "FAILED VALUE %"
        ]
    },

    "NYLMTM": {
        "raw_path": "raw/external/tmx/cds_oversight/NYLMTM",
        "file_patterns": ["NYLMTM"],
        "expected_columns": [
            "DATE",
            "CDS LEDGER BPID",
            "CDS LEDGER BPID NAME",
            "NYL MTM USD",
            "MTM CAD EQUIV"
        ]
    },

    "CNSMTM": {
        "raw_path": "raw/external/tmx/cds_oversight/CNSMTM",
        "file_patterns": ["CNSMTM"],
        "expected_columns": [
            "DATE",
            "LEDGER BPID",
            "CDS LEDGER BPID NAME",
            "SETTLEMENT DATE",
            "CNS MTM CAD",
            "CNS MTM USD",
            "CNS MTM CCAD"
        ]
    },

    "CLRDTRDS": {
        "raw_path": "raw/external/tmx/cds_oversight/CLRDTRDS",
        "file_patterns": ["CLRDTRDS"],
        "expected_columns": [
            "BUSINESS DATE",
            "LEDGER BPID",
            "LEDGER BPID NAME",
            "TRANSACTION TYPE",
            "SECURITY ID",
            "ROLE",
            "MODE OF SETTLEMENT",
            "CURRENCY",
            "TRADE DATE",
            "INTENDED SETTLEMENT DATE",
            "QUANTITY",
            "SETTLEMENT AMOUNT",
            "CCAD"
        ]
    },

    "STLDTRDS": {
        "raw_path": "raw/external/tmx/cds_oversight/STLDTRDS",
        "file_patterns": ["STLDTRDS"],
        "expected_columns": [
            "LEDGER BPID",
            "LEDGER BPID NAME",
            "TRANSACTION TYPE",
            "ROLE",
            "STATUS",
            "MODE OF SETTLEMENT",
            "CURRENCY",
            "SETTLEMENT DATE",
            "SETTLEMENT QUANTITY",
            "SETTLEMENT AMOUNT",
            "CCAD"
        ]
    },

    "RCBSE01720EN": {
    "raw_path": "raw/external/tmx/cds_oversight/RCBSE01720EN",
    "file_patterns": ["RCBSE01720EN"],
    "expected_columns": [
        "PARTICIPANT BP ID",
        "NSCC ACCOUNT",
        "MARKET",
        "VOLUME",
        "DEBITS",
        "CREDITS",
        "SELL QTY",
        "BUY QTY",
        "TRADE INDICATOR"
    ]
}

}


# COMMAND ----------

# DBTITLE 1,Helpers
def get_raw_base_path(config):
    """
    Builds the ADLS base path for the selected file_key.
    """
    return (
        f"abfss://{container}@{storage_name}.dfs.core.windows.net/"
        f"{config['raw_path']}"
    )


def get_daily_path(config):
    """
    Builds the daily raw path for the selected pull_date.
    """
    return f"{get_raw_base_path(config)}/{pull_date}"


def get_files_for_date(config):
    """
    Lists CSV files in the selected file_key daily raw path.
    """
    daily_path = get_daily_path(config)

    print(f"Scanning: {daily_path}")

    try:
        all_files = dbutils.fs.ls(daily_path)
    except Exception:
        raise Exception(f"Path not found or inaccessible: {daily_path}")

    return [
        f for f in all_files
        if not f.isDir()
        and f.name.lower().endswith(".csv")
    ]


def brief_spark_load(file_path):
    """
    Brief Spark load used only to inspect headers and confirm Spark can read rows.

    No schema inference.
    No type casting.
    No transformations.
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


def normalize_column_name(column_name):
    """
    Normalizes source column names for comparison.
    """
    return column_name.upper().strip()


# COMMAND ----------

# DBTITLE 1,Validation Functions
def check_non_empty_file(file_info):
    """
    Validates that the physical file is not zero bytes.
    """
    if file_info.size == 0:
        raise Exception(f"Zero-byte file detected: {file_info.name}")

    print(f"  [OK] File is not zero bytes: {file_info.name}")


def check_spark_has_data(df, file_name):
    """
    Validates that Spark can read at least one data row.

    This catches header-only files.
    It does not validate any values or data types.
    """
    if df.limit(1).count() == 0:
        raise Exception(f"File contains no data rows: {file_name}")

    print(f"  [OK] Spark detected at least one data row: {file_name}")

def check_file_name(file_name, config):
    """
    Validates that the file name contains the configured file pattern
    and ends with .csv.
    """
    file_name_upper = file_name.upper()
    patterns = [p.upper().strip() for p in config.get("file_patterns", [])]

    if not file_name.lower().endswith(".csv"):
        raise Exception(f"File is not a CSV file: {file_name}")

    if len(patterns) == 0:
        raise Exception("No file_patterns configured for this file type.")

    if not any(pattern in file_name_upper for pattern in patterns):
        raise Exception(
            f"File name '{file_name}' does not contain expected pattern(s): {patterns}"
        )

    print(f"  [OK] File name valid: {file_name}")

def check_expected_columns_exist(df, config, file_name):
    """
    Validates that all expected source columns exist in the raw file.

    Extra columns are allowed.
    Column order does not matter.
    Comparison is case-insensitive and trims leading/trailing spaces.
    """
    if len(df.columns) == 0:
        raise Exception(f"No columns detected in file: {file_name}")

    expected = {
        normalize_column_name(c)
        for c in config.get("expected_columns", [])
    }

    actual = {
        normalize_column_name(c)
        for c in df.columns
    }

    missing = sorted(expected - actual)

    if missing:
        raise Exception(
            f"Missing expected columns in {file_name}:\n  {missing}"
        )

    print(f"  [OK] All expected columns exist for {file_name}")

def validate_file(file_info, config):
    """
    Runs only file-level validation:
    1. Non-empty file
    2. File name
    3. Expected columns exist from brief Spark load
    """
    print("\n" + "-" * 100)
    print(f"Validating file: {file_info.name}")
    print(f"Size: {file_info.size:,} bytes")
    print("-" * 100)

    check_non_empty_file(file_info)
    check_file_name(file_info.name, config)

    df = brief_spark_load(file_info.path)

    check_expected_columns_exist(df, config, file_info.name)
    check_spark_has_data(df, file_info.name)

    return {
        "file": file_info.name,
        "status": "valid"
    }


# COMMAND ----------

# DBTITLE 1,Driver
if file_key == "--PLEASE-SELECT--":
    raise Exception("Please select one file_key.")

if file_key not in FILE_CONFIGS:
    raise Exception(f"Unsupported file_key: {file_key}")

config = FILE_CONFIGS[file_key]

# Validate files under selected file_key path

files = get_files_for_date(config)

# Keep only files that match the selected file_key pattern.
# This handles shared folders like CNSPOSNVAL containing both ABNS and EODS files.
patterns = [p.upper().strip() for p in config.get("file_patterns", [])]

files = [
    f for f in files
    if any(pattern in f.name.upper() for pattern in patterns)
]

if len(files) == 0:
    print(
        f"[EXIT] no_files — no CSV files matching {patterns} "
        f"found for file_key={file_key}, pull_date={pull_date}"
    )
    dbutils.notebook.exit("no_files")

if len(files) > 1:
    print(
        f"[WARN] {len(files)} CSV files found for file_key={file_key}, pull_date={pull_date}. "
        f"Each file will be validated independently."
    )

results = []

for file_info in files:
    result = validate_file(file_info, config)
    result["file_key"] = file_key
    results.append(result)


# COMMAND ----------

# DBTITLE 1,Summary
print("\n" + "=" * 100)
print("PER-FILE VALIDATION SUMMARY")
print("=" * 100)
print(f"{'FILE KEY':<25} {'FILE NAME':<65} {'STATUS':<12}")
print("-" * 100)

for r in results:
    print(
        f"{r['file_key']:<25} "
        f"{r['file']:<65} "
        f"{r['status']:<12}"
    )

print("\n[EXIT] valid — all files under the selected file_key passed per-file validation.")
dbutils.notebook.exit("valid")
