# Databricks notebook source
# MAGIC %md
# MAGIC # Payments Canada RTR Raw File Validation
# MAGIC
# MAGIC Per-file validation only. Run once per daily load, per dataset, **before** the RTR standardizing notebook.
# MAGIC
# MAGIC ## Validates
# MAGIC
# MAGIC **Folder level**
# MAGIC 1. Number of files in the daily folder matches the number of configured files for the dataset
# MAGIC
# MAGIC **Per file**
# MAGIC 1. Expected file exists (name per PC928.3 v3.1: `<name>_<ex|cs>_<yyyy-MM-dd>.json`)
# MAGIC 2. Zero-byte check (warning only - empty files are possible)
# MAGIC 3. Format: `.json` extension, newline-delimited JSON (one object per line, not an array), no UTF-8 BOM
# MAGIC 4. Valid JSON records (no corrupt records) from a brief Spark JSON load
# MAGIC 5. Expected top-level fields exist (from `rtr_schema.RTR`, same schemas as the standardizing notebook)
# MAGIC 6. Column count matches the expected field count
# MAGIC 7. At least one record (warning only)

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("env", "", "env")
dbutils.widgets.text("file_date", "", "file_date")  # yyyy-MM-dd
dbutils.widgets.text("container", "restricted", "container")
dbutils.widgets.text("storage_name", "", "storage_name")

dbutils.widgets.dropdown("dataset", "cs", ["cs", "exchange"], "dataset")

dbutils.widgets.dropdown(
    "file_key",
    "ALL",
    [
        "ALL",
        # cs
        "account",
        "transaction",
        "participant",
        "net_debit_cap",
        "watermark",
        "exchange",
        "reject_reason_code",
        # exchange
        "competitive_service",
        "connection_service_provider",
        "payment"
    ],
    "file_key"
)

env = dbutils.widgets.get("env").strip()
file_date = dbutils.widgets.get("file_date").strip()
container = dbutils.widgets.get("container").strip()
storage_name = dbutils.widgets.get("storage_name").strip()
dataset = dbutils.widgets.get("dataset").strip()
file_key = dbutils.widgets.get("file_key").strip()

print(f"env: {env}")
print(f"file_date: {file_date}")
print(f"container: {container}")
print(f"storage_name: {storage_name}")
print(f"dataset: {dataset}")
print(f"file_key: {file_key}")

# COMMAND ----------

# MAGIC %run /paycan/common/utils

# COMMAND ----------

from datetime import datetime

if len(env) == 0:
    env = get_env()

if len(storage_name) == 0:
    storage_name = get_lake_storage_account(env)

if len(container) == 0:
    raise Exception("container is required.")

if len(file_date) == 0:
    raise Exception("file_date is required. Expected format: yyyy-MM-dd")

try:
    datetime.strptime(file_date, "%Y-%m-%d")
except ValueError:
    raise Exception(f"Invalid file_date '{file_date}'. Expected format: yyyy-MM-dd")

print(f"resolved env: {env}")
print(f"resolved storage_name: {storage_name}")
print(f"file_date: {file_date}")

# COMMAND ----------

# Reuse the standardizing notebook's config so file names, paths and fields stay in sync.
# rtr_schema.py must be importable (same folder as this notebook, or add its folder to sys.path).
from rtr_schema import RTR

DS_CONFIG = RTR.DATASETS[dataset]

# Column count check behaviour:
#   True  -> column count mismatch fails the file (catches schema drift from Payments Canada)
#   False -> column count mismatch is a warning only
STRICT_COLUMN_COUNT = True

# Bytes read from the start of each file for the format check
FORMAT_CHECK_BYTES = 4096


# Keep in sync with the standardizing notebook read encoding
def get_read_encoding(file_cfg):
    # return "UTF-8" if file_cfg["name"] == "reject_reason_code" else "windows-1252"
    return file_cfg.get("encoding", "UTF-8")


def get_raw_schema(file_cfg):
    """
    Raw (as-delivered) schema for a file. Payment uses the first schema in the list (EX_PAYMENT_SCHEMA_RAW).
    """
    s = file_cfg["schema"]
    return s[0] if isinstance(s, list) else s


def get_daily_path():
    return (
        f"abfss://{container}@{storage_name}.dfs.core.windows.net/"
        f"{DS_CONFIG['raw_path']}/{file_date}"
    )


def get_expected_file_name(file_cfg):
    return file_cfg["file_name"].replace("{DATE}", file_date)

# COMMAND ----------

def get_files_for_date():
    """
    Lists all files (not folders) in the dataset's daily raw folder.
    Returns None if the folder does not exist.
    """
    daily_path = get_daily_path()
    print(f"Scanning: {daily_path}")

    try:
        all_files = dbutils.fs.ls(daily_path)
    except Exception:
        return None

    return [f for f in all_files if not f.isDir()]


def check_file_count(daily_files, folder_warnings):
    """
    Validates the number of files in the daily folder matches the dataset config.

    Fewer JSON files than expected -> fail (a file is missing).
    More JSON files than expected  -> warning (extra files are not loaded).
    Non-JSON files in folder       -> warning.
    """
    expected_names = {get_expected_file_name(f) for f in DS_CONFIG["files"]}
    expected_count = len(expected_names)

    json_files = [f for f in daily_files if f.name.lower().endswith(".json")]
    other_files = sorted(f.name for f in daily_files if not f.name.lower().endswith(".json"))

    missing = sorted(expected_names - {f.name for f in json_files})
    extra = sorted({f.name for f in json_files} - expected_names)

    print(f"  Files in folder: {len(daily_files)} total, {len(json_files)} JSON, expected {expected_count} JSON")

    if other_files:
        folder_warnings.append(f"Non-JSON files in folder (ignored): {other_files}")
        print(f"  [WARN] Non-JSON files in folder: {other_files}")

    if extra:
        folder_warnings.append(f"Unexpected JSON files in folder (not loaded): {extra}")
        print(f"  [WARN] Unexpected JSON files: {extra}")

    if len(json_files) < expected_count or missing:
        raise Exception(
            f"File count check failed: {len(json_files)} JSON file(s) found, expected {expected_count}. "
            f"Missing: {missing}"
        )

    print(f"  [OK] File count matches: {expected_count} expected JSON file(s) present")


def find_file(file_cfg, daily_files):
    """
    Validates the expected file name exists (exact match, per spec naming).
    """
    expected_name = get_expected_file_name(file_cfg)
    matches = [f for f in daily_files if f.name == expected_name]

    if not matches:
        prefix = file_cfg["file_name"].split("{DATE}")[0].lower()
        similar = [f.name for f in daily_files if f.name.lower().startswith(prefix)]
        raise Exception(
            f"Expected file not found: {expected_name}. "
            f"Similar files in folder: {similar if similar else 'none'}"
        )

    print(f"  [OK] File found: {expected_name}")
    return matches[0]


def brief_spark_load(file_info, file_cfg):
    """
    Brief Spark JSON load used only to inspect fields and detect malformed records.

    Schema inference (not the standardized schema), PERMISSIVE mode.
    No type casting. No transformations.
    Cached so the corrupt record column can be queried on its own.
    """
    return (
        spark.read
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("encoding", get_read_encoding(file_cfg))
        .option("lineSep", "\n")
        .json(file_info.path)
        .cache()
    )

# COMMAND ----------

def check_zero_bytes(file_info, warnings):
    """
    Zero-byte files are possible (warning only). Returns False if the file is empty,
    in which case the remaining content checks are skipped.
    """
    if file_info.size == 0:
        warnings.append(f"{file_info.name}: zero-byte file")
        print(f"  [WARN] Zero-byte file, content checks skipped: {file_info.name}")
        return False

    print(f"  [OK] File is not zero bytes ({file_info.size:,} bytes): {file_info.name}")
    return True


def check_format(file_info):
    """
    Validates file format without a full read:
    - .json extension
    - no UTF-8 BOM
    - newline-delimited JSON: first non-blank character is '{' (not '[' - JSON array)
    - first line is a complete object (ends with '}'), i.e. not pretty-printed / multi-line JSON
    """
    name = file_info.name

    if not name.lower().endswith(".json"):
        raise Exception(f"Format check failed - not a .json file: {name}")

    head = dbutils.fs.head(file_info.path, FORMAT_CHECK_BYTES)

    if head.startswith("\ufeff"):
        raise Exception(f"Format check failed - file starts with a UTF-8 BOM: {name}")

    stripped = head.lstrip()

    if len(stripped) == 0:
        raise Exception(f"Format check failed - file contains only whitespace: {name}")

    if stripped[0] == "[":
        raise Exception(
            f"Format check failed - file is a JSON array, expected newline-delimited JSON "
            f"(one object per line): {name}"
        )

    if stripped[0] != "{":
        raise Exception(
            f"Format check failed - expected first character '{{', found '{stripped[0]}': {name}"
        )

    # Only check line completeness if the first line fits in the sampled bytes
    first_line = stripped.split("\n", 1)[0].rstrip("\r ")
    if "\n" in stripped and not first_line.endswith("}"):
        raise Exception(
            f"Format check failed - first line is not a complete JSON object "
            f"(pretty-printed / multi-line JSON?): {name}"
        )

    print(f"  [OK] Format valid (.json, newline-delimited JSON objects): {name}")


def check_valid_json(df, file_name):
    """
    Validates every non-blank line parsed as a JSON record.
    Catches truncated files and bad escaping.
    """
    if "_corrupt_record" not in df.columns:
        print(f"  [OK] All records parsed as JSON: {file_name}")
        return

    bad = df.filter("_corrupt_record IS NOT NULL")
    bad_count = bad.count()

    if bad_count > 0:
        sample = [r["_corrupt_record"][:200] for r in bad.select("_corrupt_record").limit(3).collect()]
        raise Exception(
            f"{bad_count:,} malformed JSON record(s) in {file_name}. "
            f"Expected one JSON object per line. Sample:\n  " + "\n  ".join(sample)
        )

    print(f"  [OK] All records parsed as JSON: {file_name}")


def get_actual_fields(df):
    return {c.lower() for c in df.columns if c != "_corrupt_record"}


def check_expected_fields_exist(df, file_cfg, file_name, warnings):
    """
    Validates expected top-level fields exist in the raw file.

    Missing mandatory (non-nullable) field -> fail.
    Missing optional (nullable) field      -> warning.
    Comparison is case-insensitive (matches Spark default column resolution).
    """
    actual = get_actual_fields(df)

    if len(actual) == 0:
        raise Exception(f"No fields detected in file: {file_name}")

    expected = {f.name.lower(): f.nullable for f in get_raw_schema(file_cfg).fields}

    missing_mandatory = sorted(n for n, nullable in expected.items() if not nullable and n not in actual)
    missing_optional = sorted(n for n, nullable in expected.items() if nullable and n not in actual)

    if missing_mandatory:
        raise Exception(f"Missing mandatory fields in {file_name}:\n  {missing_mandatory}")

    if missing_optional:
        warnings.append(f"{file_name}: optional fields absent from every record: {missing_optional}")
        print(f"  [WARN] Optional fields absent: {missing_optional}")

    print(f"  [OK] All mandatory fields exist for {file_name}")


def check_column_count(df, file_cfg, file_name, warnings):
    """
    Validates the number of top-level fields in the file matches the schema.
    Spark infers fields present in at least one record (a field sent as null still counts).
    Reports missing / extra field names on mismatch.
    """
    actual = get_actual_fields(df)
    expected = {f.name.lower() for f in get_raw_schema(file_cfg).fields}

    actual_count, expected_count = len(actual), len(expected)

    if actual_count == expected_count and actual == expected:
        print(f"  [OK] Column count matches: {actual_count} of {expected_count}")
        return actual_count

    msg = (
        f"{file_name}: column count {actual_count}, expected {expected_count}. "
        f"Missing: {sorted(expected - actual)}; Extra (not in rtr_schema, will be dropped): {sorted(actual - expected)}"
    )

    if STRICT_COLUMN_COUNT:
        raise Exception(f"Column count check failed - {msg}")

    warnings.append(msg)
    print(f"  [WARN] {msg}")
    return actual_count


def check_has_records(row_count, file_name, warnings):
    """
    Records can legitimately be absent (warning only).
    """
    if row_count == 0:
        warnings.append(f"{file_name}: no records")
        print(f"  [WARN] File has no records: {file_name}")
        return

    print(f"  [OK] {row_count:,} record(s) found: {file_name}")

# COMMAND ----------

def validate_file(file_cfg, daily_files):
    """
    Runs file-level validation:
    1. File exists with expected name
    2. Zero-byte check (warning)
    3. Format check
    4. Valid JSON records
    5. Expected fields exist
    6. Column count
    7. At least one record (warning)
    """
    expected_name = get_expected_file_name(file_cfg)
    warnings = []

    print("\n" + "-" * 100)
    print(f"Validating: {dataset} - {file_cfg['name']} ({expected_name})")
    print("-" * 100)

    result = {
        "file_key": file_cfg["name"], "file": expected_name, "bytes": None,
        "rows": None, "cols": None, "status": "valid", "warnings": warnings, "error": None
    }

    df = None
    try:
        file_info = find_file(file_cfg, daily_files)
        result["bytes"] = file_info.size

        if not check_zero_bytes(file_info, warnings):
            result["rows"] = 0
            result["status"] = "empty"
            return result

        check_format(file_info)

        df = brief_spark_load(file_info, file_cfg)

        check_valid_json(df, file_info.name)
        check_expected_fields_exist(df, file_cfg, file_info.name, warnings)
        result["cols"] = check_column_count(df, file_cfg, file_info.name, warnings)

        row_count = df.count()
        result["rows"] = row_count
        check_has_records(row_count, file_info.name, warnings)

        if row_count == 0:
            result["status"] = "empty"
        elif warnings:
            result["status"] = "warning"

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        print(f"  [FAIL] {e}")

    finally:
        if df is not None:
            df.unpersist()

    return result

# COMMAND ----------

# Resolve which files to validate
if file_key == "ALL":
    file_cfgs = DS_CONFIG["files"]
else:
    file_cfgs = [f for f in DS_CONFIG["files"] if f["name"] == file_key]
    if len(file_cfgs) == 0:
        valid_keys = [f["name"] for f in DS_CONFIG["files"]]
        raise Exception(f"file_key '{file_key}' is not in dataset '{dataset}'. Valid keys: {valid_keys}")

daily_files = get_files_for_date()

if daily_files is None or len(daily_files) == 0:
    print(
        f"[EXIT] no_files — no files found for dataset={dataset}, "
        f"file_date={file_date} at {get_daily_path()}"
    )
    dbutils.notebook.exit("no_files")

# Folder-level file count check (only when validating ALL files for the dataset)
folder_warnings = []
folder_error = None

if file_key == "ALL":
    print("\n" + "-" * 100)
    print(f"Folder check: {get_daily_path()}")
    print("-" * 100)
    try:
        check_file_count(daily_files, folder_warnings)
    except Exception as e:
        folder_error = str(e)
        print(f"  [FAIL] {e}")

# COMMAND ----------

results = [validate_file(cfg, daily_files) for cfg in file_cfgs]

# COMMAND ----------

def fmt(v):
    return f"{v:,}" if v is not None else "-"

print("\n" + "=" * 125)
print(f"PER-FILE VALIDATION SUMMARY - dataset={dataset}, file_date={file_date}")
print("=" * 125)
print(f"{'FILE KEY':<30} {'FILE NAME':<50} {'BYTES':>12} {'ROWS':>10} {'COLS':>5} {'STATUS':>10}")
print("-" * 125)

for r in results:
    print(
        f"{r['file_key']:<30} {r['file']:<50} {fmt(r['bytes']):>12} "
        f"{fmt(r['rows']):>10} {fmt(r['cols']):>5} {r['status']:>10}"
    )

all_warnings = folder_warnings + [w for r in results for w in r["warnings"]]
if all_warnings:
    print("\nWARNINGS")
    for w in all_warnings:
        print(f"  - {w}")

failed = [r for r in results if r["status"] == "failed"]
if failed or folder_error:
    print("\nFAILURES")
    if folder_error:
        print(f"  - [folder] {folder_error}")
    for r in failed:
        print(f"  - {r['file']}: {r['error']}")
    raise Exception(
        f"Validation failed for dataset={dataset}, file_date={file_date}: "
        f"{len(failed)} of {len(results)} file(s) failed"
        + ("; folder file count check failed" if folder_error else "")
    )

print(f"\n[EXIT] valid — all {len(results)} file(s) for dataset={dataset} passed per-file validation.")
dbutils.notebook.exit("valid")
