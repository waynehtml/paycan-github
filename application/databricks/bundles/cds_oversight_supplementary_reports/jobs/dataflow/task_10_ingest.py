# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Task
# MAGIC External DataBridge (DMZ) pipeline `cds_sftp.json`:
# MAGIC 1. `download` - SFTP `CDS_<Mon>_<Fri>.zip` from TMX (key in `lake-kv-paycan-scope`).
# MAGIC 2. `extract` - unzip all files; the zip is deleted.
# MAGIC 3. `upload` - extracted files to AV scan storage, then resolved to landing.

# COMMAND ----------

# DBTITLE 1,Show widget for development purpose
# Development-only widgets.
dbutils.widgets.text("debug", "True")

# COMMAND ----------

# DBTITLE 1,Load common notebook
# MAGIC %run ../../common

# COMMAND ----------

# DBTITLE 1,Import libraries
# Standard-library imports
import logging
from datetime import date, timedelta
from pathlib import Path

# OEDS domain-specific imports
from oeds.domain.utils.ingest.ingest_utils import (
    DisConnectionOptions,
    FileVerificationOptions,
    run_ingest,
)

# COMMAND ----------

# DBTITLE 1,Init logging and context
logger = logging.getLogger(__name__)

job_ctx = get_job_context()
logger.debug("Initialized job context")

# SFTP private key name in the project secret scope (resolved by DataBridge).
SFTP_PRIVATE_KEY = "tmx-cds-sup-sftp-key"

# COMMAND ----------

# DBTITLE 1,External DataBridge connection
service_spn_id = dbutils.secrets.get(
    scope=job_ctx["secrete_scope"],
    key=job_ctx["service_spn_id_key"],
)
service_spn_secret = dbutils.secrets.get(
    scope=job_ctx["secrete_scope"],
    key=job_ctx["service_spn_secrete_key"],
)

connection = DisConnectionOptions(
    host=job_ctx.get("external_databricks_workspace_url"),
    azure_tenant_id=job_ctx.get("azure_tenant_id"),
    azure_client_id=service_spn_id,
    azure_client_secret=service_spn_secret,
    azure_workspace_resource_id=job_ctx.get("external_databricks_resource_id"),
    dis_job_id=int(job_ctx.get("external_dis_job_id")),
)

# COMMAND ----------

# DBTITLE 1,Ingestion parameters
# Standalone (non-job) runs use the same rule as main for today's date.
_today = date.today()
_wed = _today - timedelta(days=(_today.weekday() - 2) % 7)
_debug_stamp = (
    f"{_wed - timedelta(days=9):%Y%m%d}_{_wed - timedelta(days=5):%Y%m%d}"
)
_debug_date = (_wed - timedelta(days=5)).isoformat()

file_date = dbutils.jobs.taskValues.get(
    taskKey="main", key="file_date", debugValue=_debug_date
    # taskKey="main", key="file_date", debugValue="2026-10-09"
)
file_stamp = dbutils.jobs.taskValues.get(
    taskKey="main", key="file_stamp", debugValue=_debug_stamp
    # taskKey="main", key="file_stamp", debugValue="20261005_20261009"
)
# SFTP year folder = year of the week-ending Friday in the stamp.
file_year = file_stamp[-8:-4]
logger.info(f"file_stamp: {file_stamp}, file_date: {file_date}, file_year: {file_year}")

# Client-side [[...]] values only; secrets stay as server-side keyvault() lookups.
ingestion_params = {
    "env": job_ctx.get("env"),
    "file_date": file_date,
    "file_stamp": file_stamp,
    "file_year": file_year,
    "secrete_scope": job_ctx.get("secrete_scope"),
    "sftp_private_key": SFTP_PRIVATE_KEY,
    "oeds_secrete_scope": job_ctx.get("oeds_secrete_scope"),
    "dis_av_root": job_ctx.get("dis_av_root"),
    "tenant_id": job_ctx.get("azure_tenant_id"),
    "dis_oedisext_client_id_key": job_ctx.get("dis_oedisext_client_id_key"),
    "dis_oedisext_client_secret_key": job_ctx.get("dis_oedisext_client_secret_key"),
}

# Map AV scan paths back to the landing root (relative paths preserved).
verification = FileVerificationOptions(
    dest_root=job_ctx.get("dis_av_root"),
    land_root=job_ctx.get("land_root"),
)

# COMMAND ----------

# DBTITLE 1,Run ingestion
# Raises IngestTaskError on pipeline or landing-verification failure.
ingest_task_result = run_ingest(
    connection=connection,
    config_file="cds_sftp.json",
    template_params=ingestion_params,
    verification=verification,
)
logger.info(
    "Ingestion finished. Files landed under %s",
    Path(job_ctx.get("land_root")) / file_date,
)
