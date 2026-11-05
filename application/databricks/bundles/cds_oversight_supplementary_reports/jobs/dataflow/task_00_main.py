# Databricks notebook source
# MAGIC %md
# MAGIC ## Main Task
# MAGIC - Initialize shared business parameters from job context.
# MAGIC - Publish task values used by downstream workflow stages.

# COMMAND ----------

# DBTITLE 1,Show widget for development purpose
# Development-only widgets.
dbutils.widgets.text("debug", "True")
dbutils.widgets.text("rundate", "")
dbutils.widgets.text("args", "")

# COMMAND ----------

# DBTITLE 1,Load common notebook
# MAGIC %run ../../common

# COMMAND ----------

# DBTITLE 1,Import libraries
# Standard-library imports
import logging
import os
from datetime import timedelta
from pathlib import Path

from oeds.domain.utils.calendar import CalendarManager

# COMMAND ----------

# DBTITLE 1,Init logging and context
logger = logging.getLogger(__name__)

job_ctx = get_job_context()
logger.debug("Initialized job context")

calendar = CalendarManager.get_instance()

# The rundate parameter supports historical workflow runs and defaults to today.
rundate = parse_date(job_ctx["rundate"], job_ctx["python_date_format"])
logger.info(f"rundate: {rundate}")
# Publish the normalized rundate for curated processing and status records.
dbutils.jobs.taskValues.set(key="rundate", value=rundate.isoformat())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Weekly CDS batch
# MAGIC - Scheduled Wednesday, with a Thursday retry.
# MAGIC - The batch is the previous week's Monday-Friday:
# MAGIC   Wed 2026-10-14 -> `CDS_20261005_20261009.zip`.
# MAGIC - `file_date` (batch folder) is the week-ending Friday.

# COMMAND ----------

# Anchor on the most recent Wednesday on or before rundate.
anchor_wed = rundate - timedelta(days=(rundate.weekday() - 2) % 7)
week_start = anchor_wed - timedelta(days=9)  # previous Monday
week_end = anchor_wed - timedelta(days=5)  # previous Friday
file_stamp = f"{week_start:%Y%m%d}_{week_end:%Y%m%d}"
file_date = week_end.isoformat()
logger.info(f"file_stamp: {file_stamp}, file_date: {file_date}")

dbutils.jobs.taskValues.set(key="file_stamp", value=file_stamp)
dbutils.jobs.taskValues.set(key="file_date", value=file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Thursday retry
# MAGIC Skip the Thursday run when Wednesday already loaded the batch to raw.

# COMMAND ----------

raw_dir = str(Path(job_ctx.get("raw_root")) / file_date)
if rundate.weekday() == 3 and os.path.isdir(raw_dir) and os.listdir(raw_dir):
    logger.info("Batch %s already in raw (%s). Skipping retry.", file_stamp, raw_dir)
    dbutils.jobs.taskValues.set(key="flow_action", value="complete")
    dbutils.notebook.exit(f"Batch {file_stamp} already loaded.")

# COMMAND ----------

# DBTITLE 1,Landing directory
# land_dir is the landing directory for the current batch.
land_dir = str(Path(job_ctx.get("land_root")) / file_date)
logger.info(f"land_dir: {land_dir}")
dbutils.jobs.taskValues.set(key="land_dir", value=land_dir)

# COMMAND ----------

# raw_dir is the raw-zone directory for the current batch.
raw_dir = str(Path(job_ctx.get("raw_root")) / file_date)
logger.info(f"raw_dir: {raw_dir}")
dbutils.jobs.taskValues.set(key="raw_dir", value=raw_dir)
