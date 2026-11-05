# Databricks notebook source
cycle_date = "2023-01-12"

# COMMAND ----------

def test_run(cycle_date, file_name):
  dbutils.notebook.run("./1_curate_main", 0, {"cycle_date": cycle_date, "file_name": file_name})

# COMMAND ----------

file_name = "participant_institution"
test_run(cycle_date, file_name)


# COMMAND ----------

file_name = "payment"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "net_send_schedule"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "operating_schedule"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "operational_status"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "net_send_override"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "net_send_limit"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "liquidity_threshold"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "liquidity_position"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "gridlock"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "credit_position"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "credit_activity"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "loan"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "lom_status"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "minum_balance"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "pause_payments"
test_run(cycle_date, file_name)

# COMMAND ----------

file_name = "transfer"
test_run(cycle_date, file_name)
