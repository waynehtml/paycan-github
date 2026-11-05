# Databricks notebook source
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")


# COMMAND ----------

# MAGIC %run /paycan/common/utils

# COMMAND ----------

unity_catalog_name = get_unity_catalog_name(environment)

# Create views
views = [
    "tbl_tmx_cds_cleared_and_novated_in_cns",
    "tbl_tmx_cds_cns_position_values",
    "tbl_tmx_cds_payment_obligations",
    "tbl_tmx_cds_line_of_credit",
    "tbl_tmx_cds_nscc_position_values",
    "tbl_tmx_cds_usd_deposit_bank_balances",
    "tbl_tmx_cds_settlement_fails",
    "tbl_tmx_cds_nyl_daily_mark_to_market_by_participant",
    "tbl_tmx_cds_cns_daily_mark_to_market_by_participant",
    "tbl_tmx_cds_settled_in_cdsx",
    "tbl_tmx_cds_new_york_link_trade_stats"
]

for tbl_name in views:
    vwu_name = tbl_name.replace("tbl_", "vwu_", 1)

    sql_stmt = f"""
    CREATE OR REPLACE VIEW {unity_catalog_name}.view_db.{vwu_name}
    AS
    SELECT *
    FROM {unity_catalog_name}.standardized_db.{tbl_name}
    """

    print(sql_stmt)
    spark.sql(sql_stmt)
