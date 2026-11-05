# Databricks notebook source
# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../../../common/utils

# COMMAND ----------

# DBTITLE 1,Get UC name
unity_catalog_name = get_unity_catalog_name(dbutils.widgets.get("environment"))
##spark.conf.set('ad.catalog',unity_catalog_name)

# COMMAND ----------

# DBTITLE 1,Create loggin table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS  {unity_catalog_name}.standardized_db.tbl_loading_log_v2(
    load_start_ts  date ,
    load_end_ts  date ,
    load_program  string,
    load_result  string 
)
USING DELTA 
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);
""")

# COMMAND ----------

# DBTITLE 1,Create table collateral
spark.sql(f"""
CREATE TABLE IF NOT EXISTS  {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_collateral (
    date  date ,
    activity_name  string ,
    member  int ,
    fund_type string ,
    asset_type string ,
    product_type string ,
    product_symbol string ,
    bond_issuer string ,
    isin string ,
    long_description string ,
    decimal_quantity decimal(22,10),
    value decimal(20,8), 
    maturity_date date,
    issue_date date,
    time_to_maturity_at_issuance_in_years decimal(11,8),
    time_to_maturity string ,
    time_to_maturity_in_years decimal(10,8)
)
USING DELTA 
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);
""")


# COMMAND ----------

# DBTITLE 1,Create table vm
spark.sql(f"""
CREATE TABLE IF NOT EXISTS  {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_vm
(
	date date ,
	member int,
	eod_daily_vm_cash_settlement decimal(22,10)
) 
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);
""")

# COMMAND ----------

# DBTITLE 1,Create table liquidity_data
spark.sql(f"""
CREATE TABLE IF NOT EXISTS  {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_liquidity_data (
	date date ,
	scenario_name string ,
	scenario_idx int ,
	parent_member_name_id int ,
	need_fi_dplus1 decimal(22,10) ,
	need_fi_dplus2 decimal(22,10) ,
	need_fi_dplus3 decimal(22,10) ,
	need_fi_dplus4 decimal(22,10) ,
	need_futures_dplus1 decimal(22,10) ,
	need_futures_dplus2 decimal(22,10) ,
	need_futures_dplus3 decimal(22,10) ,
	need_futures_dplus4 decimal(22,10) ,
	need_options_dplus1 decimal(22,10) ,
	need_options_dplus2 decimal(22,10) ,
	need_options_dplus3 decimal(22,10) ,
	need_options_dplus4 decimal(22,10) ,
	need_eqy_dplus1 decimal(22,10) ,
	need_eqy_dplus2 decimal(22,10) ,
	need_eqy_dplus3 decimal(22,10) ,
	need_eqy_dplus4 decimal(22,10) ,
	source_fi_dplus1 decimal(22,10) ,
	source_fi_dplus2 decimal(22,10) ,
	source_eqy_dplus1 decimal(22,10) ,
	source_eqy_dplus2 decimal(22,10) ,
	eqy_col_value decimal(22,10) ,
	fi_col_value decimal(22,10) ,
	member_cf_fi_value int ,
	cash_col_value decimal(22,10) ,
	survivor_cf_fi_value bigint ,
	survivor_cf_cash_value bigint ,
	survivor_slf_cash_value bigint ,
	exposure_fi decimal(22,10)  ,
	exposure_derivatives decimal(22,10) ,
	total_cash_resources_derivatives decimal(22,10) 
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);
""")


# COMMAND ----------

# DBTITLE 1,Create table members
spark.sql(f"""
CREATE TABLE IF NOT EXISTS  {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_members(
	date date ,
	activity_name string ,
	member_name_id int ,
	fund_type string ,
	requirement_amount decimal(20,8) ,
	asset_value decimal(20,8) ,
	inventory_asset_summary_total decimal(20,8)
) 
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);
""")


# COMMAND ----------

# DBTITLE 1,Create table margins
spark.sql(f"""
CREATE TABLE IF NOT EXISTS  {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_margins(
	date date ,
	activity_name string ,
	clearing_account string ,
	member_name_id int ,
	risk_model string ,
	account_type string ,
	sub_account string ,
	reporting_level string ,
	initial_margin_offset_group string ,
	combined_commodity_code string ,
	fund_type_detail string ,
	base_margin_amount decimal(20,8) ,
	requirement_amount decimal(22,10) ,
	collateral_pool_type string ,
	customer_account_name string 
) 
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);
""")


# COMMAND ----------

# DBTITLE 1,Create table positions
spark.sql(f"""
CREATE TABLE IF NOT EXISTS  {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_positions(
	date date ,
	position_type string ,
	position_id string ,
	clearing_account string ,
	member_name int ,
	risk_model string ,
	account_type string ,
	sub_account string ,
	accounting_type string ,
	product_symbol string ,
	product_type string ,
	usage_type string ,
	underlying_symbol string ,
	expiry_date int ,
	strike_price decimal(22,10),
	call_put_code string ,
	premium_multiplier bigint ,
	contract_size bigint ,
	long bigint ,
	short bigint ,
	long_cash decimal(22,10) ,
	short_cash decimal(22,10) ,
	marginable_long bigint ,
	marginable_short bigint ,
	processed_selected_margin_price decimal(22,10) ,
	underlying_selected_margin_price decimal(22,10) ,
	combined_commodity_code string ,
	transaction_type string ,
	settlement_date int ,
	activity_name string ,
	collateral_pool_type string ,
	customer_account_name string 
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);
""")


# COMMAND ----------

# DBTITLE 1,Create table cm
spark.sql(f"""
CREATE TABLE IF NOT EXISTS  {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_cm(
	name string ,
	lcm string ,
	number int ,
	options string ,
	futures string ,
	fixed_income string 
) 
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);
""")


# COMMAND ----------

# DBTITLE 1,Create table liquidity_report
spark.sql(f"""
CREATE TABLE IF NOT EXISTS  {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_liquidity_report(
	date date ,
	day_dmp string ,
	largest_derivatives_exposure int ,
	scenario string ,
	member int ,
	available_resources int ,
	futures_exposure int ,
	options_exposure string 
) 
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);
""")


# COMMAND ----------

# DBTITLE 1,Create table Date_Dimension
spark.sql(f"""
CREATE TABLE IF NOT EXISTS  {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_date_dimension(
	The_Date date NOT NULL PRIMARY KEY,
	The_Day int ,
	The_Day_Suffix string ,
	The_Day_Name string ,
	The_Day_Of_Week int ,
	The_Day_Of_Week_In_Month int ,
	The_Day_Of_Year int ,
	Is_Weekend int  NOT NULL ,
	The_Week int ,
	The_ISO_week int ,
	The_First_Of_Week date ,
	The_Last_Of_Week date ,
	The_Week_Of_Month int ,
	The_Month int ,
	The_Month_Name string ,
	The_First_Of_Month date ,
	The_Last_Of_Month date ,
	The_First_Of_Next_Month date ,
	The_Last_Of_Next_Month date ,
	The_Quarter int ,
	The_First_Of_Quarter date ,
	The_Last_Of_Quarter date ,
	The_Year int ,
	The_ISO_Year int ,
	The_First_Of_Year date ,
	The_Last_Of_Year date ,
	Is_Leap_Year boolean ,
	Has_53_Weeks int  NOT NULL,
	Has_53_ISO_Weeks int  NOT NULL,
	MM_YYYY string ,
	Style_101 string ,
	Style_103 string ,
	Style_112 string ,
	Style_120 string 
) 
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);
""")


# COMMAND ----------

# DBTITLE 1,Create table Holiday_Dimension
spark.sql(f"""
CREATE TABLE IF NOT EXISTS  {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_holiday_dimension(
	The_Date date NOT NULL ,
	Holiday_Text string NOT NULL,
	The_Geo string,	
  FOREIGN KEY (The_Date) REFERENCES {unity_catalog_name}.standardized_db.tbl_tmx_cdcc_date_dimension(The_Date)
) 
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
);
""")