CREATE   VIEW [paycan_restricted_curated].[LYNX_LIQUIDITY_POSITION_RAW]
AS SELECT 
	[CYCLE_DATE] as cycle_date
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institution_short_name
	,[SETTLEMENT_MECHANISM_CD] as settlement_mechanism
	,[SENT_AMOUNT] as sent_amount
	,[RECEIVED_AMOUNT] as received_amount
	,[LIQUIDITY_POSITION] as liquidity_position
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_LIQUIDITY_POSITION];