CREATE VIEW [paycan_restricted_curated].[LYNX_LIQUIDITY_THRESHOLD_RAW]
AS SELECT [CREATE_DATE] as create_date
	,[ACTIVE_DATE] as active_date
	,[DELETE_DATE] as delete_date
	,[CHANGE_TS] as change_time
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institution_short_name
	,[PARTICIPANT_NUMBER] as participant_number
	,[PARTICIPANT_BIC] as participant_bic
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT] as participant_short_name
	,[SETTLEMENT_MECHANISM_CD] as settlement_mechanism
	,[THRESHOLD_AMOUNT] as threshold_amount
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_LIQUIDITY_THRESHOLD];