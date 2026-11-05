CREATE   VIEW [paycan_restricted_curated].[LYNX_LOM_STATUS_RAW]
AS SELECT 	[CYCLE_DATE] as cycle_date
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institution_short_name
	,[PARTICIPANT_NUMBER] as participant_number
	,[PARTICIPANT_BIC] as participant_bic
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT] as participant_short_name
	,[SETTLEMENT_MECHANISM_CD] as settlement_mechanism
	,[STATUS_CD] as [status]
	,[EFFECTIVE_TS] as effective_time
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_LOM_STATUS];