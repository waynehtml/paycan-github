CREATE   VIEW [paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY_RAW]
AS SELECT 
	[CREDIT_ACTIVITY_ID] as credit_activity_id
	,[CYCLE_DATE] as cycle_date
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institution_short_name
	,[PARTICIPANT_NUMBER] as participant_number
	,[PARTICIPANT_BIC] as participant_bic
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT] as participant_short_name
	,[SYSTEM_REFERENCE_CD] as system_reference
	,[TOTAL_AMOUNT] as total_amount
	,[EARMARKED_AMOUNT] as earmarked_amount
	,[EARMARKED_LOM] as earmarked_lom
	,[LAST_MODIFIED_TS] as last_modified
	,[STATUS_TYPE_CD] as status_type
	,[STATUS_CHANGE_TIME] as status_change_time
	,[STATUS_REASON_CD] as status_reason
FROM [paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY];