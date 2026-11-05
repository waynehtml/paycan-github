CREATE   VIEW [paycan_restricted_curated].[LYNX_CREDIT_POSITION_RAW]
AS SELECT 
	[CREDIT_ENTRY_ID] as credit_entry_id
	,[CYCLE_DATE] as cycle_date
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institution_short_name
	,[PARTICIPANT_NUMBER] as participant_number
	,[PARTICIPANT_BIC] as participant_bic
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT] as participant_short_name
	,[EVENT_TS] as event_time
	,[TRIGGER_CD] as trigger_code
	,[TRIGGER_NAME] as trigger_name
	,[TOTAL_LIMIT] as total_limit
	,[TOTAL_AVAILABLE] as total_available
	,[ENTRY_TYPE_CD] as entry_type_code
	,[ENTRY_TYPE_NAME] as entry_type_name
	,[ENTRY_AMOUNT] as entry_amount
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_CREDIT_POSITION];