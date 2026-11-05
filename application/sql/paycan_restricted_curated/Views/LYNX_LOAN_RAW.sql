CREATE   VIEW [paycan_restricted_curated].[LYNX_LOAN_RAW]
AS SELECT 	[LOAN_ENTRY_ID] as loan_entry_id
	,[CYCLE_DATE] as cycle_date
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institution_short_name
	,[PARTICIPANT_NUMBER] as participant_number
	,[PARTICIPANT_BIC] as participant_bic
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT] as participant_short_name
	,[ACCOUNT_ID] as account_id
	,[ACCOUNT_NAME] as account_bban
	,[SYSTEM_REFERENCE_CD] as system_reference
	,[ENTRY_TYPE] as entry_type
	,[AMOUNT] as amount
	,[ENTRY_TS] as entry_time
	,[ACCOUNT_BALANCE] as account_balance
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_LOAN];