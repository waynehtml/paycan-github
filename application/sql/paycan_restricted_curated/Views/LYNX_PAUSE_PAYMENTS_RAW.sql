CREATE   VIEW [paycan_restricted_curated].[LYNX_PAUSE_PAYMENTS_RAW]
AS SELECT [PAUSE_PAYMENTS_ID] as pause_payments_id
	,[CYCLE_DATE] as cycle_date
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institution_short_name
	,[PARTICIPANT_NUMBER] as participant_number
	,[PARTICIPANT_BIC] as participant_bic
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT] as participant_short_name
	,[ORIGINATOR_REFERENCE] as originator_reference
	,[REGISTERED_TS] as registered_time
	,[EFFECTIVE_TS] as effective_time
	,[LAST_MODIFIED_TS] as last_modified
	,[CONTRA_INSTITUTION_ACTION] as contra_institution_action
	,[CONTRA_INSTITUTION_NUMBER] as contra_institution_number
	,[CONTRA_INSTITUTION_IRN] as contra_institution_irn
	,[CONTRA_INSTITUTION_NAME] as contra_institution_name
	,[CONTRA_INSTITUTION] as contra_institution_short_name
	,[CONTRA_PARTICIPANT_NUMBER] as contra_participant_number
	,[CONTRA_PARTICIPANT_BIC] as contra_participant_bic
	,[CONTRA_PARTICIPANT_NAME] as contra_participant_name
	,[CONTRA_PARTICIPANT] as contra_participant_short_name
FROM [paycan_restricted_curated].[LYNX_PAUSE_PAYMENTS];