CREATE   VIEW [paycan_restricted_curated].[LYNX_GRIDLOCK_RAW]
AS SELECT
	[GRIDLOCK_PARTICIPANT_ID] as gridlock_participant_id
	,[GRIDLOCK_BATCH_ID] as gridlock_batch_id
	,[CYCLE_DATE] as cycle_date
	,[START_TS] as start_time
	,[END_TS] as end_time
	,[TRIGGER] as [trigger]
	,[GRIDLOCK_INTERVAL] as gridlock_interval
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institution_short_name
	,[PARTICIPANT_NUMBER] as pariticipant_number
	,[PARTICIPANT_BIC] as participant_bic
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT] as participant_short_name
	,[BEGIN_QUEUE_VALUE] as begin_queue_value
	,[END_QUEUE_VALUE] as end_queue_value
	,[BEGIN_QUEUE_VOLUME] as begin_queue_volume
	,[END_QUEUE_VOLUME] as end_queue_volume
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_GRIDLOCK];