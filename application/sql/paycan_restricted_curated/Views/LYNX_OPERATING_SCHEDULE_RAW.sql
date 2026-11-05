CREATE   VIEW [paycan_restricted_curated].[LYNX_OPERATING_SCHEDULE_RAW]
AS SELECT 	[WINDOW_LOG_ID] as window_log_id
	,[CYCLE_DATE] as cycle_date
	,[EVENT] as [event]
	,[START_TS] as start_time
	,[END_TS] as end_time
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_OPERATING_SCHEDULE];