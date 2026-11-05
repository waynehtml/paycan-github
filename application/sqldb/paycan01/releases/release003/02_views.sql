CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY_SUMMARY_VIEW]
AS SELECT [BATCH_ID]
      ,[CREDIT_ACTIVITY_ID]
      ,[CYCLE_DATE]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[TOTAL_AMOUNT]
      ,[EARMARKED_AMOUNT]
      ,[EARMARKED_LOM]
      ,[REGISTERED]
      ,[PROCESSED]
      ,[REGISTERED_TIME]
      ,[PROCESSED_TIME]
FROM [paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY_SUMMARY];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY_VIEW]
AS SELECT [LYNX_CREDIT_ACTIVITY_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[CREDIT_ACTIVITY_ID]
      ,[CYCLE_DATE]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[SYSTEM_REFERENCE_CD]
      ,[TOTAL_AMOUNT]
      ,[EARMARKED_AMOUNT]
      ,[EARMARKED_LOM]
      ,[LAST_MODIFIED_TS]
      ,[STATUS_TYPE_CD]
      ,[STATUS_CHANGE_TIME]
      ,[STATUS_REASON_CD]
      ,[STATUS_TYPE_CATEGORY]
FROM [paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY_RAW]
AS SELECT [CREDIT_ACTIVITY_ID] as credit_activity_id
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

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_CREDIT_POSITION_VIEW]
AS SELECT [LYNX_CREDIT_POSITION_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[CREDIT_ENTRY_ID]
      ,[CYCLE_DATE]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[EVENT_TS]
      ,[TRIGGER_CD]
      ,[TRIGGER_NAME]
      ,[TOTAL_LIMIT]
      ,[TOTAL_AVAILABLE]
      ,[ENTRY_TYPE_CD]
      ,[ENTRY_TYPE_NAME]
      ,[ENTRY_AMOUNT]
      ,[LAST_MODIFIED_TS]
FROM [paycan_restricted_curated].[LYNX_CREDIT_POSITION];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_CREDIT_POSITION_RAW]
AS SELECT [CREDIT_ENTRY_ID] as credit_entry_id
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

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_GRIDLOCK_VIEW]
AS SELECT [LYNX_GRIDLOCK_ID]
	,[BATCH_ID]
	,[VALIDATION_STATUS]
	,[GRIDLOCK_PARTICIPANT_ID]
	,[GRIDLOCK_BATCH_ID]
	,[CYCLE_DATE]
	,[START_TS]
	,[END_TS]
	,[TRIGGER]
	,[GRIDLOCK_INTERVAL]
	,[INSTITUTION]
	,[PARTICIPANT]
	,[BEGIN_QUEUE_VALUE]
	,[END_QUEUE_VALUE]
	,[BEGIN_QUEUE_VOLUME]
	,[END_QUEUE_VOLUME]
	,[LAST_MODIFIED_TS]
	,[MATCHED_WITH_PAY]
FROM [paycan_restricted_curated].[LYNX_GRIDLOCK];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_GRIDLOCK_RAW]
AS SELECT [GRIDLOCK_PARTICIPANT_ID] as gridlock_participant_id
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

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_LIQUIDITY_POSITION_VIEW]
AS SELECT [LYNX_LIQUIDITY_POSITION_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[CYCLE_DATE]
      ,[INSTITUTION]
      ,[SETTLEMENT_MECHANISM_CD]
      ,[SENT_AMOUNT]
      ,[RECEIVED_AMOUNT]
      ,[LIQUIDITY_POSITION]
      ,[LAST_MODIFIED_TS]
FROM [paycan_restricted_curated].[LYNX_LIQUIDITY_POSITION];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_LIQUIDITY_POSITION_RAW]
AS SELECT [CYCLE_DATE] as cycle_date
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

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_LIQUIDITY_THRESHOLD_VIEW]
AS SELECT [LYNX_LIQUIDITY_THRESHOLD_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[CREATE_DATE]
      ,[ACTIVE_DATE]
      ,[DELETE_DATE]
      ,[CHANGE_TS]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[SETTLEMENT_MECHANISM_CD]
      ,[THRESHOLD_AMOUNT]
      ,[LAST_MODIFIED_TS]
FROM [paycan_restricted_curated].[LYNX_LIQUIDITY_THRESHOLD];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_LIQUIDITY_THRESHOLD_RAW]
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

GO


CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_LOAN_VIEW]
AS SELECT [LYNX_LOAN_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[LOAN_ENTRY_ID]
      ,[CYCLE_DATE]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[ACCOUNT_NAME]
      ,[ENTRY_TYPE]
      ,[AMOUNT]
      ,[ENTRY_TS]
      ,[ACCOUNT_BALANCE]
      ,[LAST_MODIFIED_TS]
FROM [paycan_restricted_curated].[LYNX_LOAN];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_LOAN_RAW]
AS SELECT [LOAN_ENTRY_ID] as loan_entry_id
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

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_LOM_STATUS_VIEW]
AS SELECT [LYNX_LOM_STATUS_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[CYCLE_DATE]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[SETTLEMENT_MECHANISM_CD]
      ,[STATUS_CD]
      ,[EFFECTIVE_TS]
      ,[LAST_MODIFIED_TS]
FROM [paycan_restricted_curated].[LYNX_LOM_STATUS];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_LOM_STATUS_RAW]
AS SELECT [CYCLE_DATE] as cycle_date
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

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_MINUM_BALANCE_VIEW]
AS SELECT [LYNX_MINUM_BALANCE_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[CYCLE_DATE]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[SETTLEMENT_MECHANISM_CD]
      ,[MINIMUM_AMOUNT]
      ,[MET_TS]
      ,[LAST_MODIFIED_TS]
FROM [paycan_restricted_curated].[LYNX_MINUM_BALANCE];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_MINUM_BALANCE_RAW]
AS SELECT [CYCLE_DATE] as cycle_date
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institution_short_name
	,[PARTICIPANT_NUMBER] as participant_number
	,[PARTICIPANT_BIC] as participant_bic
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT] as participant_short_name
	,[SETTLEMENT_MECHANISM_CD] as settlement_mechanism
	,[MINIMUM_AMOUNT] as minimum_amount
	,[MET_TS] as met_time
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_MINUM_BALANCE];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_NET_SEND_LIMIT_VIEW]
AS SELECT [LYNX_NET_SEND_LIMIT_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[NET_SEND_LIMIT_ID]
      ,[CYCLE_DATE]
      ,[REGISTERED_TS]
      ,[FINAL_TS]
      ,[EFFECTIVE_TS]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[CONTRA_INSTITUTION]
      ,[CONTRA_PARTICIPANT]
      ,[SETTLEMENT_MECHANISM_CD]
      ,[EVENT_TRIGGER_CD]
      ,[EVENT_TRIGGER_DESC]
      ,[LIMIT_AMOUNT]
      ,[FIXED_TIME]
      ,[FINALIZED_BY]
      ,[STATUS_TYPE_CD]
      ,[LAST_MODIFIED_TS]
FROM [paycan_restricted_curated].[LYNX_NET_SEND_LIMIT];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_NET_SEND_LIMIT_RAW]
AS SELECT [NET_SEND_LIMIT_ID] as net_send_limit_id
	,[CYCLE_DATE] as cycle_date
	,[SYSTEM_REFERENCE] as system_reference
	,[ORIGINATOR_REFERENCE] as originator_reference
	,[REGISTERED_TS] as registered_time
	,[FINAL_TS] as final_time
	,[EFFECTIVE_TS] as effective_time
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institution_short_name
	,[PARTICIPANT_NUMBER] as participant_number
	,[PARTICIPANT_BIC] as participant_bic
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT] as participant_short_name
	,[CONTRA_INSTITUTION_NUMBER] as contra_institution_number
	,[CONTRA_INSTITUTION_IRN] as contra_institution_irn
	,[CONTRA_INSTITUTION_NAME] as contra_institution_name
	,[CONTRA_INSTITUTION] as contra_institution_short_name
	,[CONTRA_PARTICIPANT_NUMBER] as contra_participant_number
	,[CONTRA_PARTICIPANT_BIC] as contra_participant_bic
	,[CONTRA_PARTICIPANT_NAME] as contra_participant_name
	,[CONTRA_PARTICIPANT] as contra_participant_short_name
	,[SETTLEMENT_MECHANISM_CD] as settlement_mechanism
	,[EVENT_TRIGGER_CD] as event_trigger_code
	,[EVENT_TRIGGER_DESC] as event_trigger_description
	,[LIMIT_AMOUNT] as limit_amount
	,[FIXED_TIME] as fixed_time
	,[FINALIZED_BY] as finalized_by
	,[STATUS_TYPE_CD] as [status]
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_NET_SEND_LIMIT];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_NET_SEND_OVERRIDE_VIEW]
AS SELECT [LYNX_NET_SEND_OVERRIDE_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[NET_SEND_OVERRIDE_ID]
      ,[CYCLE_DATE]
      ,[ACTIVE_DATE]
      ,[DELETE_DATE]
      ,[CHANGE_TIME]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[CONTRA_INSTITUTION]
      ,[CONTRA_PARTICIPANT]
      ,[SETTLEMENT_MECHANISM_CD]
      ,[DAY_NUMBER]
      ,[FIXED_TIME]
      ,[EVENT_TRIGGER_CD]
      ,[EVENT_TRIGGER_DESCRIPTION]
      ,[AMOUNT]
      ,[LAST_MODIFIED_TS]
FROM [paycan_restricted_curated].[LYNX_NET_SEND_OVERRIDE];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_NET_SEND_OVERRIDE_RAW]
AS SELECT [NET_SEND_OVERRIDE_ID] as net_send_override_id
	,[CYCLE_DATE] as cycle_date
	,[ACTIVE_DATE] as active_date
	,[DELETE_DATE] as delete_date
	,[CHANGE_TIME] as change_time
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institution_short_name
	,[PARTICIPANT_NUMBER] as participant_number
	,[PARTICIPANT_BIC] as participant_bic
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT] as participant_short_name
	,[CONTRA_INSTITUTION_NUMBER] as contra_institution_number
	,[CONTRA_INSTITUTION_IRN] as contra_institution_irn
	,[CONTRA_INSTITUTION_NAME] as contra_institution_name
	,[CONTRA_INSTITUTION] as contra_institution_short_name
	,[CONTRA_PARTICIPANT_NUMBER] as contra_participant_number
	,[CONTRA_PARTICIPANT_BIC] as contra_participant_bic
	,[CONTRA_PARTICIPANT_NAME] as contra_participant_name
	,[CONTRA_PARTICIPANT] as contra_participant_short_name
	,[SETTLEMENT_MECHANISM_CD] as settlement_mechanism
	,[DAY_NUMBER] as day_number
	,[FIXED_TIME] as fixed_time
	,[EVENT_TRIGGER_CD] as event_trigger_code
	,[EVENT_TRIGGER_DESCRIPTION] as event_trigger_description
	,[AMOUNT] as amount
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_NET_SEND_OVERRIDE];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_NET_SEND_SCHEDULE_VIEW]
AS SELECT [LYNX_NET_SEND_SCHEDULE_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[NET_SEND_ID]
      ,[ACTIVE_DATE]
      ,[DELETE_DATE]
      ,[CHANGE_TIME]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[CONTRA_INSTITUTION]
      ,[CONTRA_PARTICIPANT]
      ,[SETTLEMENT_MECHANISM_CD]
      ,[DAY_NUMBER]
      ,[FIXED_TIME]
      ,[EVENT_TRIGGER_CD]
      ,[EVENT_TRIGGER_DESCRIPTION]
      ,[AMOUNT]
      ,[LAST_MODIFIED_TS]
FROM [paycan_restricted_curated].[LYNX_NET_SEND_SCHEDULE];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_NET_SEND_SCHEDULE_RAW]
AS SELECT [NET_SEND_ID] as net_send_id
	,[ACTIVE_DATE] as active_date
	,[DELETE_DATE] as delete_date
	,[CHANGE_TIME] as change_time
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institution_short_name
	,[PARTICIPANT_NUMBER] as participant_number
	,[PARTICIPANT_BIC] as participant_bic
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT] as participant_short_name
	,[CONTRA_INSTITUTION_NUMBER] as contra_institution_number
	,[CONTRA_INSTITUTION_IRN] as contra_institution_irn
	,[CONTRA_INSTITUTION_NAME] as contra_institution_name
	,[CONTRA_INSTITUTION] as contra_institution_short_name
	,[CONTRA_PARTICIPANT_NUMBER] as contra_participant_number
	,[CONTRA_PARTICIPANT_BIC] as contra_participant_bic
	,[CONTRA_PARTICIPANT_NAME] as contra_participant_name
	,[CONTRA_PARTICIPANT] as contra_participant_short_name
	,[SETTLEMENT_MECHANISM_CD] as settlement_mechanism
	,[DAY_NUMBER] as day_number
	,[FIXED_TIME] as fixed_time
	,[EVENT_TRIGGER_CD] as event_trigger_code
	,[EVENT_TRIGGER_DESCRIPTION] as event_trigger_description
	,[AMOUNT] as amount
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_NET_SEND_SCHEDULE];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_OPERATING_SCHEDULE_VIEW]
AS SELECT [LYNX_OPERATING_SCHEDULE_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[WINDOW_LOG_ID]
      ,[CYCLE_DATE]
      ,[EVENT]
      ,[START_TS]
      ,[END_TS]
      ,[LAST_MODIFIED_TS]
      ,[DURATION]
FROM [paycan_restricted_curated].[LYNX_OPERATING_SCHEDULE];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_OPERATING_SCHEDULE_RAW]
AS SELECT [WINDOW_LOG_ID] as window_log_id
	,[CYCLE_DATE] as cycle_date
	,[EVENT] as [event]
	,[START_TS] as start_time
	,[END_TS] as end_time
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_OPERATING_SCHEDULE];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_OPERATIONAL_STATUS_VIEW]
AS SELECT [LYNX_OPERATIONAL_STATUS_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[STATUS_LOG_ID]
      ,[CYCLE_DATE]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[STATUS_TYPE_CD]
      ,[EFFECTIVE_TS]
      ,[LAST_MODIFIED_TS]
FROM [paycan_restricted_curated].[LYNX_OPERATIONAL_STATUS];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_OPERATIONAL_STATUS_RAW]
AS SELECT [STATUS_LOG_ID] as status_log_id
	,[CYCLE_DATE] as cycle_date
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institutiton_short_name
	,[PARTICIPANT_NUMBER] as participant_number
	,[PARTICIPANT_BIC] as participant_bic
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT] as participant_short_name
	,[STATUS_TYPE_CD] as [status]
	,[EFFECTIVE_TS] as effective_time
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_OPERATIONAL_STATUS];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_PARTICIPANT_INSTITUTION_VIEW]
AS SELECT [LYNX_PARTICIPANT_INSTITUTION_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[INSTITUTION_NUMBER]
      ,[INSTITUTION_IRN]
      ,[INSTITUTION_DESCRIPTION]
      ,[INSTITUTION_NAME]
      ,[INSTITUTION_STATUS_CD]
      ,[PARTICIPANT_NUMBER]
      ,[PARTICIPANT_DESCRIPTION]
      ,[PARTICIPANT_NAME]
      ,[PARTICIAPNT_STATUS_CD]
      ,[CREDIT_IND]
      ,[MAX_CREDIT_AMOUNT]
      ,[MIN_CREDIT_AMOUNT]
      ,[MONETARY_IND]
      ,[ROLE_TYPE_CD]
      ,[BUSINESS_IDENT_CD]
      ,[CATEGORY_NO]
      ,[OFFICIAL_ADDR1]
      ,[OFFICIAL_ADDR2]
      ,[OFFICIAL_ADDR3]
      ,[OFFICIAL_ADDR4]
      ,[OFFICIAL_DESIGNATION]
      ,[OFFICIAL_POSTAL_CD]
      ,[LIQUIDITY_THRESHOLD_BAL]
      ,[EFFECTIVE_TS]
      ,[EXPIRE_TS]
      ,[LAST_MODIFIED_TS]
FROM [paycan_restricted_curated].[LYNX_PARTICIPANT_INSTITUTION];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_PARTICIPANT_INSTITUTION_RAW]
AS SELECT [INSTITUTION_NUMBER] as idn_institution
	,[INSTITUTION_IRN] as inst_irn
	,[INSTITUTION_NAME] as inst_name
	,[INSTITUTION_DESCRIPTION] as inst_description
	,[INSTITUTION_STATUS_CD] as inst_status_code
	,[PARTICIPANT_NUMBER] as idn_participant
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT_DESCRIPTION] as participant_description
	,[PARTICIAPNT_STATUS_CD] as participant_status_code
	,[CREDIT_IND] as credit_ind
	,[MAX_CREDIT_AMOUNT] as max_credit_amt
	,[MIN_CREDIT_AMOUNT] as min_credit_amt
	,[MONETARY_IND] as monetary_ind
	,[ROLE_TYPE_CD] as role_type
	,[BUSINESS_IDENT_CD] as bic
	,[CATEGORY_NO] as category_no
	,[OFFICIAL_ADDR1] as official_addr1
	,[OFFICIAL_ADDR2] as official_addr2
	,[OFFICIAL_ADDR3] as official_addr3
	,[OFFICIAL_ADDR4] as official_addr4
	,[OFFICIAL_DESIGNATION] as official_designation
	,[OFFICIAL_POSTAL_CD] as official_postal_code
	,[LIQUIDITY_THRESHOLD_BAL] as liquidity_threshold_bal
	,[EFFECTIVE_TS] as effective_time
	,[EXPIRE_TS] as expire_time
	,[LAST_MODIFIED_TS] as last_modified
FROM [paycan_restricted_curated].[LYNX_PARTICIPANT_INSTITUTION];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_PAUSE_PAYMENTS_VIEW]
AS SELECT [LYNX_PAUSE_PAYMENTS_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[PAUSE_PAYMENTS_ID]
      ,[CYCLE_DATE]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[ORIGINATOR_REFERENCE]
      ,[REGISTERED_TS]
      ,[EFFECTIVE_TS]
      ,[LAST_MODIFIED_TS]
      ,[CONTRA_INSTITUTION_ACTION]
      ,[CONTRA_INSTITUTION]
      ,[CONTRA_PARTICIPANT]
FROM [paycan_restricted_curated].[LYNX_PAUSE_PAYMENTS];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_PAUSE_PAYMENTS_RAW]
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

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_PAYMENT_SUMMARY_VIEW]
AS SELECT [BATCH_ID]
	,[PAYMENT_ID]
	,[PAYER_REFERENCE]
	,[SYSTEM_REFERENCE]
	,[CYCLE_DATE]
	,[PAYER_INSTITUTION]
	,[PAYER_PARTICIPANT]
	,[SETTLEMENT_MECHANISM_CD]
	,[RECEIVER_INSTITUTION]
	,[RECEIVER_PARTICIPANT]
	,[SWIFT_MSG_TYPE_CD]
	,[TRANSACTION_TYPE]
	,[AMOUNT]
	,[GRIDLOCK_SETTLED_ID]
	,[PRIORITY]
	,[REGISTERED]
	,[SETTLED]
	,[QUEUED]
	,[DIARISED]
	,[REMOVED]
	,[REPRIORITISE]
	,[DISCARDED]
	,[RESEQUENCED]
	,[INSUFFUNDS]
	,[REGISTERED_TIME]
	,[SETTLED_TIME]
	,[QUEUED_TIME]
	,[DIARISED_TIME]
	,[REMOVED_TIME]
	,[REPRIORITISE_TIME]
	,[DISCARDED_TIME]
	,[RESEQUENCED_TIME]
	,[INSUFFUNDS_TIME]
FROM [paycan_restricted_curated].[LYNX_PAYMENT_SUMMARY];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_PAYMENT_VIEW]
AS SELECT [LYNX_PAYMENT_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[PAYMENT_ID]
      ,[SYSTEM_REFERENCE]
      ,[CYCLE_DATE]
      ,[PAYER_INSTITUTION]
      ,[PAYER_PARTICIPANT]
      ,[PAYER_REFERENCE]
      ,[SETTLEMENT_MECHANISM_CD]
      ,[RECEIVER_INSTITUTION]
      ,[RECEIVER_PARTICIPANT]
      ,[SWIFT_MSG_TYPE_CD]
      ,[TRANSACTION_TYPE]
	  ,[PCRN]
      ,[AMOUNT]
	  ,[GRIDLOCK_SETTLED_ID]
      ,[PRIORITY]
      ,[LAST_MODIFIED_TS]
      ,[STATUS_TYPE_CD]
      ,[STATUS_CHANGE_TIME_TS]
      ,[STATUS_REASON]
      ,[STATUS_MILLISECOND_DURATION]
FROM [paycan_restricted_curated].[LYNX_PAYMENT];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_PAYMENT_RAW]
AS SELECT [PAYMENT_ID] as payment_id
	,[SYSTEM_REFERENCE] as system_reference
	,[CYCLE_DATE] as cycle_date
	,[PAYER_INSTITUTION_NUMBER] as payer_institution_number
	,[PAYER_INSTITUTION_IRN] as payer_institution_irn
	,[PAYER_INSTITUTION_NAME] as payer_institution_name
	,[PAYER_INSTITUTION] as payer_institution_short_name
	,[PAYER_PARTICIPANT_NUMBER] as payer_participant_number
	,[PAYER_PARTICIPANT_BIC] as payer_participant_bic
	,[PAYER_PARTICIPANT_NAME] as payer_participant_name
	,[PAYER_PARTICIPANT] as payer_participant_short_name
	,[PAYER_REFERENCE] as payer_reference
	,[SETTLEMENT_MECHANISM_CD] as settlement_mechanism
	,[RECEIVER_INSTITUTION_NUMBER] as receiver_institution_number
	,[RECEIVER_INSTITUTION_IRN] as receiver_institution_irn
	,[RECEIVER_INSTITUTION_NAME] as receiver_institution_name
	,[RECEIVER_INSTITUTION] as receiver_institution_short_name
	,[RECEIVER_PARTICIPANT_NUMBER] as receiver_participant_number
	,[RECEIVER_PARTICIPANT_BIC] as receiver_participant_bic
	,[RECEIVER_PARTICIPANT_NAME] as receiver_participant_name
	,[RECEIVER_PARTICIPANT] as receiver_participant_short_name
	,[SWIFT_MSG_TYPE_CD] as swift_msg_type
	,[TRANSACTION_TYPE] as transaction_type
	,[PCRN] as pcrn
	,[AMOUNT] as amount
	,[GRIDLOCK_SETTLED_ID] as gridlock_settled_id
	,[PRIORITY] as [priority]
	,[LAST_MODIFIED_TS] as last_modified
	,[STATUS_TYPE_CD] as status_type
	,[STATUS_CHANGE_TIME_TS] as status_change_time
	,[STATUS_REASON] as status_reason
	,[STATUS_MILLISECOND_DURATION] as status_millisecond_duration
FROM [paycan_restricted_curated].[LYNX_PAYMENT];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_TRANSFER_SUMMARY_VIEW]
AS SELECT [BATCH_ID]
      ,[TRANSFER_ID]
      ,[PAYER_REFERENCE]
      ,[CYCLE_DATE]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[FROM_ACCOUNT_BBAN]
      ,[FROM_ACCOUNT_NAME]
      ,[FROM_SETTLE_MECH_CD]
      ,[TO_ACCOUNT_BBAN]
      ,[TO_ACCOUNT_NAME]
      ,[TO_SETTLE_MECH_CD]
      ,[TRANSACTION_TYPE]
      ,[AMOUNT]
      ,[REGISTERED]
      ,[SETTLED]
      ,[REGISTERED_TIME]
      ,[SETTLED_TIME]
  FROM [paycan_restricted_curated].[LYNX_TRANSFER_SUMMARY];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_TRANSFER_VIEW]
AS SELECT [LYNX_TRANSFER_ID]
      ,[BATCH_ID]
      ,[VALIDATION_STATUS]
      ,[TRANSFER_ID]
      ,[PAYER_REFERENCE]
      ,[CYCLE_DATE]
      ,[INSTITUTION]
      ,[PARTICIPANT]
      ,[FROM_ACCOUNT_BBAN]
      ,[FROM_ACCOUNT_NAME]
      ,[FROM_SETTLE_MECH_CD]
      ,[TO_ACCOUNT_BBAN]
      ,[TO_ACCOUNT_NAME]
      ,[TO_SETTLE_MECH_CD]
      ,[TRANSACTION_TYPE]
      ,[AMOUNT]
      ,[LAST_MODIFIED_TS]
      ,[STATUS_TYPE_CD]
      ,[STATUS_CHANGE_TS]
      ,[STATUS_MILLISECOND_DURATION]
FROM [paycan_restricted_curated].[LYNX_TRANSFER];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_TRANSFER_RAW]
AS SELECT [TRANSFER_ID] as transfer_id
	,[SYSTEM_REFERENCE] as system_reference
	,[PAYER_REFERENCE] as payer_reference
	,[CYCLE_DATE] as cycle_date
	,[INSTITUTION_NUMBER] as institution_number
	,[INSTITUTION_IRN] as institution_irn
	,[INSTITUTION_NAME] as institution_name
	,[INSTITUTION] as institution_short_name
	,[PARTICIPANT_NUMBER] as participant_number
	,[PARTICIPANT_BIC] as participant_bic
	,[PARTICIPANT_NAME] as participant_name
	,[PARTICIPANT] as participant_short_name
	,[FROM_ACCOUNT_ID] as from_account_id
	,[FROM_ACCOUNT_BBAN] as from_account_bban
	,[FROM_ACCOUNT_NAME] as from_account_name
	,[FROM_SETTLE_MECH_CD] as from_settle_mech
	,[TO_ACCOUNT_ID] as to_account_id
	,[TO_ACCOUNT_BBAN] as to_account_bban
	,[TO_ACCOUNT_NAME] as to_account_name
	,[TO_SETTLE_MECH_CD] as to_settle_mech
	,[TRANSACTION_TYPE] as transaction_type
	,[AMOUNT] as amount
	,[LAST_MODIFIED_TS] as last_modified
	,[STATUS_TYPE_CD] as status_type
	,[STATUS_CHANGE_TS] as status_change_time
	,[STATUS_REASON] as status_reason
	,[STATUS_MILLISECOND_DURATION] as status_millisecond_duration
FROM [paycan_restricted_curated].[LYNX_TRANSFER];

GO

CREATE OR ALTER VIEW [paycan_restricted_curated].[LYNX_VALIDATION_VIEW]
AS SELECT [BATCH_ID]
      ,[FILE_NAME]
      ,[FILE_PRIMARY_ID]
      ,[BUSINESS_ID]
      ,[FLAG_NAME]
      ,[CREATETIME]
FROM [paycan_restricted_curated].[LYNX_VALIDATION];

