CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_CREDIT_POSITION]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_CREDIT_POSITION] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_CREDIT_POSITION] AS [Source]           
    ON [Target].[LYNX_CREDIT_POSITION_ID] = [Source].[LYNX_CREDIT_POSITION_ID] and
	[Target].[BATCH_ID] = [Source].[BATCH_ID]
WHEN MATCHED THEN          
          
  UPDATE SET 
	[Target].[VALIDATION_STATUS]	= [Source].[VALIDATION_STATUS], 
	[Target].[CREDIT_ENTRY_ID]		= [Source].[CREDIT_ENTRY_ID], 
	[Target].[CYCLE_DATE]			= [Source].[CYCLE_DATE], 
	[Target].[INSTITUTION_NUMBER]	= [Source].[INSTITUTION_NUMBER], 
	[Target].[INSTITUTION_IRN]		= [Source].[INSTITUTION_IRN], 
	[Target].[INSTITUTION_NAME]		= [Source].[INSTITUTION_NAME], 
	[Target].[PARTICIPANT_NUMBER]	= [Source].[PARTICIPANT_NUMBER], 
	[Target].[PARTICIPANT_BIC]		= [Source].[PARTICIPANT_BIC], 
	[Target].[PARTICIPANT_NAME]		= [Source].[PARTICIPANT_NAME], 
	[Target].[INSTITUTION]			= [Source].[INSTITUTION], 
	[Target].[PARTICIPANT]			= [Source].[PARTICIPANT], 
	[Target].[EVENT_TS]				= [Source].[EVENT_TS], 
	[Target].[TRIGGER_CD]			= [Source].[TRIGGER_CD], 
	[Target].[TRIGGER_NAME]			= [Source].[TRIGGER_NAME], 
	[Target].[TOTAL_LIMIT]			= [Source].[TOTAL_LIMIT], 
	[Target].[TOTAL_AVAILABLE]		= [Source].[TOTAL_AVAILABLE], 
	[Target].[ENTRY_TYPE_CD]		= [Source].[ENTRY_TYPE_CD], 
	[Target].[ENTRY_TYPE_NAME]		= [Source].[ENTRY_TYPE_NAME], 
	[Target].[ENTRY_AMOUNT]			= [Source].[ENTRY_AMOUNT], 
	[Target].[LAST_MODIFIED_TS]		= [Source].[LAST_MODIFIED_TS], 
	[Target].[ingestion_date]		= [Source].[ingestion_date],
	[Target].[batch_date]			= [Source].[batch_date]		
WHEN NOT MATCHED THEN 
	INSERT
	(
		[LYNX_CREDIT_POSITION_ID], 
		[BATCH_ID], 
		[VALIDATION_STATUS], 
		[CREDIT_ENTRY_ID], 
		[CYCLE_DATE], 
		[INSTITUTION_NUMBER], 
		[INSTITUTION_IRN], 
		[INSTITUTION_NAME], 
		[PARTICIPANT_NUMBER], 
		[PARTICIPANT_BIC], 
		[PARTICIPANT_NAME], 
		[INSTITUTION], 
		[PARTICIPANT], 
		[EVENT_TS], 
		[TRIGGER_CD], 
		[TRIGGER_NAME], 
		[TOTAL_LIMIT], 
		[TOTAL_AVAILABLE], 
		[ENTRY_TYPE_CD], 
		[ENTRY_TYPE_NAME], 
		[ENTRY_AMOUNT], 
		[LAST_MODIFIED_TS], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[LYNX_CREDIT_POSITION_ID], 
		[Source].[BATCH_ID], 
		[Source].[VALIDATION_STATUS], 
		[Source].[CREDIT_ENTRY_ID], 
		[Source].[CYCLE_DATE], 
		[Source].[INSTITUTION_NUMBER], 
		[Source].[INSTITUTION_IRN], 
		[Source].[INSTITUTION_NAME], 
		[Source].[PARTICIPANT_NUMBER], 
		[Source].[PARTICIPANT_BIC], 
		[Source].[PARTICIPANT_NAME], 
		[Source].[INSTITUTION], 
		[Source].[PARTICIPANT], 
		[Source].[EVENT_TS], 
		[Source].[TRIGGER_CD], 
		[Source].[TRIGGER_NAME], 
		[Source].[TOTAL_LIMIT], 
		[Source].[TOTAL_AVAILABLE], 
		[Source].[ENTRY_TYPE_CD], 
		[Source].[ENTRY_TYPE_NAME], 
		[Source].[ENTRY_AMOUNT], 
		[Source].[LAST_MODIFIED_TS], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END