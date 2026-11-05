CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_GRIDLOCK]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_GRIDLOCK] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_GRIDLOCK] AS [Source]           
    ON	[Target].[LYNX_GRIDLOCK_ID] = [Source].[LYNX_GRIDLOCK_ID] and
		[Target].[BATCH_ID]			= [Source].[BATCH_ID]
WHEN MATCHED THEN          
          
  UPDATE SET 
	[Target].[VALIDATION_STATUS]		=	[Source].[VALIDATION_STATUS], 
	[Target].[GRIDLOCK_PARTICIPANT_ID]	=	[Source].[GRIDLOCK_PARTICIPANT_ID], 
	[Target].[GRIDLOCK_BATCH_ID]		=	[Source].[GRIDLOCK_BATCH_ID], 
	[Target].[CYCLE_DATE]				=	[Source].[CYCLE_DATE], 
	[Target].[START_TS]					=	[Source].[START_TS], 
	[Target].[END_TS]					=	[Source].[END_TS], 
	[Target].[TRIGGER]					=	[Source].[TRIGGER], 
	[Target].[GRIDLOCK_INTERVAL]		=	[Source].[GRIDLOCK_INTERVAL], 
	[Target].[INSTITUTION_NUMBER]		=	[Source].[INSTITUTION_NUMBER], 
	[Target].[INSTITUTION_IRN]			=	[Source].[INSTITUTION_IRN], 
	[Target].[INSTITUTION_NAME]			=	[Source].[INSTITUTION_NAME], 
	[Target].[PARTICIPANT_NUMBER]		=	[Source].[PARTICIPANT_NUMBER], 
	[Target].[PARTICIPANT_BIC]			=	[Source].[PARTICIPANT_BIC], 
	[Target].[PARTICIPANT_NAME]			=	[Source].[PARTICIPANT_NAME], 
	[Target].[INSTITUTION]				=	[Source].[INSTITUTION], 
	[Target].[PARTICIPANT]				=	[Source].[PARTICIPANT], 
	[Target].[BEGIN_QUEUE_VALUE]		=	[Source].[BEGIN_QUEUE_VALUE], 
	[Target].[END_QUEUE_VALUE]			=	[Source].[END_QUEUE_VALUE], 
	[Target].[BEGIN_QUEUE_VOLUME]		=	[Source].[BEGIN_QUEUE_VOLUME], 
	[Target].[END_QUEUE_VOLUME]			=	[Source].[END_QUEUE_VOLUME], 
	[Target].[LAST_MODIFIED_TS]			=	[Source].[LAST_MODIFIED_TS], 
	[Target].[MATCHED_WITH_PAY]			=	[Source].[MATCHED_WITH_PAY], 
	[Target].[ingestion_date]			=	[Source].[ingestion_date],
	[Target].[batch_date]				=	[Source].[batch_date]
WHEN NOT MATCHED THEN 
	INSERT
	(
		[LYNX_GRIDLOCK_ID], 
		[BATCH_ID], 
		[VALIDATION_STATUS], 
		[GRIDLOCK_PARTICIPANT_ID], 
		[GRIDLOCK_BATCH_ID], 
		[CYCLE_DATE], 
		[START_TS], 
		[END_TS], 
		[TRIGGER], 
		[GRIDLOCK_INTERVAL], 
		[INSTITUTION_NUMBER], 
		[INSTITUTION_IRN], 
		[INSTITUTION_NAME], 
		[PARTICIPANT_NUMBER], 
		[PARTICIPANT_BIC], 
		[PARTICIPANT_NAME], 
		[INSTITUTION], 
		[PARTICIPANT], 
		[BEGIN_QUEUE_VALUE], 
		[END_QUEUE_VALUE], 
		[BEGIN_QUEUE_VOLUME], 
		[END_QUEUE_VOLUME], 
		[LAST_MODIFIED_TS], 
		[MATCHED_WITH_PAY], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[LYNX_GRIDLOCK_ID], 
		[Source].[BATCH_ID], 
		[Source].[VALIDATION_STATUS], 
		[Source].[GRIDLOCK_PARTICIPANT_ID], 
		[Source].[GRIDLOCK_BATCH_ID], 
		[Source].[CYCLE_DATE], 
		[Source].[START_TS], 
		[Source].[END_TS], 
		[Source].[TRIGGER], 
		[Source].[GRIDLOCK_INTERVAL], 
		[Source].[INSTITUTION_NUMBER], 
		[Source].[INSTITUTION_IRN], 
		[Source].[INSTITUTION_NAME], 
		[Source].[PARTICIPANT_NUMBER], 
		[Source].[PARTICIPANT_BIC], 
		[Source].[PARTICIPANT_NAME], 
		[Source].[INSTITUTION], 
		[Source].[PARTICIPANT], 
		[Source].[BEGIN_QUEUE_VALUE], 
		[Source].[END_QUEUE_VALUE], 
		[Source].[BEGIN_QUEUE_VOLUME], 
		[Source].[END_QUEUE_VOLUME], 
		[Source].[LAST_MODIFIED_TS], 
		[Source].[MATCHED_WITH_PAY], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END