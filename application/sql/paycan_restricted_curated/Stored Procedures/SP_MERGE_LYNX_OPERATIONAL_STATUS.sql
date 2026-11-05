CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_OPERATIONAL_STATUS]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_OPERATIONAL_STATUS] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_OPERATIONAL_STATUS] AS [Source]           
    ON	[Target].[LYNX_OPERATIONAL_STATUS_ID]	= [Source].[LYNX_OPERATIONAL_STATUS_ID] and
		[Target].[BATCH_ID]						= [Source].[BATCH_ID]
WHEN MATCHED THEN
  UPDATE SET 
	[Target].[VALIDATION_STATUS]	=	[Source].[VALIDATION_STATUS], 
	[Target].[STATUS_LOG_ID]		=	[Source].[STATUS_LOG_ID], 
	[Target].[CYCLE_DATE]			=	[Source].[CYCLE_DATE], 
	[Target].[INSTITUTION_NUMBER]	=	[Source].[INSTITUTION_NUMBER], 
	[Target].[INSTITUTION_IRN]		=	[Source].[INSTITUTION_IRN], 
	[Target].[INSTITUTION_NAME]		=	[Source].[INSTITUTION_NAME], 
	[Target].[PARTICIPANT_NUMBER]	=	[Source].[PARTICIPANT_NUMBER],
	[Target].[PARTICIPANT_BIC]		=	[Source].[PARTICIPANT_BIC], 
	[Target].[PARTICIPANT_NAME]		=	[Source].[PARTICIPANT_NAME], 
	[Target].[INSTITUTION]			=	[Source].[INSTITUTION], 
	[Target].[PARTICIPANT]			=	[Source].[PARTICIPANT], 
	[Target].[STATUS_TYPE_CD]		=	[Source].[STATUS_TYPE_CD], 
	[Target].[EFFECTIVE_TS]			=	[Source].[EFFECTIVE_TS], 
	[Target].[LAST_MODIFIED_TS]		=	[Source].[LAST_MODIFIED_TS], 
	[Target].[ingestion_date]		=	[Source].[ingestion_date],
	[Target].[batch_date]			=	[Source].[batch_date]	
WHEN NOT MATCHED THEN 
	INSERT
	(
		[LYNX_OPERATIONAL_STATUS_ID], 
		[BATCH_ID], 
		[VALIDATION_STATUS], 
		[STATUS_LOG_ID], 
		[CYCLE_DATE], 
		[INSTITUTION_NUMBER], 
		[INSTITUTION_IRN], 
		[INSTITUTION_NAME], 
		[PARTICIPANT_NUMBER],
		[PARTICIPANT_BIC], 
		[PARTICIPANT_NAME], 
		[INSTITUTION], 
		[PARTICIPANT], 
		[STATUS_TYPE_CD], 
		[EFFECTIVE_TS], 
		[LAST_MODIFIED_TS], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[LYNX_OPERATIONAL_STATUS_ID], 
		[Source].[BATCH_ID], 
		[Source].[VALIDATION_STATUS], 
		[Source].[STATUS_LOG_ID], 
		[Source].[CYCLE_DATE], 
		[Source].[INSTITUTION_NUMBER], 
		[Source].[INSTITUTION_IRN], 
		[Source].[INSTITUTION_NAME], 
		[Source].[PARTICIPANT_NUMBER],
		[Source].[PARTICIPANT_BIC], 
		[Source].[PARTICIPANT_NAME], 
		[Source].[INSTITUTION], 
		[Source].[PARTICIPANT], 
		[Source].[STATUS_TYPE_CD], 
		[Source].[EFFECTIVE_TS], 
		[Source].[LAST_MODIFIED_TS], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END