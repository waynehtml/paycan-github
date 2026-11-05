CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_CREDIT_ACTIVITY]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_CREDIT_ACTIVITY] AS [Source]           
    ON [Target].[LYNX_CREDIT_ACTIVITY_ID] = [Source].[LYNX_CREDIT_ACTIVITY_ID] and
	[Target].[BATCH_ID] = [Source].[BATCH_ID]
WHEN MATCHED THEN          
          
  UPDATE SET 
	[Target].[VALIDATION_STATUS]	=	[Source].[VALIDATION_STATUS], 
	[Target].[CREDIT_ACTIVITY_ID]	=	[Source].[CREDIT_ACTIVITY_ID], 
	[Target].[CYCLE_DATE]			=	[Source].[CYCLE_DATE], 
	[Target].[INSTITUTION_NUMBER]	=	[Source].[INSTITUTION_NUMBER], 
	[Target].[INSTITUTION_IRN]		=	[Source].[INSTITUTION_IRN], 
	[Target].[INSTITUTION_NAME]		=	[Source].[INSTITUTION_NAME], 
	[Target].[PARTICIPANT_NUMBER]	=	[Source].[PARTICIPANT_NUMBER], 
	[Target].[PARTICIPANT_BIC]		=	[Source].[PARTICIPANT_BIC], 
	[Target].[PARTICIPANT_NAME]		=	[Source].[PARTICIPANT_NAME], 
	[Target].[INSTITUTION]			=	[Source].[INSTITUTION], 
	[Target].[PARTICIPANT]			=	[Source].[PARTICIPANT], 
	[Target].[SYSTEM_REFERENCE_CD]	=	[Source].[SYSTEM_REFERENCE_CD], 
	[Target].[TOTAL_AMOUNT]			=	[Source].[TOTAL_AMOUNT],	 
	[Target].[EARMARKED_AMOUNT]		=	[Source].[EARMARKED_AMOUNT], 
	[Target].[EARMARKED_LOM]		=	[Source].[EARMARKED_LOM],	 
	[Target].[LAST_MODIFIED_TS]		=	[Source].[LAST_MODIFIED_TS], 
	[Target].[STATUS_TYPE_CD]		=	[Source].[STATUS_TYPE_CD], 
	[Target].[STATUS_CHANGE_TIME]	=	[Source].[STATUS_CHANGE_TIME], 
	[Target].[STATUS_REASON_CD]		=	[Source].[STATUS_REASON_CD], 
	[Target].[STATUS_TYPE_CATEGORY]	=	[Source].[STATUS_TYPE_CATEGORY], 
	[Target].[ingestion_date]		=	[Source].[ingestion_date],
	[Target].[batch_date]			=	[Source].[batch_date]
WHEN NOT MATCHED THEN 
	INSERT
	(
	[LYNX_CREDIT_ACTIVITY_ID], 
	[BATCH_ID], 
	[VALIDATION_STATUS], 
	[CREDIT_ACTIVITY_ID], 
	[CYCLE_DATE], 
	[INSTITUTION_NUMBER], 
	[INSTITUTION_IRN], 
	[INSTITUTION_NAME],
	[PARTICIPANT_NUMBER], 
	[PARTICIPANT_BIC], 
	[PARTICIPANT_NAME], 
	[INSTITUTION], 
	[PARTICIPANT], 
	[SYSTEM_REFERENCE_CD], 
	[TOTAL_AMOUNT], 
	[EARMARKED_AMOUNT], 
	[EARMARKED_LOM], 
	[LAST_MODIFIED_TS], 
	[STATUS_TYPE_CD], 
	[STATUS_CHANGE_TIME], 
	[STATUS_REASON_CD], 
	[STATUS_TYPE_CATEGORY], 
	[ingestion_date],
	[batch_date]
	)
	VALUES
	(
	[Source].[LYNX_CREDIT_ACTIVITY_ID], 
	[Source].[BATCH_ID], 
	[Source].[VALIDATION_STATUS], 
	[Source].[CREDIT_ACTIVITY_ID], 
	[Source].[CYCLE_DATE], 
	[Source].[INSTITUTION_NUMBER], 
	[Source].[INSTITUTION_IRN], 
	[Source].[INSTITUTION_NAME],
	[Source].[PARTICIPANT_NUMBER], 
	[Source].[PARTICIPANT_BIC], 
	[Source].[PARTICIPANT_NAME], 
	[Source].[INSTITUTION], 
	[Source].[PARTICIPANT], 
	[Source].[SYSTEM_REFERENCE_CD], 
	[Source].[TOTAL_AMOUNT], 
	[Source].[EARMARKED_AMOUNT], 
	[Source].[EARMARKED_LOM], 
	[Source].[LAST_MODIFIED_TS], 
	[Source].[STATUS_TYPE_CD], 
	[Source].[STATUS_CHANGE_TIME], 
	[Source].[STATUS_REASON_CD], 
	[Source].[STATUS_TYPE_CATEGORY], 
	[Source].[ingestion_date],
	[Source].[batch_date]
	);

END