CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_LOM_STATUS]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_LOM_STATUS] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_LOM_STATUS] AS [Source]           
    ON	[Target].[LYNX_LOM_STATUS_ID]	= [Source].[LYNX_LOM_STATUS_ID] and
		[Target].[BATCH_ID]				= [Source].[BATCH_ID]
WHEN MATCHED THEN
  UPDATE SET 
	[Target].[VALIDATION_STATUS]			=	[Source].[VALIDATION_STATUS], 
	[Target].[CYCLE_DATE]					=	[Source].[CYCLE_DATE],
	[Target].[INSTITUTION_NUMBER]			=	[Source].[INSTITUTION_NUMBER], 
	[Target].[INSTITUTION_IRN]				=	[Source].[INSTITUTION_IRN], 
	[Target].[INSTITUTION_NAME]				=	[Source].[INSTITUTION_NAME], 
	[Target].[PARTICIPANT_NUMBER]			=	[Source].[PARTICIPANT_NUMBER], 
	[Target].[PARTICIPANT_BIC]				=	[Source].[PARTICIPANT_BIC], 
	[Target].[PARTICIPANT_NAME]				=	[Source].[PARTICIPANT_NAME], 
	[Target].[INSTITUTION]					=	[Source].[INSTITUTION], 
	[Target].[PARTICIPANT]					=	[Source].[PARTICIPANT], 
	[Target].[SETTLEMENT_MECHANISM_CD]		=	[Source].[SETTLEMENT_MECHANISM_CD], 
	[Target].[STATUS_CD]					=	[Source].[STATUS_CD],	 
	[Target].[EFFECTIVE_TS]					=	[Source].[EFFECTIVE_TS], 
	[Target].[LAST_MODIFIED_TS]				=	[Source].[LAST_MODIFIED_TS], 
	[Target].[ingestion_date]				=	[Source].[ingestion_date],
	[Target].[batch_date]					=	[Source].[batch_date]	
WHEN NOT MATCHED THEN 
	INSERT
	(
		[LYNX_LOM_STATUS_ID], 
		[BATCH_ID], 
		[VALIDATION_STATUS], 
		[CYCLE_DATE],
		[INSTITUTION_NUMBER], 
		[INSTITUTION_IRN], 
		[INSTITUTION_NAME], 
		[PARTICIPANT_NUMBER], 
		[PARTICIPANT_BIC], 
		[PARTICIPANT_NAME], 
		[INSTITUTION], 
		[PARTICIPANT], 
		[SETTLEMENT_MECHANISM_CD], 
		[STATUS_CD], 
		[EFFECTIVE_TS], 
		[LAST_MODIFIED_TS], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[LYNX_LOM_STATUS_ID], 
		[Source].[BATCH_ID], 
		[Source].[VALIDATION_STATUS], 
		[Source].[CYCLE_DATE],
		[Source].[INSTITUTION_NUMBER], 
		[Source].[INSTITUTION_IRN], 
		[Source].[INSTITUTION_NAME], 
		[Source].[PARTICIPANT_NUMBER], 
		[Source].[PARTICIPANT_BIC], 
		[Source].[PARTICIPANT_NAME], 
		[Source].[INSTITUTION], 
		[Source].[PARTICIPANT], 
		[Source].[SETTLEMENT_MECHANISM_CD], 
		[Source].[STATUS_CD], 
		[Source].[EFFECTIVE_TS], 
		[Source].[LAST_MODIFIED_TS], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END