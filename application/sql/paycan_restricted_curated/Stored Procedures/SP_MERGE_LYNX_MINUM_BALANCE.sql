CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_MINUM_BALANCE]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_MINUM_BALANCE] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_MINUM_BALANCE] AS [Source]           
    ON	[Target].[LYNX_MINUM_BALANCE_ID]	= [Source].[LYNX_MINUM_BALANCE_ID] and
		[Target].[BATCH_ID]					= [Source].[BATCH_ID]
WHEN MATCHED THEN
  UPDATE SET 
	[Target].[VALIDATION_STATUS]		=	[Source].[VALIDATION_STATUS], 
	[Target].[CYCLE_DATE]				=	[Source].[CYCLE_DATE], 
	[Target].[INSTITUTION_NUMBER]		=	[Source].[INSTITUTION_NUMBER], 
	[Target].[INSTITUTION_IRN]			=	[Source].[INSTITUTION_IRN], 
	[Target].[INSTITUTION_NAME]			=	[Source].[INSTITUTION_NAME], 
	[Target].[PARTICIPANT_NUMBER]		=	[Source].[PARTICIPANT_NUMBER], 
	[Target].[PARTICIPANT_BIC]			=	[Source].[PARTICIPANT_BIC], 
	[Target].[PARTICIPANT_NAME]			=	[Source].[PARTICIPANT_NAME], 
	[Target].[INSTITUTION]				=	[Source].[INSTITUTION], 
	[Target].[PARTICIPANT]				=	[Source].[PARTICIPANT], 
	[Target].[SETTLEMENT_MECHANISM_CD]	=	[Source].[SETTLEMENT_MECHANISM_CD], 
	[Target].[MINIMUM_AMOUNT]			=	[Source].[MINIMUM_AMOUNT], 
	[Target].[MET_TS]					=	[Source].[MET_TS], 
	[Target].[LAST_MODIFIED_TS]			=	[Source].[LAST_MODIFIED_TS], 
	[Target].[ingestion_date]			=	[Source].[ingestion_date],
	[Target].[batch_date]				=	[Source].[batch_date]	
WHEN NOT MATCHED THEN 
	INSERT
	(
		[LYNX_MINUM_BALANCE_ID], 
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
		[MINIMUM_AMOUNT], 
		[MET_TS], 
		[LAST_MODIFIED_TS], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[LYNX_MINUM_BALANCE_ID], 
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
		[Source].[MINIMUM_AMOUNT], 
		[Source].[MET_TS], 
		[Source].[LAST_MODIFIED_TS], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END