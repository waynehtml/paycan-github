CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_LIQUIDITY_THRESHOLD]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_LIQUIDITY_THRESHOLD] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_LIQUIDITY_THRESHOLD] AS [Source]           
    ON	[Target].[LYNX_LIQUIDITY_THRESHOLD_ID]	= [Source].[LYNX_LIQUIDITY_THRESHOLD_ID] and
		[Target].[BATCH_ID]						= [Source].[BATCH_ID]
WHEN MATCHED THEN
  UPDATE SET 
	[Target].[VALIDATION_STATUS]		=	[Source].[VALIDATION_STATUS], 
	[Target].[CREATE_DATE]				=	[Source].[CREATE_DATE], 
	[Target].[ACTIVE_DATE]				=	[Source].[ACTIVE_DATE], 
	[Target].[DELETE_DATE]				=	[Source].[DELETE_DATE], 
	[Target].[CHANGE_TS]				=	[Source].[CHANGE_TS], 
	[Target].[INSTITUTION_NUMBER]		=	[Source].[INSTITUTION_NUMBER], 
	[Target].[INSTITUTION_IRN]			=	[Source].[INSTITUTION_IRN], 
	[Target].[INSTITUTION_NAME]			=	[Source].[INSTITUTION_NAME], 
	[Target].[PARTICIPANT_NUMBER]		=	[Source].[PARTICIPANT_NUMBER], 
	[Target].[PARTICIPANT_BIC]			=	[Source].[PARTICIPANT_BIC], 
	[Target].[PARTICIPANT_NAME]			=	[Source].[PARTICIPANT_NAME], 
	[Target].[INSTITUTION]				=	[Source].[INSTITUTION], 
	[Target].[PARTICIPANT]				=	[Source].[PARTICIPANT], 
	[Target].[SETTLEMENT_MECHANISM_CD]	=	[Source].[SETTLEMENT_MECHANISM_CD], 
	[Target].[THRESHOLD_AMOUNT]			=	[Source].[THRESHOLD_AMOUNT], 
	[Target].[LAST_MODIFIED_TS]			=	[Source].[LAST_MODIFIED_TS], 
	[Target].[ingestion_date]			=	[Source].[ingestion_date],
	[Target].[batch_date]				=	[Source].[batch_date]
WHEN NOT MATCHED THEN 
	INSERT
	(
		[LYNX_LIQUIDITY_THRESHOLD_ID], 
		[BATCH_ID], 
		[VALIDATION_STATUS], 
		[CREATE_DATE], 
		[ACTIVE_DATE], 
		[DELETE_DATE], 
		[CHANGE_TS], 
		[INSTITUTION_NUMBER], 
		[INSTITUTION_IRN], 
		[INSTITUTION_NAME], 
		[PARTICIPANT_NUMBER], 
		[PARTICIPANT_BIC], 
		[PARTICIPANT_NAME], 
		[INSTITUTION], 
		[PARTICIPANT], 
		[SETTLEMENT_MECHANISM_CD], 
		[THRESHOLD_AMOUNT], 
		[LAST_MODIFIED_TS], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[LYNX_LIQUIDITY_THRESHOLD_ID], 
		[Source].[BATCH_ID], 
		[Source].[VALIDATION_STATUS], 
		[Source].[CREATE_DATE], 
		[Source].[ACTIVE_DATE], 
		[Source].[DELETE_DATE], 
		[Source].[CHANGE_TS], 
		[Source].[INSTITUTION_NUMBER], 
		[Source].[INSTITUTION_IRN], 
		[Source].[INSTITUTION_NAME], 
		[Source].[PARTICIPANT_NUMBER], 
		[Source].[PARTICIPANT_BIC], 
		[Source].[PARTICIPANT_NAME], 
		[Source].[INSTITUTION], 
		[Source].[PARTICIPANT], 
		[Source].[SETTLEMENT_MECHANISM_CD], 
		[Source].[THRESHOLD_AMOUNT], 
		[Source].[LAST_MODIFIED_TS], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END