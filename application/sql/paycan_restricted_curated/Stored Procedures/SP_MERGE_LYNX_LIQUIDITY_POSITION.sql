CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_LIQUIDITY_POSITION]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_LIQUIDITY_POSITION] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_LIQUIDITY_POSITION] AS [Source]           
    ON	[Target].[LYNX_LIQUIDITY_POSITION_ID]	= [Source].[LYNX_LIQUIDITY_POSITION_ID] and
		[Target].[BATCH_ID]						= [Source].[BATCH_ID]
WHEN MATCHED THEN          
          
  UPDATE SET 
	[Target].[VALIDATION_STATUS]		=	[Source].[VALIDATION_STATUS], 
	[Target].[CYCLE_DATE]				=	[Source].[CYCLE_DATE], 
	[Target].[INSTITUTION_NUMBER]		=	[Source].[INSTITUTION_NUMBER],	 
	[Target].[INSTITUTION_IRN]			=	[Source].[INSTITUTION_IRN], 
	[Target].[INSTITUTION_NAME]			=	[Source].[INSTITUTION_NAME], 
	[Target].[INSTITUTION]				=	[Source].[INSTITUTION], 
	[Target].[SETTLEMENT_MECHANISM_CD]	=	[Source].[SETTLEMENT_MECHANISM_CD],	 
	[Target].[SENT_AMOUNT]				=	[Source].[SENT_AMOUNT], 
	[Target].[RECEIVED_AMOUNT]			=	[Source].[RECEIVED_AMOUNT], 
	[Target].[LIQUIDITY_POSITION]		=	[Source].[LIQUIDITY_POSITION], 
	[Target].[LAST_MODIFIED_TS]			=	[Source].[LAST_MODIFIED_TS], 
	[Target].[ingestion_date]			=	[Source].[ingestion_date],
	[Target].[batch_date]				=	[Source].[batch_date]
WHEN NOT MATCHED THEN 
	INSERT
	(
		[LYNX_LIQUIDITY_POSITION_ID], 
		[BATCH_ID], 
		[VALIDATION_STATUS], 
		[CYCLE_DATE], 
		[INSTITUTION_NUMBER], 
		[INSTITUTION_IRN], 
		[INSTITUTION_NAME], 
		[INSTITUTION], 
		[SETTLEMENT_MECHANISM_CD], 
		[SENT_AMOUNT], 
		[RECEIVED_AMOUNT], 
		[LIQUIDITY_POSITION], 
		[LAST_MODIFIED_TS], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[LYNX_LIQUIDITY_POSITION_ID], 
		[Source].[BATCH_ID], 
		[Source].[VALIDATION_STATUS], 
		[Source].[CYCLE_DATE], 
		[Source].[INSTITUTION_NUMBER], 
		[Source].[INSTITUTION_IRN], 
		[Source].[INSTITUTION_NAME], 
		[Source].[INSTITUTION], 
		[Source].[SETTLEMENT_MECHANISM_CD], 
		[Source].[SENT_AMOUNT], 
		[Source].[RECEIVED_AMOUNT], 
		[Source].[LIQUIDITY_POSITION], 
		[Source].[LAST_MODIFIED_TS], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END