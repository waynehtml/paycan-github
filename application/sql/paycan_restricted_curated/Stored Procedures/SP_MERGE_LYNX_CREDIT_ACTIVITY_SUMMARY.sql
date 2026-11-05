CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_CREDIT_ACTIVITY_SUMMARY]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_CREDIT_ACTIVITY_SUMMARY] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_CREDIT_ACTIVITY_SUMMARY] AS [Source]           
    ON	[Target].[CREDIT_ACTIVITY_ID] = [Source].[CREDIT_ACTIVITY_ID] and
		[Target].[BATCH_ID]			= [Source].[BATCH_ID]
WHEN MATCHED THEN          
          
  UPDATE SET 
	[Target].[CYCLE_DATE]		=	[Source].[CYCLE_DATE], 
	[Target].[INSTITUTION]		=	[Source].[INSTITUTION], 
	[Target].[PARTICIPANT]		=	[Source].[PARTICIPANT], 
	[Target].[TOTAL_AMOUNT]		=	[Source].[TOTAL_AMOUNT], 
	[Target].[EARMARKED_AMOUNT]	=	[Source].[EARMARKED_AMOUNT], 
	[Target].[EARMARKED_LOM]	=	[Source].[EARMARKED_LOM],
	[Target].[REGISTERED]		=	[Source].[REGISTERED], 
	[Target].[PROCESSED]		=	[Source].[PROCESSED], 
	[Target].[REGISTERED_TIME]	=	[Source].[REGISTERED_TIME], 
	[Target].[PROCESSED_TIME]	=	[Source].[PROCESSED_TIME], 
	[Target].[ingestion_date]	=	[Source].[ingestion_date],
	[Target].[batch_date]		=	[Source].[batch_date]
WHEN NOT MATCHED THEN 
	INSERT
	(
		[BATCH_ID], 
		[CREDIT_ACTIVITY_ID], 
		[CYCLE_DATE], 
		[INSTITUTION], 
		[PARTICIPANT], 
		[TOTAL_AMOUNT], 
		[EARMARKED_AMOUNT], 
		[EARMARKED_LOM],
		[REGISTERED], 
		[PROCESSED], 
		[REGISTERED_TIME], 
		[PROCESSED_TIME], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[BATCH_ID], 
		[Source].[CREDIT_ACTIVITY_ID], 
		[Source].[CYCLE_DATE], 
		[Source].[INSTITUTION], 
		[Source].[PARTICIPANT], 
		[Source].[TOTAL_AMOUNT], 
		[Source].[EARMARKED_AMOUNT], 
		[Source].[EARMARKED_LOM],
		[Source].[REGISTERED], 
		[Source].[PROCESSED], 
		[Source].[REGISTERED_TIME], 
		[Source].[PROCESSED_TIME], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END