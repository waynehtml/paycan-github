CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_VALIDATION]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_VALIDATION] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_VALIDATION] AS [Source]           
    ON	[Target].[FILE_NAME]		=	[Source].[FILE_NAME] and
		[Target].[BATCH_ID]			=	[Source].[BATCH_ID] AND
		[Target].[FILE_PRIMARY_ID]	=	[Source].[FILE_PRIMARY_ID] AND
		[Target].[FLAG_NAME]		=	[Source].[FLAG_NAME]
WHEN MATCHED THEN
  UPDATE SET 
	[Target].[BUSINESS_ID]		=	[Source].[BUSINESS_ID],	
	[Target].[CREATETIME]		=	[Source].[CREATETIME],
	[Target].[ingestion_date]	=	[Source].[ingestion_date],
	[Target].[batch_date]		=	[Source].[batch_date]
WHEN NOT MATCHED THEN 
	INSERT
	(		
		[BATCH_ID], 
		[FILE_NAME], 
		[FILE_PRIMARY_ID], 
		[BUSINESS_ID], 
		[FLAG_NAME], 
		[CREATETIME], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[BATCH_ID], 
		[Source].[FILE_NAME], 
		[Source].[FILE_PRIMARY_ID], 
		[Source].[BUSINESS_ID], 
		[Source].[FLAG_NAME], 
		[Source].[CREATETIME], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END