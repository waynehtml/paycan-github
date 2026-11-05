CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_OPERATING_SCHEDULE]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_OPERATING_SCHEDULE] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_OPERATING_SCHEDULE] AS [Source]           
    ON	[Target].[LYNX_OPERATING_SCHEDULE_ID]	= [Source].[LYNX_OPERATING_SCHEDULE_ID] and
		[Target].[BATCH_ID]						= [Source].[BATCH_ID]
WHEN MATCHED THEN
  UPDATE SET 
	[Target].[VALIDATION_STATUS]			=	[Source].[VALIDATION_STATUS], 
	[Target].[WINDOW_LOG_ID]				=	[Source].[WINDOW_LOG_ID], 
	[Target].[CYCLE_DATE]					=	[Source].[CYCLE_DATE], 
	[Target].[EVENT]						=	[Source].[EVENT], 
	[Target].[START_TS]						=	[Source].[START_TS], 
	[Target].[END_TS]						=	[Source].[END_TS], 
	[Target].[LAST_MODIFIED_TS]				=	[Source].[LAST_MODIFIED_TS], 
	[Target].[DURATION]						=	[Source].[DURATION], 
	[Target].[ingestion_date]				=	[Source].[ingestion_date],
	[Target].[batch_date]					=	[Source].[batch_date]	
WHEN NOT MATCHED THEN 
	INSERT
	(
		[LYNX_OPERATING_SCHEDULE_ID], 
		[BATCH_ID], 
		[VALIDATION_STATUS], 
		[WINDOW_LOG_ID], 
		[CYCLE_DATE], 
		[EVENT], 
		[START_TS], 
		[END_TS], 
		[LAST_MODIFIED_TS], 
		[DURATION], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[LYNX_OPERATING_SCHEDULE_ID], 
		[Source].[BATCH_ID], 
		[Source].[VALIDATION_STATUS], 
		[Source].[WINDOW_LOG_ID], 
		[Source].[CYCLE_DATE], 
		[Source].[EVENT], 
		[Source].[START_TS], 
		[Source].[END_TS], 
		[Source].[LAST_MODIFIED_TS], 
		[Source].[DURATION], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END