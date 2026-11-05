CREATE   VIEW [paycan_restricted_curated].[VW_LYNX_VALIDATION]
AS
SELECT 
	[BATCH_ID], 
	[FILE_NAME],
	[FILE_PRIMARY_ID],
	[BUSINESS_ID], 
	[FLAG_NAME],
	[CREATETIME],
	[ingestion_date]
FROM 
	[paycan_restricted_curated].[LYNX_VALIDATION](NOLOCK)