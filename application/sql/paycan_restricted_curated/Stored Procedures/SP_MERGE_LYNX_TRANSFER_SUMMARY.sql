CREATE   PROCEDURE [paycan_restricted_curated].[SP_MERGE_LYNX_TRANSFER_SUMMARY]
AS
BEGIN

MERGE [paycan_restricted_curated].[LYNX_TRANSFER_SUMMARY] AS [Target]          
USING [paycan_restricted_standardized].[LYNX_TRANSFER_SUMMARY] AS [Source]           
    ON	[Target].[TRANSFER_ID]	= [Source].[TRANSFER_ID] and
		[Target].[BATCH_ID]		= [Source].[BATCH_ID]
WHEN MATCHED THEN
  UPDATE SET 
	[Target].[PAYER_REFERENCE]		=	[Source].[PAYER_REFERENCE], 
	[Target].[CYCLE_DATE]			=	[Source].[CYCLE_DATE],
	[Target].[INSTITUTION]			=	[Source].[INSTITUTION],
	[Target].[PARTICIPANT]			=	[Source].[PARTICIPANT],
	[Target].[FROM_ACCOUNT_BBAN]	=	[Source].[FROM_ACCOUNT_BBAN],
	[Target].[FROM_ACCOUNT_NAME]	=	[Source].[FROM_ACCOUNT_NAME], 
	[Target].[FROM_SETTLE_MECH_CD]	=	[Source].[FROM_SETTLE_MECH_CD], 
	[Target].[TO_ACCOUNT_BBAN]		=	[Source].[TO_ACCOUNT_BBAN], 
	[Target].[TO_ACCOUNT_NAME]		=	[Source].[TO_ACCOUNT_NAME], 
	[Target].[TO_SETTLE_MECH_CD]	=	[Source].[TO_SETTLE_MECH_CD], 
	[Target].[TRANSACTION_TYPE]		=	[Source].[TRANSACTION_TYPE], 
	[Target].[AMOUNT]				=	[Source].[AMOUNT], 
	[Target].[REGISTERED]			=	[Source].[REGISTERED], 
	[Target].[SETTLED]				=	[Source].[SETTLED], 
	[Target].[REGISTERED_TIME]		=	[Source].[REGISTERED_TIME], 
	[Target].[SETTLED_TIME]			=	[Source].[SETTLED_TIME], 
	[Target].[ingestion_date]		=	[Source].[ingestion_date],
	[Target].[batch_date]			=	[Source].[batch_date]	
WHEN NOT MATCHED THEN 
	INSERT
	(		
		[BATCH_ID], 
		[TRANSFER_ID], 
		[PAYER_REFERENCE], 
		[CYCLE_DATE],
		[INSTITUTION],
		[PARTICIPANT],
		[FROM_ACCOUNT_BBAN],
		[FROM_ACCOUNT_NAME], 
		[FROM_SETTLE_MECH_CD], 
		[TO_ACCOUNT_BBAN], 
		[TO_ACCOUNT_NAME], 
		[TO_SETTLE_MECH_CD], 
		[TRANSACTION_TYPE], 
		[AMOUNT], 
		[REGISTERED], 
		[SETTLED], 
		[REGISTERED_TIME], 
		[SETTLED_TIME], 
		[ingestion_date],
		[batch_date]
	)
	VALUES
	(
		[Source].[BATCH_ID], 
		[Source].[TRANSFER_ID], 
		[Source].[PAYER_REFERENCE], 
		[Source].[CYCLE_DATE],
		[Source].[INSTITUTION],
		[Source].[PARTICIPANT],
		[Source].[FROM_ACCOUNT_BBAN],
		[Source].[FROM_ACCOUNT_NAME], 
		[Source].[FROM_SETTLE_MECH_CD], 
		[Source].[TO_ACCOUNT_BBAN], 
		[Source].[TO_ACCOUNT_NAME], 
		[Source].[TO_SETTLE_MECH_CD], 
		[Source].[TRANSACTION_TYPE], 
		[Source].[AMOUNT], 
		[Source].[REGISTERED], 
		[Source].[SETTLED], 
		[Source].[REGISTERED_TIME], 
		[Source].[SETTLED_TIME], 
		[Source].[ingestion_date],
		[Source].[batch_date]
	);

END